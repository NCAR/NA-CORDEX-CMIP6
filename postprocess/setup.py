#!/usr/bin/env python3
"""
setup.py - One-time setup for NA-CORDEX-CMIP6 postprocessing workflow.

Reads sim_config.yml and var_specs.yml, downloads/caches the CORDEX data
request CSV and CMOR JSON tables, creates the WRF coordinate reference file,
extracts fixed WRF fields (land mask, wind rotation angles) into wrf.fx.nc,
and writes two flat files consumed by all downstream bash scripts:

  sim.env        Shell key=value pairs for all simulation metadata
  var_table.tsv  Per-variable specs (one row per variable, tab-separated)

Run once before extract.sh / format.sh.  All outputs go to SETUPDIR.

Usage:
  python setup.py WRFDIR SETUPDIR [SIM_CONFIG] [--scripts SCRIPTS_DIR]

  WRFDIR      Parent directory containing <YYYY>_chunk/ simulation directories
  SETUPDIR    Output directory (created if needed); use the same directory
              for extract.sh OUTDIR so coordinate files are found alongside data
  SIM_CONFIG  Path to sim_config.yml (default: SCRIPTS_DIR/sim_config.yml)
  --scripts   Directory containing var_specs.yml (default: directory of this script)
  --force     Recreate outputs even if they already exist
  -v/--verbose  Print detailed output (file contents, NCO/CDO commands)
"""

import argparse
import csv
import datetime
import glob
import json
import os
import shutil
import subprocess
import sys
import urllib.request

import xarray as xr
import yaml


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="One-time setup for NA-CORDEX-CMIP6 postprocessing workflow.")
    p.add_argument("wrfdir",     metavar="WRFDIR",
                   help="Parent directory containing <YYYY>_chunk/ simulation directories")
    p.add_argument("setupdir",   metavar="SETUPDIR",
                   help="Output directory for all setup products")
    p.add_argument("sim_config", metavar="SIM_CONFIG", nargs="?",
                   help="Path to sim_config.yml (default: SCRIPTS_DIR/sim_config.yml)")
    p.add_argument("--scripts",  metavar="PATH",
                   help="Directory containing var_specs.yml (default: script directory)")
    p.add_argument("--force",    action="store_true",
                   help="Recreate outputs even if they already exist")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Print detailed output (file contents, NCO/CDO commands)")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Verbosity helper
# ---------------------------------------------------------------------------

# Set by main() after argument parsing; functions use vprint() for detail lines.
_verbose = False

def vprint(*args, **kwargs):
    if _verbose:
        print(*args, **kwargs)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd):
    """Run a shell command, printing it only in verbose mode; exit on failure."""
    vprint(f"  {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        sys.exit(f"Error: command failed (exit {result.returncode}):\n  {cmd}")


def download(url, dest, force=False):
    """Download url to dest, skipping if dest exists and not force."""
    if os.path.exists(dest) and not force:
        vprint(f"  Already exists, skipping: {dest}")
        return
    vprint(f"  Downloading {url}")
    urllib.request.urlretrieve(url, dest)
    vprint(f"  -> {dest}")


# ---------------------------------------------------------------------------
# Step 1: Download / cache upstream data files
# ---------------------------------------------------------------------------

def fetch_upstream(cfg, setupdir, force):
    """Download dreq CSV and CMOR JSON tables into setupdir."""
    print("\n=== Fetching upstream data files ===")

    dreq_dest = os.path.join(setupdir, "dreq_default.csv")

    # Check default work location first to avoid unnecessary downloads
    default_dreq = os.path.expanduser("~/work/cordex6/dreq_default.csv")
    if not force and not os.path.exists(dreq_dest):
        if os.path.exists(default_dreq):
            print(f"  Copying dreq CSV from default")
            vprint(f"    default:{default_dreq}")
            shutil.copy(default_dreq, dreq_dest)
        else:
            print("  Downloading data request CSV")
            download(cfg["dreq_csv_url"], dreq_dest, force=force)
    else:
        print("  Downloding data request CSV")
        download(cfg["dreq_csv_url"], dreq_dest, force=force)

    # CMOR JSON tables (one per frequency)
    print("  Downloding CMOR JSON tables")
    base_url = cfg["cmor_table_base_url"].rstrip("/")
    for freq in cfg["cmor_table_freqs"]:
        dest = os.path.join(setupdir, f"CORDEX-CMIP6_{freq}.json")
        url  = f"{base_url}_{freq}.json"
        download(url, dest, force=force)

    return dreq_dest


# ---------------------------------------------------------------------------
# Step 2: Build var_table.tsv
# ---------------------------------------------------------------------------

# Column order for var_table.tsv.
# standard_name and long_name are last for human readability.
VAR_TABLE_COLS = [
    "var", "freq", "units", "cell_methods", "positive",
    "levels", "refh", "plev", "quant",
    "standard_name", "long_name",
]


def load_dreq(dreq_path):
    """Return dict of {out_name: row_dict} from dreq_default.csv."""
    print(f"  loading data request")
    dreq = {}
    with open(dreq_path, newline="") as f:
        for row in csv.DictReader(f):
            dreq[row["out_name"]] = row
    return dreq


def load_cmor_tables(setupdir, freqs, scripts_dir):
    """Return dict of {varname: {field: value}} merged from WCRP CMOR JSON
    tables and any local NCAR supplemental tables.

    Upstream WCRP tables are loaded first from setupdir, then local
    supplemental tables (NCAR-CORDEX-CMIP6_*.json) from scripts_dir are
    merged on top, so local entries can supplement or override upstream
    values for variables not in the official data request.
    """
    print(f"  loading CMOR tables")
    cmor = {}

    # Upstream WCRP tables (downloaded by fetch_upstream)
    for freq in freqs:
        fname = os.path.join(setupdir, f"CORDEX-CMIP6_{freq}.json")
        if not os.path.exists(fname):
            sys.exit(f"Error: CMOR table not found: {fname}")
        with open(fname) as f:
            table = json.load(f)
        for varname, spec in table.get("variable_entry", {}).items():
            cmor[varname] = spec

    # Local NCAR supplemental tables (for variables not in the upstream dreq)
    supplemental = sorted(glob.glob(
        os.path.join(scripts_dir, "NCAR-CORDEX-CMIP6_*.json")))
    for fname in supplemental:
        vprint(f"    loading supplemental: {os.path.basename(fname)}")
        with open(fname) as f:
            table = json.load(f)
        for varname, spec in table.get("variable_entry", {}).items():
            cmor[varname] = spec

    return cmor


def build_var_table(var_specs_path, dreq_path, setupdir, freqs, scripts_dir):
    """
    Merge var_specs.yml + dreq_default.csv + CMOR JSON tables into a resolved
    per-variable table.  Returns list of row dicts.
    """
    print(f"\n=== Generating variable table ===")
    print(f"  loading variable specs")
    with open(var_specs_path) as f:
        var_specs = yaml.safe_load(f)

    dreq = load_dreq(dreq_path)
    cmor = load_cmor_tables(setupdir, freqs, scripts_dir)

    # Remove YAML anchor/alias helper keys (start with _)
    var_specs = {k: v for k, v in var_specs.items() if not k.startswith("_")}

    print(f"  building variable table")
    rows = []
    for varname, specs in var_specs.items():
        row = {"var": varname}

        # --- From dreq_default.csv ---
        dreq_row = dreq.get(varname, {})
        row["freq"]          = dreq_row.get("frequency", "")
        row["units"]         = dreq_row.get("units", "")
        row["cell_methods"]  = dreq_row.get("cell_methods", "None")
        row["long_name"]     = dreq_row.get("long_name", "")
        row["standard_name"] = dreq_row.get("standard_name", "")

        # --- From CMOR JSON tables (authoritative for positive; also the
        # only source of metadata for variables supplied by local NCAR
        # supplemental tables, which are not in the upstream dreq CSV) ---
        cmor_entry = cmor.get(varname, {})
        row["positive"] = cmor_entry.get("positive", "") or "--"

        # Fall back to CMOR entry for any field the dreq didn't provide.
        # Supplemental vars (AFWA, zlev, wbgt) only exist in the CMOR tables.
        for field in ("frequency", "units", "cell_methods",
                      "standard_name", "long_name"):
            tsv_key = "freq" if field == "frequency" else field
            if not row[tsv_key] or row[tsv_key] == "None":
                fallback = cmor_entry.get(field, "")
                if fallback:
                    row[tsv_key] = fallback

        # cell_methods still missing: write the sentinel the shell scripts expect
        if not row["cell_methods"]:
            row["cell_methods"] = "None"

        # --- From var_specs.yml ---
        row["levels"] = specs.get("levels", "single")
        row["refh"]   = str(specs["refh"])  if "refh"  in specs else "--"
        row["plev"]   = str(specs["plev"])  if "plev"  in specs else "--"
        row["quant"]  = str(specs["quant"]) if "quant" in specs else "--"

        rows.append(row)

    return rows


def write_var_table(rows, outpath):
    print(f"  writing var_table.tsv")
    with open(outpath, "w", newline="") as f:
        f.write("\t".join(VAR_TABLE_COLS) + "\n")
        for row in rows:
            f.write("\t".join(row.get(c, "--") for c in VAR_TABLE_COLS) + "\n")
    for row in rows:
        vprint(f"  {row['var']:12s}  freq={row['freq']:4s}  levels={row['levels']:8s}"
               f"  refh={row['refh']:3s}  plev={row['plev']:6s}  quant={row['quant']}")


# ---------------------------------------------------------------------------
# Step 3: Write sim.env
# ---------------------------------------------------------------------------

# Keys written to sim.env, in order.
# These map directly to sim_config.yml keys.
SIM_ENV_KEYS = [
    "activity_id",
    "calendar",
    "contact",
    "creation_date",
    "domain",
    "domain_id",
    "driving_experiment",
    "driving_experiment_id",
    "driving_institution_id",
    "driving_source_id",
    "driving_variant_label",
    "epoch",
    "grid",
    "institution",
    "institution_id",
    "license",
    "mip_era",
    "product",
    "project_id",
    "references",
    "source",
    "source_id",
    "source_type",
    "sponge_cells",
    "version_realization",
    "wrfinput_path",
]

CREATION_DATE_PLACEHOLDER = "YYYY-MM-DD"


def derive_creation_date(wrfdir):
    """
    Derive simulation completion date from the most recent wrfout file
    timestamp across all <YYYY>_chunk subdirectories of wrfdir.
    Returns a YYYY-MM-DD string, or None if no files are found.
    """

    print(f"  Deriving creation date from wrfout modtimes")

    candidates = glob.glob(os.path.join(wrfdir, "*_chunk", "wrfout*"))
    if not candidates:
        return None
    latest_mtime = max(os.path.getmtime(f) for f in candidates)
    return datetime.date.fromtimestamp(latest_mtime).strftime("%Y-%m-%d")


def write_sim_env(cfg, wrfdir, outpath):
    """Write sim.env to define simulation metadata:
    one shell variable assignment per line.

    Attempts to auto-derive creation_date from wrfout file timestamps if the
    config still contains the placeholder value.  Stores the resolved value
    and its source back into cfg for reporting at the end of main().
    """
    
    print(f"\n=== Recording simulation metadata ===")

    # Resolve creation_date; store source tag back into cfg for end-of-run report
    creation_date = str(cfg.get("creation_date", CREATION_DATE_PLACEHOLDER))
    if creation_date == CREATION_DATE_PLACEHOLDER:
        derived = derive_creation_date(wrfdir)
        if derived:
            creation_date = derived
            cfg["_creation_date_source"] = "auto-derived from wrfout timestamps"
            print(f"    result: {creation_date}")
        else:
            sys.exit(
                f"Error: creation_date is not set in sim_config.yml\n"
                f"  and could not be derived from wrfout files under\n"
                f"  {wrfdir}/*_chunk.\n"
                f"  Set creation_date manually in sim_config.yml and re-run."
            )
    else:
        cfg["_creation_date_source"] = "sim_config.yml"
    cfg["creation_date"] = creation_date

    print(f"  writing sim.env")

    with open(outpath, "w") as f:
        f.write("# sim.env - Auto-generated by setup.py.  Do not edit directly;\n")
        f.write("# edit sim_config.yml and re-run setup.py.\n\n")
        for key in SIM_ENV_KEYS:
            if key not in cfg:
                sys.exit(f"Error: Required key '{key}' missing from sim_config.yml")
            val = str(cfg[key])
            # Quote values that contain spaces or parentheses
            if " " in val or "(" in val:
                val = f'"{val}"'
            f.write(f"{key}={val}\n")
            vprint(f"  {key}={val}")


# ---------------------------------------------------------------------------
# Step 4: Create WRF coordinate reference file
# ---------------------------------------------------------------------------

def find_chunk_dir(wrfdir):
    """Return the path to the first *_chunk subdirectory found in wrfdir."""
    chunks = sorted(
        os.path.join(wrfdir, d)
        for d in os.listdir(wrfdir)
        if d.endswith("_chunk") and os.path.isdir(os.path.join(wrfdir, d))
    )
    if not chunks:
        sys.exit(f"Error: No *_chunk directories found in {wrfdir}")
    return chunks[0]


def create_coord_file(wrfdir, setupdir, force):
    """
    Create wrf.xy.coords.nc from the first wrfout_d01_* file found in the
    first *_chunk subdirectory of wrfdir.
    """
    outname = "wrf.xy.coords.nc"
    outpath = os.path.join(setupdir, outname)

    if os.path.exists(outpath) and not force:
        print(f"\n=== Coordinate file {outname} already exists (skipping) ===")
        return

    chunk_dir = find_chunk_dir(wrfdir)
    candidates = sorted(f for f in os.listdir(chunk_dir) if f.startswith("wrfout_d01_"))
    if not candidates:
        sys.exit(f"Error: No wrfout_d01_* files found in {chunk_dir}")
    coord_ref = os.path.join(chunk_dir, candidates[0])

    print(f"\n=== Generating coordinate file {outname} ===")
    vprint(f"  Source: {coord_ref}")

    # Extract lat/lon from first timestep, averaging over Time dimension
    run(f'ncwa -h -3 -a Time -C -v XLAT,XLONG "{coord_ref}" "{outpath}"')

    # Remove all existing variable and global attributes inherited from WRF,
    # then rename dimensions and variables to CF conventions
    run(f'ncatted -h '
        f'-a ,XLONG,d,, -a ,XLAT,d,, '
        f'-a \'^[A-Z0-9_-]+$\',global,d,, '
        f'-a stagger,,d,, -a coordinates,,d,, "{outpath}"')
    run(f'ncrename -h -d south_north,y -d west_east,x "{outpath}"')
    run(f'ncrename -h -v XLAT,lat -v XLONG,lon "{outpath}"')

    # Longitude monotonicity fix (NAM-12 domain specific: WRF outputs negative
    # longitudes west of the prime meridian; shift to 0-360 for monotonicity)
    run(f'ncap2 -h -O -s \'where(lon < 0) lon = lon + 360\' "{outpath}" "{outpath}"')

    # Add projection variable with all CRS attributes in one call
    run(f'ncap2 -h -A -s "crs=-9999" "{outpath}"')
    run(f'ncatted -h '
        f'-a long_name,crs,o,c,"coordinate reference system" '
        f'-a grid_mapping_name,crs,o,c,lambert_conformal_conic '
        f'-a standard_parallel,crs,o,f,"35.,60." '
        f'-a longitude_of_central_meridian,crs,o,f,-97. '
        f'-a latitude_of_projection_origin,crs,o,f,46. '
        f'-a semi_major_axis,crs,o,f,6370000. '
        f'-a semi_minor_axis,crs,o,f,6370000. '
        f'-a false_easting,crs,o,f,0. '
        f'-a false_northing,crs,o,f,0. '
        f'-a units,crs,o,c,"m" "{outpath}"')

    # Create x/y coordinate arrays (12 km grid spacing, centred on domain)
    run(f'ncap2 -h -A -s '
        f'\'x=array(-(($x.size-1)/2)*12000.,12000.,$x); '
        f'y=array(-(($y.size-1)/2)*12000.,12000.,$y)\' "{outpath}"')
    run(f'ncatted -h '
        f'-a units,x,o,c,m -a units,y,o,c,m '
        f'-a long_name,x,o,c,"x-coordinate in Cartesian system" '
        f'-a long_name,y,o,c,"y coordinate in Cartesian system" '
        f'-a standard_name,x,o,c,projection_x_coordinate '
        f'-a standard_name,y,o,c,projection_y_coordinate '
        f'-a axis,x,o,c,X -a axis,y,o,c,Y "{outpath}"')

    # Convert lat/lon to double precision and add CF metadata in one pass
    run(f'ncap2 -h -O -s \'lon=double(lon); lat=double(lat)\' "{outpath}" "{outpath}"')
    run(f'ncatted -h '
        f'-a units,lat,o,c,degrees_north '
        f'-a units,lon,o,c,degrees_east '
        f'-a long_name,lat,o,c,latitude '
        f'-a long_name,lon,o,c,longitude '
        f'-a standard_name,lat,o,c,latitude '
        f'-a standard_name,lon,o,c,longitude "{outpath}"')


# ---------------------------------------------------------------------------
# Step 5: Extract fixed WRF fields into wrf.fx.nc
# ---------------------------------------------------------------------------

# Fields to extract from the fx file for use during postprocessing.
# LANDMASK: land/sea mask (1=land, 0=ocean/water); used to mask land-only vars.
# COSALPHA, SINALPHA: wind rotation angles; used to rotate U/V to earth-relative
#   coordinates.  WRF outputs these with a singleton Time dimension; we squeeze
#   it out here so downstream code doesn't need to.
_FX_VARS = ['LANDMASK', 'COSALPHA', 'SINALPHA']

# Filename prefix for WRF files containing fixed (time-invariant) fields.
_WRF_FX_PREFIX = 'wrfout_5day_d01_'


def create_fx_file(wrfdir, setupdir, force):
    """Extract fixed WRF fields (LANDMASK, COSALPHA, SINALPHA) into wrf.fx.nc.

    Reads from the first wrfout_5day_d01_* file in the first chunk directory.
    Only the three needed fields are loaded (XLAT, XLONG, and all other WRF
    variables are never read).  The singleton Time dimension is squeezed out,
    WRF spatial dimensions are renamed to CORDEX conventions (x, y), all global
    attributes and all per-variable attributes except 'description' are dropped,
    and the result is written to setupdir/wrf.fx.nc.

    Having these fields in setupdir means postprocessing has no run-time
    dependency on wrfinput_path after setup completes.
    """
    outname = "wrf.fx.nc"
    outpath = os.path.join(setupdir, outname)

    if os.path.exists(outpath) and not force:
        print(f"\n=== Fixed-field file {outname} already exists (skipping) ===")
        return

    chunk_dir = find_chunk_dir(wrfdir)
    candidates = sorted(
        f for f in os.listdir(chunk_dir) if f.startswith(_WRF_FX_PREFIX))
    if not candidates:
        sys.exit(f"Error: No {_WRF_FX_PREFIX}* files found in {chunk_dir}")
    src = os.path.join(chunk_dir, candidates[0])

    print(f"\n=== Generating fixed-field file {outname} ===")
    vprint(f"  Source: {src}")
    vprint(f"  Variables: {', '.join(_FX_VARS)}")

    ds = xr.open_dataset(src)[_FX_VARS].squeeze('Time', drop=True)
    ds = ds.rename({'south_north': 'y', 'west_east': 'x'})

    # Strip all global attributes inherited from WRF
    ds.attrs = {}

    # Keep only the 'description' attribute on each variable; drop the rest
    for var in ds.data_vars:
        ds[var].attrs = {k: v for k, v in ds[var].attrs.items() if k == 'description'}

    # Remove Time from dataset encoding: xarray carries it through from the
    # source file even after squeeze, causing a spurious UserWarning on write.
    ds.encoding.pop('unlimited_dims', None)

    # Suppress _FillValue on fixed fields (no missing data; avoids xarray
    # inserting a default fill value on write)
    encoding = {var: {'_FillValue': None} for var in ds.data_vars}

    ds.to_netcdf(outpath, encoding=encoding)
    ds.close()

    # Remove the 'coordinates' attribute that xarray writes on each variable
    # referencing XLAT/XLONG/XTIME from the source file; these variables are
    # not present in wrf.fx.nc and the attribute serves no purpose here.
    run(f'ncatted -h -a coordinates,,d,, "{outpath}"')

    print(f"  wrote {outpath}")


# ---------------------------------------------------------------------------
# Step 6: Copy sim_config.yml into setupdir for provenance
# ---------------------------------------------------------------------------

def copy_config(config_path, setupdir, force):
    dest = os.path.join(setupdir, "sim_config.yml")
    if (os.path.abspath(config_path) == os.path.abspath(dest) or
        os.path.exists(dest) and not force):
        print(f"\n=== sim_config.yml already in setupdir (skipping copy) ===")
        return
    print(f"\n=== Copying sim_config.yml -> {dest} ===")
    shutil.copy(config_path, dest)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global _verbose
    args = parse_args()
    _verbose = args.verbose

    scripts_dir = args.scripts or os.path.dirname(os.path.realpath(__file__))
    wrfdir      = os.path.realpath(args.wrfdir)
    setupdir    = os.path.realpath(args.setupdir)
    force       = args.force

    os.makedirs(setupdir, exist_ok=True)

    if not os.path.isdir(wrfdir):
        sys.exit(f"Error: WRFDIR not found: {wrfdir}")

    # Locate config files
    config_path    = args.sim_config or os.path.join(scripts_dir, "sim_config.yml")
    var_specs_path = os.path.join(scripts_dir, "var_specs.yml")

    for path, label in [(config_path, "sim_config.yml"),
                        (var_specs_path, "var_specs.yml")]:
        if not os.path.exists(path):
            sys.exit(f"Error: {label} not found: {path}")

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    # Run setup steps
    dreq_path = fetch_upstream(cfg, setupdir, force)

    var_rows = build_var_table(
        var_specs_path, dreq_path, setupdir, cfg["cmor_table_freqs"], scripts_dir)
    write_var_table(var_rows, os.path.join(setupdir, "var_table.tsv"))

    write_sim_env(cfg, wrfdir, os.path.join(setupdir, "sim.env"))

    create_coord_file(wrfdir, setupdir, force)

    create_fx_file(wrfdir, setupdir, force)

    copy_config(config_path, setupdir, force)

    # Report creation_date prominently so the user can verify before proceeding
    creation_date        = cfg.get("creation_date", "unknown")
    creation_date_source = cfg.get("_creation_date_source", "")

    print(f"\n=== Setup complete ===")
    print(f"  Outputs in: {setupdir}")
    print(f"\n  Next step:")
    print(f"    extract.sh WRFDIR SETUPDIR YEARS [CMDDIR]")
    print(f"\n  creation_date: {creation_date}  ({creation_date_source})")
    print(f"  *** verify before proceeding ***\n")

if __name__ == "__main__":
    main()
