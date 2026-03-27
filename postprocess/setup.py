#!/usr/bin/env python3
"""
setup.py - One-time setup for NA-CORDEX-CMIP6 postprocessing workflow.

Reads sim_config.yml and var_specs.yml, downloads/caches the CORDEX data
request CSV and CMOR JSON tables, creates the WRF coordinate reference file,
and writes two flat files consumed by all downstream bash scripts:

  sim.env        Shell key=value pairs for all simulation metadata
  var_table.tsv  Per-variable specs (one row per variable, tab-separated)

Run once before extract.sh / format.sh.  All outputs go to SETUPDIR.

Usage:
  python setup.py WRFDIR SETUPDIR [--config SIM_CONFIG] [--scripts SCRIPTS_DIR]

  WRFDIR      Any WRF chunk directory containing wrfout_d01_* files
  SETUPDIR    Output directory (created if needed); use the same directory
              for extract.sh OUTDIR so coordinate files are found alongside data
  --config    Path to sim_config.yml (default: SCRIPTS_DIR/sim_config.yml)
  --scripts   Directory containing var_specs.yml (default: directory of this script)
  --force     Recreate outputs even if they already exist
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import urllib.request

import yaml


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="One-time setup for NA-CORDEX-CMIP6 postprocessing workflow.")
    p.add_argument("wrfdir",    metavar="WRFDIR",
                   help="WRF chunk directory containing wrfout_d01_* files")
    p.add_argument("setupdir",  metavar="SETUPDIR",
                   help="Output directory for all setup products")
    p.add_argument("--config",  metavar="PATH",
                   help="Path to sim_config.yml (default: SCRIPTS_DIR/sim_config.yml)")
    p.add_argument("--scripts", metavar="PATH",
                   help="Directory containing var_specs.yml (default: script directory)")
    p.add_argument("--force",   action="store_true",
                   help="Recreate outputs even if they already exist")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(cmd, description=""):
    """Run a shell command, raising on failure."""
    print(f"  {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        label = f" ({description})" if description else ""
        sys.exit(f"Error{label}: command failed with exit code {result.returncode}:\n  {cmd}")


def download(url, dest, force=False):
    """Download url to dest, skipping if dest exists and not force."""
    if os.path.exists(dest) and not force:
        print(f"  Already exists, skipping: {dest}")
        return
    print(f"  Downloading {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"  -> {dest}")


# ---------------------------------------------------------------------------
# Step 1: Download / cache upstream data files
# ---------------------------------------------------------------------------

def fetch_upstream(cfg, setupdir, force):
    """Download dreq CSV and CMOR JSON tables into setupdir."""
    print("\n=== Fetching upstream data files ===")

    # Data request CSV
    dreq_dest = os.path.join(setupdir, "dreq_default.csv")

    # Check default work location first, to avoid unnecessary downloads
    default_dreq = os.path.expanduser(
        f"~/work/cordex6/dreq_default.csv")
    if not force and not os.path.exists(dreq_dest):
        if os.path.exists(default_dreq):
            print(f"  Copying from default location: {default_dreq}")
            shutil.copy(default_dreq, dreq_dest)
        else:
            download(cfg["dreq_csv_url"], dreq_dest, force=force)
    else:
        download(cfg["dreq_csv_url"], dreq_dest, force=force)

    # CMOR JSON tables (one per frequency)
    base_url = cfg["cmor_table_base_url"].rstrip("/")
    for freq in cfg["cmor_table_freqs"]:
        fname = f"CORDEX-CMIP6_{freq}.json"
        dest  = os.path.join(setupdir, fname)
        url   = f"{base_url}_{freq}.json"
        download(url, dest, force=force)

    return dreq_dest


# ---------------------------------------------------------------------------
# Step 2: Build var_table.tsv
# ---------------------------------------------------------------------------

# Column order for var_table.tsv.
# standard_name and long_name are last for human readability.
VAR_TABLE_COLS = [
    "var", "freq", "units", "cell_methods", "positive",
    "levels", "refh", "quant",
    "standard_name", "long_name",
]


def load_dreq(dreq_path):
    """Return dict of {out_name: row_dict} from dreq_default.csv."""
    dreq = {}
    with open(dreq_path, newline="") as f:
        for row in csv.DictReader(f):
            dreq[row["out_name"]] = row
    return dreq


def load_cmor_tables(setupdir, freqs):
    """Return dict of {varname: {field: value}} from cached CMOR JSON tables."""
    cmor = {}
    for freq in freqs:
        fname = os.path.join(setupdir, f"CORDEX-CMIP6_{freq}.json")
        if not os.path.exists(fname):
            sys.exit(f"Error: CMOR table not found: {fname}\nRun setup.py first.")
        with open(fname) as f:
            table = json.load(f)
        for varname, spec in table.get("variable_entry", {}).items():
            # Later tables (day) override earlier (1hr) if a var appears in both;
            # in practice each var appears in only one table.
            cmor[varname] = spec
    return cmor


def build_var_table(var_specs_path, dreq_path, setupdir, freqs):
    """
    Merge var_specs.yml + dreq_default.csv + CMOR JSON tables into a resolved
    per-variable table.  Returns list of row dicts.
    """
    with open(var_specs_path) as f:
        var_specs = yaml.safe_load(f)

    dreq  = load_dreq(dreq_path)
    cmor  = load_cmor_tables(setupdir, freqs)

    # Remove YAML anchor/alias helper keys (start with _)
    var_specs = {k: v for k, v in var_specs.items() if not k.startswith("_")}

    rows = []
    for varname, specs in var_specs.items():
        row = {"var": varname}

        # --- From dreq_default.csv ---
        dreq_row = dreq.get(varname, {})
        row["freq"]         = dreq_row.get("frequency", "")
        row["units"]        = dreq_row.get("units", "")
        row["cell_methods"] = dreq_row.get("cell_methods", "None")
        row["long_name"]    = dreq_row.get("long_name", "")
        row["standard_name"] = dreq_row.get("standard_name", "")

        # --- From CMOR JSON tables (authoritative for positive, may supplement) ---
        cmor_entry = cmor.get(varname, {})
        row["positive"] = cmor_entry.get("positive", "")

        # Override standard_name / long_name from CMOR if dreq is empty
        if not row["standard_name"]:
            row["standard_name"] = cmor_entry.get("standard_name", "")
        if not row["long_name"]:
            row["long_name"] = cmor_entry.get("long_name", "")

        # --- From var_specs.yml ---
        row["levels"] = specs.get("levels", "single")
        row["refh"]   = str(specs["refh"])   if "refh"  in specs else ""
        row["quant"]  = str(specs["quant"])  if "quant" in specs else ""

        rows.append(row)

    return rows


def write_var_table(rows, outpath):
    print(f"\n=== Writing var_table.tsv -> {outpath} ===")
    with open(outpath, "w", newline="") as f:
        f.write("\t".join(VAR_TABLE_COLS) + "\n")
        for row in rows:
            f.write("\t".join(row.get(c, "") for c in VAR_TABLE_COLS) + "\n")
    for row in rows:
        print(f"  {row['var']:12s}  freq={row['freq']:4s}  levels={row['levels']:6s}"
              f"  refh={row['refh']:3s}  quant={row['quant']}")


# ---------------------------------------------------------------------------
# Step 3: Write sim.env
# ---------------------------------------------------------------------------

# Keys written to sim.env, in order.
# These map directly to sim_config.yml keys.
SIM_ENV_KEYS = [
    "activity_id",
    "contact",
    "creation_date",
    "domain",
    "domain_id",
    "driving_experiment",
    "driving_experiment_id",
    "driving_institution_id",
    "driving_source_id",
    "driving_variant_label",
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
    timestamp in wrfdir, using the same approach as WORKFLOW.md:
      find $WRFOUT/*chunk -type f -name 'wrfout*' -printf "%T+\\n" | ...
    Returns a YYYY-MM-DD string, or None if no files are found.
    """
    import glob
    pattern = os.path.join(wrfdir, "**", "wrfout*")
    candidates = glob.glob(pattern, recursive=True)
    if not candidates:
        return None
    latest_mtime = max(os.path.getmtime(f) for f in candidates)
    import datetime
    return datetime.date.fromtimestamp(latest_mtime).strftime("%Y-%m-%d")


def write_sim_env(cfg, wrfdir, outpath):
    """Write sim.env: one shell variable assignment per line.

    Attempts to auto-derive creation_date from wrfout file timestamps if the
    config still contains the placeholder value.  Always prints the resolved
    value and prompts the user to verify it.
    """
    print(f"\n=== Writing sim.env -> {outpath} ===")

    # Resolve creation_date
    creation_date = str(cfg.get("creation_date", CREATION_DATE_PLACEHOLDER))
    if creation_date == CREATION_DATE_PLACEHOLDER:
        derived = derive_creation_date(wrfdir)
        if derived:
            creation_date = derived
            print(f"  creation_date auto-derived from wrfout timestamps: {creation_date}")
            print(f"  ** Please verify this date is correct before proceeding. **")
        else:
            sys.exit(
                f"Error: creation_date is not set in sim_config.yml and could not\n"
                f"  be derived from wrfout files in {wrfdir}.\n"
                f"  Set creation_date manually in sim_config.yml and re-run."
            )
    else:
        print(f"  creation_date from sim_config.yml: {creation_date}")
        print(f"  ** Please verify this date is correct before proceeding. **")
    cfg["creation_date"] = creation_date

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
            print(f"  {key}={val}")


# ---------------------------------------------------------------------------
# Step 4: Create WRF coordinate reference file
# ---------------------------------------------------------------------------

def create_coord_file(wrfdir, setupdir, force):
    """
    Create wrf.xy.coords.nc from the first wrfout_d01_* file found in wrfdir.
    Mirrors the NCO operations from the original setup.sh.
    """
    outpath = os.path.join(setupdir, "wrf.xy.coords.nc")

    if os.path.exists(outpath) and not force:
        print(f"\n=== Coordinate file already exists (skipping): {outpath} ===")
        return

    # Find coord reference file
    candidates = sorted(
        f for f in os.listdir(wrfdir) if f.startswith("wrfout_d01_"))
    if not candidates:
        sys.exit(f"Error: No wrfout_d01_* files found in {wrfdir}")
    coord_ref = os.path.join(wrfdir, candidates[0])
    print(f"\n=== Creating coordinate reference file ===")
    print(f"  Source: {coord_ref}")
    print(f"  Output: {outpath}")

    # Extract and clean coordinates (mirrors setup.sh NCO operations)
    run(f'ncwa -h -3 -a Time -C -v XLAT,XLONG "{coord_ref}" "{outpath}"')
    run(f'ncatted -h -a ,XLONG,d,, "{outpath}"')
    run(f'ncatted -h -a ,XLAT,d,, "{outpath}"')
    run(f'ncatted -h -a \'^[A-Z0-9_-]+$\',global,d,, "{outpath}"')
    run(f'ncatted -h -a stagger,,d,, "{outpath}"')
    run(f'ncatted -h -a coordinates,,d,, "{outpath}"')
    run(f'ncrename -h -d south_north,y -d west_east,x "{outpath}"')
    run(f'ncrename -h -v XLAT,lat -v XLONG,lon "{outpath}"')

    # Longitude monotonicity fix (NAM-12 domain specific)
    run(f'ncap2 -h -O -s \'where(lon < 0) lon = lon + 360\' "{outpath}" "{outpath}"')

    # Projection variable
    run(f'ncap2 -h -A -s "crs=-9999" "{outpath}"')
    run(f'ncatted -h -a long_name,crs,o,c,"coordinate reference system" "{outpath}"')
    run(f'ncatted -h -a grid_mapping_name,crs,o,c,lambert_conformal_conic "{outpath}"')
    run(f'ncatted -h -a standard_parallel,crs,o,f,"35.,60." "{outpath}"')
    run(f'ncatted -h -a longitude_of_central_meridian,crs,o,f,-97. "{outpath}"')
    run(f'ncatted -h -a latitude_of_projection_origin,crs,o,f,46. "{outpath}"')
    run(f'ncatted -h -a semi_major_axis,crs,o,f,6370000. "{outpath}"')
    run(f'ncatted -h -a semi_minor_axis,crs,o,f,6370000. "{outpath}"')
    run(f'ncatted -h -a false_easting,crs,o,f,0. "{outpath}"')
    run(f'ncatted -h -a false_northing,crs,o,f,0. "{outpath}"')
    run(f'ncatted -h -a units,crs,o,c,"m" "{outpath}"')

    # x/y coordinate arrays
    run(f'ncap2 -h -A -s \'x=array(-(($x.size-1)/2)*12000.,12000.,$x); y=array(-(($y.size-1)/2)*12000.,12000.,$y)\' "{outpath}"')
    run(f'ncatted -h -a units,y,o,c,m -a units,x,o,c,m "{outpath}"')
    run(f'ncatted -h -a long_name,y,o,c,"y coordinate in Cartesian system" "{outpath}"')
    run(f'ncatted -h -a long_name,x,o,c,"x-coordinate in Cartesian system" "{outpath}"')
    run(f'ncatted -h -a standard_name,y,o,c,projection_y_coordinate "{outpath}"')
    run(f'ncatted -h -a standard_name,x,o,c,projection_x_coordinate "{outpath}"')
    run(f'ncatted -h -a axis,x,o,c,X -a axis,y,o,c,Y "{outpath}"')

    # lat/lon metadata
    run(f'ncatted -h -a units,lat,o,c,degrees_north "{outpath}"')
    run(f'ncatted -h -a units,lon,o,c,degrees_east "{outpath}"')
    run(f'ncatted -h -a long_name,lat,o,c,latitude "{outpath}"')
    run(f'ncatted -h -a long_name,lon,o,c,longitude "{outpath}"')
    run(f'ncatted -h -a standard_name,lat,o,c,latitude "{outpath}"')
    run(f'ncatted -h -a standard_name,lon,o,c,longitude "{outpath}"')
    run(f'ncap2 -O -s \'lon=double(lon)\' "{outpath}" "{outpath}"')
    run(f'ncap2 -O -s \'lat=double(lat)\' "{outpath}" "{outpath}"')

    # Global attributes
    run(f'ncatted -h -a Conventions,global,o,c,"CF-1.11" "{outpath}"')
    run(f'ncatted -h -a institution,global,o,c,"National Center for Atmospheric Research: Research Applications Laboratory" "{outpath}"')
    run(f'ncatted -h -a source,global,o,c,"Weather Research and Forecasting Model Version 4.6.1" "{outpath}"')

    print(f"  Done: {outpath}")


# ---------------------------------------------------------------------------
# Step 5: Copy sim_config.yml into setupdir for provenance
# ---------------------------------------------------------------------------

def copy_config(config_path, setupdir, force):
    dest = os.path.join(setupdir, "sim_config.yml")
    if os.path.abspath(config_path) == os.path.abspath(dest):
        return  # already in place
    if os.path.exists(dest) and not force:
        print(f"\n=== sim_config.yml already in setupdir (skipping copy) ===")
        return
    print(f"\n=== Copying sim_config.yml -> {dest} ===")
    shutil.copy(config_path, dest)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    scripts_dir = args.scripts or os.path.dirname(os.path.realpath(__file__))
    wrfdir      = os.path.realpath(args.wrfdir)
    setupdir    = os.path.realpath(args.setupdir)
    force       = args.force

    os.makedirs(setupdir, exist_ok=True)

    if not os.path.isdir(wrfdir):
        sys.exit(f"Error: WRFDIR not found: {wrfdir}")

    # Locate config files
    config_path    = args.config or os.path.join(scripts_dir, "sim_config.yml")
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
        var_specs_path, dreq_path, setupdir, cfg["cmor_table_freqs"])
    write_var_table(var_rows, os.path.join(setupdir, "var_table.tsv"))

    write_sim_env(cfg, wrfdir, os.path.join(setupdir, "sim.env"))

    create_coord_file(wrfdir, setupdir, force)

    copy_config(config_path, setupdir, force)

    print(f"\n=== Setup complete ===")
    print(f"  Outputs in: {setupdir}")
    print(f"\nNext step:")
    print(f"  extract.sh WRFDIR {setupdir} YEARS [CMDDIR]")


if __name__ == "__main__":
    main()
