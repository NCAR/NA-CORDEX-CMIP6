#!/usr/bin/env python3
# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen
"""
index.py - Generate commandfiles for computing climate indices from
compressed CORDEX-CMIP6 daily data.

Operates on the output of compress.sh (Step 4), where data are organized
into <var>.<freq> subdirectories (e.g., pr.day, tasmax.day).  Produces
one NetCDF file per index covering all available years.

Index definitions are read from gis_indexes.tsv and cleanup specs from
gis_cleanup.tsv, both expected in SETUPDIR.  Modifying the TSVs is the
intended way to add, remove, or change indices.

Generates seven commandfiles that must be run in this order:

  concat.cmd   - Concatenates per-variable decadal input files into a
                 single file per variable using ncrcat.  Must be run
                 first; all subsequent steps depend on its output.

  minmax.cmd   - ydrunmin/ydrunmax/timmin/timmax over the baseline period.
                 Computed in native input units (no conversion).

  pctile.cmd   - ydrunpctl/ydrunmean/timpctl reference files, also in
                 native units.

  indices.cmd  - Indices whose CDO operators natively output annual time
                 steps.  Unit conversion applied here, not in prereqs.

  annual.cmd   - One command per year for operators that summarise over
                 their entire input.  Each command operates on the single
                 input file for that year.  Output goes to OUTDIR/annual/.

  merge.cmd    - One mergetime per annual-loop index assembling per-year
                 files from OUTDIR/annual/ into OUTDIR/raw/.

  cleanup.cmd  - Applies corrected CF metadata to each raw index file via
                 clean_index.sh, writing final files to OUTDIR/.

Per-year temporary files in OUTDIR/annual/ can be removed after merge.cmd.
Raw index files in OUTDIR/raw/ can be removed after cleanup.cmd.

Directory layout under OUTDIR:
  concat/   Concatenated per-variable input files (intermediate)
  pctl/     Baseline percentile/minmax reference files (intermediate)
  annual/   Per-year files for annual_loop indices (intermediate)
  raw/      Raw index files before metadata cleanup
  (root)    Final cleaned index files

See gis_indexes.tsv and gis_cleanup.tsv in SETUPDIR for TSV column
documentation.
"""

import argparse
import csv
import re
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Unit conversion rules.  Single source of truth: maps units string to the
# CDO operator prepended to the input pipe in index commands (not prereqs).
# ---------------------------------------------------------------------------
UNIT_CONV = {
    "mm/day": "-mulc,86400",
    "octas":  "-mulc,0.08",
    "C":      "-subc,277.15",
}

PCTL_WINDOW  = 5   # running-window width for ydrun* operators

# Commandfile each prereq operator writes to.  Also the single source of
# truth for prereq operator names; used with CMDFILES to define all seven
# commandfiles in their required run order.
PREREQ_CMD = {
    "ydrunmin":  "minmax",
    "ydrunmax":  "minmax",
    "ydrunmean": "minmax",
    "timmin":    "minmax",
    "timmax":    "minmax",
    "ydrunpctl": "pctile",
    "timpctl":   "pctile",
}

CMDFILES = ["concat", "minmax", "pctile", "indices", "annual", "merge", "cleanup"]

# Dependency tree for prerequisite operators.  Each operator lists the
# operators whose output files it requires as additional inputs.
PREREQ_DEPS = {
    "ydrunmin":  [],
    "ydrunmax":  [],
    "ydrunmean": [],
    "ydrunpctl": ["ydrunmin", "ydrunmax"],
    "timmin":    [],
    "timmax":    [],
    "timpctl":   ["timmin", "timmax"],
}

# Set after argument parsing; effectively read-only thereafter.
FORCE      = False
MIDDLE     = ""
BL_TIMESPAN = ""


# ---------------------------------------------------------------------------
# File discovery helpers
# ---------------------------------------------------------------------------

def day_files(indir, var):
    """Sorted list of all .nc files for <var>.day."""
    d = indir / f"{var}.day"
    return sorted(d.glob(f"{var}_*.nc")) if d.is_dir() else []


def file_years(path):
    """Extract (start_year, end_year) from a CORDEX filename timespan."""
    ts = path.stem.split("_")[-1]
    return int(ts[:4]), int(ts[9:13])


def year_file(indir, var, yr):
    """The single file for <var> that contains the given year."""
    for f in day_files(indir, var):
        sy, ey = file_years(f)
        if sy <= yr <= ey:
            return f
    return None


def sftlf_file(indir):
    """First sftlf file found in sftlf.fx/."""
    d = indir / "sftlf.fx"
    files = sorted(d.glob("sftlf_*.nc")) if d.is_dir() else []
    return files[0] if files else None


# ---------------------------------------------------------------------------
# Emit helper
# ---------------------------------------------------------------------------

def emit(cmdfile, outfile, cmd):
    """Write cmd to cmdfile unless --force is unset and outfile already exists."""
    if not FORCE and outfile.exists():
        return
    cmdfile.write(cmd + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global FORCE, MIDDLE, BL_TIMESPAN
    ap = argparse.ArgumentParser(
        description="Generate commandfiles for computing climate indices.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("indir",    type=Path, help="compress.sh output directory")
    ap.add_argument("outdir",   type=Path, help="Output directory for index files")
    ap.add_argument("setupdir", type=Path, help="Directory containing gis_indexes.tsv, "
                                                 "gis_cleanup.tsv, and clean_index.sh")
    ap.add_argument("cmddir", type=Path, nargs="?", default=Path("."),
                    help="Directory for commandfiles (default: .)")
    ap.add_argument("--baseline", default="1991-2020",
                    metavar="STARTYEAR-ENDYEAR",
                    help="Reference period (default: 1991-2020)")
    ap.add_argument("--force", action="store_true",
                    help="Overwrite existing output files")
    args = ap.parse_args()

    m = re.fullmatch(r"(\d{4})-(\d{4})", args.baseline)
    if not m:
        ap.error(f"--baseline must be STARTYEAR-ENDYEAR, got: {args.baseline}")
    bstart, bend = int(m[1]), int(m[2])

    indir    = args.indir.resolve()
    outdir   = args.outdir
    setupdir = args.setupdir.resolve()
    cmddir   = args.cmddir
    tsv      = setupdir / "gis_indexes.tsv"
    cleanup_tsv = setupdir / "gis_cleanup.tsv"

    if not indir.is_dir():
        sys.exit(f"Error: INDIR not found: {indir}")
    if not setupdir.is_dir():
        sys.exit(f"Error: SETUPDIR not found: {setupdir}")
    if not tsv.is_file():
        sys.exit(f"Error: gis_indexes.tsv not found in SETUPDIR: {tsv}")
    if not cleanup_tsv.is_file():
        sys.exit(f"Error: gis_cleanup.tsv not found in SETUPDIR: {cleanup_tsv}")

    outdir.mkdir(parents=True, exist_ok=True)
    outdir = outdir.resolve()
    rawdir = outdir / "raw"
    rawdir.mkdir(exist_ok=True)
    cmddir.mkdir(parents=True, exist_ok=True)
    cmddir = cmddir.resolve()

    concatdir = outdir / "concat"
    pctldir   = outdir / "pctl"
    anndir    = outdir / "annual"
    concatdir.mkdir(exist_ok=True)
    pctldir.mkdir(exist_ok=True)
    anndir.mkdir(exist_ok=True)

    all_nc = sorted(indir.glob("*.day/*.nc"))
    if not all_nc:
        sys.exit(f"Error: No *.nc files found under {indir}/*.day/")
    sim_start = file_years(all_nc[0])[0]
    sim_end   = file_years(all_nc[-1])[1]
    timespan  = (f"{all_nc[0].stem.split('_')[-1][:4]}-"
                 f"{all_nc[-1].stem.split('_')[-1][9:13]}")

    FORCE       = args.force
    MIDDLE      = "_".join(all_nc[0].stem.split("_")[1:8])
    BL_TIMESPAN = f"{bstart}0101-{bend}1231"

    print(f"Baseline period: {bstart}-{bend}")
    print(f"  Setup dir:  {setupdir}")
    print(f"  DRS middle: {MIDDLE}")
    print(f"  Timespan:   {timespan}  ({sim_start}-{sim_end})")

    cmd_paths = {name: cmddir / f"{name}.cmd" for name in CMDFILES}
    cmd_files = {k: open(v, "w") for k, v in cmd_paths.items()}

    # ------------------------------------------------------------------
    # Pass 1: collect all unique non-sftlf variables referenced in TSV,
    # emit concat.cmd, and record which vars are available.
    # ------------------------------------------------------------------
    seen_vars = set()
    all_vars  = []
    with open(tsv, newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            for v in row["input_vars"].strip().split("+"):
                if v != "sftlf" and v not in seen_vars:
                    all_vars.append(v)
                    seen_vars.add(v)

    concat_ok = set()   # vars successfully scheduled for concatenation

    for var in all_vars:
        files = day_files(indir, var)
        if not files:
            print(f"    WARNING: {var}.day not found or empty; "
                  f"dependent indices will be skipped", file=sys.stderr)
            continue
        out       = concatdir / f"{var}_{MIDDLE}_{timespan}.nc"
        filenames = " ".join(f.name for f in files)
        emit(cmd_files["concat"], out,
             f"ncrcat -p {indir / f'{var}.day'} -o {out} {filenames}")
        concat_ok.add(var)

    # ------------------------------------------------------------------
    # ensure(spec, var, bl_pipe) - closure over prereq_done and cmd_files.
    # Recursively schedules prereq commands and returns the output Path.
    # prereq_done is keyed on (spec, var) to avoid cross-variable collisions.
    # ------------------------------------------------------------------
    prereq_done = set()

    def ensure(spec, var, bl_pipe):
        key = (spec, var)
        op  = spec.split(",")[0]
        assert op in PREREQ_CMD, f"Unknown prereq operator: {op}"
        out = pctldir / f"{var}_{spec.replace(',', '')}_{MIDDLE}_{BL_TIMESPAN}.nc"
        if key in prereq_done:
            return out

        # Recursively ensure dependencies first.  ydrunpctl passes its window
        # arg down to ydrunmin/ydrunmax; timpctl's deps (timmin/timmax) take no args.
        dep_args  = spec.split(",")[-1] if op == "ydrunpctl" else ""
        dep_files = [
            ensure(f"{dep_op},{dep_args}" if dep_args else dep_op, var, bl_pipe)
            for dep_op in PREREQ_DEPS[op]
        ]
        dep_str = " ".join(str(f) for f in dep_files)

        if op in ("ydrunmin", "ydrunmax", "ydrunmean"):
            cmd = f"cdo {spec} {bl_pipe} {out}"
        elif op == "ydrunpctl":
            cmd = f"cdo {spec} {bl_pipe} {dep_str} {out}"
        elif op in ("timmin", "timmax"):
            cmd = f"cdo {op} {bl_pipe} {out}"
        else:  # timpctl; mask dry days in pr baseline before computing percentile
            pipe = f"-setrtomiss,,1 {bl_pipe}" if var == "pr" else bl_pipe
            cmd  = f"cdo {spec} {pipe} {dep_str} {out}"

        emit(cmd_files[PREREQ_CMD[op]], out, cmd)
        prereq_done.add(key)
        return out

    # ------------------------------------------------------------------
    # Pass 2: emit indices / annual / merge commands
    # ------------------------------------------------------------------
    with open(tsv, newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            idx          = row["index"].strip()
            op           = row["cdo_operator"].strip()
            units        = row["units"].strip()
            freq         = row["output_frequency"].strip()
            prereq_specs = row["prereq_type"].strip()
            vars_list    = row["input_vars"].strip().split("+")

            primary_var = vars_list[0]

            # Check all inputs are available
            skip = False
            for v in vars_list:
                if v == "sftlf":
                    if sftlf_file(indir) is None:
                        print(f"    WARNING: sftlf.fx not found; skipping {idx}",
                              file=sys.stderr)
                        skip = True; break
                elif v not in concat_ok:
                    skip = True; break
            if skip:
                continue

            bl_pipe   = (f"-selyear,{bstart}/{bend} "
                         f"{concatdir}/{primary_var}_{MIDDLE}_{timespan}.nc")
            final_out = rawdir / f"{idx}_{MIDDLE}_{timespan}.nc"

            # Resolve prerequisites
            prereq_str = " ".join(
                str(ensure(spec, primary_var, bl_pipe))
                for spec in prereq_specs.split("+")
            ) if prereq_specs != "none" else ""

            # Secondary inputs: vars beyond the primary (e.g. tasmin for DTR, sftlf for GSL)
            def sec_inputs(yr=None):
                parts = []
                for v in vars_list[1:]:
                    if v == "sftlf":
                        parts.append(str(sftlf_file(indir)))
                    elif yr is None:
                        parts.append(f"{concatdir}/{v}_{MIDDLE}_{timespan}.nc")
                    else:
                        yf = year_file(indir, v, yr)
                        parts.append(f"-selyear,{yr} {yf}" if yf else "MISSING")
                return " ".join(parts)

            # Shared trailing fragment: optional prereqs then output file
            def tail(out):
                return (f" {prereq_str}" if prereq_str else "") + f" {out}"

            if freq == "annual":
                c    = UNIT_CONV.get(units, "")
                pipe = f"{c} {concatdir}/{primary_var}_{MIDDLE}_{timespan}.nc" if c \
                       else f"{concatdir}/{primary_var}_{MIDDLE}_{timespan}.nc"
                sec  = sec_inputs()
                cmd  = (f"cdo {op} {pipe}"
                        + (f" {sec}" if sec else "")
                        + tail(final_out))
                emit(cmd_files["indices"], final_out, cmd)

            elif freq == "annual_loop":
                c       = UNIT_CONV.get(units, "")
                yr_outs = []

                for yr in range(sim_start, sim_end + 1):
                    yf = year_file(indir, primary_var, yr)
                    if yf is None:
                        print(f"    WARNING: no file for {primary_var} "
                              f"year {yr}; skipping", file=sys.stderr)
                        continue
                    yr_out = anndir / f"{idx}_{MIDDLE}_{yr}.nc"
                    sec    = sec_inputs(yr=yr)
                    cmd    = (f"cdo {op} {f'{c} ' if c else ''}-selyear,{yr} {yf}"
                              + (f" {sec}" if sec else "")
                              + tail(yr_out))
                    emit(cmd_files["annual"], yr_out, cmd)
                    yr_outs.append(yr_out)

                if yr_outs:
                    yr_list = " ".join(str(y) for y in yr_outs)
                    emit(cmd_files["merge"], final_out,
                         f"cdo mergetime {yr_list} {final_out}")

            else:
                print(f"    WARNING: unknown output_frequency '{freq}' "
                      f"for {idx}; skipping", file=sys.stderr)

    # ------------------------------------------------------------------
    # Pass 3: emit cleanup.cmd from gis_cleanup.tsv.
    # One call to clean_index.sh per row (== per output file).
    # Raw index file is keyed on source_file column, not index, since
    # secondary variables (e.g. CDDn) live in the primary index's raw file.
    # ------------------------------------------------------------------
    with open(cleanup_tsv, newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            idx        = row["index"].strip()
            src        = row["source_file"].strip()
            raw_in     = rawdir  / f"{src}_{MIDDLE}_{timespan}.nc"
            clean_out  = outdir  / f"{idx}_{MIDDLE}_{timespan}.nc"
            cmd = (f"./clean_index.sh {idx} {raw_in} {clean_out} {setupdir}")
            emit(cmd_files["cleanup"], clean_out, cmd)

    # Close commandfiles; remove any that are empty
    for name, fh in cmd_files.items():
        fh.close()
        p = cmd_paths[name]
        if p.stat().st_size == 0:
            p.unlink()

    # Count commands (lines not starting with 'export') in each commandfile
    def count_cmds(p):
        if not p.exists():
            return 0
        return sum(1 for ln in p.read_text().splitlines()
                   if ln and not ln.startswith("export"))

    counts = {name: count_cmds(cmd_paths[name]) for name in CMDFILES}

    print()
    print("Commandfile generation complete.")
    print(f"  TSV:                     {tsv}")
    print(f"  Cleanup TSV:             {cleanup_tsv}")
    print(f"  Commandfiles:            {cmddir}")
    print(f"  Concatenated inputs:     {concatdir}")
    print(f"  Reference files:         {pctldir}")
    print(f"  Per-year temp files:     {anndir}")
    print(f"  Raw index files:         {rawdir}")
    print(f"  Final index files:       {outdir}")
    print("  " + "  ".join(f"{n.capitalize()}: {counts[n]}" for n in CMDFILES))
    print()

if __name__ == "__main__":
    main()
