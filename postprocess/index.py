#!/usr/bin/env python3
"""
index.py - Generate commandfiles for computing climate indices from
compressed CORDEX-CMIP6 daily data.

Operates on the output of compress.sh (Step 4), where data are organized
into <var>.<freq> subdirectories (e.g., pr.day, tasmax.day).  Produces
one NetCDF file per index covering all available years, written flat into
OUTDIR (no per-variable subdirectories, since these files go to GIS).

Index definitions are read from a TSV file (default: gis_indexes.tsv in
the same directory as this script).  The TSV drives command construction;
modifying the TSV is the intended way to add, remove, or change indices.

Generates five commandfiles that must be run in order:

  minmax.cmd   - ydrunmin/ydrunmax/timmin/timmax over the baseline period.
                 Computed in native input units (no conversion).

  pctile.cmd   - ydrunpctl/ydrunmean/timpctl reference files, also in
                 native units.

  indices.cmd  - Indices whose CDO operators natively output annual time
                 steps.  Unit conversion applied here, not in prereqs.
                 Includes CDO_PCTL_NBINS export before bootstrapped cmds.

  annual.cmd   - One command per year for operators that summarise over
                 their entire input.  Each command operates on the single
                 input file for that year.  Output goes to OUTDIR/annual/.

  merge.cmd    - One mergetime per annual-loop index assembling per-year
                 files from OUTDIR/annual/ into OUTDIR.

Per-year temporary files in OUTDIR/annual/ can be removed after merge.cmd.

See gis_indexes.tsv for TSV column documentation.
"""

import argparse
import csv
import os
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
PCTL_NBINS_K = 2   # sizeof(double)/sizeof(int) factor for CDO_PCTL_NBINS

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


# ---------------------------------------------------------------------------
# File discovery helpers
# ---------------------------------------------------------------------------

def day_files(indir: Path, var: str) -> list[Path]:
    """Sorted list of all .nc files for <var>.day."""
    d = indir / f"{var}.day"
    return sorted(d.glob(f"{var}_*.nc")) if d.is_dir() else []


def file_years(path: Path) -> tuple[int, int]:
    """Extract (start_year, end_year) from a CORDEX filename timespan."""
    ts = path.stem.split("_")[-1]
    return int(ts[:4]), int(ts[9:13])


def year_file(indir: Path, var: str, yr: int) -> Path | None:
    """The single file for <var> that contains the given year."""
    for f in day_files(indir, var):
        sy, ey = file_years(f)
        if sy <= yr <= ey:
            return f
    return None


def baseline_files(indir: Path, var: str, bstart: int, bend: int) -> list[Path]:
    """Files for <var> whose timespan overlaps the baseline period."""
    return [f for f in day_files(indir, var)
            if file_years(f)[0] <= bend and file_years(f)[1] >= bstart]


def sftlf_file(indir: Path) -> Path | None:
    """First sftlf file found in sftlf.fx/."""
    d = indir / "sftlf.fx"
    files = sorted(d.glob("sftlf_*.nc")) if d.is_dir() else []
    return files[0] if files else None


# ---------------------------------------------------------------------------
# CDO command construction helpers
# ---------------------------------------------------------------------------

def conv(units: str) -> str:
    """CDO unit-conversion operator string, or empty string."""
    return UNIT_CONV.get(units, "")


def full_pipe(indir: Path, var: str, units: str) -> str:
    """Full input pipe with unit conversion: '[conv] -mergetime f1 f2 ...'"""
    files = day_files(indir, var)
    if not files:
        return ""
    parts = " ".join(f'"{f}"' for f in files)
    pipe = f"-mergetime {parts}"
    c = conv(units)
    return f"{c} {pipe}" if c else pipe


def baseline_pipe(indir: Path, var: str, bstart: int, bend: int) -> str:
    """Baseline input pipe: no unit conversion, selyear-bounded."""
    files = baseline_files(indir, var, bstart, bend)
    if not files:
        return ""
    parts = " ".join(f'"{f}"' for f in files)
    return f"-selyear,{bstart}/{bend} -mergetime {parts}"


def pctl_path(pctldir: Path, var: str, stat: str,
              middle: str, bl_timespan: str) -> Path:
    """Canonical prereq file: PCTLDIR/<var>_<stat>_<middle>_<bl_ts>.nc"""
    return pctldir / f"{var}_{stat}_{middle}_{bl_timespan}.nc"


# ---------------------------------------------------------------------------
# Emit helper
# ---------------------------------------------------------------------------

def make_emit(force: bool):
    """Return an emit(cmdfile, outfile, cmd) function respecting --force."""
    def emit(cmdfile, outfile: Path, cmd: str):
        if not force and outfile.exists():
            return
        cmdfile.write(cmd + "\n")
    return emit


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    script_dir = Path(__file__).parent
    default_tsv = script_dir / "gis_indexes.tsv"

    ap = argparse.ArgumentParser(
        description="Generate commandfiles for computing climate indices.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("indir",  type=Path, help="compress.sh output directory")
    ap.add_argument("outdir", type=Path, help="Output directory for index files")
    ap.add_argument("cmddir", type=Path, nargs="?", default=Path("."),
                    help="Directory for commandfiles (default: .)")
    ap.add_argument("--tsv", type=Path, default=default_tsv,
                    help=f"Index definitions TSV (default: {default_tsv})")
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

    indir  = args.indir.resolve()
    outdir = args.outdir
    cmddir = args.cmddir
    tsv    = args.tsv

    if not indir.is_dir():
        sys.exit(f"Error: INDIR not found: {indir}")
    if not tsv.is_file():
        sys.exit(f"Error: TSV not found: {tsv}")

    outdir.mkdir(parents=True, exist_ok=True)
    outdir = outdir.resolve()
    cmddir.mkdir(parents=True, exist_ok=True)
    cmddir = cmddir.resolve()

    pctldir = outdir / "pctl"
    anndir  = outdir / "annual"
    pctldir.mkdir(exist_ok=True)
    anndir.mkdir(exist_ok=True)

    baseline_years = bend - bstart + 1
    pctl_nbins = PCTL_WINDOW * baseline_years * PCTL_NBINS_K + 2
    bl_timespan = f"{bstart}0101-{bend}1231"

    all_nc = sorted(indir.glob("*.day/*.nc"))
    if not all_nc:
        sys.exit(f"Error: No *.nc files found under {indir}/*.day/")
    middle    = "_".join(all_nc[0].stem.split("_")[1:8])
    sim_start = file_years(all_nc[0])[0]
    sim_end   = file_years(all_nc[-1])[1]
    first_ts  = all_nc[0].stem.split("_")[-1]
    last_ts   = all_nc[-1].stem.split("_")[-1]
    timespan  = f"{first_ts[:4]}-{last_ts[9:13]}"

    print(f"Baseline period: {bstart}-{bend}")
    print(f"CDO_PCTL_NBINS:  {pctl_nbins}")
    print(f"  DRS middle: {middle}")
    print(f"  Timespan:   {timespan}  ({sim_start}-{sim_end})")

    cmd_paths = {
        "minmax":  cmddir / "minmax.cmd",
        "pctile":  cmddir / "pctile.cmd",
        "indices": cmddir / "indices.cmd",
        "annual":  cmddir / "annual.cmd",
        "merge":   cmddir / "merge.cmd",
    }
    cmd_files = {k: open(v, "w") for k, v in cmd_paths.items()}
    emit = make_emit(args.force)

    # prereq_done is keyed on (spec, var) to avoid cross-variable collisions
    prereq_done = set()
    nbins_emitted = False
    counts = dict(minmax=0, pctile=0, indices=0, annual=0, merge=0)

    def ensure(spec: str, var: str, bl_pipe: str) -> Path:
        """Ensure prereq is scheduled; return its output Path."""
        key = (spec, var)
        stat = spec.replace(",", "")
        out = pctl_path(pctldir, var, stat, middle, bl_timespan)
        if key in prereq_done:
            return out

        parts = spec.split(",")
        op = parts[0]

        # Recursively ensure dependencies first
        dep_files = []
        for dep_op in PREREQ_DEPS[op]:
            if op == "ydrunpctl":
                dep_spec = f"{dep_op},{parts[-1]}"   # dep takes window only
            else:
                dep_spec = dep_op                     # timmin/timmax have no args
            dep_files.append(ensure(dep_spec, var, bl_pipe))

        # Emit this prereq's command
        dep_str = " ".join(f'"{f}"' for f in dep_files)

        if op in ("ydrunmin", "ydrunmax", "ydrunmean"):
            window = parts[1]
            cmd = f"cdo -s {op},{window} {bl_pipe} \"{out}\""
            emit(cmd_files["minmax"], out, cmd)
            counts["minmax"] += 1

        elif op == "ydrunpctl":
            pctl_val, window = parts[1], parts[2]
            cmd = (f"cdo -s ydrunpctl,{pctl_val},{window} {bl_pipe} "
                   f"{dep_str} \"{out}\"")
            emit(cmd_files["pctile"], out, cmd)
            counts["pctile"] += 1

        elif op in ("timmin", "timmax"):
            cmd = f"cdo -s {op} {bl_pipe} \"{out}\""
            emit(cmd_files["minmax"], out, cmd)
            counts["minmax"] += 1

        elif op == "timpctl":
            pctl_val = parts[1]
            masked_pipe = (f"-setrtomiss,,1 {bl_pipe}"
                           if var == "pr" else bl_pipe)
            cmd = (f"cdo -s timpctl,{pctl_val} {masked_pipe} "
                   f"{dep_str} \"{out}\"")
            emit(cmd_files["pctile"], out, cmd)
            counts["pctile"] += 1

        else:
            raise ValueError(f"Unknown prereq operator: {op}")

        prereq_done.add(key)
        return out

    with open(tsv, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            idx          = row["index"].strip()
            op           = row["cdo_operator"].strip()
            units        = row["units"].strip()
            freq         = row["output_frequency"].strip()
            prereq_specs = row["prereq_type"].strip()
            inv          = row["input_vars"].strip()
            bootstrapped = row["bootstrapped"].strip() == "1"

            vars_list   = inv.split("+")
            primary_var = vars_list[0]

            # Check inputs exist
            skip = False
            for v in vars_list:
                if v == "sftlf":
                    if sftlf_file(indir) is None:
                        print(f"    WARNING: sftlf.fx not found; skipping {idx}",
                              file=sys.stderr)
                        skip = True; break
                elif not (indir / f"{v}.day").is_dir():
                    skip = True; break
            if skip:
                continue

            bl_files = baseline_files(indir, primary_var, bstart, bend)
            if prereq_specs != "none" and not bl_files:
                print(f"    WARNING: no {primary_var} files overlap baseline "
                      f"{bstart}-{bend}; skipping {idx}", file=sys.stderr)
                continue

            bl_pipe  = baseline_pipe(indir, primary_var, bstart, bend)
            final_out = outdir / f"{idx}_{middle}_{timespan}.nc"

            # -- Resolve prerequisites ---------------------------------------
            # prereq_specs is either "none" or a "+"-separated list of specs
            # e.g. "ydrunmin,5+ydrunmax,5" or "timpctl,75"
            prereq_files = []
            if prereq_specs != "none":
                for spec in prereq_specs.split("+"):
                    prereq_files.append(ensure(spec, primary_var, bl_pipe))

            # Bootstrap operators need baseline args appended to op name
            # and CDO_PCTL_NBINS set before first use
            op_suffix = ""
            if bootstrapped:
                op_suffix = f",{bstart},{bend}"
                if not nbins_emitted:
                    cmd_files["indices"].write(
                        f"export CDO_PCTL_NBINS={pctl_nbins}\n")
                    nbins_emitted = True

            # -- Secondary input expressions ---------------------------------
            def secondary_inputs(yr=None):
                parts = []
                for v in vars_list[1:]:
                    if v == "sftlf":
                        parts.append(f'"{sftlf_file(indir)}"')
                    elif yr is None:
                        files = day_files(indir, v)
                        parts.append(
                            f"-mergetime {' '.join(f'{chr(34)}{f}{chr(34)}' for f in files)}")
                    else:
                        yf = year_file(indir, v, yr)
                        parts.append(f'"{yf}"' if yf else "MISSING")
                return " ".join(parts)

            prereq_str = " ".join(f'"{f}"' for f in prereq_files)

            # -- Emit index commands -----------------------------------------
            if freq == "annual":
                pipe = full_pipe(indir, primary_var, units)
                if not pipe:
                    continue
                sec = secondary_inputs()
                cmd = (f"cdo -s {op}{op_suffix} {pipe}"
                       f"{' ' + sec if sec else ''}"
                       f"{' ' + prereq_str if prereq_str else ''}"
                       f" \"{final_out}\"")
                emit(cmd_files["indices"], final_out, cmd)
                counts["indices"] += 1

            elif freq == "annual_loop":
                c = conv(units)
                yr_outs = []

                for yr in range(sim_start, sim_end + 1):
                    yr_out = anndir / f"{idx}_{middle}_{yr}.nc"

                    if len(vars_list) == 1:
                        yf = year_file(indir, primary_var, yr)
                        if yf is None:
                            print(f"    WARNING: no file for {primary_var} "
                                  f"year {yr}; skipping", file=sys.stderr)
                            continue
                        yr_pipe = f'{c} "{yf}"' if c else f'"{yf}"'
                        cmd = (f"cdo -s {op} {yr_pipe}"
                               f"{' ' + prereq_str if prereq_str else ''}"
                               f" \"{yr_out}\"")
                    else:
                        sec = secondary_inputs(yr=yr)
                        yf = year_file(indir, primary_var, yr)
                        if yf is None:
                            print(f"    WARNING: no file for {primary_var} "
                                  f"year {yr}; skipping", file=sys.stderr)
                            continue
                        cmd = (f'cdo -s {op} "{yf}"'
                               f"{' ' + sec if sec else ''}"
                               f"{' ' + prereq_str if prereq_str else ''}"
                               f" \"{yr_out}\"")

                    emit(cmd_files["annual"], yr_out, cmd)
                    yr_outs.append(yr_out)
                    counts["annual"] += 1

                if yr_outs:
                    yr_list = " ".join(f'"{y}"' for y in yr_outs)
                    emit(cmd_files["merge"], final_out,
                         f'cdo -s mergetime {yr_list} "{final_out}"')
                    counts["merge"] += 1

            else:
                print(f"    WARNING: unknown output_frequency '{freq}' "
                      f"for {idx}; skipping", file=sys.stderr)

    for name, fh in cmd_files.items():
        fh.close()
        p = cmd_paths[name]
        if p.stat().st_size == 0:
            p.unlink()

    print()
    print("Commandfile generation complete.")
    print(f"  TSV:                     {tsv}")
    print(f"  Commandfiles:            {cmddir}")
    print(f"  Reference files:         {pctldir}")
    print(f"  Per-year temp files:     {anndir}")
    print(f"  Final index files:       {outdir}")
    print(f"  Minmax: {counts['minmax']}  Pctile: {counts['pctile']}  "
          f"Indices: {counts['indices']}  Annual: {counts['annual']}  "
          f"Merge: {counts['merge']}")
    print()
    print("Run in order:")
    for name in ["minmax", "pctile", "indices", "annual", "merge"]:
        p = cmd_paths[name]
        if p.exists():
            print(f"  launch_multi --run RUNDIR/{name:<7} {p}")


if __name__ == "__main__":
    main()
