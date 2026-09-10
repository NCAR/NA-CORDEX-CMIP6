#!/usr/bin/env python3
# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen
"""index.py - Generate commandfiles for computing climate indexes

Example: python index.py --preset gis $indir $outdir $sdir $cmddir

Assumes that filenames follow the format <var>_<middle>_<timespan>.nc,
where <middle> uniquely identifies all the files from a single
simulation and that timespan lexically sorts into the correct ordering
for ncrcat.

Index definitions (formulas, thresholds, units, and output metadata)
are read from gis_indexes.tsv in SETUPDIR.  (Note: modifying the TSV
to add new indexes is most easily done in a spreadsheet.)  Use
--indexes or --preset to generate a subset of indexes.

Generates six commandfiles that must be run in this order:

  concat.cmd   - ncrcat's each variable's per-simulation input files into
                 a single file per variable+simulation.

  units.cmd    - Converts base variables into the unit-variants used by
                 some indexes (see UNIT_VARS below for details) via
                 ncap2's udunits function.  Also computes DTR = TX - TN.

  split.cmd    - `cdo splitseas` / `cdo splitmon` on each units.cmd
                 output, producing DJF/MAM/JJA/SON and 01-12 files
                 alongside annual versions of the indexes

  indexes.cmd  - Runs each index's `formula` against the annual,
                 seasonal, and monthly versions of its input variable(s).
                 Writes final index files directly to OUTDIR.

  derived.cmd  - Indexes computed from other indexes rather than
                 from a base variable: ETR = TXx - TNn, SDII = PTOT / R1mm.
                 Also writes directly to OUTDIR.

  cleanup.cmd  - Applies corrected CF metadata to each indexes.cmd/
                 derived.cmd output file IN PLACE via clean_index.sh.
                 (No separate raw/clean copy needed anymore, since
                 extracting a specific variable is no longer required.)

Directory layout under OUTDIR:
  tmp/    All intermediate files (concat, units, split) -- can be
          removed once cleanup.cmd is done.
  (root)  Final index files, flat, one per simulation/index/tag.

See gis_indexes.tsv in SETUPDIR for TSV column documentation.

"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Unit-conversion step: which output variable(s) to derive from which base
# variable, and in what units.  Single source of truth for units.cmd.
# Maps base variable -> list of (outvar, udunits_unit_string) pairs.
# ---------------------------------------------------------------------------
UNIT_VARS = {
    "pr":      [("RR",   "mm/day"), ("precip", "in/day")],
    "tas":     [("TG",   "degC"),   ("Tavg",   "degF")],
    "tasmax":  [("TX",   "degC"),   ("Tmax",   "degF")],
    "tasmin":  [("TN",   "degC"),   ("Tmin",   "degF")],
    "wbgt":    [("WBGT", "degF")],
}

# Variables that pass through unconverted (index formulas use them as-is,
# under their native/CMIP6 name).  Anything referenced in gis_indexes.tsv
# that isn't a UNIT_VARS output and isn't here is assumed to need concat
# only, same as these.
PASSTHROUGH_VARS = {"clt", "hurs", "humidex", "psl", "rsds", "sfcWind"}

# Indexes that require percentile/norm prerequisites (ydrunpctl, timpctl,
# etc.).  Dropped for now; kept as an explicit list so it's easy to bring
# them back once the prereq machinery is reintroduced.
SKIP_PREREQ_INDEXES = set()  # e.g. {"TX90p", "TN10p", ...} when reinstated

# Derived indexes: computed from other indexes' outputs, not from a base
# variable.  Maps output index name -> (operator, [input index names]).
# input_vars in the TSV records the same info for documentation/lookup;
# this dict is what actually drives derived.cmd's command construction.
DERIVED = {
    "ETR":  ("sub", ["TXx", "TNn"]),
    "SDII": ("div", ["PTOT", "R1mm"]),
}

SEASONS = ["DJF", "MAM", "JJA", "SON"]
MONTHS  = [f"{m:02d}" for m in range(1, 13)]

# Named subsets of indexes selectable via --preset.  Add new presets here.
PRESETS = {
    "gis": {"CDD", "PTOT", "R10mm", "R1mm", "R20mm",
            "Rx1day", "Rx5day", "Rx5dayN", "SDII",
            "HMDX", "TG",
            "TX", "CD65F",
            "TX90F", "TX95F", "TX100F", "TX105F",
            "TN", "FD", "ID", "HD65F",
            "TN65F", "TN70F", "TN75F", "TN80F",
            "WBGT", "WBGT82F", "WBGT85F", "WBGT88F", "WBGT90F"},
    "denver": { "TPCP", "DP01", "DP100", "DP200", "DP300", 
                "TAVG", "CD65F", "HD65F",
                "TMAX", "TX100F", "TX90F", "TX95F",
                "HW90F", "HW95F", "LHW90F", "LHW95F",
                "TMIN", "FD", "HFD",
                "HW68F", "HW70F", "TN68F", "TN70F"},
}

CMDFILES = ["concat", "units", "split", "indexes", "derived", "cleanup"]

END_SENTINEL = "~"  # value for the '_end' guard column

# Set after argument parsing; effectively read-only thereafter.
FORCE = False


# ---------------------------------------------------------------------------
# TSV checker
# ---------------------------------------------------------------------------

def check_end_column(path):
    """Validate existence of _end guard column in TSV file.

    TSVs are most easily edited in a spreadsheet; guard column prevents
    silent loss of trailing empty cells when copy-pasting to/from
    spreadsheet.
    """
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        if reader.fieldnames[-1] != "_end":
            sys.exit(f"Error: {path} missing '_end' guard column as last "
                     f"column (found: {reader.fieldnames[-1]!r})")
        for row in reader:
            if row.get("_end") != END_SENTINEL:
                sys.exit(f"Error: {path} line {reader.line_num}: '_end' "
                         f"guard column missing or corrupted (row may be "
                         f"missing trailing columns)")


# ---------------------------------------------------------------------------
# File discovery / simulation grouping
# ---------------------------------------------------------------------------

def parse_fname(path):
    """Split <var>_<middle>_<timespan>.nc into (var, middle, timespan).

    middle may itself contain underscores; timespan is whatever's after
    the last underscore before '.nc' (opaque -- not parsed further).
    Returns None if the filename doesn't have at least var_middle_timespan.
    """
    stem = path.stem
    parts = stem.split("_")
    if len(parts) < 3:
        return None
    var, middle, timespan = parts[0], "_".join(parts[1:-1]), parts[-1]
    return var, middle, timespan


def group_simulations(indir):
    """Group all <var>_<middle>_<timespan>.nc files in indir by middle.

    Returns dict: middle -> {var: [sorted Paths]} (sorted lexically by
    timespan, which is opaque but assumed sortable).
    """
    sims = defaultdict(lambda: defaultdict(list))
    for f in sorted(indir.glob("*.nc")):
        parsed = parse_fname(f)
        if parsed is None:
            print(f"    WARNING: skipping unparseable filename: {f.name}",
                  file=sys.stderr)
            continue
        var, middle, _ts = parsed
        sims[middle][var].append(f)
    for middle in sims:
        for var in sims[middle]:
            sims[middle][var].sort()  # lexical == timespan order
    return sims


def sim_timespan(files):
    """Overall timespan tag for a simulation: first file's start through
    last file's end, both taken as opaque strings split on '-'."""
    first_ts = files[0].stem.split("_")[-1]
    last_ts  = files[-1].stem.split("_")[-1]
    start = first_ts.split("-")[0]
    end   = last_ts.split("-")[-1]
    return start if start == end else f"{start}-{end}"


# ---------------------------------------------------------------------------
# Emit helper
# ---------------------------------------------------------------------------

def emit(cmdfile, outfile, cmd):
    """Write cmd to cmdfile unless --force is unset and outfile already exists."""
    if not FORCE and outfile.exists():
        return
    cmdfile.write(cmd + "\n")


def emit_multi(cmdfile, outfiles, cmd):
    """Like emit(), but for one command that produces several output files
    (e.g. splitseas/splitmon). Skips only if --force is unset and ALL
    outfiles already exist."""
    if not FORCE and all(o.exists() for o in outfiles):
        return
    cmdfile.write(cmd + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global FORCE
    ap = argparse.ArgumentParser(
        description="Generate commandfiles for computing climate indexes.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("indir",    type=Path,
                    help="Directory containing <var>_<middle>_<timespan>.nc input files")
    ap.add_argument("outdir",   type=Path, help="Output directory for index files")
    ap.add_argument("setupdir", type=Path,
                    help="Directory containing gis_indexes.tsv and clean_index.sh")
    ap.add_argument("cmddir", type=Path, nargs="?", default=Path("."),
                    help="Directory for commandfiles (default: .)")
    ap.add_argument("--force", action="store_true",
                    help="Overwrite existing output files")
    sel = ap.add_mutually_exclusive_group()
    sel.add_argument("--indexes", metavar="ind1,ind2,...",
                    help="Only generate these indexes (comma-separated, "
                         "matching the 'index' column). Default: all rows.")
    sel.add_argument("--preset", choices=sorted(PRESETS),
                    help="Only generate a named preset subset of indexes.")
    args = ap.parse_args()

    indir    = args.indir.resolve()
    setupdir = args.setupdir.resolve()
    cmddir   = args.cmddir
    tsv      = setupdir / "gis_indexes.tsv"

    if not indir.is_dir():
        sys.exit(f"Error: INDIR not found: {indir}")
    if not setupdir.is_dir():
        sys.exit(f"Error: SETUPDIR not found: {setupdir}")
    if not tsv.is_file():
        sys.exit(f"Error: gis_indexes.tsv not found in SETUPDIR: {tsv}")
    check_end_column(tsv)

    with open(tsv, newline="") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    for row in rows:
        for k in row:
            if row[k] is not None:
                row[k] = row[k].strip()

    # Resolve which indexes to generate.  selected=None means "all rows".
    if args.preset:
        selected = set(PRESETS[args.preset])
    elif args.indexes:
        selected = {s.strip() for s in args.indexes.split(",") if s.strip()}
    else:
        selected = None

    known = {row["index"] for row in rows}
    if selected is not None:
        unknown = selected - known - set(DERIVED)
        if unknown:
            sys.exit(f"Error: unknown index/indexes not in gis_indexes.tsv: "
                      f"{', '.join(sorted(unknown))}")

    outdir = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)
    outdir = outdir.resolve()
    tmpdir = outdir / "tmp"
    tmpdir.mkdir(exist_ok=True)
    cmddir.mkdir(parents=True, exist_ok=True)
    cmddir = cmddir.resolve()

    FORCE = args.force

    sims = group_simulations(indir)
    if not sims:
        sys.exit(f"Error: no <var>_<middle>_<timespan>.nc files found in {indir}")

    print(f"Found {len(sims)} simulation(s) in {indir}")
    for middle in sims:
        print(f"  {middle}: {sorted(sims[middle])}")

    cmd_paths = {name: cmddir / f"{name}.cmd" for name in CMDFILES}
    cmd_files = {k: open(v, "w") for k, v in cmd_paths.items()}

    # rows actually in play, keyed by index name, honoring selection and
    # dropping percentile/norm-based indexes for now
    active_rows = {
        row["index"]: row for row in rows
        if row["index"] not in SKIP_PREREQ_INDEXES
        and row["index"] not in DERIVED
        and (selected is None or row["index"] in selected)
    }

    # Which base variables does each active formula-driven index need?
    # (DERIVED indexes are handled separately, from other indexes' outputs.)
    needed_vars = {v for row in active_rows.values()
                   for v in row["input_vars"].split("+")}
    # DTR is a units-step pseudo-variable, not itself in UNIT_VARS/PASSTHROUGH;
    # its inputs (TX, TN) are pulled in via the normal mechanism below.
    needed_vars.discard("DTR")

    active_derived = {k: v for k, v in DERIVED.items()
                       if selected is None or k in selected}

    for middle, varfiles in sims.items():
        process_simulation(middle, varfiles, active_rows, active_derived,
                            outdir, tmpdir, setupdir, cmd_files)

    # Close commandfiles; remove any that are empty
    for name, fh in cmd_files.items():
        fh.close()
        p = cmd_paths[name]
        if p.stat().st_size == 0:
            p.unlink()

    def count_cmds(p):
        return sum(1 for _ in p.read_text().splitlines()) if p.exists() else 0

    counts = {name: count_cmds(cmd_paths[name]) for name in CMDFILES}

    print()
    print("Commandfile generation complete.")
    print(f"  TSV:              {tsv}")
    print(f"  Commandfiles:     {cmddir}")
    print(f"  Intermediates:    {tmpdir}")
    print(f"  Final index files:{outdir}")
    print("  " + "  ".join(f"{n.capitalize()}: {counts[n]}" for n in CMDFILES))
    print()


# ---------------------------------------------------------------------------
# Per-simulation processing
# ---------------------------------------------------------------------------

def process_simulation(middle, varfiles, active_rows, active_derived,
                        outdir, tmpdir, setupdir, cmd_files):
    """Emit all commands for one simulation ('middle')."""
    all_files = [f for files in varfiles.values() for f in files]
    ts = sim_timespan(sorted(all_files, key=lambda f: f.stem.split("_")[-1]))

    # -- concat: one file per raw variable actually present --------------
    concat_ok = {}
    for var, files in varfiles.items():
        out = tmpdir / f"{var}_{middle}_{ts}.nc"
        filenames = " ".join(str(f) for f in files)
        emit(cmd_files["concat"], out, f"ncrcat -O -o {out} {filenames}")
        concat_ok[var] = out

    # -- units: derive index-native variables + DTR -----------------------
    # unit_files[outvar] -> path to the annual (unsplit) units-converted file
    unit_files = {}
    for basevar, outs in UNIT_VARS.items():
        if basevar not in concat_ok:
            continue
        for outvar, unit in outs:
            out = tmpdir / f"{outvar}_{middle}_{ts}.nc"
            cmd = (f'ncap2 -O -s \'{outvar}=udunits({basevar},"{unit}")\' '
                   f'{concat_ok[basevar]} {out}')
            emit(cmd_files["units"], out, cmd)
            unit_files[outvar] = out

    for var in PASSTHROUGH_VARS:
        if var in concat_ok:
            unit_files[var] = concat_ok[var]

    # DTR = TX - TN (daily), computed here since it's a units-step
    # pseudo-variable rather than an index in its own right.
    if "TX" in unit_files and "TN" in unit_files:
        out = tmpdir / f"DTR_{middle}_{ts}.nc"
        cmd = f"cdo sub {unit_files['TX']} {unit_files['TN']} {out}"
        emit(cmd_files["units"], out, cmd)
        unit_files["DTR"] = out

    # -- split: splitseas/splitmon on each units.cmd output ---------------
    # split_files[var]["ann"] = unsplit path
    # split_files[var]["seas"][SEASON] = path
    # split_files[var]["mon"][MM] = path
    split_files = {}
    for var, f in unit_files.items():
        entry = {"ann": f, "seas": {}, "mon": {}}

        seas_base = tmpdir / f"{var}_{middle}_{ts}_"
        seas_outs = [Path(f"{seas_base}{s}.nc") for s in SEASONS]
        emit_multi(cmd_files["split"], seas_outs,
                   f"cdo splitseas {f} {seas_base}")
        entry["seas"] = dict(zip(SEASONS, seas_outs))

        mon_base = tmpdir / f"{var}_{middle}_{ts}_"
        mon_outs = [Path(f"{mon_base}{m}.nc") for m in MONTHS]
        emit_multi(cmd_files["split"], mon_outs,
                   f"cdo splitmon {f} {mon_base}")
        entry["mon"] = dict(zip(MONTHS, mon_outs))

        split_files[var] = entry

    # -- indexes: run each formula on annual + seasonal + monthly ---------
    # raw_index_files[idx]["ann"|SEASON|MM] -> Path, for use by derived step
    raw_index_files = defaultdict(dict)
    skipped_by_var = defaultdict(list)  # invar -> [idx, ...], for one warning/var

    for idx, row in active_rows.items():
        invar = row["input_vars"].split("+")[0]  # DTR's only input is itself
        if invar not in split_files:
            skipped_by_var[invar].append(idx)
            continue

        formula = row["formula"]
        if "$threshold" in formula:
            formula = formula.replace("$threshold", row["threshold"])

        for tag, infile in _tags(split_files[invar]):
            outfile = outdir / f"{idx}_{middle}_{ts}{_tagsuffix(tag)}.nc"
            cmd = f"cdo {formula} {infile} {outfile}"
            emit(cmd_files["indexes"], outfile, cmd)
            raw_index_files[idx][tag] = outfile

    for invar, idxs in sorted(skipped_by_var.items()):
        print(f"    WARNING: {middle}: {invar} not available; "
              f"skipping {', '.join(sorted(idxs))}", file=sys.stderr)

    # -- derived: combine other indexes' outputs ---------------------------
    skipped_derived = []
    for idx, (op, inputs) in active_derived.items():
        in_a, in_b = inputs
        if in_a not in raw_index_files or in_b not in raw_index_files:
            skipped_derived.append(idx)
            continue
        tags = set(raw_index_files[in_a]) & set(raw_index_files[in_b])
        for tag in tags:
            outfile = outdir / f"{idx}_{middle}_{ts}{_tagsuffix(tag)}.nc"
            fa = raw_index_files[in_a][tag]
            fb = raw_index_files[in_b][tag]
            cmd = f"cdo {op} {fa} {fb} {outfile}"
            emit(cmd_files["derived"], outfile, cmd)
            raw_index_files[idx][tag] = outfile

    if skipped_derived:
        print(f"    WARNING: {middle}: missing inputs; "
              f"skipping derived {', '.join(sorted(skipped_derived))}",
              file=sys.stderr)

    # -- cleanup: apply CF metadata, in place, one call per (index, tag) ---
    for idx, by_tag in raw_index_files.items():
        for tag, f in by_tag.items():
            cmd = f"./clean_index.sh {idx} {f} {setupdir}"
            # Cleanup mutates f in place, so its own existence can't be
            # used to detect "already done" -- always emit; commandfile
            # runners are expected to be idempotent/re-runnable here.
            cmd_files["cleanup"].write(cmd + "\n")


def _tags(split_entry):
    """Yield (tag, path) for annual + each season + each month of a
    split_files[var] entry. tag is 'ann', a SEASON string, or an MM string."""
    yield "ann", split_entry["ann"]
    for s, f in split_entry["seas"].items():
        yield s, f
    for m, f in split_entry["mon"].items():
        yield m, f


def _tagsuffix(tag):
    """Filename suffix for a tag: '' for annual, '_DJF' / '_01' otherwise."""
    return "" if tag == "ann" else f"_{tag}"


if __name__ == "__main__":
    main()
