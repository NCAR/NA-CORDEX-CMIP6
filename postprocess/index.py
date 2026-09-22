#!/usr/bin/env python3
# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen
"""index.py - Generate commandfiles for computing climate indexes

Example: python index.py --preset gis $indir $outdir $sdir $cmddir

Assumes that filenames follow the format <var>_<middle>_<timespan>.nc,
where <middle> uniquely identifies all the files from a single
simulation and that timespan lexically sorts into the correct ordering
for ncrcat.

Two input layouts are recognized automatically (see find_inputs):
  flat:   INDIR/<var>_<middle>_<timespan>.nc  (e.g., yearly files)
  nested: INDIR/<var>.day/<var>_<middle>_<timespan>.nc  (e.g., 5-year
          files, as written by compress.sh)
If INDIR contains any *.day subdirectories, the nested layout is used
and only files in those subdirectories are read.

Index definitions (formulas, thresholds, units, and output metadata)
are read from gis_indexes.tsv in SETUPDIR.  (Note: modifying the TSV
to add new indexes is most easily done in a spreadsheet.)  Use
--indexes or --preset to generate a subset of indexes.

Generates six commandfiles that must be run in this order:

  units.cmd    - Changes units (temp -> degC,degF; pr -> in/mm day-1)
                 to those needed by various indexes.  Variables in
                 original units are passed through with symlinks.

  concat.cmd   - ncrcat's each variable's per-simulation units.cmd
                 outputs (or passthrough symlinks) into a single file
                 per variable+simulation.

  split.cmd    - `cdo splitseas` / `cdo splitmon` on each units.cmd
                 output, producing DJF/MAM/JJA/SON and 01-12 files
                 alongside annual versions of the indexes

  indexes.cmd  - Runs each index's `formula` against the annual,
                 seasonal, and monthly versions of its input variable(s).
                 Writes final index files directly to OUTDIR.

  derived.cmd  - Indexes computed from other indexes rather than
                 from a base variable, e.g. ETR = TXx - TNn,
                 SDII = PTOT / R1mm. Also writes directly to OUTDIR.

  cleanup.cmd  - Applies corrected CF metadata to each indexes.cmd/
                 derived.cmd output file IN PLACE via clean_index.sh.

Note: to stay under the scheduler's limit on total tasks, some steps have
tasks bundled together into sub-commandfiles that are run serially.
Commands are bundled by (index, tag) into <step>/<index>.<tag>.cmd, and
<step>.cmd just contains one `csh <subfile>` line per bundle.

Directory layout under OUTDIR:
  tmp/units/<var>/   Per-input-file, unit-converted (or symlinked)
                      files, keyed by OUTPUT var name.
  tmp/concat/<var>/  Per-simulation concatenated files, keyed by var.
  tmp/split/<var>/{seas,mon}/
                      Per-simulation seasonal/monthly split files.
  tmp/               (all of the above) -- can be removed once
                      cleanup.cmd is done.
  <idx>/<freq>/      Outputs grouped by index & annual/seasonal/monthly

Directory layout under CMDDIR:
  units.cmd, indexes.cmd, cleanup.cmd
                              - dispatcher files: one `csh <subfile>`
                              line per bundle (units: (outvar, middle);
                              indexes/cleanup: (index, tag)).
  units/, indexes/, cleanup/ - the bundled subfiles themselves.

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

# Model outputs have precip as flux (kg/m^2/s) but we want it as lwe
# (mm/day); udunits won't do that conversion, so we do it manually by
# changing the units to mm/s (1 kg H2O / m^2 == 1mm LWE).
RELABEL_UNITS = {
    "pr": "mm s-1",
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
    "gis": {"CDD", "CWD", "PTOT", "R10mm", "R1mm", "R20mm",
            "Rx1day", "Rx5day", "Rx5dayN", "SDII",
            "HMDX", "TAVG", "CD65F", "HD65F", "FD", "ID",
            "TMAX", "TX90F", "TX95F", "TX100F", "TX105F",
            "TMIN", "FD", "TN65F", "TN70F", "TN75F", "TN80F",
            "WBGT", "WBGT82F", "WBGT85F", "WBGT88F", "WBGT90F"},
    "denver": { "DP01", "DP100", "DP200", "DP300", "TPCP",
                "CDD", "CDDn", "CWD", "CWDn",
                "TAVG", "CD65F", "HD65F",
                "TMAX", "TX90F", "TX95F", "TX100F",
                "HW90F", "HW95F", "LHW90F", "LHW95F",
                "TMIN", "FD", "HFD", "ID",
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


def find_inputs(indir):
    """Return (layout, sorted list of .nc Paths) for indir.

    layout is 'nested' if indir has *.day subdirectories (files are read
    from those only), else 'flat' (files are read from indir itself).
    """
    if any(d.is_dir() for d in indir.glob("*.day")):
        return "nested", sorted(indir.glob("*.day/*.nc"))
    return "flat", sorted(indir.glob("*.nc"))


def group_simulations(files):
    """Group <var>_<middle>_<timespan>.nc files (as found by find_inputs)
    by middle.

    Returns dict: middle -> {var: [sorted Paths]} (sorted lexically by
    timespan, which is opaque but assumed sortable).
    """
    sims = defaultdict(lambda: defaultdict(list))
    for f in files:
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


def nco_loop_spec(files):
    """If files are a contiguous run of single-year <YYYY>.nc timespans
    (one year per file, step 1), return (first_filename, nfiles, ndigits)
    for use with ncrcat's -n loop syntax.  Otherwise return None.
    """
    tss = [f.stem.split("_")[-1] for f in files]
    if not all(len(t) == 4 and t.isdigit() for t in tss):
        return None
    years = [int(t) for t in tss]
    if years != list(range(years[0], years[0] + len(years))):
        return None
    return files[0].name, len(files), 4


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
    """Write cmd to cmdfile unless --force is unset and outfile already
    exists. Returns True if the command was written."""
    if not FORCE and outfile.exists():
        return False
    cmdfile.write(cmd + "\n")
    return True


def emit_multi(cmdfile, outfiles, cmd):
    """Like emit(), but for one command that produces several output files
    (e.g. splitseas/splitmon). Skips only if --force is unset and ALL
    outfiles already exist."""
    if not FORCE and all(o.exists() for o in outfiles):
        return
    cmdfile.write(cmd + "\n")


class Bundler:
    """Collects commands into per-(index, tag) subfiles instead of writing
    them straight to a single flat commandfile, to stay under the PBS
    scheduler's total-task limit.  Used for indexes.cmd and cleanup.cmd,
    each of which would otherwise have one line per simulation per index
    per tag (thousands of tasks).

    Call add(idx, tag, cmd) once per simulation's command; write_all()
    then writes each bundle to CMDDIR/<name>/<idx>.<tag>.cmd (one command
    per simulation) and writes a `csh <subfile>` dispatcher line for each
    bundle into the top-level CMDDIR/<name>.cmd.
    """

    def __init__(self, name, cmddir):
        self.name = name
        self.subdir = cmddir / name
        self.toplevel = cmddir / f"{name}.cmd"
        self.bundles = defaultdict(list)  # (idx, tag) -> [cmd, ...]

    def add(self, idx, tag, cmd):
        self.bundles[(idx, tag)].append(cmd)

    def write_all(self):
        if not self.bundles:
            return
        self.subdir.mkdir(parents=True, exist_ok=True)
        with open(self.toplevel, "w") as top:
            for (idx, tag), cmds in sorted(self.bundles.items()):
                subfile = self.subdir / f"{idx}.{tag}.cmd"
                subfile.write_text("\n".join(cmds) + "\n")
                top.write(f"csh {subfile}\n")

    def count(self):
        return sum(len(cmds) for cmds in self.bundles.values())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global FORCE
    ap = argparse.ArgumentParser(
        description="Generate commandfiles for computing climate indexes.",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("indir",    type=Path,
                    help="Directory containing <var>_<middle>_<timespan>.nc input "
                         "files, either directly or in <var>.day subdirectories")
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

    layout, infiles = find_inputs(indir)
    sims = group_simulations(infiles)
    if not sims:
        sys.exit(f"Error: no <var>_<middle>_<timespan>.nc files found in "
                 f"{indir} ({layout} layout)")

    print(f"Found {len(sims)} simulation(s) in {indir} ({layout} layout)")
    for middle in sims:
        print(f"  {middle}: {sorted(sims[middle])}")

    # concat/units/split/derived stay flat, one commandfile each.
    # indexes/cleanup get bundled by (index, tag) to stay under PBS's
    # total-task limit -- see Bundler.
    FLAT_CMDFILES = ["concat", "split", "derived"]
    BUNDLED_CMDFILES = ["units", "indexes", "cleanup"]

    cmd_paths = {name: cmddir / f"{name}.cmd" for name in FLAT_CMDFILES}
    cmd_files = {k: open(v, "w") for k, v in cmd_paths.items()}
    bundlers = {name: Bundler(name, cmddir) for name in BUNDLED_CMDFILES}
    cmd_files.update(bundlers)

    # rows actually in play, keyed by index name, honoring selection
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

    active_derived = {k: v for k, v in DERIVED.items()
                       if selected is None or k in selected}

    for middle, varfiles in sims.items():
        process_simulation(middle, varfiles, active_rows, active_derived,
                            outdir, tmpdir, setupdir, cmd_files)

    # Close flat commandfiles; remove any that are empty
    for name, fh in cmd_files.items():
        if name in bundlers:
            continue
        fh.close()
        p = cmd_paths[name]
        if p.stat().st_size == 0:
            p.unlink()

    # Write out bundled commandfiles (subfiles + dispatcher)
    for bundler in bundlers.values():
        bundler.write_all()

    def count_cmds(p):
        return sum(1 for _ in p.read_text().splitlines()) if p.exists() else 0

    counts = {name: count_cmds(cmd_paths[name]) for name in FLAT_CMDFILES}
    counts.update({name: b.count() for name, b in bundlers.items()})

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

    # -- units: per-input-file unit conversion happens before concat
    #    to avoid memory problems.
    #
    # units_dir[outvar] == list of converted per-file Paths, file-index-
    #    aligned with varfiles[basevar], used by concat step

    # If concat output already exists, we don't need to redo units
    def concat_done(outvar):
        out = tmpdir / "concat" / outvar / f"{outvar}_{middle}_{ts}.nc"
        return not FORCE and out.exists()

    # units_dir[outvar] contains the paths used in the concat step below,
    # which needs them to tell whether or not to generate the units &
    # concat commands.
    units_dir = {}
    for basevar, outs in UNIT_VARS.items():
        if basevar not in varfiles:
            continue
        relabel = RELABEL_UNITS.get(basevar)

        for outvar, unit in outs:
            vardir = tmpdir / "units" / outvar
            outfiles = [vardir / f.name for f in varfiles[basevar]]
            units_dir[outvar] = outfiles

            if concat_done(outvar):
                continue
            vardir.mkdir(parents=True, exist_ok=True)
            for f, out in zip(varfiles[basevar], outfiles):
                script_parts = []
                if relabel:
                    script_parts.append(f'{basevar}@units="{relabel}"')
                script_parts.append(f'{outvar}=udunits({basevar},"{unit}")')
                script_parts.append(f'{outvar}@units="{unit}"')
                script = "; ".join(script_parts)
                # Note: output file still has basevar; -v in concat drops it
                if FORCE or not out.exists():
                    cmd_files["units"].add(
                        outvar, middle, f"ncap2 -O -s '{script}' {f} {out}")

    # Symlink files that don't need units conversion
    for var in PASSTHROUGH_VARS:
        if var not in varfiles:
            continue
        vardir = tmpdir / "units" / var
        links = [vardir / f.name for f in varfiles[var]]
        units_dir[var] = links

        if concat_done(var):
            continue
        vardir.mkdir(parents=True, exist_ok=True)
        for f, link in zip(varfiles[var], links):
            if not link.exists():
                link.symlink_to(f.resolve())

    # -- concat: one file per output variable actually present -----------
    # -v drops the original input variable still hanging around after units
    unit_files = {}
    for outvar, files in units_dir.items():
        cdir = tmpdir / "concat" / outvar
        cdir.mkdir(parents=True, exist_ok=True)
        out = cdir / f"{outvar}_{middle}_{ts}.nc"
        indir = files[0].parent
        concat = f"ncrcat -O -v {outvar} -p {indir} -o {out}"
        loop = nco_loop_spec(files)
        if loop:
            first, n, ndigits = loop
            emit(cmd_files["concat"], out,
                 f"{concat} -n {n},{ndigits},1 {first}")
        else:
            filenames = " ".join(f.name for f in files)
            emit(cmd_files["concat"], out, f"{concat} {filenames}")
        unit_files[outvar] = out


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
    needs_cleanup = set()  # (idx, tag) pairs whose file was (re)computed
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
            outfile = _outfile(outdir, idx, tag, middle, ts)
            if FORCE or not outfile.exists():
                cmd = f"cdo {formula} {infile} {outfile}"
                cmd_files["indexes"].add(idx, tag, cmd)
                needs_cleanup.add((idx, tag))
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
            outfile = _outfile(outdir, idx, tag, middle, ts)
            fa = raw_index_files[in_a][tag]
            fb = raw_index_files[in_b][tag]
            cmd = f"cdo {op} {fa} {fb} {outfile}"
            if emit(cmd_files["derived"], outfile, cmd):
                needs_cleanup.add((idx, tag))
            raw_index_files[idx][tag] = outfile

    if skipped_derived:
        print(f"    WARNING: {middle}: missing inputs; "
              f"skipping derived {', '.join(sorted(skipped_derived))}",
              file=sys.stderr)

    # -- cleanup: apply CF metadata, in place, one call per (index, tag) ---
    # Cleanup mutates its file in place, so its own output can't be used to
    # detect "already done" -- instead, piggyback on the indexes/derived
    # existence check: only clean up files that were just (re)computed.
    for idx, by_tag in raw_index_files.items():
        for tag, f in by_tag.items():
            if (idx, tag) not in needs_cleanup:
                continue
            cmd = f"./clean_index.sh {idx} {f} {setupdir}"
            cmd_files["cleanup"].add(idx, tag, cmd)


def _freq(tag):
    """Group splits (annual, seasonal, monthly) by tag."""
    if tag == "ann":
        return "ann"
    return "seas" if tag in SEASONS else "mon"


def _outfile(outdir, idx, tag, middle, ts):
    """Output path for one (index, tag): OUTDIR/<idx>/<freq>/<name>.nc,
    creating the directory if needed."""
    d = outdir / idx / _freq(tag)
    d.mkdir(parents=True, exist_ok=True)
    return d / f"{idx}_{middle}_{ts}{_tagsuffix(tag)}.nc"


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
