#!/usr/bin/env python3
# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis
"""
ba_format.py - Generate commandfiles that reformat bias-adjusted output.

Bias-adjustment counterpart to postprocess/format.sh + the concatenation
half of aggregate.sh.  Reads the product config from SETUPDIR (written by
ba_setup.py), inventories the annual bias-adjusted files, assigns each
(variable, year) to an experiment, groups years into multi-year chunks,
and writes one ba_cmorize.py command per output file.

Mapping rules (all from the config's `sources` list):
  - Each source directory contributes the years listed under each
    experiment.  The experiment token in the input filename is ignored
    (some inputs are mislabeled); mismatches are reported.
  - Years in a source but not listed under an experiment are skipped.
  - Every listed year must exist for every variable in that source.
  - No (experiment, variable, year) may come from two sources.
Any violation stops the script before commandfiles are written.

Chunks end on years divisible by chunk_years (as in aggregate.sh), so
1953-2014 becomes 1953-1955, 1956-1960, ..., 2011-2014.

Outputs:
  CMDDIR/<var>.day.cmd  one ba_cmorize.py command per output file
  CMDDIR/manifest.tsv   every input file found and what happened to it
  OUTDIR/<var>.day/     (created; filled when the commandfiles run)

Usage:
  python ba_format.py [--force] SETUPDIR OUTDIR [CMDDIR]
"""

import argparse
import glob
import os
import re
import sys
from collections import defaultdict

import yaml

FNAME_RE = re.compile(
    r"^(?P<var>[A-Za-z0-9]+)_(?P<mid>.+)_day_(?P<y0>\d{4})0101-(?P<y1>\d{4})1231\.nc$")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("setupdir", metavar="SETUPDIR")
    p.add_argument("outdir", metavar="OUTDIR")
    p.add_argument("cmddir", metavar="CMDDIR", nargs="?", default=".")
    p.add_argument("--force", action="store_true",
                   help="Write commands even for outputs that already exist")
    return p.parse_args()


def chunk_ranges(y0, y1, n):
    """Split y0..y1 into ranges ending on years divisible by n."""
    ends = [y for y in range(y0, y1 + 1) if y % n == 0]
    if not ends:
        return [(y0, y1)]
    ranges = [(y0, ends[0])]
    ranges += [(a + 1, b) for a, b in zip(ends[:-1], ends[1:])]
    if ends[-1] < y1:
        ranges.append((ends[-1] + 1, y1))
    return ranges


def main():
    args = parse_args()
    setupdir = os.path.realpath(args.setupdir)
    outdir = os.path.realpath(args.outdir)
    cmddir = os.path.realpath(args.cmddir)

    with open(os.path.join(setupdir, "ba_config.yml")) as f:
        cfg = yaml.safe_load(f)

    errors = []
    manifest = []   # rows: var, year, source, simf, file_exp, experiment, status, infile, outfile
    selected = {}   # (exp, var, year) -> manifest row index
    mismatches = defaultdict(int)  # (source_path, var) -> count

    # --- Inventory and experiment assignment, per source ---------------------
    for src in cfg["sources"]:
        srcdir = os.path.join(cfg["source_root"], src["path"])
        files = sorted(glob.glob(os.path.join(srcdir, "simf-*", "*.nc")))
        if not files:
            errors.append(f"no files found under {srcdir}/simf-*/")
            continue

        year_to_exp = {}
        for exp, (a, b) in src["experiments"].items():
            for y in range(a, b + 1):
                if y in year_to_exp:
                    errors.append(f"{src['path']}: year {y} listed under both "
                                  f"{year_to_exp[y]} and {exp}")
                year_to_exp[y] = exp

        found = defaultdict(set)  # var -> years found in this source
        seen = {}                 # (var, year) -> file, to catch simf overlaps
        for path in files:
            fname = os.path.basename(path)
            m = FNAME_RE.match(fname)
            if not m or m["y0"] != m["y1"]:
                errors.append(f"unexpected filename (not an annual daily file): {path}")
                continue
            var, year = m["var"], int(m["y0"])
            file_exp = m["mid"].split("_")[2]
            simf = os.path.basename(os.path.dirname(path))

            if (var, year) in seen:
                errors.append(f"{var} {year} appears twice in {src['path']}: "
                              f"{seen[(var, year)]} and {path}")
                continue
            seen[(var, year)] = path
            found[var].add(year)

            exp = year_to_exp.get(year)
            row = {"var": var, "year": year, "source": src["path"], "simf": simf,
                   "file_exp": file_exp, "experiment": exp or "-",
                   "status": "selected" if exp else "skipped-unmapped",
                   "infile": path, "outfile": "-"}
            if exp:
                if file_exp != exp:
                    mismatches[(src["path"], var)] += 1
                key = (exp, var, year)
                if key in selected:
                    prev = manifest[selected[key]]
                    errors.append(f"{exp} {var} {year} comes from two sources: "
                                  f"{prev['source']} and {src['path']}")
                else:
                    selected[key] = len(manifest)
            manifest.append(row)

        # Every listed year must be present for every variable in this source
        listed = set(year_to_exp)
        for var, years in sorted(found.items()):
            missing = sorted(listed - years)
            if missing:
                errors.append(f"{src['path']}: {var} missing listed years "
                              f"{missing[0]}..{missing[-1]} ({len(missing)} years)")

    # --- Gaps between sources within an experiment ----------------------------
    years_by = defaultdict(list)  # (exp, var) -> years
    for exp, var, year in selected:
        years_by[(exp, var)].append(year)
    for (exp, var), years in sorted(years_by.items()):
        years.sort()
        gaps = sorted(set(range(years[0], years[-1] + 1)) - set(years))
        if gaps:
            errors.append(f"{exp} {var}: no source provides years "
                          f"{gaps[0]}..{gaps[-1]} ({len(gaps)} years)")

    # --- Variable consistency across experiments (warning only) --------------
    vars_by_exp = defaultdict(set)
    for exp, var, _ in selected:
        vars_by_exp[exp].add(var)
    all_vars = set().union(*vars_by_exp.values()) if vars_by_exp else set()
    for exp, vs in sorted(vars_by_exp.items()):
        if vs != all_vars:
            print(f"WARNING: {exp} is missing variables {sorted(all_vars - vs)}")

    if errors:
        print("Errors found; no commandfiles written:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        sys.exit(1)

    # --- Chunk and write commandfiles -----------------------------------------
    os.makedirs(cmddir, exist_ok=True)
    cmds = defaultdict(list)  # var -> command lines
    nskip = 0
    for (exp, var), years in sorted(years_by.items()):
        for a, b in chunk_ranges(years[0], years[-1], cfg["chunk_years"]):
            fname = (f"{var}_{cfg['domain_id']}_{cfg['driving_source_id']}_{exp}_"
                     f"{cfg['driving_variant_label']}_{cfg['institution_id']}_"
                     f"{cfg['source_id']}_{cfg['version_realization']}_day_"
                     f"{a}0101-{b}1231.nc")
            outfile = os.path.join(outdir, f"{var}.day", fname)
            rows = [manifest[selected[(exp, var, y)]] for y in range(a, b + 1)]
            for r in rows:
                r["outfile"] = outfile
            if os.path.exists(outfile) and not args.force:
                nskip += 1
                continue
            os.makedirs(os.path.dirname(outfile), exist_ok=True)
            infiles = " ".join(r["infile"] for r in rows)
            cmds[var].append(f"python ba_cmorize.py {var} {exp} {outfile} {setupdir} {infiles}")

    for var, lines in sorted(cmds.items()):
        with open(os.path.join(cmddir, f"{var}.day.cmd"), "w") as f:
            f.write("\n".join(lines) + "\n")

    cols = ["var", "year", "experiment", "status", "file_exp", "source", "simf",
            "infile", "outfile"]
    with open(os.path.join(cmddir, "manifest.tsv"), "w") as f:
        f.write("\t".join(cols) + "\n")
        for r in sorted(manifest, key=lambda r: (r["var"], r["year"], r["source"])):
            f.write("\t".join(str(r[c]) for c in cols) + "\n")

    # --- Summary ---------------------------------------------------------------
    print(f"=== ba_format: {cfg['id']} ===")
    for exp in sorted(vars_by_exp):
        ys = [y for (e, _, y) in selected if e == exp]
        print(f"  {exp}: {len(vars_by_exp[exp])} vars, {min(ys)}-{max(ys)}")
    nunmapped = sum(r["status"] == "skipped-unmapped" for r in manifest)
    print(f"  input files: {len(manifest)} found, {len(selected)} selected, "
          f"{nunmapped} skipped (unmapped years)")
    if mismatches:
        print("  filename experiment label differs from assigned experiment "
              "(fixed in output):")
        for (path, var), n in sorted(mismatches.items()):
            print(f"    {var:14s} {n:3d} files  {path}")
    ncmd = sum(len(v) for v in cmds.values())
    print(f"  commands: {ncmd} written, {nskip} skipped (output exists)")
    print(f"  commandfiles and manifest.tsv in: {cmddir}")


if __name__ == "__main__":
    main()
