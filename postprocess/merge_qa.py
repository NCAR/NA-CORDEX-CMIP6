#!/usr/bin/env python3
"""
merge_qa.py - Merge multiple esgf-qa cluster.json files into one.

Usage:
  python merge_qa.py OUTFILE FILE [FILE ...]
  python merge_qa.py OUTFILE --find SEARCHDIR

  OUTFILE     Output JSON file
  FILE        One or more cluster.json files to merge
  --find DIR  Find and merge all *.cluster.json files under DIR

The result has the same structure as a single cluster.json:
  error/fail/warn: deep-merged, with file lists concatenated at the leaves
  info:            files/datasets summed, id wildcarded to common prefix,
                   parent_dir set to common ancestor, inter_ds_con_checks_ref
                   deep-merged, other fields taken from the first input file
"""

import argparse
import glob
import json
import os
import sys


def parse_args():
    p = argparse.ArgumentParser(description="Merge esgf-qa cluster.json files.")
    p.add_argument("outfile", metavar="OUTFILE", help="Output JSON file")
    p.add_argument("files", metavar="FILE", nargs="*", help="Input cluster.json files")
    p.add_argument("--find", metavar="DIR",
                   help="Find all *.cluster.json files under DIR")
    return p.parse_args()


def deep_merge(base, addition):
    """Recursively merge addition into base.

    At non-dict leaves (lists), concatenate.
    At dict nodes, recurse.
    """
    for key, val in addition.items():
        if key not in base:
            base[key] = val
        elif isinstance(base[key], dict) and isinstance(val, dict):
            deep_merge(base[key], val)
        elif isinstance(base[key], list) and isinstance(val, list):
            base[key] = base[key] + val
        else:
            # Scalar leaf: keep base value (first file wins)
            pass
    return base


def common_prefix(strings):
    """Return the longest common dot-separated prefix of a list of strings."""
    if not strings:
        return ""
    parts = [s.split(".") for s in strings]
    common = []
    for segment_group in zip(*parts):
        if len(set(segment_group)) == 1:
            common.append(segment_group[0])
        else:
            break
    return ".".join(common) + (".*" if common else "*")


def common_ancestor(paths):
    """Return the longest common filesystem path ancestor."""
    if not paths:
        return ""
    return os.path.commonpath(paths)


def merge_info(infos):
    """Merge a list of info dicts into one."""
    if not infos:
        return {}

    merged = dict(infos[0])  # copy first as base for scalar fields

    # Sum numeric fields
    merged["files"]    = str(sum(int(i.get("files",    0)) for i in infos))
    merged["datasets"] = str(sum(int(i.get("datasets", 0)) for i in infos))

    # Wildcard id to common dot-separated prefix
    ids = [i["id"] for i in infos if "id" in i]
    merged["id"] = common_prefix(ids)

    # Common ancestor of parent_dirs
    pdirs = [i["parent_dir"] for i in infos if "parent_dir" in i]
    merged["parent_dir"] = common_ancestor(pdirs) if pdirs else ""

    # Deep-merge inter_ds_con_checks_ref
    refs = {}
    for info in infos:
        deep_merge(refs, info.get("inter_ds_con_checks_ref", {}))
    merged["inter_ds_con_checks_ref"] = refs

    return merged


def merge_files(paths):
    """Load and merge a list of cluster.json files."""
    result = {}
    infos  = []

    for path in paths:
        with open(path) as f:
            data = json.load(f)

        for key, val in data.items():
            if key == "info":
                infos.append(val)
            elif isinstance(val, dict):
                if key not in result:
                    result[key] = {}
                deep_merge(result[key], val)
            # ignore unexpected non-dict top-level keys

    result["info"] = merge_info(infos)
    return result


def main():
    args = parse_args()

    paths = list(args.files)
    if args.find:
        found = sorted(glob.glob(
            os.path.join(args.find, "**", "*.cluster.json"), recursive=True))
        if not found:
            sys.exit(f"Error: no *.cluster.json files found under {args.find}")
        paths.extend(found)

    if not paths:
        sys.exit("Error: no input files specified")

    missing = [p for p in paths if not os.path.exists(p)]
    if missing:
        sys.exit("Error: files not found:\n  " + "\n  ".join(missing))

    print(f"Merging {len(paths)} file(s)...")
    merged = merge_files(paths)

    with open(args.outfile, "w") as f:
        json.dump(merged, f, indent=4)

    print(f"Written to: {args.outfile}")

    # Summary
    info = merged.get("info", {})
    print(f"  datasets: {info.get('datasets', '?')}")
    print(f"  files:    {info.get('files', '?')}")
    for severity in ("error", "fail", "warn"):
        if severity in merged:
            print(f"  {severity} categories: {len(merged[severity])}")


if __name__ == "__main__":
    main()
