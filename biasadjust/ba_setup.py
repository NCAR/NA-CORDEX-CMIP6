#!/usr/bin/env python3
# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis
"""
ba_setup.py - One-time setup for reformatting bias-adjusted output.

Bias-adjustment counterpart to postprocess/setup.py.  Run once per product
(one product = one ba_config/<id>.yml).  Everything downstream reads from
SETUPDIR, so the whole product uses one consistent set of inputs:

  ba_config.yml     copy of the product config (provenance; read by ba_format.py)
  ba_attrs.yml      copy of the attribute rules (read by ba_cmorize.py)
  ba_meta.yml       creation_date and driving_experiment long names
  ba.env            shell key=value pairs for relocate_ba.sh
  var_table.tsv     raw var_table.tsv plus a <var>Adjust row for every
                    variable, so postprocess/compress.sh finds quant for
                    the Adjust names
  ncrepack-cordex, ncrepack-cordex-check
                    copied from the raw setup dir (used by repack.sh, qa.sh)

ba_config.yml and ba_attrs.yml are refreshed on every run, so re-run
ba_setup.py after editing either.  creation_date is generated on the first
run and preserved on re-runs (it sets the DRS version), unless --new-date
is given.

Usage:
  python ba_setup.py SETUPDIR CONFIG [--post DIR] [--force] [--new-date]
"""

import argparse
import csv
import datetime
import os
import shutil
import stat
import sys

import yaml

REQUIRED_KEYS = [
    "id", "source_root", "raw_setupdir", "ba_id", "domain_id",
    "driving_source_id", "driving_variant_label", "institution_id",
    "source_id", "version_realization", "calendar", "drs_project",
    "drs_activity", "chunk_years", "sources",
]

REPACK_SCRIPTS = ["ncrepack-cordex", "ncrepack-cordex-check"]


def parse_args():
    here = os.path.dirname(os.path.realpath(__file__))
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("setupdir", metavar="SETUPDIR")
    p.add_argument("config", metavar="CONFIG", help="ba_config/<id>.yml")
    p.add_argument("--post", default=os.path.join(here, "..", "postprocess"),
                   help="postprocess directory (default: ../postprocess)")
    p.add_argument("--force", action="store_true",
                   help="Overwrite existing setup files (keeps creation_date)")
    p.add_argument("--new-date", action="store_true",
                   help="Generate a new creation_date (changes the DRS version)")
    return p.parse_args()


def copy(src, dest, force):
    if os.path.exists(dest) and not force:
        print(f"  exists, skipping: {os.path.basename(dest)}")
        return
    shutil.copy(src, dest)
    print(f"  {os.path.basename(dest)}")


def main():
    args = parse_args()
    here = os.path.dirname(os.path.realpath(__file__))
    setupdir = os.path.realpath(args.setupdir)
    post = os.path.realpath(args.post)
    os.makedirs(setupdir, exist_ok=True)

    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    missing = [k for k in REQUIRED_KEYS if k not in cfg]
    if missing:
        sys.exit(f"Error: config missing keys: {', '.join(missing)}")

    raw_sdir = cfg["raw_setupdir"]
    raw_vt = os.path.join(raw_sdir, "var_table.tsv")
    if not os.path.isfile(raw_vt):
        sys.exit(f"Error: var_table.tsv not found in raw_setupdir: {raw_sdir}")

    print(f"=== ba_setup: {cfg['id']} -> {setupdir} ===")

    # Config and attribute rules: always refreshed, so edits take effect
    # when ba_setup.py is re-run
    copy(args.config, os.path.join(setupdir, "ba_config.yml"), True)
    copy(os.path.join(here, "ba_attrs.yml"),
         os.path.join(setupdir, "ba_attrs.yml"), True)

    # var_table.tsv with Adjust rows appended
    vt_out = os.path.join(setupdir, "var_table.tsv")
    if os.path.exists(vt_out) and not args.force:
        print("  exists, skipping: var_table.tsv")
    else:
        with open(raw_vt) as f:
            lines = [ln.rstrip("\n") for ln in f if ln.strip()]
        header, rows = lines[0], lines[1:]
        adjust = []
        for ln in rows:
            fields = ln.split("\t")
            if not fields[0].endswith("Adjust"):
                adjust.append("\t".join([fields[0] + "Adjust"] + fields[1:]))
        with open(vt_out, "w") as f:
            f.write("\n".join([header] + rows + adjust) + "\n")
        print(f"  var_table.tsv ({len(adjust)} Adjust rows added)")

    # ncrepack-cordex scripts
    for name in REPACK_SCRIPTS:
        src = os.path.join(raw_sdir, name)
        if not os.path.isfile(src):
            sys.exit(f"Error: {name} not found in raw_setupdir: {raw_sdir}")
        dest = os.path.join(setupdir, name)
        copy(src, dest, args.force)
        os.chmod(dest, os.stat(dest).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    # creation_date: keep existing unless --new-date
    meta_path = os.path.join(setupdir, "ba_meta.yml")
    creation_date = None
    if os.path.exists(meta_path) and not args.new_date:
        with open(meta_path) as f:
            creation_date = (yaml.safe_load(f) or {}).get("creation_date")
    if not creation_date:
        creation_date = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ")

    # driving_experiment long names from postprocess/sim_config/exp-info.tsv
    exp_info = os.path.join(post, "sim_config", "exp-info.tsv")
    with open(exp_info, newline="") as f:
        exp_names = {r["driving_experiment_id"]: r["driving_experiment"]
                     for r in csv.DictReader(f, delimiter="\t")}
    used = {e for s in cfg["sources"] for e in s["experiments"]}
    unknown = used - exp_names.keys()
    if unknown:
        sys.exit(f"Error: experiments not in {exp_info}: {sorted(unknown)}")

    with open(meta_path, "w") as f:
        yaml.safe_dump({"creation_date": creation_date,
                        "driving_experiment": {e: exp_names[e] for e in sorted(used)}},
                       f, sort_keys=False)
    print(f"  ba_meta.yml (creation_date={creation_date})")

    # ba.env for relocate_ba.sh
    with open(os.path.join(setupdir, "ba.env"), "w") as f:
        f.write("# ba.env - Auto-generated by ba_setup.py.  Do not edit.\n")
        f.write(f"creation_date={creation_date}\n")
        f.write(f"drs_project={cfg['drs_project']}\n")
        f.write(f"drs_activity={cfg['drs_activity']}\n")
        f.write(f"ba_id={cfg['ba_id']}\n")
    print("  ba.env")

    print("=== ba_setup complete ===")


if __name__ == "__main__":
    main()
