#!/usr/bin/env python3
# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis
"""
ba_cmorize.py - Reformat annual bias-adjusted files into one CF/CORDEX file.

Bias-adjustment counterpart to postprocess/cmorize.sh.  Called by the
commandfiles from ba_format.py (copied into the run directory with
launch_multi --copy).  Reads consecutive annual ISIMIP3BASD output files
for one variable and writes a single multi-year file that:

  - rebuilds the coordinates: lat/lon with attributes and bounds, time at
    12:00 with time_bnds (00:00 to 00:00), scalar height from var_table
  - replaces the dangling grid_mapping with a latitude_longitude crs
  - sets driving_experiment(_id) from the assigned experiment (fixes
    mislabeled inputs), plus new tracking_id, creation_date, title
  - filters global attributes by the rules in ba_attrs.yml
  - writes uncompressed NETCDF4_CLASSIC; compression is done afterwards by
    postprocess/compress.sh

All checks run before anything is written.  Output goes to a temporary
file and is renamed into place only on success.

Usage:
  python ba_cmorize.py VAR EXPERIMENT OUTFILE SETUPDIR INFILE [INFILE ...]
"""

import datetime
import os
import re
import sys
import uuid

import cftime
import netCDF4
import numpy as np
import yaml

FILL = np.float32(1.0e20)
BLOCK = 32  # time steps per read/write block (~110 MB at 600x1400)

# Global attribute output order; anything kept but not listed follows these.
GLOBAL_ORDER = [
    "Conventions", "activity_id", "contact", "creation_date", "domain",
    "domain_id", "driving_experiment", "driving_experiment_id",
    "driving_institution_id", "driving_source_id", "driving_variant_label",
    "frequency", "grid", "institution", "institution_id", "license",
    "mip_era", "product", "project_id", "references", "source", "source_id",
    "source_type", "title", "tracking_id", "variable_id",
    "version_realization",
]
BC_ORDER = ["bc_method", "bc_method_id", "bc_observation", "bc_observation_id",
            "bc_period"]
VAR_ATTRS = ["standard_name", "long_name", "units", "cell_methods"]
CONFIG_CHECK = ["driving_source_id", "driving_variant_label", "institution_id",
                "source_id", "version_realization"]


def die(msg):
    sys.exit(f"Error: {msg}")


def load_setup(setupdir, var):
    def yml(name):
        with open(os.path.join(setupdir, name)) as f:
            return yaml.safe_load(f)
    cfg, rules, meta = yml("ba_config.yml"), yml("ba_attrs.yml"), yml("ba_meta.yml")

    row = None
    with open(os.path.join(setupdir, "var_table.tsv")) as f:
        header = f.readline().rstrip("\n").split("\t")
        for line in f:
            fields = line.rstrip("\n").split("\t")
            if fields[0] == var:
                row = dict(zip(header, fields))
                break
    if row is None:
        die(f"{var} not found in {setupdir}/var_table.tsv")
    return cfg, rules, meta, row


def check_inputs(dsets, infiles, var, exp, cfg, rules):
    """Validate inputs; return (kept global attrs, var attrs, lat, lon)."""
    keep = set(rules["keep_global"])
    keep_ba = set(rules["keep_ba"])
    drop = set(rules["drop_global"])

    kept, vattrs = None, None
    lat0 = lon0 = None
    for ds, path in zip(dsets, infiles):
        name = os.path.basename(path)
        if var not in ds.variables:
            die(f"{var} not in {name}")
        if ds[var].dimensions != ("time", "lat", "lon"):
            die(f"{name}: {var} dims {ds[var].dimensions}, expected (time, lat, lon)")

        # Global attributes: every one must be classified
        attrs = {a: ds.getncattr(a) for a in ds.ncattrs()}
        unlisted = [a for a in attrs if a not in keep | keep_ba | drop]
        if unlisted:
            die(f"{name}: global attributes not listed in ba_attrs.yml: {unlisted}")
        this = {a: str(v) for a, v in attrs.items() if a in keep | keep_ba}
        if kept is None:
            kept = this
        elif this != kept:
            diff = sorted(a for a in set(this) | set(kept) if this.get(a) != kept.get(a))
            die(f"{name}: kept attributes differ from {os.path.basename(infiles[0])}: {diff}")

        for k in CONFIG_CHECK:
            if attrs.get(k) != cfg[k]:
                die(f"{name}: {k}={attrs.get(k)!r}, config says {cfg[k]!r}")
        if attrs.get("frequency") != "day":
            die(f"{name}: frequency={attrs.get('frequency')!r}, expected 'day'")
        if attrs.get("driving_experiment_id") != exp:
            print(f"WARNING: {name}: driving_experiment_id="
                  f"{attrs.get('driving_experiment_id')!r}, set to {exp!r}")

        this_v = {a: ds[var].getncattr(a) for a in VAR_ATTRS if a in ds[var].ncattrs()}
        if vattrs is None:
            vattrs = this_v
        elif this_v != vattrs:
            die(f"{name}: {var} attributes differ from first input")

        cal = ds["time"].getncattr("calendar")
        if cal != cfg["calendar"]:
            die(f"{name}: calendar {cal!r}, config says {cfg['calendar']!r}")

        lat, lon = ds["lat"][:].astype("f8"), ds["lon"][:].astype("f8")
        if lat0 is None:
            lat0, lon0 = lat, lon
        elif not (np.array_equal(lat, lat0) and np.array_equal(lon, lon0)):
            die(f"{name}: lat/lon differ from first input")

    for c, axis in ((lat0, "lat"), (lon0, "lon")):
        d = np.diff(c)
        if not (np.all(d > 0) and np.allclose(d, d[0], rtol=0, atol=1e-5)):
            die(f"{axis} is not ascending with uniform spacing")
    return kept, vattrs, lat0, lon0


def build_time(dsets, cal, units, outfile):
    """Return day-start values in output units; check one step per day,
    no gaps, and that the span matches the dates in the output filename."""
    starts = []
    for ds in dsets:
        t = ds["time"]
        dates = cftime.num2date(t[:], t.getncattr("units"), cal,
                                only_use_cftime_datetimes=True)
        starts += [cftime.datetime(d.year, d.month, d.day, calendar=cal) for d in dates]
    vals = np.asarray(cftime.date2num(starts, units, cal), dtype="f8")

    if not np.allclose(vals, np.round(vals)):
        die("time units do not give whole days at 00:00")
    if not np.all(np.diff(vals) == 1):
        bad = np.where(np.diff(vals) != 1)[0][0]
        die(f"time not consecutive daily after {starts[bad]}")

    m = re.search(r"_day_(\d{4})(\d{2})(\d{2})-(\d{4})(\d{2})(\d{2})\.nc$", outfile)
    if not m:
        die(f"cannot parse dates from output filename {outfile}")
    first = cftime.datetime(*map(int, m.groups()[:3]), calendar=cal)
    last = cftime.datetime(*map(int, m.groups()[3:]), calendar=cal)
    if starts[0] != first or starts[-1] != last:
        die(f"time spans {starts[0]} to {starts[-1]}, filename says {first} to {last}")
    return vals


def bounds(c):
    h = (c[1] - c[0]) / 2
    return np.stack([c - h, c + h], axis=1)


def main():
    if len(sys.argv) < 6:
        sys.exit(__doc__)
    var, exp, outfile, setupdir = sys.argv[1:5]
    infiles = sys.argv[5:]

    cfg, rules, meta, vrow = load_setup(setupdir, var)
    cal = cfg["calendar"]
    units = rules["time_units"]

    dsets = [netCDF4.Dataset(p) for p in infiles]
    for ds in dsets:
        ds.set_auto_maskandscale(False)

    kept, vattrs, lat, lon = check_inputs(dsets, infiles, var, exp, cfg, rules)
    tvals = build_time(dsets, cal, units, outfile)

    # --- Global attributes ----------------------------------------------------
    if rules.get("drop_empty_ba", True):
        kept = {a: v for a, v in kept.items() if not (a.startswith("ba_") and v == "")}
    fields = {"driving_source_id": cfg["driving_source_id"],
              "driving_experiment_id": exp, "domain_id": cfg["domain_id"],
              "source_id": cfg["source_id"],
              "bc_method_id": kept.get("bc_method_id", ""),
              "bc_observation_id": kept.get("bc_observation_id", ""),
              "bc_period": kept.get("bc_period", "")}
    gattrs = dict(kept)
    gattrs.update(rules["set_global"])
    gattrs.update({
        "creation_date": meta["creation_date"],
        "domain_id": cfg["domain_id"],
        "driving_experiment": meta["driving_experiment"][exp],
        "driving_experiment_id": exp,
        "title": rules["title"].format(**fields),
        "tracking_id": f"hdl:21.14103/{uuid.uuid4()}",
        "variable_id": var,
        rules["id_attribute"]: (f"{cfg['version_realization']}-"
                                f"{cfg['institution_id']}-{cfg['ba_id']}"),
        "bias_adjustment": rules["bias_adjustment"].format(**fields),
    })
    now = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    gattrs["history"] = (f"{now}: reformatted from {len(infiles)} annual "
                         f"ISIMIP3BASD output files by ba_cmorize.py")

    ba_order = [a for a in rules["keep_ba"] if a in gattrs]
    head = [a for a in GLOBAL_ORDER if a in gattrs]
    tail = BC_ORDER + [rules["id_attribute"], "bias_adjustment"] + ba_order + ["history"]
    other = [a for a in gattrs if a not in head and a not in tail]
    order = head + other + [a for a in tail if a in gattrs]

    # --- Write ------------------------------------------------------------------
    refh = vrow.get("refh", "--")
    positive = vrow.get("positive", "--")
    tmpfile = outfile + ".tmp"
    os.makedirs(os.path.dirname(os.path.abspath(outfile)), exist_ok=True)
    out = netCDF4.Dataset(tmpfile, "w", format="NETCDF4_CLASSIC")
    out.set_auto_maskandscale(False)

    out.createDimension("time", None)
    out.createDimension("bnds", 2)
    out.createDimension("lat", len(lat))
    out.createDimension("lon", len(lon))

    v = out.createVariable("time", "f8", ("time",))
    v.setncatts({"standard_name": "time", "long_name": "time", "units": units,
                 "calendar": cal, "axis": "T", "bounds": "time_bnds"})
    v[:] = tvals + 0.5
    out.createVariable("time_bnds", "f8", ("time", "bnds"))[:] = \
        np.stack([tvals, tvals + 1], axis=1)

    for name, vals, sn, un, ax in (("lat", lat, "latitude", "degrees_north", "Y"),
                                   ("lon", lon, "longitude", "degrees_east", "X")):
        v = out.createVariable(name, "f8", (name,))
        v.setncatts({"standard_name": sn, "long_name": sn, "units": un,
                     "axis": ax, "bounds": f"{name}_bnds"})
        v[:] = vals
        out.createVariable(f"{name}_bnds", "f8", (name, "bnds"))[:] = bounds(vals)

    coords = None
    if refh not in ("", "--"):
        v = out.createVariable("height", "f8", ())
        v.setncatts({"standard_name": "height", "long_name": "height",
                     "units": "m", "positive": "up", "axis": "Z"})
        v.assignValue(float(refh))
        coords = "height"

    v = out.createVariable("crs", "i4", ())
    v.setncatts({"grid_mapping_name": "latitude_longitude"})

    dv = out.createVariable(var, "f4", ("time", "lat", "lon"), fill_value=FILL,
                            chunksizes=(1, len(lat), len(lon)))
    vat = {a: vattrs[a] for a in VAR_ATTRS if a in vattrs}
    if positive not in ("", "--"):
        vat["positive"] = positive
    if coords:
        vat["coordinates"] = coords
    vat["grid_mapping"] = "crs"
    vat["missing_value"] = FILL
    dv.setncatts(vat)

    i0, nnan = 0, 0
    for ds in dsets:
        src = ds[var]
        fv = src.getncattr("_FillValue") if "_FillValue" in src.ncattrs() else None
        for j in range(0, src.shape[0], BLOCK):
            block = np.asarray(src[j:j + BLOCK], dtype="f4")
            nan = np.isnan(block)
            nnan += int(nan.sum())
            block[nan] = FILL
            if fv is not None and np.float32(fv) != FILL:
                block[block == np.float32(fv)] = FILL
            dv[i0 + j:i0 + j + block.shape[0]] = block
        i0 += src.shape[0]

    for a in order:
        out.setncattr(a, gattrs[a])
    out.close()
    for ds in dsets:
        ds.close()

    os.replace(tmpfile, outfile)
    if nnan:
        print(f"NOTE: {nnan} NaN values set to fill")
    print("Done")


if __name__ == "__main__":
    main()
