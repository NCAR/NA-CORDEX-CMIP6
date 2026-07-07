#!/bin/bash
# indrun.sh - 21-year and 31-year running means of climate indexes.
#
# Not part of the regular per-simulation workflow: it needs a scenario
# run's climate indexes prepended with the matching historical run's
# indexes before computing the running mean, so it lives outside
# $topdir/index and is run by hand.
#
# Usage:
#   ./indrun.sh <scratch> <id>   # <id> must be a scenario run, e.g. mpi-245

set -euo pipefail

if [[ $# -ne 2 ]]; then
    echo "Usage: $0 <scratch> <id>"
    exit 1
fi

scratch=$1
id=$2

## Historical run is named by convention: <model>-hist
histid=${id%-*}-hist

if [[ "$histid" == "$id" ]]; then
    echo "Error: $id has no '-<scenario>' suffix; can't derive historical id"
    exit 1
fi

scenidxdir=$scratch/$id/index/data
histidxdir=$scratch/$histid/index/data

for d in "$scenidxdir" "$histidxdir"; do
    if [[ ! -d $d ]]; then
        echo "Error: index directory not found: $d"
        exit 1
    fi
done

outdir=$scratch/index/$id
catdir=$outdir/cat
mkdir -p "$catdir"

for scenfile in "$scenidxdir"/*.nc; do
    fname=${scenfile##*/}
    idx=${fname%%_*}

    ## middle: DRS fields between the index name and the timespan,
    ## e.g. NAM-12_MPI-ESM1-2-HR_ssp245_r1i1p1f1_NCAR_WRF461S-SN_v1-r1
    rest=${fname#${idx}_}
    middle=${rest%_*.nc}

    histfile=$(find "$histidxdir" -maxdepth 1 -name "${idx}_*.nc")
    if [[ -z "$histfile" ]]; then
        echo "  WARNING: no historical file for $idx; skipping" >&2
        continue
    fi

    echo "$idx"

    catfile=$catdir/$idx.nc
    ncrcat -O "$histfile" "$scenfile" "$catfile"
    cdo runmean,21 "$catfile" "$outdir/${idx}_${middle}_21yr.nc"
    cdo runmean,31 "$catfile" "$outdir/${idx}_${middle}_31yr.nc"
done
