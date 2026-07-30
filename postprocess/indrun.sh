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

## Parse an index.py output filename into its fields:
##   {idx}_{middle}_{tstart}-{tend}[_{seas}].nc
## e.g. NAM-12_MPI-ESM1-2-HR_ssp245_r1i1p1f1_NCAR_WRF461S-SN_v1-r1 for middle,
## and an optional trailing DJF/MAM/JJA/SON season tag.
## On success prints "idx<TAB>middle<TAB>tstart<TAB>tend<TAB>seas" (seas may
## be empty) and returns 0; returns 1 on filenames that don't match.
parse_fname() {
    local fname=$1
    if [[ $fname =~ ^([^_]+)_(.+)_([0-9]{4})-([0-9]{4})(_([A-Z]{3}))?\.nc$ ]]; then
        printf '%s\t%s\t%s\t%s\t%s\n' \
            "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}" \
            "${BASH_REMATCH[4]}" "${BASH_REMATCH[6]}"
        return 0
    fi
    return 1
}

for scenfile in "$scenidxdir"/*.nc; do
    fname=${scenfile##*/}

    if ! fields=$(parse_fname "$fname"); then
        echo "  WARNING: unrecognized filename pattern: $fname; skipping" >&2
        continue
    fi
    IFS=$'\t' read -r idx middle tstart tend seas <<<"$fields"

    ## Find the matching historical file: same index, same season (both
    ## empty for annual). Excludes other seasons/indexes that plain
    ## "${idx}_*.nc" globbing would otherwise also match.
    histfile=""
    for cand in "$histidxdir"/"${idx}"_*.nc; do
        [[ -e $cand ]] || continue
        if hfields=$(parse_fname "${cand##*/}"); then
            IFS=$'\t' read -r hidx hmiddle htstart htend hseas <<<"$hfields"
            if [[ "$hidx" == "$idx" && "$hseas" == "$seas" ]]; then
                histfile=$cand
                break
            fi
        fi
    done

    if [[ -z "$histfile" ]]; then
        echo "  WARNING: no historical file for $idx${seas:+ ($seas)}; skipping" >&2
        continue
    fi

    echo "$idx${seas:+ ($seas)}"

    catfile=$catdir/${idx}${seas:+_$seas}.nc
    ncrcat -O "$histfile" "$scenfile" "$catfile"

    outbase=${idx}_${middle}
    tag="${htstart}-${tend}${seas:+_$seas}"
    cdo runmean,21 "$catfile" "$outdir/${outbase}_21yr_${tag}.nc"
    cdo runmean,31 "$catfile" "$outdir/${outbase}_31yr_${tag}.nc"
done
