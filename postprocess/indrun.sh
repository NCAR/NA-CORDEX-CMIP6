#!/bin/bash
# indrun.sh - 21-year and 31-year running means of climate indexes.
#
# Not part of the regular per-simulation workflow: it needs a scenario
# run's climate indexes prepended with the matching historical run's
# indexes before computing the running mean, so it lives outside
# $topdir/index and is run by hand.
#
# Generates three commandfiles: concat.cmd, run21.cmd, run31.cmd.
# concat.cmd must complete before the other two, e.g.:
#   launch_multi --chain cmddir/concat.cmd cmddir/run21.cmd cmddir/run31.cmd
#
# Usage:
#   ./indrun.sh <scratch> <id> <cmddir> [histidxdir]
#   # <id> must be a scenario run, e.g. mpi-245
#   # histidxdir optionally overrides the historical index/data dir,
#   # e.g. if the historical run has already been moved to campaign

set -euo pipefail

if [[ $# -ne 3 && $# -ne 4 ]]; then
    echo "Usage: $0 <scratch> <id> <cmddir> [histidxdir]"
    exit 1
fi

scratch=$1
id=$2
cmddir=$3

scenidxdir=$scratch/$id/index/data

if [[ $# -eq 4 ]]; then
    histidxdir=$4
else
    ## Historical run is named by convention: <model>-hist
    histid=${id%-*}-hist

    if [[ "$histid" == "$id" ]]; then
        echo "Error: $id has no '-<scenario>' suffix; can't derive historical id"
        exit 1
    fi

    histidxdir=$scratch/$histid/index/data
fi

for d in "$scenidxdir" "$histidxdir"; do
    if [[ ! -d $d ]]; then
        echo "Error: index directory not found: $d"
        exit 1
    fi
done

outdir=$scratch/index/$id
catdir=$outdir/cat
mkdir -p "$catdir" "$cmddir"

concatcmd=$cmddir/concat.cmd
run21cmd=$cmddir/run21.cmd
run31cmd=$cmddir/run31.cmd
: > "$concatcmd"
: > "$run21cmd"
: > "$run31cmd"

## Write cmd to cmdfile unless --force is unset and outfile already
## exists. Returns 0 (written) or 1 (skipped), matching index.py's emit().
FORCE=${FORCE:-0}
emit() {
    local cmdfile=$1 outfile=$2 cmd=$3
    if [[ $FORCE -eq 0 && -e $outfile ]]; then
        return 1
    fi
    echo "$cmd" >> "$cmdfile"
    return 0
}

## Parse an index.py output filename into its fields:
##   {idx}_{middle}_{tstart}-{tend}[_{period}].nc
## where period is a 3-letter season or 2-digit month.
## On sucesss, prints parsed fields separated by tabs
## and returns 0; returns 1 on filenames that don't match.
parse_fname() {
    local fname=$1
    if [[ $fname =~ ^([^_]+)_(.+)_([0-9]{8})-([0-9]{8})(_([A-Z]{3}|[0-9]{2}))?\.nc$ ]]; then
        printf '%s\t%s\t%s\t%s\t%s\n' \
            "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}" \
            "${BASH_REMATCH[4]}" "${BASH_REMATCH[6]}"
        return 0
    fi
    return 1
}

nconcat=0 nrun21=0 nrun31=0

for scenfile in "$scenidxdir"/*/{ann,seas,mon}/*.nc; do
    [[ -e $scenfile ]] || continue
    fname=${scenfile##*/}

    if ! fields=$(parse_fname "$fname"); then
        echo "  WARNING: unrecognized filename pattern: $fname; skipping" >&2
        continue
    fi
    IFS=$'\t' read -r idx middle tstart tend seas <<<"$fields"

    ## freq dir (ann/seas/mon) is the scenario file's own parent dir; the
    ## historical file lives under the same <idx>/<freq>/ dir, but
    ## middle/timespan differ (e.g. "historical" vs "ssp245"), so it
    ## still needs to be found by season/month match within that dir.
    freq=${scenfile%/*}
    freq=${freq##*/}
    histfile=""
    for cand in "$histidxdir/$idx/$freq"/*.nc; do
        [[ -e $cand ]] || continue
        if hfields=$(parse_fname "${cand##*/}"); then
            IFS=$'\t' read -r hidx hmiddle htstart htend hseas <<<"$hfields"
            if [[ "$hseas" == "$seas" ]]; then
                histfile=$cand
                break
            fi
        fi
    done

    if [[ -z "$histfile" ]]; then
        echo "  WARNING: no historical file for $idx${seas:+ ($seas)}; skipping" >&2
        continue
    fi

    catsubdir=$catdir/$idx/$freq
    mkdir -p "$catsubdir"
    catfile=$catsubdir/${idx}${seas:+_$seas}.nc

    if emit "$concatcmd" "$catfile" \
            "ncrcat -O '$histfile' '$scenfile' '$catfile'"; then
        ((++nconcat))
    fi

    outbase=${idx}_${middle}
    tag="${htstart}-${tend}${seas:+_$seas}"
    freqdir=$outdir/$idx/$freq
    mkdir -p "$freqdir"

    out21=$freqdir/${outbase}_21yr_${tag}.nc
    if emit "$run21cmd" "$out21" "cdo runmean,21 '$catfile' '$out21'"; then
        ((++nrun21))
    fi

    out31=$freqdir/${outbase}_31yr_${tag}.nc
    if emit "$run31cmd" "$out31" "cdo runmean,31 '$catfile' '$out31'"; then
        ((++nrun31))
    fi

    echo "$idx${seas:+ ($seas)}"
done

echo
echo "Commandfile generation complete."
echo "  Concat: $nconcat  Run21: $nrun21  Run31: $nrun31"
echo "  Commandfiles: $cmddir"
