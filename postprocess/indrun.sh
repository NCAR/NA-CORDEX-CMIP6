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
#   launch_multi --chain concat.cmd run21.cmd run31.cmd
#
# Usage:
#   ./indrun.sh <scratch> <id> [outdir] [--histdir DIR]
#   ./indrun.sh --badj <indir> <outdir>
#
#   Standard mode:
#   # <id> must be a scenario run, e.g. mpi-245
#   # outdir defaults to <scratch>/index/<id>; holds cmd/ and data/
#   # --histdir overrides the historical index/data dir, e.g. if the
#   # historical run has already been moved to campaign
#
#   --badj mode (bias-adjusted indexes):
#   # historical and scenario files sit side by side under
#   # <indir>/<idx>/<freq>/, with one scenario per tree
#   # <indir> is the folder containing <idx>/<freq>; <outdir> is required
#   # and holds cmd/ and data/
#   # --histdir is not allowed

set -euo pipefail

usage() {
    echo "Usage: $0 <scratch> <id> [outdir] [--histdir DIR]"
    echo "       $0 --badj <indir> <outdir>"
    exit 1
}

badj=0
histdir=""
pos=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --badj)
            badj=1
            shift
            ;;
        --histdir)
            [[ $# -ge 2 ]] || usage
            histdir=$2
            shift 2
            ;;
        *)
            pos+=("$1")
            shift
            ;;
    esac
done

if [[ $badj -eq 1 ]]; then
    [[ ${#pos[@]} -eq 2 && -z $histdir ]] || usage
    scendir=${pos[0]}
    histdir=$scendir
    outdir=${pos[1]}
else
    [[ ${#pos[@]} -ge 2 && ${#pos[@]} -le 3 ]] || usage
    scratch=${pos[0]}
    id=${pos[1]}
    outdir=${pos[2]:-}

    scendir=$scratch/$id/index/data
    outdir=${outdir:-$scratch/index/$id}

    if [[ -z $histdir ]]; then
        ## Historical run is named by convention: <model>-hist
        histid=${id%-*}-hist

        if [[ "$histid" == "$id" ]]; then
            echo "Error: $id has no '-<scenario>' suffix; can't derive historical id"
            exit 1
        fi

        histdir=$scratch/$histid/index/data
    fi
fi

for d in "$scendir" "$histdir"; do
    if [[ ! -d $d ]]; then
        echo "Error: index directory not found: $d"
        exit 1
    fi
done

cmddir=$outdir/cmd
datadir=$outdir/data
catdir=$datadir/cat
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

for scenfile in "$scendir"/*/{ann,seas,mon}/*.nc; do
    [[ -e $scenfile ]] || continue
    fname=${scenfile##*/}

    if ! fields=$(parse_fname "$fname"); then
        echo "  WARNING: unrecognized filename pattern: $fname; skipping" >&2
        continue
    fi
    IFS=$'\t' read -r idx middle tstart tend seas <<<"$fields"

    ## --badj: historical files share the scenario tree; they are only
    ## ever paired as the historical half, so skip them silently here.
    if [[ $badj -eq 1 && $middle == *_historical_* ]]; then
        continue
    fi

    ## freq dir (ann/seas/mon) is the scenario file's own parent dir; the
    ## historical file lives under the same <idx>/<freq>/ dir, but
    ## middle/timespan differ (e.g. "historical" vs "ssp245"), so it
    ## still needs to be found by season/month match within that dir.
    ## In --badj mode that dir also holds the scenario file itself, so a
    ## candidate must additionally end before the scenario file starts.
    freq=${scenfile%/*}
    freq=${freq##*/}
    histfile=""
    for cand in "$histdir/$idx/$freq"/*.nc; do
        [[ -e $cand ]] || continue
        if hfields=$(parse_fname "${cand##*/}"); then
            IFS=$'\t' read -r hidx hmiddle htstart htend hseas <<<"$hfields"
            if [[ $badj -eq 1 && ! $htend < $tstart ]]; then
                continue
            fi
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
    freqdir=$datadir/$idx/$freq
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
echo "  Output files: $datadir"
