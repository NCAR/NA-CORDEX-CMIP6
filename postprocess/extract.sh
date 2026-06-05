#!/bin/bash

# extract.sh - Generate per-variable commandfiles for extracting variables
# from raw WRF output.  Designed for use with launch_multi and launch_cf,
# matching the pattern established by aggregate.sh.
#
# Requires setup.py to have been run first (SETUPDIR must contain sim.env
# and var_table.tsv).
#
# All variables are routed to postprocess.variables.py, except fx which
# goes to postprocess.fx.py.

set -euo pipefail

# Default variable list for NA-CORDEX-CMIP6 postprocessing.
#
# wbgt and utci have been omitted from DEFAULT_VARS because they take
# much longer to run (3 hour or more).  To process them, pass them
# explicitly via --vars.
#
# Note: utci is produced automatically when wbgt runs; submitting utci as
# a standalone job will cause a file conflict. Use --vars wbgt to get both.
# Note also: tasmin & tasmax are produced automatically when tas is run.

DEFAULT_VARS="fx,pr,tas,\
hurs,huss,ps,psl,rsds,sfcWind,uas,vas,\
cape,cin,prw,fzra,wchill,heatidx,humidex,\
evspsbl,mrro,mrros,mrso,snw,snd,snm,\
clt,hfls,hfss,rlds,rlus,rsus,\
ua50m,va50m,ua100m,va100m,ua150m,va150m,\
ua700,ua500,ua250,va700,va500,va250,\
hus700,hus500,hus250,ta700,ta500,ta250,zg700,zg500,zg250"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] WRFDIR SETUPDIR OUTDIR YEARS [CMDDIR]

Generate commandfiles for extracting CMORized variables from WRF output.

Arguments:
  WRFDIR    Top-level directory containing *_chunk/ simulation directories
  SETUPDIR  Directory containing sim.env (produced by setup.py)
  OUTDIR    Output directory (variable subdirectories created here)
  YEARS     Year or year range (e.g., 1980 or 1980-2020, inclusive)
  CMDDIR    Directory for commandfiles (default: current directory)

Options:
  --vars VAR[,VAR,...]  Comma-separated list of variables to process
                        (default: all supported variables except wbgt/utci/
                         humidex, which require the thermofeel package)
  --scripts PATH        Directory containing the postprocess scripts
                        (default: directory containing extract.sh)
  --force               Overwrite existing output (default: skip variables
                        whose output directory already exists and is non-empty)
  -h, --help            Show this help message
EOF
    exit 1
}

# Chunk-directory naming convention for NA-CORDEX-CMIP6 simulations:
# Each decade is run as a separate simulation with 2.5 years of spinup.
# The simulation for decade XXXX starts with year (XXXX - 3), so the
# chunk directory for year Y is named ((Y/10)*10 - 3)_chunk.
# Adjust this function if the simulation layout changes.
chunk_dir_for_year() {
    local year="$1"
    local sim_start=$(( (year / 10) * 10 - 3 ))
    echo "${sim_start}_chunk"
}

# Parse options
VARS="$DEFAULT_VARS"
SCRIPTS_DIR="$(dirname "$(realpath "$0")")"
FORCE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --vars)    VARS="$2"; shift 2 ;;
        --scripts) SCRIPTS_DIR="$(realpath "$2")"; shift 2 ;;
        --force)   FORCE=1; shift ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 4 ]] && usage

WRFDIR="$(realpath "$1")"
mkdir -p "$2"
SETUPDIR="$(realpath "$2")"
mkdir -p "$3"
OUTDIR="$(realpath "$3")"
YEARS_ARG="$4"
CMDDIR="${5:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

# Parse year range
if [[ "$YEARS_ARG" =~ ^([0-9]{4})-([0-9]{4})$ ]]; then
    START_YEAR="${BASH_REMATCH[1]}"
    END_YEAR="${BASH_REMATCH[2]}"
elif [[ "$YEARS_ARG" =~ ^([0-9]{4})$ ]]; then
    START_YEAR="${BASH_REMATCH[1]}"
    END_YEAR="${BASH_REMATCH[1]}"
else
    echo "Error: YEARS must be a year (e.g. 1980) or range (e.g. 1980-2020), got: $YEARS_ARG" >&2
    exit 1
fi

if [[ $START_YEAR -gt $END_YEAR ]]; then
    echo "Error: Start year ($START_YEAR) must not be greater than end year ($END_YEAR)" >&2
    exit 1
fi

# Validate inputs
[[ ! -d "$WRFDIR" ]]   && { echo "Error: WRFDIR not found: $WRFDIR" >&2; exit 1; }
[[ ! -d "$SETUPDIR" ]] && { echo "Error: SETUPDIR not found: $SETUPDIR" >&2; exit 1; }
[[ ! -f "$SETUPDIR/sim.env" ]] && { echo "Error: sim.env not found in $SETUPDIR" >&2; exit 1; }
[[ ! -f "$SETUPDIR/var_table.tsv" ]] && { echo "Error: var_table.tsv not found in $SETUPDIR" >&2; exit 1; }

for s in postprocess.machinery.py postprocess.fx.py; do
    [[ ! -f "$SCRIPTS_DIR/$s" ]] && {
        echo "Error: $s not found in $SCRIPTS_DIR" >&2
        exit 1
    }
done


# Verify that each year's chunk directory exists and contains expected files
for (( year = START_YEAR; year <= END_YEAR; year++ )); do
    chunk="$(chunk_dir_for_year "$year")"
    chunkpath="$WRFDIR/$chunk"
    if [[ ! -d "$chunkpath" ]]; then
        echo "Error: Chunk directory not found for year $year: $chunkpath" >&2
        exit 1
    fi
    if ! ls "$chunkpath"/wrfout_hour_d01_${year}-* &>/dev/null; then
        echo "Error: No wrfout_hour_d01_${year}-* files found in $chunkpath" >&2
        exit 1
    fi
done

# Split comma-separated var list into an array
IFS=',' read -ra VARLIST <<< "$VARS"

# Generate one commandfile per variable
generated_cmds=()

for var in "${VARLIST[@]}"; do
    if [[ "$var" == "utci" ]]; then
        echo "Note: utci is produced automatically when wbgt runs; skipping standalone utci job."
        continue
    fi

# Skip if output directory exists and is non-empty, unless --force
    if [[ $FORCE -eq 0 ]]; then
        if [[ "$var" == "fx" ]]; then
            if [[ -d "$OUTDIR/sftlf" && -n "$(ls -A "$OUTDIR/sftlf" 2>/dev/null)" ]] && \
               [[ -d "$OUTDIR/orog"  && -n "$(ls -A "$OUTDIR/orog"  2>/dev/null)" ]]; then
                echo "Skipping fx: output directories (sftlf, orog) already exist and are non-empty"
                continue
            fi
        elif [[ -d "$OUTDIR/$var" && -n "$(ls -A "$OUTDIR/$var" 2>/dev/null)" ]]; then
            echo "Skipping $var: output directory already exists and is non-empty"
            continue
        fi
    fi

    cmdfile="$CMDDIR/${var}.cmd"
    > "$cmdfile"

    if [[ "$var" == "fx" ]]; then
        # fx variables are time-invariant; a single command suffices.
        echo "python ./postprocess.fx.py $SETUPDIR $OUTDIR" >> "$cmdfile"
    else
        for (( year = START_YEAR; year <= END_YEAR; year++ )); do
            chunk="$WRFDIR/$(chunk_dir_for_year "$year")"
            echo "python ./postprocess.machinery.py $SETUPDIR $chunk $year $var $OUTDIR" >> "$cmdfile"
        done
    fi

    if [[ ! -s "$cmdfile" ]]; then
        rm "$cmdfile"
    else
        generated_cmds+=("$cmdfile")
    fi
done

# Summary
echo ""
echo "Commandfiles written to: $CMDDIR"
