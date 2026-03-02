#!/bin/bash

# extract.sh - Generate per-variable commandfiles for extracting and CMORizing
# variables from raw WRF output.  Designed for use with launch_multi and
# launch_cf, matching the pattern established by aggregate.sh.

set -euo pipefail

# Default variable list for NA-CORDEX-CMIP6 postprocessing
DEFAULT_VARS="fx,clt,evspsbl,hurs,huss,pr,ps,psl,rlds,rsds,sfcWind,tas,tasmax,tasmin,uas,vas"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] WRFDIR OUTDIR YEARS [CMDDIR]

Generate commandfiles for extracting CMORized variables from WRF output.

Arguments:
  WRFDIR    Top-level directory containing *_chunk/ simulation directories
  OUTDIR    Output directory (variable subdirectories created here)
  YEARS     Year or year range (e.g., 1980 or 1980-2020, inclusive)
  CMDDIR    Directory for commandfiles (default: current directory)

Options:
  --vars VAR[,VAR,...]  Comma-separated list of variables to process
                        (default: all supported variables)
  --scripts PATH        Directory containing postprocess.core.variables.py,
                        cmorize.compress.sh, and var_specs.yml
                        (default: directory containing extract.sh)
  -h, --help            Show this help message
EOF
    exit 1
}

# Chunk-directory naming convention for NA-CORDEX-CMIP6 simulations:
# Each decade is run as a separate simulation with 3 years of spinup.
# The simulation for decade XXXX starts at year (XXXX - 3), so the
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

while [[ $# -gt 0 ]]; do
    case "$1" in
        --vars)   VARS="$2"; shift 2 ;;
        --scripts) SCRIPTS_DIR="$(realpath "$2")"; shift 2 ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 3 ]] && usage

WRFDIR="$(realpath "$1")"
OUTDIR="$(realpath "$2")"
YEARS_ARG="$3"
CMDDIR="${4:-.}"
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
[[ ! -d "$WRFDIR" ]] && { echo "Error: WRFDIR not found: $WRFDIR" >&2; exit 1; }

for f in postprocess.core.variables.py cmorize.compress.sh var_specs.yml; do
    [[ ! -f "$SCRIPTS_DIR/$f" ]] && {
        echo "Error: Required script not found: $SCRIPTS_DIR/$f" >&2
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

mkdir -p "$OUTDIR" "$CMDDIR"

# Split comma-separated var list into an array
IFS=',' read -ra VARLIST <<< "$VARS"

# Get the chunk path for the first year (used for fx)
FIRST_CHUNK="$WRFDIR/$(chunk_dir_for_year "$START_YEAR")"

# Generate one commandfile per variable
generated_cmds=()

for var in "${VARLIST[@]}"; do
    cmdfile="$CMDDIR/${var}.cmd"
    > "$cmdfile"

    if [[ "$var" == "fx" ]]; then
        # fx variables are time-invariant; generate a single command using
        # the first year's chunk directory
        echo "python ./postprocess.core.variables.py $FIRST_CHUNK $START_YEAR fx $OUTDIR" >> "$cmdfile"
    else
        for (( year = START_YEAR; year <= END_YEAR; year++ )); do
            chunk="$WRFDIR/$(chunk_dir_for_year "$year")"
            echo "python ./postprocess.core.variables.py $chunk $year $var $OUTDIR" >> "$cmdfile"
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
echo ""
echo "To run with launch_multi:"
echo "  launch_multi --run RUNDIR --workflow cordex ${CMDDIR}/*.cmd"
