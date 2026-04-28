#!/bin/bash

# extract.sh - Generate per-variable commandfiles for extracting variables
# from raw WRF output.  Designed for use with launch_multi and launch_cf,
# matching the pattern established by aggregate.sh.
#
# Requires setup.sh to have been run first (OUTDIR must contain cached CMOR
# JSON tables).
#
# Variables are routed to one of two worker scripts:
#   postprocess.core.variables.py    - standard CORDEX core variables
#   postprocess.hyd-atm.variables.py - supplemental hydro/atmo variables
#                                      (AFWA diagnostics, pressure-level
#                                      vars, height-AGL winds, etc.)

set -euo pipefail

# Default variable list for NA-CORDEX-CMIP6 postprocessing.
#
# wbgt is intentionally NOT in DEFAULT_HYDATM_VARS because it requires the
# thermofeel package, which is not in the standard npl conda env.  To process
# wbgt, utci, and humidex are intentionally NOT in DEFAULT_HYDATM_VARS because
# they require the thermofeel package...
DEFAULT_CORE_VARS="fx,clt,evspsbl,hurs,huss,pr,ps,psl,rlds,rsds,sfcWind,tas,tasmax,tasmin,uas,vas"
DEFAULT_HYDATM_VARS="cape,cin,prw,fzra,wchill,heatidx,\
mrro,mrros,mrso,snw,snd,\
rsus,rlus,hfls,hfss,snm,\
ua50m,va50m,ua100m,va100m,ua150m,va150m,\
ta700,ta500,ta250,ua700,ua500,ua250,va700,va500,va250,\
zg700,zg500,zg250,hus700,hus500,hus250"
DEFAULT_VARS="${DEFAULT_CORE_VARS},${DEFAULT_HYDATM_VARS}"

# wbgt, utci, and humidex are intentionally NOT in DEFAULT_HYDATM_VARS because
# they require the thermofeel package. Pass them explicitly via --vars.
# Note: utci is produced automatically when wbgt runs; submitting utci as a
# standalone job will cause a file conflict. Use --vars wbgt to get both.
HYDATM_VARS="cape cin prw fzra wchill heatidx wbgt utci humidex
mrro mrros mrso snw snd
rsus rlus hfls hfss snm
ua50m va50m ua100m va100m ua150m va150m
ta700 ta500 ta250 ua700 ua500 ua250 va700 va500 va250
zg700 zg500 zg250 hus700 hus500 hus250"

script_for_var() {
    local v="$1"
    local hv
    for hv in $HYDATM_VARS; do
        [[ "$v" == "$hv" ]] && { echo "postprocess.hyd-atm.variables.py"; return; }
    done
    echo "postprocess.core.variables.py"
}

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
                        (default: all supported variables except wbgt;
                         wbgt requires the thermofeel package)
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

[[ $# -lt 3 ]] && usage

WRFDIR="$(realpath "$1")"
mkdir -p $2
OUTDIR="$(realpath "$2")"
YEARS_ARG="$3"
CMDDIR="${4:-.}"
mkdir -p $CMDDIR
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

for s in postprocess.core.variables.py postprocess.hyd-atm.variables.py; do
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

mkdir -p "$OUTDIR" "$CMDDIR"

# Split comma-separated var list into an array
IFS=',' read -ra VARLIST <<< "$VARS"

# Get the chunk path for the first year (used for fx)
FIRST_CHUNK="$WRFDIR/$(chunk_dir_for_year "$START_YEAR")"

# Generate one commandfile per variable
generated_cmds=()

for var in "${VARLIST[@]}"; do
    if [[ "$var" == "utci" ]]; then
        echo "Note: utci is produced automatically when wbgt runs; skipping standalone utci job."
        continue
    fi

    # Skip if output directory exists and is non-empty, unless --force
    vardir="$OUTDIR/$var"
    if [[ $FORCE -eq 0 && -d "$vardir" && -n "$(ls -A "$vardir" 2>/dev/null)" ]]; then
        echo "Skipping $var: output directory already exists and is non-empty"
        continue
    fi

    cmdfile="$CMDDIR/${var}.cmd"
    > "$cmdfile"

    script="$(script_for_var "$var")"

    if [[ "$var" == "fx" ]]; then
        # fx variables are time-invariant; generate a single command using
        # the first year's chunk directory.  fx is always handled by core.
        echo "python ./postprocess.core.variables.py $FIRST_CHUNK $START_YEAR fx $OUTDIR" >> "$cmdfile"
    else
        for (( year = START_YEAR; year <= END_YEAR; year++ )); do
            chunk="$WRFDIR/$(chunk_dir_for_year "$year")"
            echo "python ./$script $chunk $year $var $OUTDIR" >> "$cmdfile"
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
