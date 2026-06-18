#!/bin/bash

# extract.sh - Generate per-variable commandfiles for extracting variables
# from raw WRF output.  Designed for use with launch_multi and launch_cf,
# matching the pattern established by aggregate.sh.
#
# Requires setup.py to have been run first (SETUPDIR must contain sim.env
# and var_table.tsv).
#
# All variables are routed to postproc_engine.py, except:
#   fx   -> postproc_fx.py   (time-invariant; single command)
#   wbgt -> postproc_wbgt.py (per-month parallelism; see below)
#
# wbgt/utci workflow:
#   Commandfiles are written to CMDDIR/wbgt/ instead of CMDDIR/, so they
#   can be launched as a chain without interfering with the main *.cmd glob:
#     launch_multi --workflow cordex --run RUNDIR CMDDIR/*.cmd
#     launch_multi --chain --workflow cordex --run RUNDIR \
#         CMDDIR/wbgt/wbgt.cmd CMDDIR/wbgt/wbgt_cat.cmd
#
#   Per-day output files are staged in OUTDIR/_temp/<var>.1hr/<year>/ to
#   keep them separate from the annual files that aggregate.sh expects.
#   After wbgt_cat.cmd runs, annual files land in OUTDIR/wbgt.1hr/ and
#   OUTDIR/utci.1hr/ alongside all other extracted variables.

set -euo pipefail

# Default variable list for NA-CORDEX-CMIP6 postprocessing.
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
hus700,hus500,hus250,ta700,ta500,ta250,zg700,zg500,zg250,\
wbgt"

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
                        (default: all supported variables)
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
#
# Exception: future runs begin in 2015, using the end of the historical
# run as spinup, so 2015-2019 live in 2015_chunk rather than 2012_chunk.
# The standard formula resumes at 2020 (-> 2017_chunk).
#
# Relies on driving_experiment_id, set by sourcing sim.env before this
# function is first called.
chunk_dir_for_year() {
    local year="$1"
    if [[ "$driving_experiment_id" =~ ^ssp[0-9]{3}$ ]] && [[ $year -ge 2015 && $year -le 2019 ]]; then
        echo "2015_chunk"
    else
        local sim_start=$(( (year / 10) * 10 - 3 ))
        echo "${sim_start}_chunk"
    fi
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

for script in engine vars fx wbgt; do
    [[ ! -f "$SCRIPTS_DIR/postproc_${script}.py" ]] && {
        echo "Error: postproc_${script}.py not found in $SCRIPTS_DIR" >&2
        exit 1
    }
done

# Load simulation metadata for filename construction.
# Used to build wbgt/utci annual output filenames in wbgt_cat.cmd.
source "$SETUPDIR/sim.env"
fname_base="${domain_id}_${driving_source_id}_${driving_experiment_id}_${driving_variant_label}_${institution_id}_${source_id}_${version_realization}"

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
        elif [[ "$var" == "wbgt" ]]; then
            if [[ -d "$OUTDIR/wbgt.1hr" && -n "$(ls -A "$OUTDIR/wbgt.1hr" 2>/dev/null)" ]] && \
               [[ -d "$OUTDIR/utci.1hr" && -n "$(ls -A "$OUTDIR/utci.1hr" 2>/dev/null)" ]]; then
                echo "Skipping wbgt: output directories (wbgt.1hr, utci.1hr) already exist and are non-empty"
                continue
            fi
        elif [[ -d "$OUTDIR/$var" && -n "$(ls -A "$OUTDIR/$var" 2>/dev/null)" ]]; then
            echo "Skipping $var: output directory already exists and is non-empty"
            continue
        fi
    fi

    # wbgt/utci: per-month extraction then concatenation (by year, to
    # keep filesystem & scheduler happy)
    if [[ "$var" == "wbgt" ]]; then
        wbgt_cmddir="$CMDDIR/wbgt"
        mkdir -p "$wbgt_cmddir"

        cmdfile="$wbgt_cmddir/wbgt_mon.cmd"
        > "$cmdfile"

        for (( year = START_YEAR; year <= END_YEAR; year++ )); do
            chunk="$WRFDIR/$(chunk_dir_for_year "$year")"
            tmpdir="$OUTDIR/_temp"
            for month in $( seq -w 12 ); do
                ls "$chunk"/wrfout_hour_d01_${year}-${month}-* &>/dev/null || continue
                echo "python ./postproc_wbgt.py $SETUPDIR $chunk $year $month $tmpdir" >> "$cmdfile"
            done
        done

        # afterwards, concatenate daily files from tmpdir into OUTDIR.
        cat_cmdfile="$wbgt_cmddir/wbgt_cat.cmd"
        > "$cat_cmdfile"

        for (( year = START_YEAR; year <= END_YEAR; year++ )); do
            for wvar in wbgt utci; do
                # $wvar.1hr/$year subdir is created by postproc_wbgt
                tmpvardir="$OUTDIR/_temp/${wvar}.1hr/$year"
                outvardir="$OUTDIR/${wvar}.1hr"
                mkdir -p "$outvardir"
                ann_out="$outvardir/${wvar}_${fname_base}_1hr_${year}01010000-${year}12312300.nc"
                echo "ncrcat -h $tmpvardir/\*.nc $ann_out" >> "$cat_cmdfile"
            done
        done
        continue
    fi

    cmdfile="$CMDDIR/${var}.cmd"
    > "$cmdfile"

    if [[ "$var" == "fx" ]]; then
        # fx variables are time-invariant; a single command suffices.
        echo "python ./postproc_fx.py $SETUPDIR $OUTDIR" >> "$cmdfile"
    else
        for (( year = START_YEAR; year <= END_YEAR; year++ )); do
            chunk="$WRFDIR/$(chunk_dir_for_year "$year")"
            echo "python ./postproc_engine.py $SETUPDIR $chunk $year $var $OUTDIR" >> "$cmdfile"
        done
    fi
done

find "$CMDDIR" -empty -delete

# Summary
echo ""
echo "Commandfiles written to: $CMDDIR"
