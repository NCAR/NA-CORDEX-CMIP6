#!/bin/bash

# index.sh - Generate commandfiles for computing climate indices from
# compressed CORDEX-CMIP6 daily data.
#
# Operates on the output of compress.sh (Step 4), where data are organized
# into <var>.<freq> subdirectories (e.g., pr.day, tasmax.day).  Produces
# one NetCDF file per index covering all available years, written flat into
# OUTDIR (no per-variable subdirectories, since these files go to GIS).
#
# Index definitions are read from a TSV file (default: climate_indices.tsv
# in the same directory as this script).  The TSV drives command construction;
# modifying the TSV is the intended way to add, remove, or change indices.
#
# Generates five commandfiles that must be run in order:
#
#   minmax.cmd   - ydrunmin/ydrunmax/timmin/timmax over baseline period.
#                  Required by pctile.cmd and indices.cmd.
#
#   pctile.cmd   - ydrunpctl/ydrunmean/timpctl reference files from minmax.
#                  Required by annual.cmd.
#
#   indices.cmd  - Indices whose operators natively output annual time steps.
#                  Includes a CDO_PCTL_NBINS export before bootstrapped cmds.
#
#   annual.cmd   - One command per year for operators that summarise over the
#                  entire input period.  Writes per-year files to OUTDIR/annual/.
#
#   merge.cmd    - One mergetime per annual-loop index, assembling per-year
#                  files into OUTDIR.
#
# Per-year temporary files in OUTDIR/annual/ can be removed after merge.cmd
# completes.
#
# --- TSV columns ---
#
#   index          Output filename stem (e.g. TG10p)
#   cdo_operator   CDO operator or pipe fragment (e.g. eca_tg10p,
#                  yearsum -setrtomiss,0,1).  Used verbatim in the command.
#   input_vars     Input variable name(s).  Use + to separate multiple vars
#                  (e.g. tasmax+tasmin).  Special token "sftlf" causes that
#                  input to be resolved from <var>.fx rather than <var>.day.
#   units          Expected units of the input.  Drives inline unit conversion:
#                    mm/day  -> -mulc,86400  (pr arrives as kg m-2 s-1)
#                    octas   -> -mulc,0.08   (clt arrives as 0-100%)
#                    C       -> -subc,277.15 (tas arrives in K)
#                    other   -> no conversion
#   long_name      Human-readable description (not used by script)
#   output_frequency
#                  annual      : operator natively outputs annual steps
#                  annual_loop : operator summarises entire input; needs
#                                selyear loop + mergetime
#   prereq_type    Prerequisite descriptor (drives minmax/pctile generation):
#                    none
#                    bootstrap:ydrunmin+ydrunmax,W
#                                     etccdi bootstrapped; generates
#                                     ydrunmin and ydrunmax with window W
#                                     in minmax.cmd; appends
#                                     ,BSTART,BEND to operator and passes
#                                     minmax files as infile2/infile3
#                    ydrunmean,W      generates ydrunmean with window W
#                                     in pctile.cmd; passed as infile2
#                    ydrunpctl,P,W    generates ydrunmin+ydrunmax in
#                                     minmax.cmd, ydrunpctl,P,W in
#                                     pctile.cmd; passed as infile2
#                    timpctl,P        generates timmin+timmax in minmax.cmd
#                                     (wet-day masked for pr), timpctl,P
#                                     in pctile.cmd; passed as infile2
#   description    Human-readable description (not used by script)
#   notes          Freeform notes (not used by script)
#
# --- Unit conversion notes ---
#
# Unit conversions are applied as a CDO operator prepended to the input pipe.
# The conversion for a given units value is defined in the unit_conv() function
# below; that function is the single authority for conversion rules.
#
# GD4 uses units "C" which triggers -subc,277.15, converting K to C while
# also shifting the 4-degree threshold (the operator itself adds a gtc,0 mask
# via the cdo_operator field).
#
# --- Usage ---
#
# Usage: index.sh [OPTIONS] INDIR OUTDIR [CMDDIR]
#
#   INDIR    Output from compress.sh (contains <var>.<freq> subdirs)
#   OUTDIR   Output directory for index NetCDF files
#   CMDDIR   Directory for commandfiles (default: current directory)
#
# Options:
#   --tsv FILE                    Index definitions TSV (default: see above)
#   --baseline STARTYEAR-ENDYEAR  Reference period (default: 1991-2020)
#   --force                       Overwrite existing output files
#   -h, --help                    Show this help message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TSV="${SCRIPT_DIR}/climate_indices.tsv"
BASELINE_START=1991
BASELINE_END=2020
FORCE=0

usage() {
    sed -n '/^# Usage:/,/^[^#]/{ /^[^#]/d; s/^# \{0,3\}//; p }' "$0" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tsv)      TSV="$2"; shift 2 ;;
        --baseline)
            if [[ "$2" =~ ^([0-9]{4})-([0-9]{4})$ ]]; then
                BASELINE_START="${BASH_REMATCH[1]}"
                BASELINE_END="${BASH_REMATCH[2]}"
            else
                echo "Error: --baseline must be STARTYEAR-ENDYEAR, got: $2" >&2; exit 1
            fi
            shift 2 ;;
        --force)    FORCE=1; shift ;;
        -h|--help)  usage ;;
        -*)         echo "Error: Unknown option $1" >&2; usage ;;
        *)          break ;;
    esac
done

[[ $# -lt 2 ]] && usage

INDIR="$(realpath "$1")"
mkdir -p "$2"
OUTDIR="$(realpath "$2")"
CMDDIR="${3:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

[[ ! -d "$INDIR" ]] && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }
[[ ! -f "$TSV"   ]] && { echo "Error: TSV not found: $TSV" >&2; exit 1; }

PCTLDIR="$OUTDIR/pctl"
ANNDIR="$OUTDIR/annual"
mkdir -p "$PCTLDIR" "$ANNDIR"

MINMAX_CMD="$CMDDIR/minmax.cmd"
PCTILE_CMD="$CMDDIR/pctile.cmd"
INDEX_CMD="$CMDDIR/indices.cmd"
ANNUAL_CMD="$CMDDIR/annual.cmd"
MERGE_CMD="$CMDDIR/merge.cmd"
> "$MINMAX_CMD"; > "$PCTILE_CMD"; > "$INDEX_CMD"; > "$ANNUAL_CMD"; > "$MERGE_CMD"

# CDO_PCTL_NBINS for etccdi bootstrapping: window=5, double/int ratio=2
PCTL_WINDOW=5
BASELINE_YEARS=$(( BASELINE_END - BASELINE_START + 1 ))
CDO_PCTL_NBINS=$(( PCTL_WINDOW * BASELINE_YEARS * 2 + 2 ))

echo "Baseline period: ${BASELINE_START}-${BASELINE_END}"
echo "CDO_PCTL_NBINS:  ${CDO_PCTL_NBINS}"

# ---------------------------------------------------------------------------
# Extract shared DRS components and simulation timespan from input files
# ---------------------------------------------------------------------------

all_files=$(find "$INDIR" -maxdepth 2 -name "*.nc" -path "*/*.day/*" | sort)
[[ -z "$all_files" ]] && {
    echo "Error: No *.nc files found under $INDIR/*/*.day/" >&2; exit 1
}

first_base="$(basename "$(echo "$all_files" | head -1)" .nc)"
last_base="$(basename "$(echo "$all_files" | tail -1)" .nc)"
middle="$(echo "$first_base" | cut -d_ -f2-8)"
first_ts="${first_base##*_}"; last_ts="${last_base##*_}"
timespan="${first_ts%%-*}-${last_ts##*-}"
SIM_START="${first_ts:0:4}"
SIM_END="${last_ts##*-}"; SIM_END="${SIM_END:0:4}"

echo "  DRS middle: $middle"
echo "  Timespan:   $timespan  (${SIM_START}-${SIM_END})"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

# Space-separated sorted list of all files for a <var>.day variable
day_inputs() {
    find "$INDIR/${1}.day" -maxdepth 1 -name "${1}_*.nc" -type f | sort | tr '\n' ' ' | sed 's/ $//'
}

# Files for a variable whose timespan overlaps the baseline period
baseline_inputs() {
    local vardir="$INDIR/${1}.day"
    [[ -d "$vardir" ]] || return 0
    local f base ts sy ey
    while IFS= read -r f; do
        base="$(basename "$f" .nc)"; ts="${base##*_}"
        sy="${ts:0:4}"; ey="${ts##*-}"; ey="${ey:0:4}"
        [[ $sy -le $BASELINE_END && $ey -ge $BASELINE_START ]] && echo "$f" || true
    done < <(find "$vardir" -maxdepth 1 -name "${1}_*.nc" -type f | sort)
}

# Append a line to a commandfile; skip if outfile already exists (unless --force)
emit() {
    local cmdfile="$1" outfile="$2"; shift 2
    [[ -f "$outfile" && $FORCE -eq 0 ]] && return 0
    echo "$*" >> "$cmdfile"
}

# Return CDO inline unit-conversion operator for a given units string, or empty.
# This function is the single authority for unit conversion rules.
unit_conv() {
    case "$1" in
        mm/day) echo "-mulc,86400"  ;;
        octas)  echo "-mulc,0.08"   ;;
        C)      echo "-subc,277.15" ;;
        *)      echo ""             ;;
    esac
}

# Build a CDO input pipe for a single variable: "[conv] -mergetime file1 ..."
# Usage: var_pipe VAR UNITS
var_pipe() {
    local var="$1" units="$2"
    local inputs conv
    inputs="$(day_inputs "$var")"
    [[ -z "$inputs" ]] && { echo ""; return; }
    conv="$(unit_conv "$units")"
    if [[ -n "$conv" ]]; then
        echo "${conv} -mergetime ${inputs}"
    else
        echo "-mergetime ${inputs}"
    fi
}

# Canonicalise a prereq filename: <var>_<stat>_<middle>_<timespan>.nc in PCTLDIR
pctl_file() { echo "$PCTLDIR/${1}_${2}_${middle}_${timespan}.nc"; }

# Tracking sets to avoid duplicate minmax/pctile commands across TSV rows
declare -A minmax_emitted=()
declare -A pctile_emitted=()

# Emit ydrunmin + ydrunmax for a variable into minmax.cmd (once per var+window)
ensure_ydrunminmax() {
    local var="$1" bl_inputs="$2"
    local key="${var}_ydrun${PCTL_WINDOW}"
    [[ -n "${minmax_emitted[$key]:-}" ]] && return 0
    local f_min f_max
    f_min="$(pctl_file "$var" "ydrunmin${PCTL_WINDOW}")"
    f_max="$(pctl_file "$var" "ydrunmax${PCTL_WINDOW}")"
    emit "$MINMAX_CMD" "$f_min" \
        "cdo -s ydrunmin,${PCTL_WINDOW} -mergetime ${bl_inputs} ${f_min}"
    emit "$MINMAX_CMD" "$f_max" \
        "cdo -s ydrunmax,${PCTL_WINDOW} -mergetime ${bl_inputs} ${f_max}"
    minmax_emitted[$key]=1
    (( nminmax += 2 )) || true
}

# Emit timmin + timmax for a variable into minmax.cmd (once per var+mask combo).
# wet_mask=1 applies setrtomiss,0,1 + unit conversion before computing min/max.
ensure_timminmax() {
    local var="$1" bl_inputs="$2" units="$3" wet_mask="${4:-0}"
    local key="${var}_tim_wet${wet_mask}"
    [[ -n "${minmax_emitted[$key]:-}" ]] && return 0
    local conv pipe f_min f_max stem
    conv="$(unit_conv "$units")"
    if [[ $wet_mask -eq 1 ]]; then
        pipe="-setrtomiss,0,1 ${conv} -mergetime ${bl_inputs}"
        stem="${var}_wetday"
    else
        pipe="${conv:+${conv} }-mergetime ${bl_inputs}"
        stem="$var"
    fi
    f_min="$(pctl_file "$stem" "timmin")"
    f_max="$(pctl_file "$stem" "timmax")"
    emit "$MINMAX_CMD" "$f_min" "cdo -s timmin ${pipe} ${f_min}"
    emit "$MINMAX_CMD" "$f_max" "cdo -s timmax ${pipe} ${f_max}"
    minmax_emitted[$key]=1
    (( nminmax += 2 )) || true
}

nminmax=0; npctile=0; nindex=0; nannual=0; nmerge=0
nbins_emitted=0

# ---------------------------------------------------------------------------
# Main loop: read TSV and emit commands
# ---------------------------------------------------------------------------

while IFS=$'\t' read -r idx op input_vars units long_name freq prereq desc notes; do
    # Skip header and blank lines
    [[ "$idx" == "index" || -z "$idx" ]] && continue

    # Split input_vars on '+' into an array
    IFS='+' read -ra vars <<< "$input_vars"
    primary_var="${vars[0]}"

    # Check required input directories/files exist; silently skip if absent
    skip=0
    for v in "${vars[@]}"; do
        if [[ "$v" == "sftlf" ]]; then
            if ! find "$INDIR/sftlf.fx" -maxdepth 1 -name "sftlf_*.nc" \
                    -type f 2>/dev/null | grep -q .; then
                echo "    WARNING: sftlf.fx not found; skipping ${idx}" >&2
                skip=1; break
            fi
        elif [[ ! -d "$INDIR/${v}.day" ]]; then
            skip=1; break
        fi
    done
    [[ $skip -eq 1 ]] && continue

    # Build baseline file list for primary variable
    bl_inputs="$(baseline_inputs "$primary_var" | tr '\n' ' ' | sed 's/ $//')"

    # Warn and skip if prereq needs baseline but none found
    if [[ "$prereq" != "none" && -z "$bl_inputs" ]]; then
        echo "    WARNING: no ${primary_var} files overlap baseline" \
             "${BASELINE_START}-${BASELINE_END}; skipping ${idx}" >&2
        continue
    fi

    # Build the main input pipe for the primary variable
    main_pipe="$(var_pipe "$primary_var" "$units")"
    [[ -z "$main_pipe" ]] && continue

    # Build secondary input expressions (no unit conversion on secondary vars)
    extra_inputs=""
    for (( i=1; i<${#vars[@]}; i++ )); do
        v="${vars[$i]}"
        if [[ "$v" == "sftlf" ]]; then
            sf="$(find "$INDIR/sftlf.fx" -maxdepth 1 -name "sftlf_*.nc" \
                      -type f | head -1)"
            extra_inputs="${extra_inputs} ${sf}"
        else
            extra_inputs="${extra_inputs} -mergetime $(day_inputs "$v")"
        fi
    done

    final_out="$OUTDIR/${idx}_${middle}_${timespan}.nc"

    # ------------------------------------------------------------------
    # Parse prereq_type: emit minmax/pctile commands; build extra op args
    # ------------------------------------------------------------------
    op_args=""        # appended to operator (e.g. ,1991,2020 for bootstrap)
    prereq_infiles="" # extra infiles after main input (bootstrap minmax pair)
    prereq_infile2="" # single prereq file (pctile/mean) for annual_loop

    case "$prereq" in
        none)
            ;;

        bootstrap:ydrunmin+ydrunmax,*)
            ensure_ydrunminmax "$primary_var" "$bl_inputs"
            f_min="$(pctl_file "$primary_var" "ydrunmin${PCTL_WINDOW}")"
            f_max="$(pctl_file "$primary_var" "ydrunmax${PCTL_WINDOW}")"
            op_args=",${BASELINE_START},${BASELINE_END}"
            prereq_infiles="${f_min} ${f_max}"
            if [[ $nbins_emitted -eq 0 ]]; then
                echo "export CDO_PCTL_NBINS=${CDO_PCTL_NBINS}" >> "$INDEX_CMD"
                nbins_emitted=1
            fi
            ;;

        ydrunmean,*)
            window="${prereq##*,}"
            f_mean="$(pctl_file "$primary_var" "ydrunmean${window}")"
            key="${primary_var}_ydrunmean${window}"
            if [[ -z "${pctile_emitted[$key]:-}" ]]; then
                emit "$PCTILE_CMD" "$f_mean" \
                    "cdo -s ydrunmean,${window} -mergetime ${bl_inputs} ${f_mean}"
                pctile_emitted[$key]=1
                (( npctile++ )) || true
            fi
            prereq_infile2="$f_mean"
            ;;

        ydrunpctl,*,*)
            pctl="${prereq#ydrunpctl,}"; pctl="${pctl%,*}"
            window="${prereq##*,}"
            ensure_ydrunminmax "$primary_var" "$bl_inputs"
            f_min="$(pctl_file "$primary_var" "ydrunmin${window}")"
            f_max="$(pctl_file "$primary_var" "ydrunmax${window}")"
            f_pctl="$(pctl_file "$primary_var" "p${pctl}")"
            key="${primary_var}_ydrunpctl${pctl}_${window}"
            if [[ -z "${pctile_emitted[$key]:-}" ]]; then
                emit "$PCTILE_CMD" "$f_pctl" \
                    "cdo -s ydrunpctl,${pctl},${window} -mergetime ${bl_inputs} \
${f_min} ${f_max} ${f_pctl}"
                pctile_emitted[$key]=1
                (( npctile++ )) || true
            fi
            prereq_infile2="$f_pctl"
            ;;

        timpctl,*)
            pctl="${prereq##*,}"
            wet_mask=0; [[ "$primary_var" == "pr" ]] && wet_mask=1
            ensure_timminmax "$primary_var" "$bl_inputs" "$units" "$wet_mask"
            local_stem="${primary_var}$( [[ $wet_mask -eq 1 ]] && echo "_wetday" || echo "" )"
            f_min="$(pctl_file "$local_stem" "timmin")"
            f_max="$(pctl_file "$local_stem" "timmax")"
            f_pctl="$(pctl_file "$local_stem" "p${pctl}")"
            key="${primary_var}_timpctl${pctl}_wet${wet_mask}"
            if [[ -z "${pctile_emitted[$key]:-}" ]]; then
                conv="$(unit_conv "$units")"
                if [[ $wet_mask -eq 1 ]]; then
                    bl_pipe="-setrtomiss,0,1 ${conv} -mergetime ${bl_inputs}"
                else
                    bl_pipe="${conv:+${conv} }-mergetime ${bl_inputs}"
                fi
                emit "$PCTILE_CMD" "$f_pctl" \
                    "cdo -s timpctl,${pctl} ${bl_pipe} ${f_min} ${f_max} ${f_pctl}"
                pctile_emitted[$key]=1
                (( npctile++ )) || true
            fi
            prereq_infile2="$f_pctl"
            ;;

        *)
            echo "    WARNING: unknown prereq_type '${prereq}' for ${idx}; skipping" >&2
            continue
            ;;
    esac

    # ------------------------------------------------------------------
    # Emit index commands based on output_frequency
    # ------------------------------------------------------------------
    case "$freq" in

        annual)
            emit "$INDEX_CMD" "$final_out" \
                "cdo -s ${op}${op_args} ${main_pipe}${extra_inputs} \
${prereq_infiles} ${final_out}"
            (( nindex++ )) || true
            ;;

        annual_loop)
            yr_outs=""
            for yr in $(seq "$SIM_START" "$SIM_END"); do
                yr_out="$ANNDIR/${idx}_${middle}_${yr}.nc"
                if [[ ${#vars[@]} -eq 1 ]]; then
                    conv="$(unit_conv "$units")"
                    yr_pipe="${conv:+${conv} }-selyear,${yr} -mergetime $(day_inputs "$primary_var")"
                    emit "$ANNUAL_CMD" "$yr_out" \
                        "cdo -s ${op} ${yr_pipe} ${prereq_infile2} ${yr_out}"
                else
                    yr_inputs=""
                    for (( i=0; i<${#vars[@]}; i++ )); do
                        v="${vars[$i]}"
                        if [[ "$v" == "sftlf" ]]; then
                            sf="$(find "$INDIR/sftlf.fx" -maxdepth 1 \
                                      -name "sftlf_*.nc" -type f | head -1)"
                            yr_inputs="${yr_inputs} ${sf}"
                        else
                            yr_inputs="${yr_inputs} -selyear,${yr} -mergetime $(day_inputs "$v")"
                        fi
                    done
                    emit "$ANNUAL_CMD" "$yr_out" \
                        "cdo -s ${op}${yr_inputs} ${prereq_infile2} ${yr_out}"
                fi
                yr_outs="${yr_outs} ${yr_out}"
                (( nannual++ )) || true
            done
            emit "$MERGE_CMD" "$final_out" \
                "cdo -s mergetime${yr_outs} ${final_out}"
            (( nmerge++ )) || true
            ;;

        *)
            echo "    WARNING: unknown output_frequency '${freq}' for ${idx}; skipping" >&2
            ;;
    esac

done < <(grep -v '^#' "$TSV")

# ---------------------------------------------------------------------------
# Remove empty commandfiles
# ---------------------------------------------------------------------------

for f in "$MINMAX_CMD" "$PCTILE_CMD" "$INDEX_CMD" "$ANNUAL_CMD" "$MERGE_CMD"; do
    [[ -f "$f" && ! -s "$f" ]] && rm "$f" || true
done

echo ""
echo "Commandfile generation complete."
echo "  TSV:                     $TSV"
echo "  Commandfiles:            $CMDDIR"
echo "  Reference files:         $PCTLDIR"
echo "  Per-year temp files:     $ANNDIR"
echo "  Final index files:       $OUTDIR"
printf "  Minmax: %d  Pctile: %d  Indices: %d  Annual: %d  Merge: %d\n" \
    $nminmax $npctile $nindex $nannual $nmerge
echo ""
echo "Run in order:"
[[ -f "$MINMAX_CMD" ]] && echo "  launch_multi --run RUNDIR/minmax  $MINMAX_CMD"
[[ -f "$PCTILE_CMD" ]] && echo "  launch_multi --run RUNDIR/pctile  $PCTILE_CMD"
[[ -f "$INDEX_CMD"  ]] && echo "  launch_multi --run RUNDIR/indices $INDEX_CMD"
[[ -f "$ANNUAL_CMD" ]] && echo "  launch_multi --run RUNDIR/annual  $ANNUAL_CMD"
[[ -f "$MERGE_CMD"  ]] && echo "  launch_multi --run RUNDIR/merge   $MERGE_CMD"
