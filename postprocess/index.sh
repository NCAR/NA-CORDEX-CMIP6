#!/bin/bash

# index.sh - Generate commandfiles for computing climate indices from
# compressed CORDEX-CMIP6 daily data.
#
# Operates on the output of compress.sh (Step 4), where data are organized
# into <var>.<freq> subdirectories (e.g., pr.day, tasmax.day).  Produces
# one NetCDF file per index covering all available years, written flat into
# OUTDIR (no per-variable subdirectories, since these files go to GIS).
#
# Index definitions are read from a TSV file (default: gis_indexes.tsv in
# the same directory as this script).  The TSV drives command construction;
# modifying the TSV is the intended way to add, remove, or change indices.
#
# Generates five commandfiles that must be run in order:
#
#   minmax.cmd   - ydrunmin/ydrunmax/timmin/timmax over the baseline period.
#                  Computed in native input units (no conversion).
#                  Required by pctile.cmd and (for bootstrap) indices.cmd.
#
#   pctile.cmd   - ydrunpctl/ydrunmean/timpctl reference files, also in
#                  native units.  Required by annual.cmd.
#
#   indices.cmd  - Indices whose CDO operators natively output annual time
#                  steps.  Unit conversion applied here, not in prereqs.
#                  Includes CDO_PCTL_NBINS export before bootstrapped cmds.
#
#   annual.cmd   - One command per year for operators that summarise over
#                  their entire input.  Each command operates on the single
#                  input file for that year (no mergetime needed since year
#                  boundaries align with file boundaries).  Per-year output
#                  goes to OUTDIR/annual/.
#
#   merge.cmd    - One mergetime per annual-loop index, assembling per-year
#                  files from OUTDIR/annual/ into OUTDIR.
#
# Per-year temporary files in OUTDIR/annual/ can be removed after merge.cmd.
#
# --- TSV columns ---
#
#   index          Output filename stem (e.g. TG10p)
#   cdo_operator   CDO operator or pipe fragment used verbatim in the command
#                  (e.g. eca_tg10p, yearsum -setrtomiss,,0)
#   input_vars     Input variable name(s).  Use + to separate multiple vars
#                  (e.g. tasmax+tasmin).  Special token "sftlf" resolves from
#                  <var>.fx rather than <var>.day.
#   units          Expected input units.  Drives inline unit conversion applied
#                  in index commands (not in prereq commands):
#                    mm/day  -> -mulc,86400  (pr arrives as kg m-2 s-1)
#                    octas   -> -mulc,0.08   (clt arrives as 0-100%)
#                    C       -> -subc,277.15 (tas arrives in K)
#                    other   -> no conversion
#   long_name      Human-readable name (informational only)
#   output_frequency
#                  annual      : operator natively outputs annual steps
#                  annual_loop : operator summarises entire input; needs
#                                per-year commands + mergetime
#   prereq_type    Prerequisite descriptor:
#                    none
#                    bootstrap:ydrunmin+ydrunmax,W
#                                     etccdi bootstrapped operator; generates
#                                     ydrunmin,W and ydrunmax,W in minmax.cmd;
#                                     appends ,BSTART,BEND to operator name;
#                                     passes minmax files as infile2/infile3
#                    ydrunmean,W      generates ydrunmean,W in pctile.cmd;
#                                     passed as infile2
#                    ydrunpctl,P,W    generates ydrunmin,W + ydrunmax,W in
#                                     minmax.cmd, ydrunpctl,P,W in pctile.cmd;
#                                     passed as infile2
#                    timpctl,P        generates timmin + timmax in minmax.cmd,
#                                     timpctl,P in pctile.cmd; passed as infile2
#                  All prereq files are computed in native input units.
#   description    Human-readable description (informational only)
#   notes          Freeform notes (informational only)
#
# --- Unit conversion ---
#
# The unit_conv() function is the single authority for conversion rules.
# Conversions are applied only in index commands, never in prereq commands.
# Prereq operators (min, max, mean, pctl) are all order-preserving or
# unit-consistent, so prereq files remain in native units and the index
# commands convert both the input data and interpret prereq files consistently.
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
#   --tsv FILE                    Index definitions TSV
#                                 (default: gis_indexes.tsv beside this script)
#   --baseline STARTYEAR-ENDYEAR  Reference period (default: 1991-2020)
#   --force                       Overwrite existing output files
#   -h, --help                    Show this help message

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TSV="${SCRIPT_DIR}/gis_indexes.tsv"
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

# CDO_PCTL_NBINS for etccdi bootstrapping: window * baseline_years * 2 + 2
PCTL_WINDOW=5
BASELINE_YEARS=$(( BASELINE_END - BASELINE_START + 1 ))
CDO_PCTL_NBINS=$(( PCTL_WINDOW * BASELINE_YEARS * 2 + 2 ))

echo "Baseline period: ${BASELINE_START}-${BASELINE_END}"
echo "CDO_PCTL_NBINS:  ${CDO_PCTL_NBINS}"

# ---------------------------------------------------------------------------
# Extract shared DRS middle components and simulation timespan
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

# Sorted list of all .nc files for a <var>.day variable, space-separated
day_inputs() {
    find "$INDIR/${1}.day" -maxdepth 1 -name "${1}_*.nc" -type f | sort | tr '\n' ' ' | sed 's/ $//'
}

# The single input file for a given variable and year (year must be contained
# within exactly one file; prints nothing if not found)
year_file() {
    local var="$1" yr="$2"
    local f base ts sy ey
    while IFS= read -r f; do
        base="$(basename "$f" .nc)"; ts="${base##*_}"
        sy="${ts:0:4}"; ey="${ts##*-}"; ey="${ey:0:4}"
        [[ $sy -le $yr && $ey -ge $yr ]] && { echo "$f"; return; }
    done < <(find "$INDIR/${var}.day" -maxdepth 1 -name "${var}_*.nc" \
                  -type f | sort)
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
    done < <(find "$vardir" -maxdepth 1 -name "${var}_*.nc" -type f | sort)
}

# Append a line to a commandfile; skip if outfile already exists (unless --force)
emit() {
    local cmdfile="$1" outfile="$2"; shift 2
    [[ -f "$outfile" && $FORCE -eq 0 ]] && return 0
    echo "$*" >> "$cmdfile"
}

# Return the CDO operator string for unit conversion, or empty string.
# This function is the single authority for unit conversion rules.
# Conversions are applied in index commands only, never in prereq commands.
unit_conv() {
    case "$1" in
        mm/day) echo "-mulc,86400"  ;;
        octas)  echo "-mulc,0.08"   ;;
        C)      echo "-subc,277.15" ;;
        *)      echo ""             ;;
    esac
}

# Build the full CDO input pipe for a variable over all years:
# "[conv] -mergetime file1 file2 ..."
# Unit conversion is included; used for native-annual index commands.
full_pipe() {
    local var="$1" units="$2"
    local inputs conv
    inputs="$(day_inputs "$var")"
    [[ -z "$inputs" ]] && { echo ""; return; }
    conv="$(unit_conv "$units")"
    [[ -n "$conv" ]] && echo "${conv} -mergetime ${inputs}" \
                     || echo "-mergetime ${inputs}"
}

# Build a baseline pipe for prereq commands: no unit conversion, selyear-bounded
# so the output time axis reflects exactly the baseline period.
baseline_pipe() {
    local var="$1" bl_inputs="$2"
    echo "-selyear,${BASELINE_START}/${BASELINE_END} -mergetime ${bl_inputs}"
}

# Canonical prereq filename: PCTLDIR/<var>_<stat>_<middle>_<bl_timespan>.nc
# Using the baseline timespan in the filename so it accurately reflects the
# period over which the prereq was computed.
BL_TIMESPAN="${BASELINE_START}0101-${BASELINE_END}1231"
pctl_file() { echo "$PCTLDIR/${1}_${2}_${middle}_${BL_TIMESPAN}.nc"; }

# Tracking sets to avoid emitting duplicate minmax/pctile commands
declare -A minmax_emitted=()
declare -A pctile_emitted=()

# Emit ydrunmin + ydrunmax for a variable into minmax.cmd (once per var)
# No unit conversion; all prereqs are computed in native units.
ensure_ydrunminmax() {
    local var="$1" bl_inputs="$2"
    local key="${var}_ydrun${PCTL_WINDOW}"
    [[ -n "${minmax_emitted[$key]:-}" ]] && return 0
    local f_min f_max bl_pipe
    f_min="$(pctl_file "$var" "ydrunmin${PCTL_WINDOW}")"
    f_max="$(pctl_file "$var" "ydrunmax${PCTL_WINDOW}")"
    bl_pipe="$(baseline_pipe "$var" "$bl_inputs")"
    emit "$MINMAX_CMD" "$f_min" \
        "cdo -s ydrunmin,${PCTL_WINDOW} ${bl_pipe} ${f_min}"
    emit "$MINMAX_CMD" "$f_max" \
        "cdo -s ydrunmax,${PCTL_WINDOW} ${bl_pipe} ${f_max}"
    minmax_emitted[$key]=1
    (( nminmax += 2 )) || true
}

# Emit timmin + timmax for a variable into minmax.cmd (once per var).
# No unit conversion, no wet-day masking: min/max are order-preserving so
# masking and conversion can be deferred to the pctile step.
ensure_timminmax() {
    local var="$1" bl_inputs="$2"
    local key="${var}_tim"
    [[ -n "${minmax_emitted[$key]:-}" ]] && return 0
    local f_min f_max bl_pipe
    f_min="$(pctl_file "$var" "timmin")"
    f_max="$(pctl_file "$var" "timmax")"
    bl_pipe="$(baseline_pipe "$var" "$bl_inputs")"
    emit "$MINMAX_CMD" "$f_min" "cdo -s timmin ${bl_pipe} ${f_min}"
    emit "$MINMAX_CMD" "$f_max" "cdo -s timmax ${bl_pipe} ${f_max}"
    minmax_emitted[$key]=1
    (( nminmax += 2 )) || true
}

nminmax=0; npctile=0; nindex=0; nannual=0; nmerge=0
nbins_emitted=0

# ---------------------------------------------------------------------------
# Main loop: read TSV and emit commands
# ---------------------------------------------------------------------------

while IFS=$'\t' read -r idx op input_vars units long_name freq prereq desc notes; do
    [[ "$idx" == "index" || -z "$idx" ]] && continue

    # Split input_vars on '+' into an array
    IFS='+' read -ra vars <<< "$input_vars"
    primary_var="${vars[0]}"

    # Check required inputs exist; silently skip if absent, warn for sftlf
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

    # Baseline file list for primary variable
    bl_inputs="$(baseline_inputs "$primary_var" | tr '\n' ' ' | sed 's/ $//')"
    if [[ "$prereq" != "none" && -z "$bl_inputs" ]]; then
        echo "    WARNING: no ${primary_var} files overlap baseline" \
             "${BASELINE_START}-${BASELINE_END}; skipping ${idx}" >&2
        continue
    fi

    final_out="$OUTDIR/${idx}_${middle}_${timespan}.nc"

    # ------------------------------------------------------------------
    # Parse prereq_type; emit minmax/pctile commands; set op_args and
    # prereq_infiles / prereq_infile2 for use in index commands below
    # ------------------------------------------------------------------
    op_args=""         # appended to operator name (bootstrap only)
    prereq_infiles=""  # infile2+infile3 for bootstrap (minmax pair)
    prereq_infile2=""  # infile2 for annual_loop (pctile/mean file)

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
                bl_pipe="$(baseline_pipe "$primary_var" "$bl_inputs")"
                emit "$PCTILE_CMD" "$f_mean" \
                    "cdo -s ydrunmean,${window} ${bl_pipe} ${f_mean}"
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
                bl_pipe="$(baseline_pipe "$primary_var" "$bl_inputs")"
                emit "$PCTILE_CMD" "$f_pctl" \
                    "cdo -s ydrunpctl,${pctl},${window} ${bl_pipe} ${f_min} ${f_max} ${f_pctl}"
                pctile_emitted[$key]=1
                (( npctile++ )) || true
            fi
            prereq_infile2="$f_pctl"
            ;;

        timpctl,*)
            pctl="${prereq##*,}"
            # timmin/timmax are computed in native units; no masking needed.
            # Wet-day masking (setrtomiss,,1) is applied in the timpctl step
            # so the percentile is over wet days only.
            ensure_timminmax "$primary_var" "$bl_inputs"
            f_min="$(pctl_file "$primary_var" "timmin")"
            f_max="$(pctl_file "$primary_var" "timmax")"
            f_pctl="$(pctl_file "$primary_var" "p${pctl}")"
            key="${primary_var}_timpctl${pctl}"
            if [[ -z "${pctile_emitted[$key]:-}" ]]; then
                bl_pipe="$(baseline_pipe "$primary_var" "$bl_inputs")"
                # Apply wet-day mask in the baseline pipe for pr so the
                # percentile is computed over wet days only
                if [[ "$primary_var" == "pr" ]]; then
                    bl_pipe="-setrtomiss,,1 ${bl_pipe}"
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
    # Emit index commands
    # ------------------------------------------------------------------
    case "$freq" in

        annual)
            # Build full multi-year input pipe with unit conversion
            main_pipe="$(full_pipe "$primary_var" "$units")"
            [[ -z "$main_pipe" ]] && continue

            # Secondary inputs (no unit conversion on secondary vars)
            extra_inputs=""
            for (( i=1; i<${#vars[@]}; i++ )); do
                v="${vars[$i]}"
                if [[ "$v" == "sftlf" ]]; then
                    sf="$(find "$INDIR/sftlf.fx" -maxdepth 1 \
                              -name "sftlf_*.nc" -type f | head -1)"
                    extra_inputs="${extra_inputs} ${sf}"
                else
                    extra_inputs="${extra_inputs} -mergetime $(day_inputs "$v")"
                fi
            done

            emit "$INDEX_CMD" "$final_out" \
                "cdo -s ${op}${op_args} ${main_pipe}${extra_inputs} ${prereq_infiles} ${final_out}"
            (( nindex++ )) || true
            ;;

        annual_loop)
            yr_outs=""
            conv="$(unit_conv "$units")"

            for yr in $(seq "$SIM_START" "$SIM_END"); do
                yr_out="$ANNDIR/${idx}_${middle}_${yr}.nc"

                if [[ ${#vars[@]} -eq 1 ]]; then
                    # Single-variable: use the one file that contains this year
                    yr_file="$(year_file "$primary_var" "$yr")"
                    if [[ -z "$yr_file" ]]; then
                        echo "    WARNING: no file found for ${primary_var} year ${yr}; skipping" >&2
                        continue
                    fi
                    yr_pipe="${conv:+${conv} }${yr_file}"
                    emit "$ANNUAL_CMD" "$yr_out" \
                        "cdo -s ${op} ${yr_pipe} ${prereq_infile2} ${yr_out}"
                else
                    # Multi-variable: build per-variable single-file inputs
                    yr_inputs=""
                    for (( i=0; i<${#vars[@]}; i++ )); do
                        v="${vars[$i]}"
                        if [[ "$v" == "sftlf" ]]; then
                            sf="$(find "$INDIR/sftlf.fx" -maxdepth 1 \
                                      -name "sftlf_*.nc" -type f | head -1)"
                            yr_inputs="${yr_inputs} ${sf}"
                        else
                            vf="$(year_file "$v" "$yr")"
                            if [[ -z "$vf" ]]; then
                                echo "    WARNING: no file found for ${v} year ${yr}; skipping ${idx} ${yr}" >&2
                                vf="MISSING"
                            fi
                            yr_inputs="${yr_inputs} ${vf}"
                        fi
                    done
                    emit "$ANNUAL_CMD" "$yr_out" \
                        "cdo -s ${op}${yr_inputs} ${prereq_infile2} ${yr_out}"
                fi

                yr_outs="${yr_outs} ${yr_out}"
                (( nannual++ )) || true
            done

            [[ -z "$yr_outs" ]] && continue
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
