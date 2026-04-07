#!/bin/bash
#
# gen_climate_index_cmds.sh - Generate CDO commandfiles for climate indices
#
# Usage: gen_climate_index_cmds.sh INDIR OUTDIR CMDDIR
#
#   INDIR   Parent directory containing per-variable subdirectories
#           (e.g., core/day/ containing pr/, tas/, tasmax/, tasmin/, etc.)
#   OUTDIR  Output directory (subdirs created per index)
#   CMDDIR  Directory where commandfiles are written
#
# Input data is assumed to be CORDEX-compliant:
#   - One variable per file, one variable per directory
#   - Directory names match variable names
#   - Precipitation in kg m-2 s-1 (converted to mm/day)
#   - Temperature in Kelvin
#
# Indices are calculated on an annual basis, one output file per input file.

set -e

usage() {
    echo "Usage: $(basename "$0") INDIR OUTDIR CMDDIR"
    echo ""
    echo "  INDIR   Parent directory containing per-variable subdirectories"
    echo "  OUTDIR  Output directory (subdirs created per index)"
    echo "  CMDDIR  Directory where commandfiles are written"
    exit 1
}

[[ $# -ne 3 ]] && usage

INDIR="$1"
OUTDIR="$2"
CMDDIR="$3"

[[ -d "$INDIR" ]] || { echo "Error: INDIR not found: $INDIR"; exit 1; }

# Create cmddir if needed
mkdir -p "$CMDDIR"

# Helper: get files for a variable (empty if variable dir doesn't exist)
get_varfiles() {
    local var="$1"
    local vardir="$INDIR/$var"
    if [[ -d "$vardir" ]]; then
        find "$vardir" -maxdepth 1 -name "${var}_*.nc" -type f | sort
    fi
}

# Helper: transform input filename to output filename
# e.g., pr_EUR-11_... -> cdd_EUR-11_...
make_outfile() {
    local infile="$1"
    local oldvar="$2"
    local newindex="$3"
    local outdir="$4"
    local base
    base="$(basename "$infile")"
    echo "${outdir}/${base/${oldvar}_/${newindex}_}"
}

# Helper: append a command to the appropriate commandfile
# Commands are grouped by index
emit() {
    local index="$1"
    local cmd="$2"
    echo "$cmd" >> "$CMDDIR/${index}.cmd"
}

# Clear any existing commandfiles
rm -f "$CMDDIR"/*.cmd

#=============================================================================
# PRECIPITATION INDICES (pr)
#=============================================================================
# pr is in kg m-2 s-1, need mm/day: multiply by 86400

pr_files=$(get_varfiles pr)

if [[ -n "$pr_files" ]]; then
    for f in $pr_files; do
        # CDD - Consecutive dry days (etccdi version)
        out=$(make_outfile "$f" pr cdd "$OUTDIR/cdd")
        emit cdd "cdo -s etccdi_cdd -mulc,86400 $f $out"

        # CWD - Consecutive wet days (etccdi version)
        out=$(make_outfile "$f" pr cwd "$OUTDIR/cwd")
        emit cwd "cdo -s etccdi_cwd -mulc,86400 $f $out"

        # RR1 / R1mm - Wet days (etccdi version)
        out=$(make_outfile "$f" pr r1mm "$OUTDIR/r1mm")
        emit r1mm "cdo -s etccdi_r1mm -mulc,86400 $f $out"

        # R10mm - Heavy precipitation days
        out=$(make_outfile "$f" pr r10mm "$OUTDIR/r10mm")
        emit r10mm "cdo -s eca_r10mm -mulc,86400 $f $out"

        # R20mm - Very heavy precipitation days
        out=$(make_outfile "$f" pr r20mm "$OUTDIR/r20mm")
        emit r20mm "cdo -s eca_r20mm -mulc,86400 $f $out"

        # RX1day - Max 1-day precipitation (etccdi version)
        out=$(make_outfile "$f" pr rx1day "$OUTDIR/rx1day")
        emit rx1day "cdo -s etccdi_rx1day -mulc,86400 $f $out"

        # RX5day - Max 5-day precipitation (etccdi version)
        out=$(make_outfile "$f" pr rx5day "$OUTDIR/rx5day")
        emit rx5day "cdo -s etccdi_rx5day -mulc,86400 $f $out"

        # SDII - Simple daily intensity index
        out=$(make_outfile "$f" pr sdii "$OUTDIR/sdii")
        emit sdii "cdo -s eca_sdii -mulc,86400 $f $out"

        # PRCPTOT - Annual total precipitation
        out=$(make_outfile "$f" pr prcptot "$OUTDIR/prcptot")
        emit prcptot "cdo -s yearsum -mulc,86400 $f $out"
    done
fi

#=============================================================================
# TASMIN INDICES (daily minimum temperature)
#=============================================================================

tasmin_files=$(get_varfiles tasmin)

if [[ -n "$tasmin_files" ]]; then
    for f in $tasmin_files; do
        # FD - Frost days (etccdi version)
        out=$(make_outfile "$f" tasmin fd "$OUTDIR/fd")
        emit fd "cdo -s etccdi_fd $f $out"

        # TR - Tropical nights (etccdi version)
        out=$(make_outfile "$f" tasmin tr "$OUTDIR/tr")
        emit tr "cdo -s etccdi_tr $f $out"

        # CFD - Consecutive frost days
        out=$(make_outfile "$f" tasmin cfd "$OUTDIR/cfd")
        emit cfd "cdo -s eca_cfd $f $out"

        # TNn - Min of daily min temperature
        out=$(make_outfile "$f" tasmin tnn "$OUTDIR/tnn")
        emit tnn "cdo -s yearmin $f $out"

        # TNx - Max of daily min temperature
        out=$(make_outfile "$f" tasmin tnx "$OUTDIR/tnx")
        emit tnx "cdo -s yearmax $f $out"
    done
fi

#=============================================================================
# TASMAX INDICES (daily maximum temperature)
#=============================================================================

tasmax_files=$(get_varfiles tasmax)

if [[ -n "$tasmax_files" ]]; then
    for f in $tasmax_files; do
        # SU - Summer days (etccdi version)
        out=$(make_outfile "$f" tasmax su "$OUTDIR/su")
        emit su "cdo -s etccdi_su $f $out"

        # ID - Ice days (etccdi version)
        out=$(make_outfile "$f" tasmax id "$OUTDIR/id")
        emit id "cdo -s etccdi_id $f $out"

        # CSU - Consecutive summer days
        out=$(make_outfile "$f" tasmax csu "$OUTDIR/csu")
        emit csu "cdo -s eca_csu $f $out"

        # TXx - Max of daily max temperature
        out=$(make_outfile "$f" tasmax txx "$OUTDIR/txx")
        emit txx "cdo -s yearmax $f $out"

        # TXn - Min of daily max temperature
        out=$(make_outfile "$f" tasmax txn "$OUTDIR/txn")
        emit txn "cdo -s yearmin $f $out"
    done
fi

#=============================================================================
# TAS INDICES (daily mean temperature)
#=============================================================================

tas_files=$(get_varfiles tas)

if [[ -n "$tas_files" ]]; then
    for f in $tas_files; do
        # HD - Heating degree days
        out=$(make_outfile "$f" tas hd "$OUTDIR/hd")
        emit hd "cdo -s eca_hd $f $out"

        # Annual mean temperature
        out=$(make_outfile "$f" tas tas_yearmean "$OUTDIR/tas_yearmean")
        emit tas_yearmean "cdo -s yearmean $f $out"
    done
fi

#=============================================================================
# MULTI-VARIABLE INDICES (tasmax + tasmin)
#=============================================================================

# For these, we need matching files from both variables
# Assuming filenames have identical structure except for variable name

if [[ -n "$tasmax_files" && -n "$tasmin_files" ]]; then
    for txf in $tasmax_files; do
        # Derive corresponding tasmin file
        txbase=$(basename "$txf")
        tnbase="${txbase/tasmax/tasmin}"
        tnf="$INDIR/tasmin/$tnbase"
        
        if [[ -f "$tnf" ]]; then
            # ETR - Extreme temperature range
            out=$(make_outfile "$txf" tasmax etr "$OUTDIR/etr")
            emit etr "cdo -s eca_etr $txf $tnf $out"

            # DTR - Mean diurnal temperature range
            out=$(make_outfile "$txf" tasmax dtr "$OUTDIR/dtr")
            emit dtr "cdo -s yearmean -sub $txf $tnf $out"
        fi
    done
fi

#=============================================================================
# WIND INDICES (sfcWind)
#=============================================================================

sfcwind_files=$(get_varfiles sfcWind)

if [[ -n "$sfcwind_files" ]]; then
    for f in $sfcwind_files; do
        # FG - Mean wind speed
        out=$(make_outfile "$f" sfcWind fg "$OUTDIR/fg")
        emit fg "cdo -s yearmean $f $out"

        # FGcalm - Calm days (wind <= 2 m/s)
        out=$(make_outfile "$f" sfcWind fgcalm "$OUTDIR/fgcalm")
        emit fgcalm "cdo -s yearsum -lec,2 $f $out"

        # FG6Bft - Days with wind >= 10.8 m/s (6 Beaufort)
        out=$(make_outfile "$f" sfcWind fg6bft "$OUTDIR/fg6bft")
        emit fg6bft "cdo -s yearsum -gec,10.8 $f $out"
    done
fi

#=============================================================================
# HUMIDITY INDICES (hurs)
#=============================================================================

hurs_files=$(get_varfiles hurs)

if [[ -n "$hurs_files" ]]; then
    for f in $hurs_files; do
        # RH - Mean relative humidity
        out=$(make_outfile "$f" hurs rh "$OUTDIR/rh")
        emit rh "cdo -s yearmean $f $out"
    done
fi

#=============================================================================
# PRESSURE INDICES (psl)
#=============================================================================

psl_files=$(get_varfiles psl)

if [[ -n "$psl_files" ]]; then
    for f in $psl_files; do
        # PP - Mean sea level pressure
        out=$(make_outfile "$f" psl pp "$OUTDIR/pp")
        emit pp "cdo -s yearmean $f $out"
    done
fi

#=============================================================================
# RADIATION INDICES (rsds)
#=============================================================================

rsds_files=$(get_varfiles rsds)

if [[ -n "$rsds_files" ]]; then
    for f in $rsds_files; do
        # Mean downwelling shortwave
        out=$(make_outfile "$f" rsds rsds_yearmean "$OUTDIR/rsds_yearmean")
        emit rsds_yearmean "cdo -s yearmean $f $out"
    done
fi

#=============================================================================
# CREATE OUTPUT DIRECTORIES
#=============================================================================

for cmd in "$CMDDIR"/*.cmd; do
    [[ -f "$cmd" ]] || continue
    idx=$(basename "$cmd" .cmd)
    mkdir -p "$OUTDIR/$idx"
done

#=============================================================================
# SUMMARY
#=============================================================================

echo "Commandfiles written to: $CMDDIR"
echo "Output directories created in: $OUTDIR"
echo ""
echo "Indices generated:"
echo ""

# Count commands per index
for cmd in "$CMDDIR"/*.cmd; do
    [[ -f "$cmd" ]] || continue
    idx=$(basename "$cmd" .cmd)
    n=$(wc -l < "$cmd")
    echo "  $idx: $n commands"
done
