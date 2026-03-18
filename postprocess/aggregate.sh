#!/bin/bash

# aggregate.sh - Generate commandfiles for NA-CORDEX (WRF) aggregation
# Aggregates hourly -> daily -> monthly according to CORDEX-CMIP6 specs.
# Also generates commandfiles to copy hourly and fx files into the output tree.
#
# Output is organized in per-variable subdirectories named <var>.<freq>.
# Use relocate.sh to move the results into a CORDEX DRS output tree.
#
# The data request CSV (dreq_default.csv) is expected in INDIR, where it is
# placed by format.sh.

set -euo pipefail

# URL shown in help if CSV is missing from INDIR
DR_CSV_URL="https://raw.githubusercontent.com/WCRP-CORDEX/data-request-table/main/data-request/dreq_default.csv"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR [CMDDIR]

Generate CDO commandfiles for aggregating CORDEX data to requested frequencies,
and cp commandfiles for placing hourly and fx files into the output tree.

Arguments:
  INDIR    Input directory containing <var>.<freq> subdirectories with yearly
           NetCDF files; must also contain dreq_default.csv (placed here
           by format.sh)
  OUTDIR   Output directory root (subdirs named <var>.<freq> created here)
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force     Overwrite existing output files
  --csv PATH  Path to dreq_default.csv (overrides INDIR lookup)
  -h, --help  Show this help message

The script generates separate commandfiles per variable and frequency
(<var>.<freq>.cmd) for parallel execution via launch_cf.
EOF
    exit 1
}

# Parse arguments
FORCE=0
CSV_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        --csv) CSV_OVERRIDE="$2"; shift 2 ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 2 ]] && usage

INDIR="$(realpath "$1")"
mkdir -p "$2"
OUTDIR="$(realpath "$2")"
CMDDIR="${3:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

# Resolve CSV path: explicit override, or look in INDIR (placed there by format.sh)
if [[ -n "$CSV_OVERRIDE" ]]; then
    DR_CSV="$CSV_OVERRIDE"
else
    DR_CSV="$INDIR/dreq_default.csv"
fi

# Validate inputs
[[ ! -d "$INDIR" ]] && { echo "Error: Input directory not found: $INDIR" >&2; exit 1; }
[[ ! -f "$DR_CSV" ]] && {
    cat >&2 <<EOF
Error: Data request CSV not found: $DR_CSV

Re-run format.sh with the same OUTDIR, or download manually:
  curl -L -o "$DR_CSV" "$DR_CSV_URL"
EOF
    exit 1
}

# Validate CSV layout (columns must match exactly; update if dreq format changes)
expected_header="out_name,frequency,units,long_name,standard_name,cell_methods,priority,comment"
[[ "$(head -1 "$DR_CSV")" != "$expected_header" ]] && {
    echo "Error: Unexpected CSV header in $DR_CSV" >&2
    echo "  Expected: $expected_header" >&2
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"

# Aggregatable variables (non-fx): used to skip irrelevant input directories
AGGVARS=$(tail -n +2 "$DR_CSV" | grep -v ',fx,' | cut -f1 -d, | sort -u)

# Hourly instantaneous variables: need time/time_bnds fixup after daily aggregation.
# time units are assumed to be 'days since ...' throughout (guaranteed by cmorize.sh).
POINTVARS=$(awk -F',' 'NR>1 && $2 ~ /hr/ && $6 ~ /time: point/ {print $1}' "$DR_CSV")

# Function to extract year from CORDEX filename timespan field
# (YYYYMMDDhhmm-YYYYMMDDhhmm or YYYYMMDD-YYYYMMDD or YYYYMM-YYYYMM)
extract_year() {
    local timespan="$1"
    echo "${timespan:0:4}"
}

# Function to generate output filename
gen_outfile() {
    local var="$1" domain="$2" drvsrc="$3" drvexp="$4" drvvar="$5" inst="$6"
    local src="$7" verreal="$8" freq="$9" startyear="${10}" endyear="${11}"

    local startdate enddate
    case "$freq" in
        mon) startdate="${startyear}01"; enddate="${endyear}12" ;;
        day) startdate="${startyear}0101"; enddate="${endyear}1231" ;;
        *hr) startdate="${startyear}01010000"; enddate="${endyear}12312300" ;;
        fx) echo "${var}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_${freq}.nc"; return ;;
    esac

    echo "${var}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_${freq}_${startdate}-${enddate}.nc"
}

# Function to compute year ranges for output files
compute_ranges() {
    local freq="$1" minyear="$2" maxyear="$3"
    local -a ranges

    case "$freq" in
        mon)
            # 10-year chunks: start at year ending in 1 (or minyear),
            # end at year ending in 0 (or maxyear)
            local start=$(( (minyear/10)*10 + 1 ))
            [[ $start -gt $minyear ]] && start=$minyear

            while [[ $start -le $maxyear ]]; do
                local end=$(( (start/10)*10 + 10 ))
                [[ $end -gt $maxyear ]] && end=$maxyear
                ranges+=("$start-$end")
                start=$((end + 1))
            done
            ;;
        day)
            # 5-year chunks: start at year ending in 1 or 6 (or minyear),
            # end at year ending in 5 or 0 (or maxyear)
            local start=$(( (minyear/5)*5 + 1 ))
            [[ $start -gt $minyear ]] && start=$minyear

            while [[ $start -le $maxyear ]]; do
                local end=$((start + 4))
                [[ $end -gt $maxyear ]] && end=$maxyear
                ranges+=("$start-$end")
                start=$((end + 1))
            done
            ;;
        *hr)
            # 1-year chunks
            for ((year=minyear; year<=maxyear; year++)); do
                ranges+=("$year-$year")
            done
            ;;
    esac

    printf "%s\n" "${ranges[@]}"
}

echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue

    # Input subdirs from format/compress are flat <var> dirs.
    # infreq is determined from the first filename below.
    varname="$(basename "$vardir")"

    # Find all NetCDF files
    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && {
        echo "Warning: No NetCDF files found in $vardir" >&2
        continue
    }

    # Parse first file to get DRS components and input frequency from filename
    fname=$(basename "${files[0]}")
    # Format: var_domain_drvsrc_drvexp_drvvar_inst_src_verreal_freq_timespan.nc
    base="${fname%.nc}"
    IFS='_' read -r _var domain drvsrc drvexp drvvar inst src verreal infreq timespan <<< "$base"

    # fx variables: just copy into <var>.fx subdir
    if [[ "$infreq" == "fx" ]]; then
        fx_cmdfile="$CMDDIR/${varname}.fx.cmd"
        > "$fx_cmdfile"

        for f in "${files[@]}"; do
            [[ ! -f "$f" ]] && continue
            outdir_var="$OUTDIR/${varname}.fx"
            outpath="$outdir_var/$(basename "$f")"
            if [[ -f "$outpath" && $FORCE -eq 0 ]]; then
                continue
            fi
            mkdir -p "$outdir_var"
            echo "cp $f $outpath" >> "$fx_cmdfile"
        done

        [[ ! -s "$fx_cmdfile" ]] && rm "$fx_cmdfile"
        continue
    fi

    # Check if variable is in data request
    echo "$AGGVARS" | grep -qw "$varname" || {
        echo "$varname is not a variable in data request, skipping" >&2
        continue
    }

    echo "Processing: $varname ($infreq)"

    # Determine years available
    declare -A yearfiles
    minyear=9999
    maxyear=0

    for f in "${files[@]}"; do
        fname=$(basename "$f")
        [[ ! "$fname" =~ \.nc$ ]] && continue
        base="${fname%.nc}"
        timespan="${base##*_}"
        [[ -z "$timespan" ]] && continue
        year=$(extract_year "$timespan")
        yearfiles[$year]="$f"
        [[ $year -lt $minyear ]] && minyear=$year
        [[ $year -gt $maxyear ]] && maxyear=$year
    done

    [[ $minyear -eq 9999 ]] && {
        echo "Warning: Could not determine year range for $dirname" >&2
        continue
    }

    echo "  Years available: $minyear-$maxyear ($varname)"

    # Generate hourly copy commandfile if input is hourly
    if [[ "$infreq" =~ hr$ ]]; then
        hr_cmdfile="$CMDDIR/${varname}.${infreq}.cmd"
        > "$hr_cmdfile"

        for year in $(seq "$minyear" "$maxyear"); do
            [[ -z "${yearfiles[$year]:-}" ]] && continue
            src_file="${yearfiles[$year]}"
            outdir_var="$OUTDIR/${varname}.${infreq}"
            outpath="$outdir_var/$(basename "$src_file")"

            if [[ -f "$outpath" && $FORCE -eq 0 ]]; then
                continue
            fi

            mkdir -p "$outdir_var"
            echo "cp $src_file $outpath" >> "$hr_cmdfile"
        done

        [[ ! -s "$hr_cmdfile" ]] && rm "$hr_cmdfile"
    fi

    # Process each target frequency (day, mon)
    for freq in day mon; do
        cmdfile="$CMDDIR/${varname}.${freq}.cmd"
        > "$cmdfile"

        # Determine input files for this frequency
        declare -A freq_yearfiles=()
        freq_minyear=9999
        freq_maxyear=0

        if [[ "$freq" == "day" ]]; then
            if [[ "$infreq" == "day" || "$infreq" =~ hr$ ]]; then
                for year in "${!yearfiles[@]}"; do
                    freq_yearfiles[$year]="${yearfiles[$year]}"
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
            else
                rm "$cmdfile"
                continue
            fi
        elif [[ "$freq" == "mon" ]]; then
            if [[ "$infreq" == "day" ]]; then
                for year in "${!yearfiles[@]}"; do
                    freq_yearfiles[$year]="${yearfiles[$year]}"
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
            else
                # Look for daily output files from previous aggregation
                daily_dir="$OUTDIR/${varname}.day"
                daily_pattern="${varname}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_day_*.nc"

                shopt -s nullglob
                for f in "$daily_dir"/$daily_pattern; do
                    [[ ! -f "$f" ]] && continue
                    base="${f%.nc}"
                    timespan="${base##*_}"
                    [[ -z "$timespan" ]] && continue
                    year=$(extract_year "$timespan")
                    freq_yearfiles[$year]="$f"
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
                shopt -u nullglob
            fi
        fi

        if [[ "${#freq_yearfiles[@]}" -eq 0 ]]; then
            rm "$cmdfile"
            continue
        fi

        echo "  Generating $cmdfile"
        echo "    Input years available: $freq_minyear-$freq_maxyear"

        mapfile -t ranges < <(compute_ranges "$freq" "$freq_minyear" "$freq_maxyear")

        nskipped=0
        ncommands=0

        for range in "${ranges[@]}"; do
            IFS='-' read -r startyear endyear <<< "$range"

            infiles=()
            for ((year=startyear; year<=endyear; year++)); do
                [[ -n "${freq_yearfiles[$year]:-}" ]] && infiles+=("${freq_yearfiles[$year]}")
            done

            [[ ${#infiles[@]} -eq 0 ]] && continue

            outdir_var="$OUTDIR/${varname}.${freq}"
            outfile="$(gen_outfile "$varname" "$domain" "$drvsrc" "$drvexp" "$drvvar" "$inst" "$src" "$verreal" "$freq" "$startyear" "$endyear")"
            outpath="$outdir_var/$outfile"

            ((ncommands++)) || true

            if [[ -f "$outpath" && $FORCE -eq 0 ]]; then
                ((nskipped++)) || true
                continue
            fi

            mkdir -p "$outdir_var"

            # Determine CDO operator and generate command.
            # For instantaneous (time: point) variables aggregated to daily,
            # settbounds,hour ensures correct [day 00:00, day+1 00:00] bounds,
            # and ncap2 shifts the time coordinate from 11:30 to 12:00.
            # bash -c ensures && semantics regardless of the user's default shell.
            # Time units are assumed to be 'days since ...' throughout (guaranteed by cmorize.sh).
            case "$freq" in
                mon)
                    echo "cdo monmean -mergetime ${infiles[*]} $outpath" >> "$cmdfile"
                    ;;
                day)
                    if [[ "$infreq" == "day" ]]; then
                        echo "cdo mergetime ${infiles[*]} $outpath" >> "$cmdfile"
                    elif echo "$POINTVARS" | grep -qw "$varname"; then
                        ncap2_expr='time=time+0.5/24.0'
                        echo "bash -c \"cdo daymean -settbounds,hour -mergetime ${infiles[*]} $outpath && ncap2 -A -s '$ncap2_expr' $outpath\"" >> "$cmdfile"
                    else
                        echo "cdo daymean -mergetime ${infiles[*]} $outpath" >> "$cmdfile"
                    fi
                    ;;
                *) echo "Error: Unknown frequency $freq" >&2; exit 1 ;;
            esac
        done

        ngenerated=$((ncommands - nskipped))
        echo "    Generated $ngenerated commands, skipped $nskipped/$ncommands existing files"

        [[ ! -s "$cmdfile" ]] && rm "$cmdfile"

    done

    unset yearfiles
done

echo ""
echo "Commandfile generation complete!"
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "To run with launch_cf:"
echo "  launch_cf <varname>.<freq>.cmd"
