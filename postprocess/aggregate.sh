#!/bin/bash

# aggregate_cordex.sh - Generate commandfiles for NA-CORDEX (WRF) aggregation
# Aggregates hourly -> daily ->monthly according to CORDEX-CMIP6 specs
# Also generates commandfiles to copy hourly files into the DRS output tree.

set -euo pipefail

# Default location for data request CSV
DR_CSV_DEFAULT="/glade/work/${USER}/cordex6/dreq_default.csv"
DR_CSV_URL="https://raw.githubusercontent.com/WCRP-CORDEX/data-request-table/main/data-request/dreq_default.csv"

# project id (esgf-qa want this as root of output tree)
PROJECT="cordex-cmip6"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR VERSION [CMDDIR]

Generate CDO commandfiles for aggregating CORDEX data to requested frequencies,
and cp commandfiles for placing hourly files into the DRS output tree.

Arguments:
  INDIR    Input directory containing variable subdirectories with yearly
           NetCDF files
  OUTDIR   Output directory tree root
  VERSION  Dataset version string (e.g. v20250101)
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force     Overwrite existing output files
  --csv PATH  Path to dreq_default.csv
              (default: $DR_CSV_DEFAULT)
  -h, --help  Show this help message

The script generates separate commandfiles per variable and frequency
(<var>.<freq>.cmd) for parallel execution via launch_cf.

Data request CSV can be downloaded from:
  $DR_CSV_URL
EOF
    exit 1
}

# Parse arguments
FORCE=0
DR_CSV="$DR_CSV_DEFAULT"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        --csv) DR_CSV="$2"; shift 2 ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 3 ]] && usage

INDIR="$(realpath "$1")"
OUTDIR="$(realpath "$2")"
VERSION="$3"
CMDDIR="${4:-.}"
CMDDIR="$(realpath "$CMDDIR")"

# Validate version string
if ! [[ "$VERSION" =~ ^v[0-9]{8}$ ]]; then
    echo "Error: VERSION must match vYYYYMMDD (e.g. v20250101), got: $VERSION" >&2
    exit 1
fi

# Validate inputs
[[ ! -d "$INDIR" ]] && { echo "Error: Input directory not found: $INDIR" >&2; exit 1; }
[[ ! -f "$DR_CSV" ]] && {
    cat >&2 <<EOF
Error: Data request CSV not found: $DR_CSV

Download it from:
  $DR_CSV_URL

Example:
  mkdir -p $(dirname "$DR_CSV_DEFAULT")
  curl -L -o "$DR_CSV_DEFAULT" "$DR_CSV_URL"
EOF
    exit 1
}

# Create output directories
mkdir -p "$OUTDIR" "$CMDDIR"

# Validate CSV layout (columns must match exactly; update if dreq format changes)
expected_header="out_name,frequency,units,long_name,standard_name,cell_methods,priority,comment"
[[ "$(head -1 "$DR_CSV")" != "$expected_header" ]] && {
    echo "Error: Unexpected CSV header in $DR_CSV" >&2
    echo "  Expected: $expected_header" >&2
    exit 1
}

# Aggregatable variables (non-fx): used to skip irrelevant input directories
AGGVARS=$(tail -n +2 "$DR_CSV" | grep -v ',fx,' | cut -f1 -d, | sort -u)

# Hourly instantaneous variables: need time/time_bnds fixup after daily aggregation.
# time units are assumed to be 'days since ...' throughout (guaranteed by cmorize.compress.py).
POINTVARS=$(awk -F',' 'NR>1 && $2 ~ /hr/ && $6 ~ /time: point/ {print $1}' "$DR_CSV")

# Function to parse CORDEX filename and extract DRS components
parse_filename() {
    local fname="$1"
    local base="${fname%.nc}"
    
    # Format: var_domain_drvsrc_drvexp_drvvar_inst_src_verreal_freq[_time]
    IFS='_' read -r var domain drvsrc drvexp drvvar inst src verreal freq timespan <<< "$base"
    
    echo "$var" "$domain" "$drvsrc" "$drvexp" "$drvvar" "$inst" "$src" "$verreal" "$freq" "$timespan"
}

# Function to extract year from timespan
# (YYYY or YYYYMM-YYYYMM or YYYYMMDDhhmm-YYYYMMDDhhmm)
extract_year() {
    local timespan="$1"
    echo "${timespan:0:4}"
}

# Function to generate output directory path
gen_outdir
    local drvsrc="$1" drvexp="$2" drvvar="$3" src="$4" verreal="$5" freq="$6" var="$7"
    echo "$OUTDIR/$PROJECT/$drvsrc/$drvexp/$drvvar/$src/$verreal/$freq/$var/$VERSION"
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

# Process each variable directory
echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue
    
    varname=$(basename "$vardir")
    
    # Check if variable is in data request
    echo "$AGGVARS" | grep -qw "$varname" || {
        echo "$varname is not a variable in data request, skipping" >&2
        continue
    }
    
    # Find all NetCDF files
    files=("$vardir"/*.nc)
    [[ ! -f "${files[0]}" ]] && {
        echo "Warning: No NetCDF files found for $varname" >&2
        continue
    }
    
    echo "Processing variable: $varname"
    
    # Parse first file to get DRS components
    fname=$(basename "${files[0]}")
    read -r var domain drvsrc drvexp drvvar inst src verreal infreq timespan <<< "$(parse_filename "$fname")"
    
    # Determine years available
    declare -A yearfiles
    minyear=9999
    maxyear=0
    
    for f in "${files[@]}"; do
        fname=$(basename "$f")
        [[ ! "$fname" =~ \.nc$ ]] && continue
        
        read -r _ _ _ _ _ _ _ _ _ timespan <<< "$(parse_filename "$fname")"
        [[ -z "$timespan" ]] && continue
        
        year=$(extract_year "$timespan")
        yearfiles[$year]="$f"
        
        [[ $year -lt $minyear ]] && minyear=$year
        [[ $year -gt $maxyear ]] && maxyear=$year
    done
    
    [[ $minyear -eq 9999 ]] && {
        echo "Warning: Could not determine year range for $varname" >&2
        continue
    }
    
    echo "  Years available: $minyear-$maxyear"

    # Generate hourly copy commandfile if input is hourly
    if [[ "$infreq" =~ hr$ ]]; then
        hr_cmdfile="$CMDDIR/${varname}.hr.cmd"
        > "$hr_cmdfile"

        for year in $(seq "$minyear" "$maxyear"); do
            [[ -z "${yearfiles[$year]:-}" ]] && continue
            src_file="${yearfiles[$year]}"
            outdir="$(gen_outdir "$drvsrc" "$drvexp" "$drvvar" "$src" "$verreal" "$infreq" "$varname")"
            outfile="$(basename "$src_file")"
            outpath="$outdir/$outfile"

            if [[ -f "$outpath" && $FORCE -eq 0 ]]; then
                continue
            fi

            mkdir -p "$outdir"
            echo "cp $src_file $outpath" >> "$hr_cmdfile"
        done

        [[ ! -s "$hr_cmdfile" ]] && rm "$hr_cmdfile"
    fi
    
    # Process each target frequency (day, mon) and check if inputs exist
    for freq in day mon; do
        cmdfile="$CMDDIR/${varname}.${freq}.cmd"
        > "$cmdfile"  # Truncate/create
        
        # Determine where input files should be for this frequency
        declare -A freq_yearfiles=()
        freq_minyear=9999
        freq_maxyear=0
        
        if [[ "$freq" == "day" ]]; then
            if [[ "$infreq" == "day" ]]; then
                # Input is already daily: merge into DRS tree in 5-year chunks
                for year in "${!yearfiles[@]}"; do
                    freq_yearfiles[$year]="${yearfiles[$year]}"
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
            elif [[ "$infreq" =~ hr$ ]]; then
                # Input is hourly: aggregate to daily
                for year in "${!yearfiles[@]}"; do
                    freq_yearfiles[$year]="${yearfiles[$year]}"
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
            else
                # Input is neither hourly nor daily, skip
                rm "$cmdfile"
                continue
            fi
        elif [[ "$freq" == "mon" ]]; then
            # Monthly aggregation: need daily input files
            # Check input directory first (if input is already daily)
            if [[ "$infreq" == "day" ]]; then
                for year in "${!yearfiles[@]}"; do
                    freq_yearfiles[$year]="${yearfiles[$year]}"
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
            else
                # Look for daily output files from previous aggregation
                daily_outdir="$(gen_outdir "$drvsrc" "$drvexp" "$drvvar" "$src" "$verreal" "day" "$varname")"
                daily_pattern="${varname}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_day_*.nc"
                
                shopt -s nullglob
                for f in "$daily_outdir"/$daily_pattern; do
                    [[ ! -f "$f" ]] && continue
                    fname=$(basename "$f")
                    read -r _ _ _ _ _ _ _ _ _ timespan <<< "$(parse_filename "$fname")"
                    [[ -z "$timespan" ]] && continue
                    year=$(extract_year "$timespan")
                    freq_yearfiles[$year]="$f"
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
                shopt -u nullglob
            fi
        fi
        
        # Skip if no input files found for this frequency
        if [[ "${#freq_yearfiles[@]}" -eq 0 ]]; then
            rm "$cmdfile"
            continue
        fi
        
        echo "  Generating $cmdfile"
        echo "    Input years available: $freq_minyear-$freq_maxyear"
        
        # Compute output ranges
        mapfile -t ranges < <(compute_ranges "$freq" "$freq_minyear" "$freq_maxyear")
        
        nskipped=0
        ncommands=0
        
        for range in "${ranges[@]}"; do
            IFS='-' read -r startyear endyear <<< "$range"
            
            # Collect input files for this range
            infiles=()
            for ((year=startyear; year<=endyear; year++)); do
                [[ -n "${freq_yearfiles[$year]:-}" ]] && infiles+=("${freq_yearfiles[$year]}")
            done
            
            [[ ${#infiles[@]} -eq 0 ]] && continue
            
            # Generate output path and filename
            outdir="$(gen_outdir "$drvsrc" "$drvexp" "$drvvar" "$src" "$verreal" "$freq" "$varname")"
            outfile="$(gen_outfile "$varname" "$domain" "$drvsrc" "$drvexp" "$drvvar" "$inst" "$src" "$verreal" "$freq" "$startyear" "$endyear")"
            outpath="$outdir/$outfile"
            
            ((ncommands++)) || true
            
            # Check for existing files
            if [[ -f "$outpath" && $FORCE -eq 0 ]]; then
                ((nskipped++)) || true
                continue
            fi
            
            # Determine CDO operator and generate command.
            # For instantaneous (time: point) variables aggregated to daily,
            # settbounds,hour ensures correct [day 00:00, day+1 00:00] bounds,
            # and ncap2 shifts the time coordinate from 11:30 to 12:00.
            # bash -c ensures && semantics regardless of the user's default shell.
            # Time units are assumed to be 'days since ...' throughout (guaranteed by cmorize.compress.py).
            mkdir -p "$outdir"
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
        
        # Report stats
        ngenerated=$((ncommands - nskipped))
        echo "    Generated $ngenerated commands, skipped $nskipped/$ncommands existing files"
        
        # Clean up empty commandfile
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
