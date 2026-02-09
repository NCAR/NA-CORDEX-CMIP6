#!/bin/bash

# aggregate_cordex.sh - Generate commandfiles for aggregating NA-CORDEX WRF output
# Aggregates hourly→daily and daily→monthly according to CORDEX-CMIP6 specifications

set -euo pipefail

# Default location for data request CSV
DR_CSV_DEFAULT="/glade/work/${USER}/cordex6/dreq_default.csv"
DR_CSV_URL="https://raw.githubusercontent.com/WCRP-CORDEX/data-request-table/main/data-request/dreq_default.csv"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR [CMDDIR]

Generate CDO commandfiles for aggregating CORDEX data to requested frequencies.

Arguments:
  INDIR   Input directory containing variable subdirectories with yearly
          NetCDF files
  OUTDIR  Root output directory (starts at driving_source_id level)
  CMDDIR  Directory for commandfiles (default: current directory)

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

[[ $# -lt 2 ]] && usage

INDIR="$(realpath "$1")"
OUTDIR="$(realpath "$2")"
CMDDIR="${3:-.}"
CMDDIR="$(realpath "$CMDDIR")"

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

# Read variable frequencies from CSV (skip header, columns 1 and 2)
# Note: variables can have multiple requested frequencies
declare -A varfreq
while IFS=, read -r var freq rest; do
    [[ "$var" == "out_name" ]] && continue  # Skip header
    [[ -z "$var" || -z "$freq" ]] && continue
    if [[ -n "${varfreq[$var]:-}" ]]; then
        varfreq["$var"]+=" $freq"
    else
        varfreq["$var"]="$freq"
    fi
done < "$DR_CSV"

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

# Function to generate output directory path (without version element)
gen_outdir() {
    local drvsrc="$1" drvexp="$2" drvvar="$3" src="$4" verreal="$5" freq="$6" var="$7"
    echo "$OUTDIR/$drvsrc/$drvexp/$drvvar/$src/$verreal/$freq/$var"
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
            local start=$minyear
            [[ $((start % 10)) -ne 1 ]] && start=$(( (start/10)*10 + 1 ))
            [[ $start -lt $minyear ]] && start=$minyear
            
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
            local start=$minyear
            local mod5=$((start % 5))
            [[ $mod5 -ne 1 ]] && start=$(( (start/5)*5 + 1 ))
            [[ $start -lt $minyear ]] && start=$minyear
            
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
    [[ -z "${varfreq[$varname]:-}" ]] && {
        echo "$varname is not a variable in data request, skipping" >&2
        continue
    }
    
    reqfreq="${varfreq[$varname]}"
    
    # Skip fx (invariant) variables
    [[ "$reqfreq" =~ fx ]] && {
        echo "$varname is static, skipping" >&2
        continue
    }
    
    # Find all NetCDF files
    files=("$vardir"/*.nc)
    [[ ! -f "${files[0]}" ]] && {
        echo "Warning: No NetCDF files found for $varname" >&2
        continue
    }
    
    echo "Processing variable: $varname (requested frequency: $reqfreq)"
    
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
    
    # Determine what aggregations to generate based on what input files exist:
    # - If we have sub-daily files, generate daily
    # - If we have daily files (either as input or as output from previous
    #   aggregation), generate monthly
    declare -a freqs_to_generate=()
    
    if [[ "$infreq" =~ hr$ ]]; then
        # Have sub-daily input files, generate daily
        freqs_to_generate=(day)
        # Also check if daily outputs already exist for monthly aggregation
        daily_outdir="$(gen_outdir "$drvsrc" "$drvexp" "$drvvar" "$src" "$verreal" "day" "$varname")"
        daily_pattern="${varname}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_day_*.nc"
        if compgen -G "$daily_outdir/$daily_pattern" > /dev/null; then
            freqs_to_generate+=(mon)
        fi
    elif [[ "$infreq" == "day" ]]; then
        # Have daily input files, generate monthly
        freqs_to_generate=(mon)
    fi
    
    # Generate commands for each frequency
    for freq in "${freqs_to_generate[@]}"; do
        cmdfile="$CMDDIR/${varname}.${freq}.cmd"
        > "$cmdfile"  # Truncate/create
        
        echo "  Generating $cmdfile"
        
        # Determine input files for this frequency
        if [[ "$freq" == "mon" && "$infreq" =~ hr$ ]]; then
            # Monthly from sub-daily: use daily output files from previous step
            indir_for_freq="$(gen_outdir "$drvsrc" "$drvexp" "$drvvar" "$src" "$verreal" "day" "$varname")"
            inpat="${varname}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_day_*.nc"
        else
            # Daily from sub-daily or monthly from daily: use input directory
            indir_for_freq="$vardir"
            inpat="*.nc"
        fi
        
        # Compute output ranges
        mapfile -t ranges < <(compute_ranges "$freq" "$minyear" "$maxyear")
        
        for range in "${ranges[@]}"; do
            IFS='-' read -r startyear endyear <<< "$range"
            
            # Collect input files for this range
            infiles=()
            for ((year=startyear; year<=endyear; year++)); do
                if [[ "$freq" == "mon" && "$infreq" =~ hr$ ]]; then
                    # Look for daily output files
                    pattern="${varname}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_day_${year}*.nc"
                    for f in "$indir_for_freq"/$pattern; do
                        [[ -f "$f" ]] && infiles+=("$f")
                    done
                else
                    # Use input files directly
                    [[ -n "${yearfiles[$year]:-}" ]] && infiles+=("${yearfiles[$year]}")
                fi
            done
            
            [[ ${#infiles[@]} -eq 0 ]] && continue
            
            # Generate output path and filename
            outdir="$(gen_outdir "$drvsrc" "$drvexp" "$drvvar" "$src" "$verreal" "$freq" "$varname")"
            outfile="$(gen_outfile "$varname" "$domain" "$drvsrc" "$drvexp" "$drvvar" "$inst" "$src" "$verreal" "$freq" "$startyear" "$endyear")"
            outpath="$outdir/$outfile"
            
            # Check for existing files
            if [[ -f "$outpath" && $FORCE -eq 0 ]]; then
                echo "Error: Output file exists: $outpath" >&2
                echo "Use --force to overwrite" >&2
                exit 1
            fi
            
            # Determine CDO operator
            case "$freq" in
                mon) cdoop="monmean" ;;
                day) cdoop="daymean" ;;
                *) echo "Error: Unknown frequency $freq" >&2; exit 1 ;;
            esac
            
            # Generate command
            mkdir -p "$outdir"
            echo "cdo $cdoop -mergetime ${infiles[*]} $outpath" >> "$cmdfile"
        done
        
        # Report stats
        ncommands=$(wc -l < "$cmdfile")
        echo "    Generated $ncommands commands"
    done
done

echo ""
echo "Commandfile generation complete!"
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "To run with launch_cf:"
echo "  launch_cf <varname>.<freq>.cmd"
