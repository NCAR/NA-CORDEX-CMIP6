#!/bin/bash

# aggregate.sh - Generate commandfiles for NA-CORDEX (WRF) aggregation
# Aggregates hourly -> daily -> monthly according to CORDEX-CMIP6 specs.
# Also generates commandfiles to copy hourly and fx files into the output tree.
#
# Output is organized in per-variable subdirectories named <var>.<freq>.
# Use relocate.sh to move the results into a CORDEX DRS output tree.
#
# Variable metadata is read from var_table.tsv in SETUPDIR, where it is
# placed by setup.py.

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR SETUPDIR OUTDIR [CMDDIR]

Generate CDO commandfiles for aggregating CORDEX data to requested frequencies,
and cp commandfiles for placing hourly and fx files into the output tree.

Arguments:
  INDIR    Input directory containing <var>.<freq> subdirectories with yearly
           NetCDF files
  SETUPDIR Output directory from setup.py (contains var_table.tsv)
  OUTDIR   Output directory root (subdirs named <var>.<freq> created here)
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force     Overwrite existing output files
  -h, --help  Show this help message

The script generates separate commandfiles per variable and frequency
(<var>.<freq>.cmd) for parallel execution via launch_cf.
EOF
    exit 1
}

# Parse arguments
FORCE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force) FORCE=1; shift ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 3 ]] && usage

INDIR="$(realpath "$1")"
SETUPDIR="$(realpath "$2")"
mkdir -p "$3"
OUTDIR="$(realpath "$3")"
CMDDIR="${4:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

# Validate inputs
[[ ! -d "$INDIR" ]]    && { echo "Error: Input directory not found: $INDIR" >&2; exit 1; }
[[ ! -d "$SETUPDIR" ]] && { echo "Error: SETUPDIR not found: $SETUPDIR" >&2; exit 1; }

VAR_TABLE="$SETUPDIR/var_table.tsv"
[[ ! -f "$VAR_TABLE" ]] && {
    cat >&2 <<EOF
Error: var_table.tsv not found: $VAR_TABLE

Run setup.py before aggregate.sh.
EOF
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"

# Non-fx variables: used to skip irrelevant input directories.
# var_table.tsv columns: var, freq, units, cell_methods, positive,
#                        levels, refh, quant, standard_name, long_name
AGGVARS=$(awk -F'\t' 'NR>1 && $2 != "fx" {print $1}' "$VAR_TABLE" | sort -u)

# Hourly instantaneous variables: need time/time_bnds fixup after daily
# aggregation.  Time units are assumed to be 'days since ...' throughout
# (guaranteed by cmorize.sh).
POINTVARS=$(awk -F'\t' 'NR>1 && $2 ~ /hr/ && $4 ~ /time: point/ {print $1}' "$VAR_TABLE")

# Function to extract year from CORDEX filename timespan field
# (YYYYMMDDhhmm-YYYYMMDDhhmm or YYYYMMDD-YYYYMMDD or YYYYMM-YYYYMM)
extract_year() {
    local timespan="$1"
    echo "${timespan:0:4}"
}

# Extract end year from CORDEX filename timespan field
# (YYYYMMDDhhmm-YYYYMMDDhhmm or YYYYMMDD-YYYYMMDD or YYYYMM-YYYYMM)
extract_end_year() {
    local timespan="$1"
    local endpart="${timespan##*-}"
    echo "${endpart:0:4}"
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

# Compute year ranges for output files according to CORDEX-CMIP6 chunking rules.
#
# Monthly: 10-year chunks starting at years ending in 1 (or minyear for the
#   leading partial), ending at years ending in 0 (or maxyear).
# Daily: 5-year chunks starting at years ending in 1 or 6 (or minyear for the
#   leading partial), ending at years ending in 0 or 5 (or maxyear).
# Hourly: 1-year chunks.
compute_ranges() {
    local freq="$1" minyear="$2" maxyear="$3"
    local -a ranges
    local chunksize boundary start end

    case "$freq" in
        mon) chunksize=10 ;;
        day) chunksize=5 ;;
        *hr)
            for ((year=minyear; year<=maxyear; year++)); do
                ranges+=("$year-$year")
            done
            printf "%s\n" "${ranges[@]}"
            return
            ;;
    esac

    # Find chunk end-years: years in [minyear,maxyear] that are 0 mod chunksize
    local -a endyears=()
    local first_end=$(( ((minyear + chunksize - 1) / chunksize) * chunksize ))
    for ((y=first_end; y<=maxyear; y+=chunksize)); do
        endyears+=("$y")
    done

    local n=${#endyears[@]}
    if [[ $n -eq 0 ]]; then
        ranges+=("$minyear-$maxyear")
    else
        ranges+=("$minyear-${endyears[0]}")
        for ((i=1; i<n; i++)); do
            ranges+=("$(( endyears[i-1] + 1 ))-${endyears[$i]}")
        done
        if [[ ${endyears[$((n-1))]} -lt $maxyear ]]; then
            ranges+=("$(( endyears[n-1] + 1 ))-$maxyear")
        fi
    fi
    
    printf "%s\n" "${ranges[@]}"
}

# Select files whose year span overlaps a target range.
# Args: startyear endyear file_entry [file_entry ...]
# Each file_entry is "startyear:endyear:filepath".
# Prints matching filepaths, one per line.
select_overlapping_files() {
    local range_start="$1" range_end="$2"
    shift 2
    local entry fstart fend fpath
    for entry in "$@"; do
        IFS=':' read -r fstart fend fpath <<< "$entry"
        if [[ $fstart -le $range_end && $fend -ge $range_start ]]; then
            echo "$fpath"
        fi
    done
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
        echo "$varname is not a variable in var_table.tsv, skipping" >&2
        continue
    }

    echo "Processing: $varname ($infreq)"

    # Determine years available from yearly input files
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
        echo "Warning: Could not determine year range for $varname" >&2
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

        # Collect input files for this frequency as "startyear:endyear:filepath"
        # entries. Daily uses the per-year input files directly; monthly reads
        # from multi-year daily output files, parsing both start and end years
        # from each filename.
        file_entries=()
        freq_minyear=9999
        freq_maxyear=0

        if [[ "$freq" == "day" ]]; then
            if [[ "$infreq" == "day" || "$infreq" =~ hr$ ]]; then
                for year in "${!yearfiles[@]}"; do
                    file_entries+=("$year:$year:${yearfiles[$year]}")
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
            else
                rm "$cmdfile"
                continue
            fi
        elif [[ "$freq" == "mon" ]]; then
            if [[ "$infreq" == "day" ]]; then
                # Input is already daily single-year files
                for year in "${!yearfiles[@]}"; do
                    file_entries+=("$year:$year:${yearfiles[$year]}")
                    [[ $year -lt $freq_minyear ]] && freq_minyear=$year
                    [[ $year -gt $freq_maxyear ]] && freq_maxyear=$year
                done
            else
                # Read from daily output files (multi-year); parse both start
                # and end years from each filename's timespan field.
                daily_dir="$OUTDIR/${varname}.day"
                daily_pattern="${varname}_${domain}_${drvsrc}_${drvexp}_${drvvar}_${inst}_${src}_${verreal}_day_*.nc"

                shopt -s nullglob
                for f in "$daily_dir"/$daily_pattern; do
                    [[ ! -f "$f" ]] && continue
                    base="${f%.nc}"
                    timespan="${base##*_}"
                    [[ -z "$timespan" ]] && continue
                    fstart=$(extract_year "$timespan")
                    fend=$(extract_end_year "$timespan")
                    file_entries+=("$fstart:$fend:$f")
                    [[ $fstart -lt $freq_minyear ]] && freq_minyear=$fstart
                    [[ $fend -gt $freq_maxyear ]] && freq_maxyear=$fend
                done
                shopt -u nullglob
            fi
        fi

        if [[ "${#file_entries[@]}" -eq 0 ]]; then
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

            # Select input files overlapping this output range
            mapfile -t infiles < <(select_overlapping_files "$startyear" "$endyear" "${file_entries[@]}" | sort)

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
echo "To run with launch_multi:"
echo "  launch_multi --workflow cordex --run RUNDIR $CMDDIR/*.cmd"
