#!/bin/bash

# aggregate2.sh - Generate commandfiles for NA-CORDEX (WRF) aggregation
#
# Reorders operations relative to aggregate.sh for better wallclock performance:
# averaging is done on annual files first, concatenation into multi-year chunks
# last.  Subdaily and fx files are linked (not copied) into the output tree.
#
# Workflow:
#   Step 1 (in-script): Link all input files into TEMPDIR
#   Step 2 (avg.cmd):   Average subdaily -> daily annual files in TEMPDIR
#   Step 3 (mon.cmd):   Average daily -> monthly annual files in TEMPDIR
#   Step 4 (cat.cmd):   Link subdaily/fx from TEMPDIR into OUTDIR;
#                       concatenate daily/monthly into chunked files in OUTDIR
#
# Run order:
#   launch_multi avg.cmd
#   launch_multi mon.cmd
#   launch_multi cat.cmd
#
# Input directories are named <var>.<freq> (e.g. tas.1hr, orog.fx).
# TEMPDIR holds annual files at all frequencies.
# OUTDIR holds final chunked files (5-yr daily, 10-yr monthly) plus
# linked subdaily and fx files.

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR SETUPDIR TEMPDIR OUTDIR [CMDDIR]

Generate commandfiles for two-stage (average-then-concatenate) aggregation.

Arguments:
  INDIR    Input directory containing <var>.<freq> subdirectories
  SETUPDIR Directory containing var_table.tsv (from setup.py)
  TEMPDIR  Staging directory for annual files at all frequencies
  OUTDIR   Output directory for final chunked files
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force     Overwrite existing output files
  -h, --help  Show this help message
EOF
    exit 1
}

FORCE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)   FORCE=1; shift ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 4 ]] && usage

INDIR="$(realpath "$1")"
SETUPDIR="$(realpath "$2")"
mkdir -p "$3"
TEMPDIR="$(realpath "$3")"
mkdir -p "$4"
OUTDIR="$(realpath "$4")"
CMDDIR="${5:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

[[ ! -d "$INDIR" ]]    && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }
[[ ! -d "$SETUPDIR" ]] && { echo "Error: SETUPDIR not found: $SETUPDIR" >&2; exit 1; }

VAR_TABLE="$SETUPDIR/var_table.tsv"
[[ ! -f "$VAR_TABLE" ]] && { echo "Error: var_table.tsv not found: $VAR_TABLE" >&2; exit 1; }

# Load point variables (time: point cell_methods) from var_table.
# These need time coordinate rounding after daymean.
POINTVARS=$(awk -F'\t' 'NR>1 && $4 ~ /time: point/ {print $1}' "$VAR_TABLE")

is_point_var() { echo "$POINTVARS" | grep -qw "$1"; }

# ---------------------------------------------------------------------------
# Filename helpers
# ---------------------------------------------------------------------------

# Replace the freq field in a CORDEX filename (second-to-last _-delimited
# field) and truncate the timespan to just the year (annual files).
# Args: infile newfreq year
make_annual_fname() {
    local infile="$1" newfreq="$2" year="$3"
    local base stem timespan startdate enddate
    base="$(basename "$infile" .nc)"
    stem="${base%_*}"; stem="${stem%_*}"   # strip freq and timespan

    case "$newfreq" in
        *hr) startdate="${year}01010000"; enddate="${year}12312300" ;;
        day) startdate="${year}0101";     enddate="${year}1231" ;;
        mon) startdate="${year}01";       enddate="${year}12" ;;
    esac
    echo "${stem}_${newfreq}_${startdate}-${enddate}.nc"
}

# Build output filename for a multi-year chunk.
# Args: infile freq startyear endyear
make_chunk_fname() {
    local infile="$1" freq="$2" startyear="$3" endyear="$4"
    local base stem startdate enddate
    base="$(basename "$infile" .nc)"
    stem="${base%_*}"; stem="${stem%_*}"

    case "$freq" in
        day) startdate="${startyear}0101";  enddate="${endyear}1231" ;;
        mon) startdate="${startyear}01";    enddate="${endyear}12" ;;
    esac
    echo "${stem}_${freq}_${startdate}-${enddate}.nc"
}

# ---------------------------------------------------------------------------
# compute_ranges: CORDEX-CMIP6 chunking rules
#
# Monthly: 10-year chunks ending at years divisible by 10.
# Daily:    5-year chunks ending at years divisible by  5.
# ---------------------------------------------------------------------------
compute_ranges() {
    local freq="$1" minyear="$2" maxyear="$3"
    local -a ranges endyears=()
    local chunksize

    case "$freq" in
        mon) chunksize=10 ;;
        day) chunksize=5  ;;
        *) echo "Error: compute_ranges called with unsupported freq $freq" >&2; return 1 ;;
    esac

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
        [[ ${endyears[$((n-1))]} -lt $maxyear ]] && \
            ranges+=("$(( endyears[n-1] + 1 ))-$maxyear")
    fi

    printf "%s\n" "${ranges[@]}"
}

# ---------------------------------------------------------------------------
# Commandfile handles
# ---------------------------------------------------------------------------
avg_cmd="$CMDDIR/avg.cmd"
mon_cmd="$CMDDIR/mon.cmd"
cat_cmd="$CMDDIR/cat.cmd"
> "$avg_cmd"
> "$mon_cmd"
> "$cat_cmd"

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue

    dirname="$(basename "$vardir")"
    if [[ "$dirname" == _* ]]; then
	continue  # skip staging directories (e.g., _temp for wbgt)
    fi
    if [[ "$dirname" != *.* ]]; then
        echo "Warning: $dirname not in var.freq format, skipping" >&2
        continue
    fi
    varname="${dirname%.*}"
    infreq="${dirname##*.}"

    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && {
        echo "Warning: No NetCDF files found in $vardir" >&2
        continue
    }

    echo "Processing: $varname ($infreq)"

    # -------------------------------------------------------------------
    # Step 1: Link input files into TEMPDIR/<var>.<infreq>/
    # Done immediately (not via commandfile) since ln is fast.
    # -------------------------------------------------------------------
    tmpvardir="$TEMPDIR/${varname}.${infreq}"
    mkdir -p "$tmpvardir"

    for f in "${files[@]}"; do
        [[ ! -f "$f" ]] && continue
        dest="$tmpvardir/$(basename "$f")"
        if [[ ! -e "$dest" || $FORCE -eq 1 ]]; then
            ln -f "$f" "$dest"
        fi
    done

    # fx: link into OUTDIR too (step 4, done in-script), then skip.
    if [[ "$infreq" == "fx" ]]; then
        outvardir="$OUTDIR/${varname}.fx"
        mkdir -p "$outvardir"
        for f in "${files[@]}"; do
            [[ ! -f "$f" ]] && continue
            dest="$outvardir/$(basename "$f")"
            if [[ ! -e "$dest" || $FORCE -eq 1 ]]; then
                ln -f "$f" "$dest"
            fi
        done
        continue
    fi

    # Collect yearfiles map
    declare -A yearfiles
    minyear=9999
    maxyear=0

    for f in "${files[@]}"; do
        [[ ! -f "$f" ]] && continue
        base="$(basename "$f" .nc)"
        timespan="${base##*_}"
        year="${timespan:0:4}"
        yearfiles[$year]="$f"
        [[ $year -lt $minyear ]] && minyear=$year
        [[ $year -gt $maxyear ]] && maxyear=$year
    done

    [[ $minyear -eq 9999 ]] && {
        echo "Warning: Could not determine year range for $varname" >&2
        unset yearfiles; continue
    }

    echo "  Years: $minyear-$maxyear"

    # -------------------------------------------------------------------
    # Step 2 (avg.cmd): subdaily -> daily, one command per year.
    # Step 4 (cat.cmd): link subdaily annual files into OUTDIR.
    # -------------------------------------------------------------------
    if [[ "$infreq" =~ hr$ ]]; then
        day_tmpdir="$TEMPDIR/${varname}.day"
        mkdir -p "$day_tmpdir"

        out_hrdir="$OUTDIR/${varname}.${infreq}"
        mkdir -p "$out_hrdir"

        round_time="time=round(time*2)/2"
        round_bnds="time_bnds=round(time_bnds)"

        for year in $(seq "$minyear" "$maxyear"); do
            [[ -z "${yearfiles[$year]:-}" ]] && continue
            src="${yearfiles[$year]}"

            # Step 4: link subdaily into OUTDIR
            dest="$out_hrdir/$(basename "$src")"
            if [[ ! -e "$dest" || $FORCE -eq 1 ]]; then
                echo "ln -f $src $dest" >> "$cat_cmd"
            fi

            # Step 2: daymean into TEMPDIR/<var>.day/
            day_fname="$(make_annual_fname "$src" "day" "$year")"
            day_out="$day_tmpdir/$day_fname"

            [[ -f "$day_out" && $FORCE -eq 0 ]] && continue

            if is_point_var "$varname"; then
                echo "bash -c \"cdo daymean $src $day_out && ncap2 -h -O -s '${round_time}; ${round_bnds}' $day_out $day_out\"" >> "$avg_cmd"
            else
                echo "cdo daymean $src $day_out" >> "$avg_cmd"
            fi
        done
    fi

    # -------------------------------------------------------------------
    # Step 3 (mon.cmd): daily -> monthly, one command per year.
    # Input is either INDIR (if already daily) or TEMPDIR (if subdaily).
    # -------------------------------------------------------------------
    if [[ "$infreq" == "day" || "$infreq" =~ hr$ ]]; then
        mon_tmpdir="$TEMPDIR/${varname}.mon"
        mkdir -p "$mon_tmpdir"

        if [[ "$infreq" == "day" ]]; then
            src_daydir="$INDIR/${varname}.day"
        else
            src_daydir="$TEMPDIR/${varname}.day"
        fi

        for year in $(seq "$minyear" "$maxyear"); do
            # For subdaily input, the daily file is in TEMPDIR and may not
            # exist yet at script-generation time; reference it by expected name.
            if [[ "$infreq" == "day" ]]; then
                [[ -z "${yearfiles[$year]:-}" ]] && continue
                day_src="${yearfiles[$year]}"
            else
                [[ -z "${yearfiles[$year]:-}" ]] && continue
                day_fname="$(make_annual_fname "${yearfiles[$year]}" "day" "$year")"
                day_src="$src_daydir/$day_fname"
            fi

            mon_fname="$(make_annual_fname "${yearfiles[$year]}" "mon" "$year")"
            mon_out="$mon_tmpdir/$mon_fname"

            [[ -f "$mon_out" && $FORCE -eq 0 ]] && continue

            echo "cdo monmean $day_src $mon_out" >> "$mon_cmd"
        done
    fi

    # -------------------------------------------------------------------
    # Step 4 (cat.cmd): concatenate annual daily/monthly files in TEMPDIR
    # into chunked files in OUTDIR using ncrcat.
    # time coordinate is written as a single chunk (not the time dimension).
    # -------------------------------------------------------------------
    for freq in day mon; do
        [[ "$freq" == "day" && ! "$infreq" =~ (hr$|^day$) ]] && continue
        [[ "$freq" == "mon" && ! "$infreq" =~ (hr$|^day$) ]] && continue

        src_dir="$TEMPDIR/${varname}.${freq}"
        out_dir="$OUTDIR/${varname}.${freq}"
        mkdir -p "$out_dir"

        mapfile -t ranges < <(compute_ranges "$freq" "$minyear" "$maxyear")

        for range in "${ranges[@]}"; do
            IFS='-' read -r startyear endyear <<< "$range"

            # Collect annual files overlapping this chunk
            infiles=()
            for year in $(seq "$startyear" "$endyear"); do
                [[ -z "${yearfiles[$year]:-}" ]] && continue
                ann_fname="$(make_annual_fname "${yearfiles[$year]}" "$freq" "$year")"
                infiles+=("$ann_fname")
            done
            [[ ${#infiles[@]} -eq 0 ]] && continue

            chunk_fname="$(make_chunk_fname "${yearfiles[$minyear]}" "$freq" "$startyear" "$endyear")"
            chunk_out="$out_dir/$chunk_fname"

            [[ -f "$chunk_out" && $FORCE -eq 0 ]] && continue

            # ncrcat -p sets the input path so only filenames are listed.
            # --cnk_dmn time,1 chunks the time coordinate variable as a
            # single chunk for efficient CDO/NCO access.
            echo "ncrcat -h -7 --cnk_dmn time,1 -p $src_dir ${infiles[*]} $chunk_out" >> "$cat_cmd"
        done
    done

    unset yearfiles
done

# Remove empty commandfiles
for f in "$avg_cmd" "$mon_cmd" "$cat_cmd"; do
    [[ ! -s "$f" ]] && rm "$f"
done

echo ""
echo "Step 1 (links) complete."
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "Run order: avg.cmd mon.cmd cat.cmd"
