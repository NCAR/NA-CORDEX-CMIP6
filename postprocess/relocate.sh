#!/bin/bash

# relocate.sh - Generate commandfiles for moving aggregated CORDEX data
# into a CORDEX DRS output tree.
#
# Reads from INDIR (the OUTDIR from aggregate.sh), where data is organized
# in <var>.<freq> subdirectories, and generates cp commandfiles to place
# files into the appropriate DRS paths under OUTDIR.
#
# Usage: relocate.sh [OPTIONS] INDIR OUTDIR VERSION [CMDDIR]

set -euo pipefail

# project id (esgf-qa wants this as root of output tree)
PROJECT="cordex-cmip6"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR VERSION [CMDDIR]

Generate commandfiles for placing aggregated CORDEX files into a DRS output tree.

Arguments:
  INDIR    Input directory containing <var>.<freq> subdirectories
           (the OUTDIR from aggregate.sh)
  OUTDIR   DRS output tree root
  VERSION  Dataset version string (e.g. v20250101)
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force     Overwrite existing output files
  -h, --help  Show this help message

One commandfile per variable (<var>.<freq>.cmd) is generated for parallel
execution via launch_cf.
EOF
    exit 1
}

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
mkdir -p "$2"
OUTDIR="$(realpath "$2")"
VERSION="$3"
CMDDIR="${4:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

# Validate version string
if ! [[ "$VERSION" =~ ^v[0-9]{8}$ ]]; then
    echo "Error: VERSION must match vYYYYMMDD (e.g. v20250101), got: $VERSION" >&2
    exit 1
fi

[[ ! -d "$INDIR" ]] && { echo "Error: Input directory not found: $INDIR" >&2; exit 1; }

mkdir -p "$OUTDIR" "$CMDDIR"

# Function to generate DRS output directory path from a CORDEX filename
# Format: var_domain_drvsrc_drvexp_drvvar_inst_src_verreal_freq[_timespan].nc
# DRS:    PROJECT/drvsrc/drvexp/drvvar/src/verreal/freq/var/VERSION/
gen_drs_dir() {
    local fname="$1" freq="$2"
    local base="${fname%.nc}"
    IFS='_' read -r var domain drvsrc drvexp drvvar inst src verreal _freq _rest <<< "$base"
    echo "$OUTDIR/$PROJECT/$drvsrc/$drvexp/$drvvar/$src/$verreal/$freq/$var/$VERSION"
}

echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue

    dirname="$(basename "$vardir")"
    varname="${dirname%.*}"
    freq="${dirname##*.}"

    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && {
        echo "Warning: No NetCDF files found in $vardir" >&2
        continue
    }

    echo "Processing: $dirname"

    cmdfile="$CMDDIR/${varname}.${freq}.cmd"
    > "$cmdfile"
    ncommands=0
    nskipped=0

    for infile in "${files[@]}"; do
        [[ ! -f "$infile" ]] && continue
        fname="$(basename "$infile")"

        drsdir="$(gen_drs_dir "$fname" "$freq")"
        outpath="$drsdir/$fname"

        ((ncommands++)) || true

        if [[ -f "$outpath" && $FORCE -eq 0 ]]; then
            ((nskipped++)) || true
            continue
        fi

        mkdir -p "$drsdir"
        echo "cp $infile $outpath" >> "$cmdfile"
    done

    ngenerated=$((ncommands - nskipped))
    echo "  $dirname: $ngenerated commands, skipped $nskipped/$ncommands existing"

    [[ ! -s "$cmdfile" ]] && rm "$cmdfile"
done

echo ""
echo "Commandfile generation complete!"
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "To run with launch_cf:"
echo "  launch_multi --run RUNDIR $CMDDIR/*.cmd"
