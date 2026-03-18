#!/bin/bash

# relocate.sh - Populate a CORDEX DRS output tree by hard-linking files
# from the aggregate output directory.
#
# Input is organized in <var>.<freq> subdirectories (the OUTDIR from
# aggregate.sh).  Output is a CORDEX DRS tree rooted at OUTDIR.
#
# Hard links are used rather than copies to avoid duplicating the data.
# INDIR and OUTDIR must be on the same filesystem for this to work.
#
# After the tree is populated, use Globus to transfer from OUTDIR to
# campaign storage.
#
# Usage: relocate.sh [OPTIONS] INDIR OUTDIR VERSION

set -euo pipefail

# CORDEX-CMIP6 DRS constants
PROJECT="CORDEX-CMIP6"
ACTIVITY_ID="DD"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR VERSION

Populate a CORDEX DRS output tree by hard-linking aggregated files.

Arguments:
  INDIR    Input directory containing <var>.<freq> subdirectories
           (the OUTDIR from aggregate.sh)
  OUTDIR   DRS output tree root
  VERSION  Dataset version string (e.g. v20250101)

Options:
  --force     Overwrite existing output files
  --dry-run   Print what would be done without executing
  -h, --help  Show this help message
EOF
    exit 1
}

FORCE=0
DRY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)   FORCE=1; shift ;;
        --dry-run) DRY=1; shift ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 3 ]] && usage

INDIR="$(realpath "$1")"
OUTDIR="$(realpath "$2")"
VERSION="$3"

# Validate version string
if ! [[ "$VERSION" =~ ^v[0-9]{8}$ ]]; then
    echo "Error: VERSION must match vYYYYMMDD (e.g. v20250101), got: $VERSION" >&2
    exit 1
fi

[[ ! -d "$INDIR" ]] && { echo "Error: Input directory not found: $INDIR" >&2; exit 1; }

do_cmd() {
    if [[ $DRY -eq 1 ]]; then
        echo "  $*"
    else
        "$@"
    fi
}

nlinked=0
nskipped=0
nfailed=0

echo "Scanning input directory: $INDIR"
[[ $DRY -eq 1 ]] && echo "(dry run)"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue

    dirname="$(basename "$vardir")"
    varname="${dirname%.*}"
    freq="${dirname##*.}"

    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    echo "Processing: $dirname"

    for infile in "${files[@]}"; do
        [[ ! -f "$infile" ]] && continue
        fname="$(basename "$infile")"

        # Parse DRS components from CORDEX filename:
        # var_domain_drvsrc_drvexp_drvvar_inst_src_verreal_freq[_timespan].nc
        base="${fname%.nc}"
        IFS='_' read -r _var domain drvsrc drvexp drvvar inst src verreal _rest <<< "$base"

        # DRS: PROJECT/activity_id/domain_id/institution_id/driving_source_id/
        #      driving_experiment_id/driving_variant_label/source_id/
        #      version_realization/frequency/variable_id/version/
        drsdir="$OUTDIR/$PROJECT/$ACTIVITY_ID/$domain/$inst/$drvsrc/$drvexp/$drvvar/$src/$verreal/$freq/$varname/$VERSION"
        outpath="$drsdir/$fname"

        if [[ -f "$outpath" ]]; then
            if [[ $FORCE -eq 1 ]]; then
                do_cmd rm "$outpath"
            else
                (( nskipped++ )) || true
                continue
            fi
        fi

        do_cmd mkdir -p "$drsdir"
        if do_cmd ln "$infile" "$outpath"; then
            (( nlinked++ )) || true
        else
            (( nfailed++ )) || true
        fi
    done
done

echo ""
echo "Done."
echo "  Linked:  $nlinked"
echo "  Skipped: $nskipped"
echo "  Failed:  $nfailed"
if [[ $nfailed -gt 0 ]]; then
    echo "  WARNING: Some files failed to link. Check for cross-device link errors."
    exit 1
fi
