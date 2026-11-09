#!/bin/bash
# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen
#
# relocate_ba.sh - Populate a DRS output tree for bias-adjusted files by
# hard-linking files from the repack output directory.
#
# COPIED from postprocess/relocate.sh (commit c8d7a95) and modified for
# bias-adjusted output.  Changes from the original:
#   - PROJECT and ACTIVITY_ID come from ba.env (drs_project, drs_activity)
#     instead of being hardcoded
#   - version comes from creation_date in ba.env (not sim.env)
#   - no extra DRS level for the bias adjustment: the BA ID is already
#     part of the version element in the filename (e.g.
#     v1-r1-PIK-ISIMIP3-ERA5Land-1985-2014, CORDEX-CMIP5 Adjust
#     convention), so the same 13 DRS levels as relocate.sh
# If relocate.sh changes upstream, diff against it and port fixes.
#
# Input is organized in <var>.<freq> subdirectories (the repack OUTDIR).
# Output is a DRS tree rooted at OUTDIR.
#
# Hard links are used rather than copies to avoid duplicating the data.
# INDIR and OUTDIR must be on the same filesystem for this to work.
#
# After the tree is populated, use Globus to transfer from OUTDIR to
# campaign storage.
#
# The dataset version string is derived from creation_date in ba.env
# (set by ba_setup.py) and converted to vYYYYMMDD format.  Use --version
# to override if needed.
#
# Usage: relocate_ba.sh [OPTIONS] INDIR SETUPDIR OUTDIR

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR SETUPDIR OUTDIR

Populate a DRS output tree by hard-linking bias-adjusted files.

Arguments:
  INDIR    Input directory containing <var>.<freq> subdirectories
           (the OUTDIR from repack.sh)
  SETUPDIR Output directory from ba_setup.py (contains ba.env)
  OUTDIR   DRS output tree root

Options:
  --version VERSION  Override dataset version string (default: derived from
                     creation_date in ba.env, format vYYYYMMDD)
  --force            Overwrite existing output files
  --dry-run          Print what would be done without executing
  -h, --help         Show this help message
EOF
    exit 1
}

FORCE=0
DRY=0
VERSION_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)   FORCE=1; shift ;;
        --dry-run) DRY=1; shift ;;
        --version) VERSION_OVERRIDE="$2"; shift 2 ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 3 ]] && usage

INDIR="$(realpath "$1")"
SETUPDIR="$(realpath "$2")"
OUTDIR="$(realpath "$3")"

[[ ! -d "$INDIR" ]]    && { echo "Error: Input directory not found: $INDIR" >&2; exit 1; }
[[ ! -d "$SETUPDIR" ]] && { echo "Error: SETUPDIR not found: $SETUPDIR" >&2; exit 1; }

# DRS settings and version string from ba.env
BA_ENV="$SETUPDIR/ba.env"
[[ ! -f "$BA_ENV" ]] && {
    echo "Error: ba.env not found: $BA_ENV" >&2
    echo "Run ba_setup.py before relocate_ba.sh." >&2
    exit 1
}
get_env() { grep "^$1=" "$BA_ENV" | cut -d= -f2-; }
PROJECT="$(get_env drs_project)"
ACTIVITY_ID="$(get_env drs_activity)"
for v in PROJECT ACTIVITY_ID; do
    [[ -z "${!v}" ]] && { echo "Error: $v not set in $BA_ENV" >&2; exit 1; }
done

if [[ -n "$VERSION_OVERRIDE" ]]; then
    VERSION="$VERSION_OVERRIDE"
else
    # Convert YYYY-MM-DDTHH:MM:SSZ -> vYYYYMMDD
    creation_date="$(get_env creation_date | cut -dT -f1)"
    [[ -z "$creation_date" ]] && {
        echo "Error: creation_date not found in $BA_ENV" >&2
        exit 1
    }
    VERSION="v${creation_date//-/}"
fi

# Validate version string
if ! [[ "$VERSION" =~ ^v[0-9]{8}$ ]]; then
    echo "Error: VERSION must match vYYYYMMDD (e.g. v20250101), got: $VERSION" >&2
    exit 1
fi

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
echo "Version: $VERSION"
echo "DRS root: $OUTDIR/$PROJECT/$ACTIVITY_ID"
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
        #      version_realization-ba_id/frequency/variable_id/version/
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
