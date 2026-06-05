#!/bin/bash

# plot.sh - Generate commandfiles for plotting post-processed CORDEX-CMIP6
# NetCDF files.  Designed for use with launch_multi and launch_cf, matching
# the pattern established by compress.sh and format.sh.
#
# Reads NetCDF files from <var>.<freq> subdirectories under INDIR and
# generates plot.postprocess.var.py commands, writing figures to corresponding
# subdirectories under OUTDIR.  One commandfile per variable.

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR [CMDDIR]

Generate commandfiles for plotting CORDEX-CMIP6 NetCDF files.

Arguments:
  INDIR    Input directory containing <var>.<freq> subdirectories
  OUTDIR   Output directory for figures (subdirs created per variable)
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force         Overwrite existing output files
  --scripts PATH  Directory containing plot.postprocess.var.py
                  (default: directory containing plot.sh)
  -h, --help      Show this help message
EOF
    exit 1
}

FORCE=0
SCRIPTS_DIR="$(dirname "$(realpath "$0")")"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --force)   FORCE=1; shift ;;
        --scripts) SCRIPTS_DIR="$(realpath "$2")"; shift 2 ;;
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

[[ ! -d "$INDIR" ]] && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }

[[ ! -f "$SCRIPTS_DIR/plot.postprocess.var.py" ]] && {
    echo "Error: plot.postprocess.var.py not found in $SCRIPTS_DIR" >&2
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"

echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue
    dirname="$(basename "$vardir")"
    varname="${dirname%.*}"
    freq="${dirname##*.}"

    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    cmdfile="$CMDDIR/${dirname}.cmd"
    > "$cmdfile"
    ncommands=0
    nskipped=0

    outdir_var="$OUTDIR/$dirname"

    for infile in "${files[@]}"; do
        [[ ! -f "$infile" ]] && continue
        stem="$(basename "$infile" .nc)"
        outfile="$outdir_var/${stem}.png"

        if [[ -f "$outfile" && $FORCE -eq 0 ]]; then
            (( nskipped++ )) || true
            (( ncommands++ )) || true
            continue
        fi

        mkdir -p "$outdir_var"
        echo "python ./plot.postprocess.var.py $infile $outdir_var" >> "$cmdfile"
        (( ncommands++ )) || true
    done

    ngenerated=$(( ncommands - nskipped ))
    echo "  $dirname: $ngenerated commands, skipped $nskipped/$ncommands existing"

    [[ ! -s "$cmdfile" ]] && rm "$cmdfile"
done

echo ""
echo "Commandfiles written to: $CMDDIR"
