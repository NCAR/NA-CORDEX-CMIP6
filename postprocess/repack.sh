#!/bin/bash

# repack.sh - Generate commandfile for the ncrepack-cordex repack step.
#
# For each .nc file in INDIR, creates a symlink in the mirrored OUTDIR tree,
# then writes a command to run ncrepack-cordex -o on that symlink.  Using -o
# causes ncrepack-cordex to overwrite the symlink with the repacked file,
# breaking the link.  Any file that remains a symlink after the step failed.
#
# Usage:
#   repack.sh INDIR SETUPDIR OUTDIR [CMDDIR]
#
# INDIR    compress/data tree (read-only input)
# SETUPDIR setup directory (contains ncrepack-cordex script)
# OUTDIR   repack/data tree (created; initially populated with symlinks)
# CMDDIR   directory for generated commandfile(s) (default: current directory)

set -euo pipefail

usage() {
    cat >&2 <<USAGE
Usage: $(basename "$0") INDIR SETUPDIR OUTDIR [CMDDIR]

Generate a commandfile for repacking compressed CORDEX-CMIP6 NetCDF files
using ncrepack-cordex.

Arguments:
  INDIR    Output directory from compress.sh (contains variable subdirectories)
  SETUPDIR Output directory from setup.py (contains ncrepack-cordex script)
  OUTDIR   Output directory for repacked files
  CMDDIR   Directory for commandfile (default: current directory)
USAGE
    exit 1
}

[[ $# -lt 3 ]] && usage

INDIR=$(realpath "$1")
SDIR=$(realpath "$2")
mkdir -p "$3"
OUTDIR=$(realpath "$3")
CMDDIR="${4:-.}"
mkdir -p "$CMDDIR"
CMDDIR=$(realpath "$CMDDIR")

[[ ! -d "$INDIR" ]] && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }
[[ ! -d "$SDIR"  ]] && { echo "Error: SETUPDIR not found: $SDIR" >&2; exit 1; }

repack_cmd="$SDIR/ncrepack-cordex"
[[ ! -x "$repack_cmd" ]] && {
    echo "Error: ncrepack-cordex not found or not executable: $repack_cmd" >&2
    exit 1
}

cmdfile="$CMDDIR/repack.cmd"
> "$cmdfile"
ncommands=0

echo "Scanning input directory: $INDIR"

for dir in "$INDIR"/*/; do
    [[ ! -d "$dir" ]] && continue
    dirname="$(basename "$dir")"

    files=("$dir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    outdir_var="$OUTDIR/$dirname"
    mkdir -p "$outdir_var"

    for infile in "${files[@]}"; do
        [[ ! -f "$infile" ]] && continue
        dest="$outdir_var/$(basename "$infile")"

        # Create symlink only if not already present (supports re-runs)
        [[ ! -e "$dest" ]] && ln -s "$infile" "$dest"

        echo "$repack_cmd -o $dest" >> "$cmdfile"
        (( ncommands++ )) || true
    done

    echo -n "$dirname "
done

[[ ! -s "$cmdfile" ]] && rm "$cmdfile"

echo ""
echo ""
echo "$ncommands Commands written to: $cmdfile"

