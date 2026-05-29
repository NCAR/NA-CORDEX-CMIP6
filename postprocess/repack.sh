#!/bin/bash
# repack.sh - Generate commandfile for the ncrepack-cordex repack step.
#
# For each .nc file in INDIR, creates a symlink in the mirrored OUTDIR tree,
# then writes a command to run ncrepack-cordex -o on that symlink.  Using -o
# causes ncrepack-cordex to overwrite the symlink with the repacked file,
# breaking the link.  Any file that remains a symlink after the step failed.
#
# Usage:
#   repack.sh INDIR SETUPDIR OUTDIR CMDDIR
#
# INDIR    compress/data tree (read-only input)
# SETUPDIR setup directory (contains ncrepack-cordex script)
# OUTDIR   repack/data tree (created; initially populated with symlinks)
# CMDDIR   directory for generated commandfile(s) (created if needed)

set -euo pipefail

if [ $# -ne 4 ]; then
    echo "Usage: $0 INDIR SETUPDIR OUTDIR CMDDIR" >&2
    exit 1
fi

indir=$(realpath "$1")
sdir=$(realpath "$2")
outdir=$(realpath "$3")
cmddir=$(realpath "$4")

repack_cmd="$sdir/ncrepack-cordex"

if [ ! -x "$repack_cmd" ]; then
    echo "Error: ncrepack-cordex not found or not executable: $repack_cmd" >&2
    exit 1
fi

mkdir -p "$outdir" "$cmddir"

cmdfile="$cmddir/repack.cmd"
rm -f "$cmdfile"

# Mirror directory structure with symlinks, one command per file
find "$indir" -name '*.nc' | sort | while read -r src; do
    # Reproduce the subdirectory path under outdir
    rel="${src#$indir/}"
    dest="$outdir/$rel"
    mkdir -p "$(dirname "$dest")"

    # Create symlink only if not already present (supports re-runs)
    if [ ! -e "$dest" ]; then
        ln -s "$src" "$dest"
    fi

    echo "$repack_cmd -o $dest" >> "$cmdfile"
done

n=$(wc -l < "$cmdfile")
echo "repack.sh: wrote $n commands to $cmdfile"
