#!/bin/bash

# compress.sh - Generate commandfiles for applying lossy compression to
# formatted CORDEX-CMIP6 NetCDF files.  Designed for use with launch_multi
# and launch_cf, matching the pattern established by format.sh.
#
# Reads formatted files from INDIR (the OUTDIR from format.sh) and generates
# ncks --ppc commands, writing compressed output to OUTDIR.  One commandfile
# per variable.
#
# Compression precision per variable is specified in var_specs.yml.
# Variables with no qnt entry receive lossless deflate compression only
# (-7 -L1), as required by the CORDEX spec.

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR [CMDDIR]

Generate commandfiles for compressing formatted CORDEX-CMIP6 NetCDF files.

Arguments:
  INDIR    Output directory from format.sh (contains variable subdirectories)
  OUTDIR   Output directory for compressed files
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force       Overwrite existing output files
  --scripts PATH  Directory containing var_specs.yml
                  (default: directory containing compress.sh)
  -h, --help    Show this help message
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
mkdir -p $2
OUTDIR="$(realpath "$2")"
CMDDIR="${3:-.}"
mkdir -p $CMDDIR
CMDDIR="$(realpath "$CMDDIR")"

[[ ! -d "$INDIR" ]] && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }

[[ ! -f "$SCRIPTS_DIR/var_specs.yml" ]] && {
    echo "Error: var_specs.yml not found in $SCRIPTS_DIR" >&2
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"

# Helper: look up a field in var_specs.yml for a given variable.
# Returns empty string if not found.
get_spec() {
    local var="$1" field="$2"
    python3 -c "
import yaml
s = yaml.safe_load(open('$SCRIPTS_DIR/var_specs.yml'))
v = s.get('$var', {})
val = v.get('$field')
print('' if val is None else val)
"
}

echo "Scanning input directory: $INDIR"

for dir in "$INDIR"/*/; do
    [[ ! -d "$dir" ]] && continue
    dirname="$(basename "$dir")"
    varname="${dirname%.*}"      # strip .freq suffix

    files=("$dir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    # Look up quantization level for this variable
    qnt="$(get_spec "$varname" qnt)"

    cmdfile="$CMDDIR/${dirname}.cmd"
    > "$cmdfile"
    ncommands=0
    nskipped=0

    for infile in "${files[@]}"; do
        [[ ! -f "$infile" ]] && continue
        fname="$(basename "$infile")"

        outdir_var="$OUTDIR/$dirname"
        outfile="$outdir_var/$fname"

        if [[ -f "$outfile" && $FORCE -eq 0 ]]; then
            (( nskipped++ )) || true
            (( ncommands++ )) || true
            continue
        fi

        mkdir -p "$outdir_var"

        if [[ -n "$qnt" ]]; then
            # Lossy compression: --ppc at specified precision, plus deflate
            echo "ncks -h -O -7 -L1 --ppc ${varname}=${qnt} --chunk_cache 4000000000 --chunk_map rd1 $infile $outfile" >> "$cmdfile"
        else
            # Lossless deflate only (required by CORDEX spec for fx variables)
            echo "ncks -h -O -7 -L1 --chunk_cache 4000000000 --chunk_map rd1 $infile $outfile" >> "$cmdfile"
        fi
        (( ncommands++ )) || true
    done

    ngenerated=$(( ncommands - nskipped ))
    echo "  $dirname: $ngenerated commands (qnt=${qnt:-lossless}), skipped $nskipped/$ncommands existing"

    [[ ! -s "$cmdfile" ]] && rm "$cmdfile"
done

echo ""
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "To run with launch_multi:"
echo "  launch_multi --run RUNDIR ${CMDDIR}/*.cmd"
