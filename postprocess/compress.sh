#!/bin/bash

# compress.sh - Generate commandfiles for applying lossy compression to
# formatted CORDEX-CMIP6 NetCDF files.  Designed for use with launch_multi
# and launch_cf, matching the pattern established by format.sh.
#
# Reads formatted files from INDIR (the OUTDIR from format.sh) and generates
# ncks --ppc commands, writing compressed output to OUTDIR.  One commandfile
# per variable.
#
# Compression precision per variable is read from var_table.tsv in SETUPDIR
# (placed there by setup.py).  Variables with no quant entry receive lossless
# deflate compression only (-7 -L1), as required by the CORDEX spec.

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR SETUPDIR OUTDIR [CMDDIR]

Generate commandfiles for compressing formatted CORDEX-CMIP6 NetCDF files.

Arguments:
  INDIR    Output directory from format.sh (contains variable subdirectories)
  SETUPDIR Output directory from setup.py (contains var_table.tsv)
  OUTDIR   Output directory for compressed files
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force       Overwrite existing output files
  -h, --help    Show this help message
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

[[ $# -lt 3 ]] && usage

INDIR="$(realpath "$1")"
SETUPDIR="$(realpath "$2")"
mkdir -p "$3"
OUTDIR="$(realpath "$3")"
CMDDIR="${4:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

[[ ! -d "$INDIR" ]]    && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }
[[ ! -d "$SETUPDIR" ]] && { echo "Error: SETUPDIR not found: $SETUPDIR" >&2; exit 1; }

VAR_TABLE="$SETUPDIR/var_table.tsv"
[[ ! -f "$VAR_TABLE" ]] && {
    echo "Error: var_table.tsv not found: $VAR_TABLE" >&2
    echo "Run setup.py before compress.sh." >&2
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"

# Build lookup: varname -> quant from var_table.tsv
# var_table.tsv columns: var, freq, units, cell_methods, positive,
#                        levels, refh, quant, standard_name, long_name
declare -A VAR_QUANT
while IFS=$'\t' read -r var freq units cell_methods positive levels refh quant _rest; do
    [[ "$var" == "var" ]] && continue  # skip header
    VAR_QUANT[$var]="$quant"
    echo "$var ${VAR_QUANT[$var]}"
done < "$VAR_TABLE"



echo "Scanning input directory: $INDIR"

for dir in "$INDIR"/*/; do
    [[ ! -d "$dir" ]] && continue
    dirname="$(basename "$dir")"
    varname="${dirname%.*}"      # strip .freq suffix

    files=("$dir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    quant="${VAR_QUANT[$varname]:-}"

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

        if [[ -n "$quant" && "$quant" != "--" ]]; then
            # Lossy compression: --ppc at specified precision, plus deflate
            echo "ncks -h -O -7 -L1 --ppc ${varname}=${quant} --chunk_cache 4000000000 --chunk_map rd1 $infile $outfile" >> "$cmdfile"
        else
            # Lossless deflate only (required by CORDEX spec for fx variables)
            echo "ncks -h -O -7 -L1 --chunk_cache 4000000000 --chunk_map rd1 $infile $outfile" >> "$cmdfile"
        fi
        (( ncommands++ )) || true
    done

    ngenerated=$(( ncommands - nskipped ))
    echo "  $dirname: $ngenerated commands (quant=${quant:--}), skipped $nskipped/$ncommands existing"

    [[ ! -s "$cmdfile" ]] && rm "$cmdfile"
done

echo ""
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "To run with launch_multi:"
echo "  launch_multi --run RUNDIR ${CMDDIR}/*.cmd"
