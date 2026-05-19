#!/bin/bash

# format.sh - Generate commandfiles for applying CF and CORDEX-CMIP6 metadata
# to extracted WRF variables.  Designed for use with launch_multi and
# launch_cf, matching the pattern established by extract.sh.
#
# Reads extracted files from INDIR (the OUTDIR from extract.sh) and generates
# cmorize.sh commands, writing output to OUTDIR.  One commandfile per variable.
#
# Requires setup.py to have been run first: SETUPDIR must contain
#   wrf.xy.coords.nc   - coordinate reference file
#   sim.env            - simulation metadata (shell key=value pairs)
#   var_table.tsv      - per-variable specs (tab-separated)

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR SETUPDIR OUTDIR [CMDDIR]

Generate commandfiles for formatting (CMORizing) extracted WRF variables.

Arguments:
  INDIR     Output directory from extract.sh (contains variable subdirectories)
  SETUPDIR  Output directory from setup.py (contains ancillary files)
  OUTDIR    Output directory for formatted files
  CMDDIR    Directory for commandfiles (default: current directory)

Options:
  --force         Overwrite existing output files
  --scripts PATH  Directory containing cmorize.sh
                  (default: directory containing format.sh)
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

# Check that ancillary files from setup.py exist in SETUPDIR
for f in wrf.xy.coords.nc sim.env var_table.tsv; do
    [[ ! -f "$SETUPDIR/$f" ]] && {
        echo "Error: Required file not found: $SETUPDIR/$f" >&2
        echo "Run setup.py before format.sh." >&2
        exit 1
    }
done

[[ ! -f "$SCRIPTS_DIR/cmorize.sh" ]] && {
    echo "Error: cmorize.sh not found in $SCRIPTS_DIR" >&2
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"

# Build set of variables in var_table.tsv (where metadata info lives).
# Note that the 'freq' column is the CORDEX requested frequency;
# actual frequency depends on what we saved and is inferred from
# subdirectory name
declare -A VAR_KNOWN
while IFS=$'\t' read -r var _rest; do
    [[ "$var" == "var" ]] && continue  # skip header
    VAR_KNOWN[$var]=1
done < "$SETUPDIR/var_table.tsv"

echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue
    dirname="$(basename "$vardir")"

    # Subdirectories are named <var>.<freq> (e.g. tas.1hr, orog.fx).
    if [[ "$dirname" != *.* ]]; then
        echo "  $dirname: not in var.freq format, skipping" >&2
        continue
    fi
    varname="${dirname%.*}"
    freq="${dirname##*.}"

    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    [[ -z "${VAR_KNOWN[$varname]:-}" ]] && {
        echo "  $dirname: $varname not in var_table.tsv, skipping" >&2
        continue
    }

    cmdfile="$CMDDIR/${varname}.${freq}.cmd"
    > "$cmdfile"
    ncommands=0
    nskipped=0

    for infile in "${files[@]}"; do
        [[ ! -f "$infile" ]] && continue
        fname="$(basename "$infile")"
        outfile="$OUTDIR/$dirname/$fname"

        if [[ -f "$outfile" && $FORCE -eq 0 ]]; then
            (( nskipped++ )) || true
            (( ncommands++ )) || true
            continue
        fi

        # cmorize.sh arguments: var freq infile outfile setupdir
        # Per-variable metadata is read by cmorize.sh from var_table.tsv
        echo "./cmorize.sh $varname $freq $infile $outfile $SETUPDIR" >> "$cmdfile"
        (( ncommands++ )) || true
    done

    ngenerated=$(( ncommands - nskipped ))
    echo "  $dirname: $ngenerated commands, skipped $nskipped/$ncommands existing"

    [[ ! -s "$cmdfile" ]] && rm "$cmdfile"
done

echo ""
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "To run with launch_multi:"
echo "  launch_multi --run RUNDIR --workflow cordex ${CMDDIR}/*.cmd"
