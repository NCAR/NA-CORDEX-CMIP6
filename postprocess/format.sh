#!/bin/bash

# format.sh - Generate commandfiles for applying CF and CORDEX-CMIP6 metadata
# to extracted WRF variables.  Designed for use with launch_multi and
# launch_cf, matching the pattern established by extract.sh.
#
# Reads extracted files from INDIR (the OUTDIR from extract.sh / setup.py)
# and generates cmorize.sh commands, writing output to OUTDIR.
# One commandfile per variable.
#
# Requires setup.py to have been run first: INDIR must contain
#   wrf.xy.coords.nc   - coordinate reference file
#   sim.env            - simulation metadata (shell key=value pairs)
#   var_table.tsv      - per-variable specs (tab-separated)

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR [CMDDIR]

Generate commandfiles for formatting (CMORizing) extracted WRF variables.

Arguments:
  INDIR    Output directory from extract.sh / setup.py (contains variable
           subdirectories and ancillary files produced by setup.py)
  OUTDIR   Output directory for formatted files
  CMDDIR   Directory for commandfiles (default: current directory)

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

[[ $# -lt 2 ]] && usage

INDIR="$(realpath "$1")"
mkdir -p "$2"
OUTDIR="$(realpath "$2")"
CMDDIR="${3:-.}"
mkdir -p "$CMDDIR"
CMDDIR="$(realpath "$CMDDIR")"

[[ ! -d "$INDIR" ]] && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }

# Check that ancillary files from setup.py exist in INDIR
for f in wrf.xy.coords.nc sim.env var_table.tsv; do
    [[ ! -f "$INDIR/$f" ]] && {
        echo "Error: Required file not found: $INDIR/$f" >&2
        echo "Run setup.py with the same OUTDIR as extract.sh." >&2
        exit 1
    }
done

[[ ! -f "$SCRIPTS_DIR/cmorize.sh" ]] && {
    echo "Error: cmorize.sh not found in $SCRIPTS_DIR" >&2
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"

# Copy ancillary files into OUTDIR so cmorize.sh can find them
# alongside the formatted data (needed by downstream steps)
for f in sim.env var_table.tsv; do
    cp "$INDIR/$f" "$OUTDIR/$f"
done

# Build lookup: varname -> freq (to know which variables are in the data request)
declare -A VAR_FREQ
while IFS=$'\t' read -r var freq _rest; do
    [[ "$var" == "var" ]] && continue  # skip header
    VAR_FREQ[$var]="$freq"
done < "$INDIR/var_table.tsv"

echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue
    varname="$(basename "$vardir")"

    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    [[ -z "${VAR_FREQ[$varname]:-}" ]] && {
        echo "  $varname: not in var_table.tsv, skipping" >&2
        continue
    }

    cmdfile="$CMDDIR/${varname}.cmd"
    > "$cmdfile"
    ncommands=0
    nskipped=0

    for infile in "${files[@]}"; do
        [[ ! -f "$infile" ]] && continue
        fname="$(basename "$infile")"
        outfile="$OUTDIR/$varname/$fname"

        if [[ -f "$outfile" && $FORCE -eq 0 ]]; then
            (( nskipped++ )) || true
            (( ncommands++ )) || true
            continue
        fi

        # cmorize.sh arguments: var infile outfile
        # All other metadata is read by cmorize.sh from sim.env and var_table.tsv
        echo "./cmorize.sh $varname $infile $outfile" >> "$cmdfile"
        (( ncommands++ )) || true
    done

    ngenerated=$(( ncommands - nskipped ))
    echo "  $varname: $ngenerated commands, skipped $nskipped/$ncommands existing"

    [[ ! -s "$cmdfile" ]] && rm "$cmdfile"
done

echo ""
echo "Commandfiles written to: $CMDDIR"
echo ""
echo "To run with launch_multi:"
echo "  launch_multi --run RUNDIR --workflow cordex ${CMDDIR}/*.cmd"
