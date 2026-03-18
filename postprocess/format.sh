#!/bin/bash

# format.sh - Generate commandfiles for applying CF and CORDEX-CMIP6 metadata
# to extracted WRF variables.  Designed for use with launch_multi and
# launch_cf, matching the pattern established by extract.sh.
#
# Reads extracted files from INDIR (the OUTDIR from extract.sh) and generates
# cmorize.sh commands, writing output to OUTDIR.  One commandfile per variable.
#
# Coordinate reference files must exist in INDIR (created by setup.sh using
# the same directory as extract.sh OUTDIR).

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR OUTDIR [CMDDIR]

Generate commandfiles for formatting (CMORizing) extracted WRF variables.

Arguments:
  INDIR    Output directory from extract.sh (contains variable subdirectories
           and coordinate files produced by setup.sh)
  OUTDIR   Output directory for formatted files
  CMDDIR   Directory for commandfiles (default: current directory)

Options:
  --force         Overwrite existing output files
  --scripts PATH  Directory containing cmorize.sh and var_specs.yml
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
mkdir -p $2
OUTDIR="$(realpath $2)"
CMDDIR="${3:-.}"
mkdir -p $CMDDIR
CMDDIR="$(realpath $CMDDIR)"

[[ ! -d "$INDIR" ]] && { echo "Error: INDIR not found: $INDIR" >&2; exit 1; }

# Check that coordinate file exists in INDIR (produced by setup.sh)
for cf in wrf.xy.coords.nc; do
    [[ ! -f "$INDIR/$cf" ]] && {
        echo "Error: Coordinate file not found: $INDIR/$cf" >&2
        echo "Run setup.sh with the same OUTDIR as extract.sh." >&2
        exit 1
    }
done

for f in cmorize.sh var_specs.yml; do
    [[ ! -f "$SCRIPTS_DIR/$f" ]] && {
        echo "Error: $f not found in $SCRIPTS_DIR" >&2
        exit 1
    }
done

# DR_CSV lives in INDIR alongside extracted data (placed there by setup.sh)
DR_CSV="$INDIR/dreq_default.csv"
[[ ! -f "$DR_CSV" ]] && {
    echo "Error: dreq_default.csv not found in $INDIR" >&2
    echo "Run setup.sh with the same OUTDIR as extract.sh." >&2
    exit 1
}

mkdir -p "$OUTDIR" "$CMDDIR"
cp "$DR_CSV" "$OUTDIR"

# Load var_specs to get levels and refh per variable.
# Python one-liner handles YAML anchors/aliases.
get_spec() {
    local var="$1" field="$2"
    python3 -c "
import yaml
s = yaml.safe_load(open('$SCRIPTS_DIR/var_specs.yml'))
v = s.get('$var', {})
print(v.get('$field', 'None'))
"
}

# Build associative arrays of CMOR metadata keyed by variable name
declare -A VAR_FREQ VAR_UNITS VAR_CELL VAR_STDN VAR_LN
while IFS=',' read -r out_name frequency units long_name standard_name cell_methods _rest; do
    VAR_FREQ[$out_name]="$frequency"
    VAR_UNITS[$out_name]="$units"
    VAR_CELL[$out_name]="$cell_methods"
    VAR_STDN[$out_name]="$standard_name"
    VAR_LN[$out_name]="$long_name"
done < <(tail -n +2 "$DR_CSV")

echo "Scanning input directory: $INDIR"

for vardir in "$INDIR"/*/; do
    [[ ! -d "$vardir" ]] && continue
    varname="$(basename "$vardir")"

    files=("$vardir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    # Get variable metadata
    freq="${VAR_FREQ[$varname]:-}"
    units="${VAR_UNITS[$varname]:-}"
    cell="${VAR_CELL[$varname]:-None}"
    stdn="${VAR_STDN[$varname]:-}"
    ln="${VAR_LN[$varname]:-}"
    levels="$(get_spec "$varname" levels)"
    refh="$(get_spec "$varname" refh)"

    [[ -z "$freq" ]] && {
        echo "  $varname: not in data request CSV, skipping" >&2
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

        # cmorize.sh arguments:
        # var infile freq units lev refh cell ln stdn outfile indir
        echo "./cmorize.sh $varname $infile $freq \"$units\" $levels $refh \"$cell\" \"$ln\" \"$stdn\" $outfile $INDIR" >> "$cmdfile"
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
