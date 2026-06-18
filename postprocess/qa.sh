#!/bin/bash
# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen
#
# qa.sh - Generate commandfiles for QA checks on CORDEX-CMIP6 NetCDF files.
#
# Runs two QA tools:
#
#   esgqa - ESGF compliance checker.
#             Requires a DRS-organized input tree (INDIR_DRS, the
#             version-level node, e.g. v1-r1/).  One commandfile per
#             frequency; output written to OUTDIR/esgqa/<freq>.
#             Output directories are cleared before writing
#             commandfiles, as required by esgqa.
#

#   ncrepack-cordex-check - Checks CORDEX internal packing
#             requirements.  Reads from the flat var.freq layout
#             (INDIR_FLAT, the repack/data/ directory).  One
#             commandfile per frequency; within each frequency, one
#             invocation per variable (all years passed at once).
#
# Usage:
#   qa.sh [OPTIONS] INDIR_DRS INDIR_FLAT SETUPDIR OUTDIR CMDDIR
#
# Arguments:
#   INDIR_DRS   Version-level node of the DRS tree (e.g. .../v1-r1);
#               used by esgqa
#   INDIR_FLAT  Flat repack/data/ directory (var.freq/ subdirs); used
#               by ncrepack-cordex-check
#   SETUPDIR    Output directory from setup.py (provides esgqa test flags
#               via qa_config.sh, if present; otherwise defaults are
#               used)
#   OUTDIR      Root output directory for QA results
#   CMDDIR      Directory for commandfiles

set -euo pipefail

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") [OPTIONS] INDIR_DRS INDIR_FLAT SETUPDIR OUTDIR CMDDIR

Generate commandfiles for QA checks on CORDEX-CMIP6 NetCDF files.

Arguments:
  INDIR_DRS   Version-level node of DRS tree (used by esgqa)
  INDIR_FLAT  Flat repack/data/ directory with var.freq/ subdirs (used by ncrepack-cordex-check)
  SETUPDIR    Output directory from setup.py
  OUTDIR      Root output directory for QA results
  CMDDIR      Directory for commandfiles

Options:
  --tests TESTS   esgqa test flags (default: "-t wcrp_cordex_cmip6:1.0 -t cf:1.9")
  --force         Clear and regenerate all commandfiles
  -h, --help      Show this help message
EOF
    exit 1
}

# Default esgqa tests; override with --tests
ESGQA_TESTS="-t wcrp_cordex_cmip6:1.0 -t cf:1.9"
FORCE=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tests)   ESGQA_TESTS="$2"; shift 2 ;;
        --force)   FORCE=1; shift ;;
        -h|--help) usage ;;
        -*) echo "Error: Unknown option $1" >&2; usage ;;
        *) break ;;
    esac
done

[[ $# -lt 5 ]] && usage

INDIR_DRS="$(realpath "$1")"
INDIR_FLAT="$(realpath "$2")"
SETUPDIR="$(realpath "$3")"
mkdir -p "$4"
OUTDIR="$(realpath "$4")"
mkdir -p "$5"
CMDDIR="$(realpath "$5")"

[[ ! -d "$INDIR_DRS" ]]  && { echo "Error: INDIR_DRS not found: $INDIR_DRS" >&2; exit 1; }
[[ ! -d "$INDIR_FLAT" ]] && { echo "Error: INDIR_FLAT not found: $INDIR_FLAT" >&2; exit 1; }
[[ ! -d "$SETUPDIR" ]]   && { echo "Error: SETUPDIR not found: $SETUPDIR" >&2; exit 1; }

NCCHECK="$SETUPDIR/ncrepack-cordex-check"
[[ ! -x "$NCCHECK" ]] && { echo "Error: ncrepack-cordex-check not found or not executable: $NCCHECK" >&2; exit 1; }

ESGQA_OUTDIR="$OUTDIR/esgqa"
NCCHECK_OUTDIR="$OUTDIR/ncrepack-check"
mkdir -p "$ESGQA_OUTDIR" "$NCCHECK_OUTDIR"

# Frequencies handled as bulk (fx, mon): one esgqa call per frequency.
# Frequencies handled per-variable (day, 1hr, 6hr): one esgqa call per variable.
BULK_FREQS=(fx mon)
PERVAR_FREQS=(day 1hr 6hr)


# -----------------------------------------------------------------------
# esgqa commandfiles
# -----------------------------------------------------------------------
# Each line in the commandfile is one esgqa invocation.  esgqa requires
# its output directory to be empty, so we clear them here rather than at
# run time.

echo "Generating esgqa commandfiles..."
ESGQA_CMD="$CMDDIR/esgqa.cmd"
> "$ESGQA_CMD"
n_esgqa=0

for freq in "${BULK_FREQS[@]}"; do
    freqdir="$INDIR_DRS/$freq"
    [[ ! -d "$freqdir" ]] && continue

    outd="$ESGQA_OUTDIR/$freq"
    if [[ -d "$outd" && $FORCE -eq 1 ]]; then
        rm -rf "$outd"
    fi
    mkdir -p "$outd"

    echo "esgqa -P 1 -o $outd $ESGQA_TESTS $freqdir" >> "$ESGQA_CMD"
    echo "  esgqa: $freq"
    (( n_esgqa++ )) || true
done

for freq in "${PERVAR_FREQS[@]}"; do
    freqdir="$INDIR_DRS/$freq"
    [[ ! -d "$freqdir" ]] && continue

    for vardir in "$freqdir"/*/; do
        [[ ! -d "$vardir" ]] && continue
        varname="$(basename "$vardir")"

        outd="$ESGQA_OUTDIR/$freq/$varname"
        if [[ -d "$outd" && $FORCE -eq 1 ]]; then
            rm -rf "$outd"
        fi
        mkdir -p "$outd"

        echo "esgqa -o $outd $ESGQA_TESTS $vardir" >> "$ESGQA_CMD"
        echo "  esgqa: $freq/$varname"
        (( n_esgqa++ )) || true
    done
done

[[ ! -s "$ESGQA_CMD" ]] && rm "$ESGQA_CMD"
echo "  Total esgqa commands: $n_esgqa"


# -----------------------------------------------------------------------
# ncrepack-cordex-check commandfiles
# -----------------------------------------------------------------------
# One commandfile per frequency; within each frequency, one invocation
# per variable (all files for that variable passed at once).

echo ""
echo "Generating ncrepack-cordex-check commandfiles..."

declare -A NCCHECK_CMDS   # freq -> cmdfile path
declare -A NCCHECK_COUNTS # freq -> number of commands

for dir in "$INDIR_FLAT"/*/; do
    [[ ! -d "$dir" ]] && continue
    dirname="$(basename "$dir")"

    # dirname is var.freq (e.g. tas.day); split on last dot
    varname="${dirname%.*}"
    freq="${dirname##*.}"

    files=("$dir"*.nc)
    [[ ! -f "${files[0]:-}" ]] && continue

    # Initialize cmdfile for this frequency if not yet seen
    if [[ -z "${NCCHECK_CMDS[$freq]:-}" ]]; then
        NCCHECK_CMDS[$freq]="$CMDDIR/nccheck.${freq}.cmd"
        > "${NCCHECK_CMDS[$freq]}"
        NCCHECK_COUNTS[$freq]=0
    fi

    outd="$NCCHECK_OUTDIR/$freq/$varname"
    mkdir -p "$outd"

    echo "$NCCHECK ${files[*]} > $outd/nccheck.${varname}.${freq}.txt 2>&1" >> "${NCCHECK_CMDS[$freq]}"
    echo "  ncrepack-cordex-check: $dirname (${#files[@]} files)"
    (( NCCHECK_COUNTS[$freq]++ )) || true
done

# Remove empty commandfiles and report
for freq in "${!NCCHECK_CMDS[@]}"; do
    cmdfile="${NCCHECK_CMDS[$freq]}"
    [[ ! -s "$cmdfile" ]] && rm "$cmdfile" && continue
    echo "  Total nccheck commands for $freq: ${NCCHECK_COUNTS[$freq]}"
done


# -----------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------
echo ""
echo "Commandfiles written to: $CMDDIR"
