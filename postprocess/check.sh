#!/bin/bash

# check.sh - Scan a launch_multi run directory for failed tasks.
#
# Each commandfile submitted via launch_multi gets its own subdirectory in
# RUNDIR, and each task in that commandfile produces a .o<jobid>.N output
# file.  A task is considered successful if its output file ends with a line
# starting "Done".  Tasks with no such line are flagged as failures.
#
# Usage: check.sh RUNDIR
#
# Output:
#   Prints a summary table: one line per commandfile with counts of
#   succeeded / failed / total tasks.  Exits 0 if all tasks succeeded,
#   1 if any failed.
#
# Notes:
#   - Assumes all jobs have finished.  Check with `qstat` first.
#   - Empty stdout files (step-NNNNN.out in the stdout subdirectory) are
#     expected for successful tasks; non-empty ones may indicate warnings.

set -euo pipefail

usage() {
    echo "Usage: $(basename "$0") RUNDIR" >&2
    exit 1
}

[[ $# -ne 1 ]] && usage

RUNDIR="$(realpath "$1")"
[[ ! -d "$RUNDIR" ]] && { echo "Error: RUNDIR not found: $RUNDIR" >&2; exit 1; }

any_failed=0
header_printed=0

print_header() {
    printf '%-30s  %6s  %6s  %6s\n' 'Job' 'Done' 'Failed' 'Total'
    printf '%-30s  %6s  %6s  %6s\n' '---' '----' '------' '-----'
    header_printed=1
}

for jobdir in "$RUNDIR"/*/; do
    [[ ! -d "$jobdir" ]] && continue
    jobname="$(basename "$jobdir")"

    # Find all task output files (.oNNNNNN.N pattern)
    ofiles=("$jobdir"*.o*.*)
    # If glob didn't expand (no files), skip
    [[ ! -f "${ofiles[0]:-}" ]] && {
        [[ $header_printed -eq 0 ]] && print_header
        printf '%-30s  %6s  %6s  %6s\n' "$jobname" '-' '-' '0 (no output files)'
        continue
    }

    ndone=0
    nfail=0
    ntotal=0

    for ofile in "${ofiles[@]}"; do
        [[ ! -f "$ofile" ]] && continue
        (( ntotal++ )) || true
        # Check if last non-empty line starts with "Done"
        if tail -n 20 "$ofile" | grep -q '^Done'; then
            (( ndone++ )) || true
        else
            (( nfail++ )) || true
        fi
    done

    [[ $header_printed -eq 0 ]] && print_header

    if [[ $nfail -gt 0 ]]; then
        any_failed=1
        printf '%-30s  %6d  %6d  %6d  <-- FAILED\n' "$jobname" "$ndone" "$nfail" "$ntotal"
        # List the specific failing files for easier debugging
        for ofile in "${ofiles[@]}"; do
            [[ ! -f "$ofile" ]] && continue
            if ! tail -n 20 "$ofile" | grep -q '^Done'; then
                echo "    $ofile"
            fi
        done
    else
        printf '%-30s  %6d  %6d  %6d\n' "$jobname" "$ndone" "$nfail" "$ntotal"
    fi
done

# Check for non-empty stdout files (warnings/errors from tasks)
echo ""
echo "Checking stdout files for unexpected output..."
nnoisy=0
for jobdir in "$RUNDIR"/*/; do
    [[ ! -d "$jobdir/stdout" ]] && continue
    for f in "$jobdir/stdout"/*.out; do
        [[ ! -f "$f" ]] && continue
        if [[ -s "$f" ]]; then
            echo "  Non-empty stdout: $f"
            (( nnoisy++ )) || true
        fi
    done
done
[[ $nnoisy -eq 0 ]] && echo "  All stdout files are empty (good)."

echo ""
if [[ $any_failed -eq 1 ]]; then
    echo "RESULT: Some tasks failed.  Review the files listed above."
    exit 1
else
    echo "RESULT: All tasks completed successfully."
    exit 0
fi
