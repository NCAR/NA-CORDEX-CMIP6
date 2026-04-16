# env.sh - Per-simulation environment for the NA-CORDEX-CMIP6 postprocessing
# workflow.  Source this once per shell session before running any workflow
# steps:
#
#   source env.sh
#
# Edit the USER SETTINGS block below when switching simulations.  Everything
# under DERIVED PATHS is computed from those values; you should not normally
# need to touch it.
#
# Safe to re-source.

# ---------------------------------------------------------------------------
# USER SETTINGS - edit these per simulation
# ---------------------------------------------------------------------------

# Raw WRF output: parent directory containing <YYYY>_chunk/ subdirectories
raw=$(realpath /glade/campaign/ral/risc/collections/na-cordex-cmip6/raw/ERA5/eval)

# Scratch root for all postprocessing output
scratch=$(realpath /glade/derecho/scratch/jsallen)

# Subdirectory of $scratch for this simulation's outputs
sim=era5

# Year range to process (e.g. 1980-2023, or a single year like 1980)
years=1980

# PBS account for launch_multi (-A flag).  Leave commented to inherit from
# whatever $PROJECT is already set to in your shell.
export PROJECT=NRIS0001

# Conda environment for the Python steps
conda_env=na_cordex

# ---------------------------------------------------------------------------
# DERIVED PATHS - computed from the above; usually no need to edit
# ---------------------------------------------------------------------------

# Repo / scripts location: this file lives in postprocess/, so $post is its
# directory regardless of where it's sourced from.
post=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Top-level working directory for this simulation
topdir=$scratch/$sim

# Step 0: setup
sdir=$topdir/setup

# Step 1: extract
edir=$topdir/extract
outdir1=$edir/data
cmddir1=$edir/cmd
rundir1=$edir/run

# Step 2: format
fdir=$topdir/format
indir2=$outdir1
outdir2=$fdir/data
cmddir2=$fdir/cmd
rundir2=$fdir/run

# Step 3: aggregate (two passes: daily, then monthly)
adir=$topdir/aggregate
indir3=$outdir2
outdir3=$adir/data
cmddir3a=$adir/cmd1
cmddir3b=$adir/cmd2
rundir3a=$adir/run1
rundir3b=$adir/run2

# Step 4: compress
cdir=$topdir/compress
indir4=$outdir3
outdir4=$cdir/data
cmddir4=$cdir/cmd
rundir4=$cdir/run

# Step 5: plot
pdir=$topdir/plot
indir5=$outdir4
outdir5=$pdir/figs
cmddir5=$pdir/cmd
rundir5=$pdir/run

# Step 6: relocate (DRS tree goes directly under $topdir)
indir6=$outdir4
outdir6=$topdir

# Step 7: QA
qdir=$topdir/qa
outdir7=$qdir/qa
cmddir7=$qdir/cmd
rundir7=$qdir/run
# indir7 is set after the DRS tree exists; uncomment after step 6:
#indir7=$(find $topdir/CORDEX-CMIP6 -type d -name v1-r1)

# ---------------------------------------------------------------------------
# Conda activation
# ---------------------------------------------------------------------------

# Initialize conda for this shell if needed, then activate the env.
if ! command -v conda >/dev/null 2>&1; then
    if [[ -f /glade/u/apps/opt/conda/etc/profile.d/conda.sh ]]; then
        source /glade/u/apps/opt/conda/etc/profile.d/conda.sh
    fi
fi

if command -v conda >/dev/null 2>&1; then
    conda activate "$conda_env"
else
    echo "Warning: conda not found; skipping 'conda activate $conda_env'" >&2
fi

# ---------------------------------------------------------------------------
# Sanity check / summary
# ---------------------------------------------------------------------------

echo "NA-CORDEX-CMIP6 postprocessing environment loaded:"
echo "  sim     = $sim"
echo "  years   = $years"
echo "  raw     = $raw"
echo "  topdir  = $topdir"
echo "  post    = $post"
echo "  PROJECT = ${PROJECT:-(unset)}"
[[ ! -d $raw  ]] && echo "  WARNING: \$raw not found: $raw" >&2
[[ ! -d $post ]] && echo "  WARNING: \$post not found: $post" >&2
