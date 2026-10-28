# source shell_vars.bash <model>
# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis
#
# Bias-adjustment counterpart to postprocess/shell_vars.bash.
# <model> is a config name (ece, mpi, era, nor, cesm): biasadjust/ba_config/<model>.yml

if [[ $# -ne 1 ]]; then
    echo "Usage: source shell_vars.bash <model>"
    return 1
fi

vname=(ba post baconfig scratch topdir sdir \
  fdir indir1 outdir1 cmddir1 rundir1 \
  cdir indir2 outdir2 cmddir2 rundir2 \
  rdir indir3 outdir3 cmddir3 rundir3 \
  indir4 outdir4 \
  qdir indir5flat outdir5 cmddir5 rundir5)
for v in "${vname[@]}"; do unset "$v"; done
unset v vname

export id=$1

## Script directories
ba=$(dirname $(realpath $BASH_SOURCE))
post=$(realpath $ba/../postprocess)

baconfig=$ba/ba_config/$id.yml
if [[ ! -f $baconfig ]]; then
    echo "invalid id: $id (no $baconfig)"
    return 1
fi

## Where the processing happens.  relocate_ba.sh hard-links, so the
## repack and DRS trees must be on the same filesystem.
scratch=/glade/derecho/scratch/mcginnis/jacob/na-cordex-ba
topdir=$scratch/$id

# Step 0: setup
sdir=$topdir/setup

# Step 1: format (metadata fix + concatenation)
fdir=$topdir/format
outdir1=$fdir/data
cmddir1=$fdir/cmd
rundir1=$fdir/run

# Step 2: compress (postprocess/compress.sh)
cdir=$topdir/compress
indir2=$outdir1
outdir2=$cdir/data
cmddir2=$cdir/cmd
rundir2=$cdir/run

# Step 3: repack (postprocess/repack.sh)
rdir=$topdir/repack
indir3=$outdir2
outdir3=$rdir/data
cmddir3=$rdir/cmd
rundir3=$rdir/run

# Step 4: relocate into DRS tree (relocate_ba.sh)
indir4=$outdir3
outdir4=$topdir

# Step 5: QA (postprocess/qa.sh)
qdir=$topdir/qa
indir5flat=$outdir3
outdir5=$qdir/qa
cmddir5=$qdir/cmd
rundir5=$qdir/run

echo "NA-CORDEX-CMIP6 bias-adjustment reformatting environment:"
echo "      ba: $ba"
echo "    post: $post"
echo "      id: $id"
echo "  config: $baconfig"
echo "  topdir: $topdir"
