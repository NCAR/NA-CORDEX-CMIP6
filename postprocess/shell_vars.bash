# source shell_vars.sh id

## Validate argument
if [[ $# -ne 1 ]]; then
    echo "Usage: source shell_vars.sh <id>"
    return 1
fi

export id=$1

## The directory where all the scripts (including this one) live.
post=$(dirname $(realpath $BASH_SOURCE))

if ! grep -qP "^$id\t" $post/sim_info/sim-info.tsv; then
    echo "invalid id: $id"
    echo "not found in $post/sim_info/sim-info.tsv"
    return 1
fi


## simulation configuration
simconfig=$post/sim_info/$id.sim_config.yml
start_year=$(grep start_year $simconfig | cut -f2 -d: | xargs)
end_year=$(grep end_year $simconfig | cut -f2 -d: | xargs)
years=${start_year}-${end_year}

## Where the raw data lives
base=/glade/campaign/ral/risc/collections/na-cordex-cmip6/raw
raw=$base/$(grep '^path:' $simconfig | cut -f2 -d: | tr -d ' "')


# Step 0: setup
## Where you want the postprocessing to happen
scratch=/glade/derecho/scratch/$USER/na-cordex
topdir=$scratch/$id
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

# Step 3: aggregate
adir=$topdir/aggregate
indir3=$outdir2
outdir3=$adir/data
tmpdir3=$adir/tmp
cmddir3=$adir/cmd
rundir3=$adir/run

# Step 4: compress
cdir=$topdir/compress
indir4=$outdir3
outdir4=$cdir/data
cmddir4=$cdir/cmd
rundir4=$cdir/run

# Step 5: repack
rdir=$topdir/repack
indir5=$outdir4
outdir5=$rdir/data
cmddir5=$rdir/cmd
rundir5=$rdir/run

# Step 6: relocate into DRS tree
indir6=$outdir5
outdir6=$topdir

# Step 7: QA
qdir=$topdir/qa
indir7flat=$outdir5
outdir7=$qdir/qa
cmddir7=$qdir/cmd
rundir7=$qdir/run

# Step 8: plot
pdir=$topdir/plot
indir8=$outdir5
outdir8=$pdir/figs
cmddir8=$pdir/cmd
rundir8=$pdir/run

# Step 9: move to campaign using globus

# Step 10: calculate climate indexes for GIS
idir=$topdir/index
indir10=$outdir5
outdir10=$idir/data
cmddir10=$idir/cmd
rundir10=$idir/run

echo "NA-CORDEX-CMIP6 postprocessing environment:"
echo "    post: $post"
echo "      id: $id"
echo "   years: $years"
echo "  topdir: $topdir"
echo "     raw: $raw"
