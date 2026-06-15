# source shell_vars.sh id 

# stopifnot arg #1 exists


## The directory where all the scripts (including this one) live.
## This is black magic for csh that tells you where this script lives
## even though you're sourcing it rather than running it.
set post = `ls -l /proc/$$/fd | sed -e 's/^[^/]*//' | grep "shell_vars.csh" | dirname`

## simulation configuration
set simconfig  = $post/sim_info/$id.sim_config.yml
set start_year = `grep start_year $simconfig | cut -f2 -d:`
set end_year   = `grep end_year $simconfig | cut -f2 -d:`
set years = ${start_year}-${end_year}

## Where the raw data lives
set base = /glade/campaign/ral/risc/collections/na-cordex-cmip6/raw
set raw = $base/`grep '^path:' $simconfig | cut -f2 -d: | tr -d ' \"'`


# Step 0: setup
## Where you want the postprocessing to happen
set scratch = /glade/derecho/scratch/$USER/na-cordex
set topdir = $scratch/$id
set sdir = $topdir/setup

# Step 1: extract
set edir = $topdir/extract
set outdir1 = $edir/data
set cmddir1 = $edir/cmd
set rundir1 = $edir/run

# Step 2: format
set fdir = $topdir/format
set indir2  = $outdir1
set outdir2 = $fdir/data
set cmddir2 = $fdir/cmd
set rundir2 = $fdir/run

# Step 3: aggregate
set adir = $topdir/aggregate
set indir3  = $outdir2
set outdir3 = $adir/data
set tmpdir3 = $adir/tmp
set cmddir3 = $adir/cmd
set rundir3 = $adir/run

# Step 4: compress
set cdir = $topdir/compress
set indir4  = $outdir3
set outdir4 = $cdir/data
set cmddir4 = $cdir/cmd
set rundir4 = $cdir/run

# Step 5: repack
set rdir = $topdir/repack
set indir5  = $outdir4
set outdir5 = $rdir/data
set cmddir5 = $rdir/cmd
set rundir5 = $rdir/run

# Step 6 : relocate into DRS tree
set indir6 = $outdir5
set outdir6 = $topdir

# Step 7: QA
set qdir = $topdir/qa
set indir7drs = `find $topdir/CORDEX-CMIP6 -type d -name v1-r1`
set indir7flat = $outdir5
set outdir7 = $qdir/qa
set cmddir7 = $qdir/cmd
set rundir7 = $qdir/run

#set simname = `basename $topdir`

# Step 8: plotting
set pdir = $topdir/plot
set indir8  = $outdir5
set outdir8 = $pdir/figs
set cmddir8 = $pdir/cmd
set rundir8 = $pdir/run

# Step 9: move to campaign using globus

# Step 10: calculate climate indexes for GIS
set idir = $topdir/index
set indir10  = $outdir5
set outdir10 = $idir/data
set cmddir10 = $idir/cmd
set rundir10 = $idir/run

