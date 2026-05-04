# NA-CORDEX-CMIP6 Postprocessing Workflow

This file contains the commands for running the post-processing
workflow pipeline on NCAR's Casper system.

The basic design is that there's a directory for each step, with
subdirectories `cmd`, `run`, and `data`.  For each step, you first run
a script to generate commandfiles in `cmd`, then you run those
commandfiles using `launch_multi`.  The files the commands generate
get put in `data`, and the outputs from the processes all get captured
in `run`.

The commands are meant to be run interactively, not as a script; for
each step, you need to run that step, wait for it to finish, check
whether everythin processed correctly, and fix any errors before
moving on to the next one.

Shell variables, loops, etc, are written for tcsh; if you use bash,
you'll have to translate.  For testing on a single year, change the
year range to a single year and add `/test` to `$topdir`.

This code is currently set up for post-processing an ERA5 downscaling;
modify as appropriate for other simulations.  And, of course, you'll
need to modify all the paths.

The workflow steps are:

0. Setup
1. Extract
2. Format
3. Aggregate
4. Compress
5. Plot
6. Relocate
7. QA
8. Move

___

```
tcsh

################
# Step 0: setup

conda activate nac6


set raw = `realpath ~/image/collections/na-cordex-cmip6/raw/ERA5/eval/`
set scratch = `realpath ~/glade-scratch/na-cordex/`
set here = `realpath ~/work/cordex6`
set post = $here/NA-CORDEX-CMIP6/postprocess
set topdir = $scratch/era5

set sdir = $topdir/setup

python $post/setup.py $raw $sdir


################
# Step 1: extract

# If you're re-running the extract stage (e.g., if new variables were
# added) and you don't want to regenerate everything, just change
# $cmddir1 and $rundir1

set edir = $topdir/extract
set outdir1 = $edir/data
set cmddir1 = $edir/cmd
set rundir1 = $edir/run

$post/extract.sh $raw $outdir1 1980-2023 $cmddir1

$post/launch_multi --workflow cordex --run $rundir1 $cmddir1/*cmd

## NOTE: if you're extracting wbgt/utci, runs can take 2-3 hours;
## override --workflow defaults with --wall (at end of flags)


## wait until it finishes, check everything ran correctly
cd $rundir1
foreach i (*)
echo $i
cat $i/stdout*/* | sort
tail -q -n 1 $i/*.o*
echo ------------
end
cd $topdir


#chmod -R -w $outdir1

################
# Step 2: format

set fdir = $topdir/format

set indir2  = $outdir1
set outdir2 = $fdir/data
set cmddir2 = $fdir/cmd
set rundir2 = $fdir/run


$post/format.sh $indir2 $sdir $outdir2 $cmddir2

$post/launch_multi --workflow cordex --run $rundir2 $cmddir2/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir2
cat */stdout*/* | uniq -c
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
cd $topdir



################
# Step 3: aggregate

set adir = $topdir/aggregate

set indir3  = $outdir2
set outdir3 = $adir/data
set cmddir3a = $adir/cmd1
set cmddir3b = $adir/cmd2
set rundir3a = $adir/run1
set rundir3b = $adir/run2


$post/aggregate.sh $indir3 $sdir $outdir3 $cmddir3a

$post/launch_multi --workflow cordex --run $rundir3a $cmddir3a/*cmd

# wait 'til it finishes, then do it again to generate monthly files

$post/aggregate.sh $indir3 $sdir $outdir3 $cmddir3b

$post/launch_multi --workflow cordex --run $rundir3b $cmddir3b/*cmd


################
## wait until it finishes, check everything ran correctly
cd $rundir3a
wc */stdout*/* | tail -1
tail -q -n 1 */*.o* | cut -f 1 -d : | sort | uniq -c
cd $rundir3b
wc */stdout*/* | tail -1
tail -q -n 1 */*.o* | cut -f 1 -d : | sort | uniq -c
cd $topdir


################
# Step 4: compress

set cdir = $topdir/compress

set indir4  = $outdir3
set outdir4 = $cdir/data
set cmddir4 = $cdir/cmd
set rundir4 = $cdir/run


$post/compress.sh $indir4 $sdir $outdir4 $cmddir4

$post/launch_multi --workflow cordex --run $rundir4 $cmddir4/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir4
wc */stdout*/*
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
cd $topdir


################
# Step 5: plot

set pdir = $topdir/plot

set indir5  = $outdir4
set outdir5 = $pdir/figs
set cmddir5 = $pdir/cmd
set rundir5 = $pdir/run


$post/plot.sh $indir5 $outdir5 $cmddir5

$post/launch_multi --workflow cordex --run $rundir5 $cmddir5/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir5
wc */stdout*/*
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
cd $topdir


## viewing all the plots is probably easier if you download them

echo "scp -r casper.hpc.ucar.edu:$outdir5 cordex-plots"



################
# Step 6: relocate into DRS tree

set indir6 = $outdir4
set outdir6 = .

$post/relocate.sh --dry-run $indir6 $sdir $outdir6 | tail

$post/relocate.sh $indir6 $sdir $outdir6


################
# Step 7: QA

set qdir = $topdir/qa

set indir7  = `find $topdir/CORDEX-CMIP6 -type d -name v1-r1`
set outdir7 = $qdir/qa
set cmddir7 = $qdir/cmd
set rundir7 = $qdir/run

mkdir -p $outdir7 $cmddir7 $rundir7

set cmdfile = $cmddir7/qa.cmd
rm -f $cmdfile; touch $cmdfile

set tests = " -t wcrp_cordex_cmip6:latest -t cf:1.9"

# Note: output dirctories must be empty


foreach freq  (fx mon)
  rm -rf $outdir7/$freq
  mkdir -p $outdir7/$freq
  echo esgqa -o $outdir7/$freq $tests $indir7/$freq >> $cmdfile
end

foreach freq  (day 1hr)
  foreach var (`/bin/ls -1 $indir7/$freq`)
    rm -rf $outdir7/$freq/$var
    mkdir -p $outdir7/$freq/$var
    echo esgqa -o $outdir7/$freq/$var $tests $indir7/$freq/$var >> $cmdfile
  end
end


cd $rundir7
cp $cmdfile .
echo module restore default > config_env.sh
echo conda activate nac6 >> config_env.sh

launch_cf -A $PROJECT -l walltime=00:05:00 -q casper -j oe -N esgf_qa $cmdfile


## wait until it finishes, check everything ran correctly
cd $rundir7
wc stdout*/*
tail -q -n 1 *.o* | cut -f 1 -d : | uniq -c
cd $topdir

# merge cluser.json files so you only need to upload one
set simname = `basename $topdir`
python $post/merge-qa.py $qdir/$simname.qa.merged.json --find $qdir/qa
echo "scp casper.hpc.ucar.edu:$qdir/$simname.qa.merged.json ."


# download results, then check the cluster.json file at:
https://cmiphub.dkrz.de/info/display_qc_results.html


################
# Step 8: use globus to move final results from scratch to campaign

# login to globus & transfer files using the web interface

chmod -R ug+rwX o+rX 


################
# Step 9: generate climate indexes for GIS

set idir = $topdir/index

set indir9  = $outdir4
set outdir9 = $idir/data
set cmddir9 = $idir/cmd
set rundir9 = $idir/run

python $post/index.py $indir9 $outdir9 $cmddir9


# launch jobs as dependent chain

$post/launch_multi --chain --run $rundir9 --wall 00:30:00 --mem 50GB\
		   $cmddir9/concat.cmd $cmddir9/minmax.cmd \
		   $cmddir9/pctile.cmd  $cmddir9/indices.cmd \
		   $cmddir9/annual.cmd $cmddir9/merge.cmd

## check everything ran correctly

cd $rundir9

foreach i (*)
  echo =====================
  echo $i
  wc -l $i/*.cmd
  grep Done $i/*.o* | wc -l
  wc $i/stdout*/* | tail -1
  grep Done $i/*.o* | cut -f 2 -d = | cut -f 3-4 -d ' ' | sort -n | uniq -c
end

cd $topdir



```

## Installing the ESGF compliance checker

```
https://github.com/ESGF/esgf-qa


# First, create a bare conda environment (mamba is faster):

mamba create -n esgf-qa python=3.10


# Then install the package into it:

# esgvoc wants this
setenv PYTHONIOENCODING utf-8

conda activate esgf-qa
pip install esgf-qa
pip install esgvoc
rehash
esgvoc config set universe:branch=esgvoc_dev
esgvoc config add cordex-cmip6
esgvoc install


# It spawns a whole mess of subprocesses; Be sure to run it on a
# Casper interactive node, not on a login node.
```
