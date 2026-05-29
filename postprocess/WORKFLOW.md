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
5. Repack
6. Plot
7. Relocate
8. QA
9. Move

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

$post/extract.sh $raw $sdir $outdir1 1980-2023 $cmddir1

$post/launch_multi --workflow cordex --run $rundir1 $cmddir1/*cmd


## NOTE: if you're extracting wbgt/utci, runs can take 2-3 hours;
## override --workflow defaults with --wall (at end of flags)

set cmdxtra = $edir/cmd.xtra
set runxtra = $edir/run.xtra

$post/extract.sh --vars wbgt $raw $sdir $outdir1 1980-2023 $cmdxtra

$post/launch_multi --workflow cordex --run $runxtra --wall 03:00:00 $cmdxtra/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir1
foreach i (*)
echo $i
cat $i/stdout*/* | sort
tail -q -n 1 $i/*.o* | cut -f 1 -d : | uniq -c
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

set tmpdir3 = $adir/tmp
set cmddir3 = $adir/cmd
set rundir3 = $adir/run

$post/agg2.sh $indir3 $sdir $tmpdir3 $outdir3 $cmddir3


################
## wait until it finishes, check everything ran correctly
cd $rundir3
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
# Step 5: repack

set rdir = $topdir/repack

set indir5  = $outdir4
set outdir5 = $rdir/data
set cmddir5 = $rdir/cmd
set rundir5 = $rdir/run

$post/repack.sh $indir5 $sdir $outdir5 $cmddir5

$post/launch_multi --workflow cordex --run $rundir5 $cmddir5/repack.cmd


## wait until it finishes, check everything ran correctly
## symlinks indicate files that ncrepack-cordex did not overwrite (failures)
cd $rundir5
wc */stdout*/*
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
find $outdir5 -type l
cd $topdir


################
# Step 6: plot

set pdir = $topdir/plot

set indir6  = $outdir5
set outdir6 = $pdir/figs
set cmddir6 = $pdir/cmd
set rundir6 = $pdir/run


$post/plot.sh $indir6 $outdir6 $cmddir6

$post/launch_multi --workflow cordex --run $rundir6 $cmddir6/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir6
wc */stdout*/*
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
cd $topdir


## viewing all the plots is probably easier if you download them

echo "scp -r casper.hpc.ucar.edu:$outdir6 cordex-plots"



################
# Step 7: relocate into DRS tree

set indir7 = $outdir5
set outdir7 = .

$post/relocate.sh --dry-run $indir7 $sdir $outdir7 | tail

$post/relocate.sh $indir7 $sdir $outdir7


################
# Step 8: QA

set qdir = $topdir/qa

set indir8  = `find $topdir/CORDEX-CMIP6 -type d -name v1-r1`
set outdir8 = $qdir/qa
set cmddir8 = $qdir/cmd
set rundir8 = $qdir/run

mkdir -p $outdir8 $cmddir8 $rundir8

set cmdfile = $cmddir8/qa.cmd
rm -f $cmdfile; touch $cmdfile

set tests = " -t wcrp_cordex_cmip6:latest -t cf:1.9"

# Note: output dirctories must be empty


foreach freq  (fx mon)
  rm -rf $outdir8/$freq
  mkdir -p $outdir8/$freq
  echo esgqa -o $outdir8/$freq $tests $indir8/$freq >> $cmdfile
end

foreach freq  (day 1hr 6hr)
  foreach var (`/bin/ls -1 $indir8/$freq`)
    rm -rf $outdir8/$freq/$var
    mkdir -p $outdir8/$freq/$var
    echo esgqa -o $outdir8/$freq/$var $tests $indir8/$freq/$var >> $cmdfile
  end
end


cd $rundir8
cp $cmdfile .
echo module restore default > config_env.sh
echo conda activate nac6 >> config_env.sh

launch_cf -A $PROJECT -l walltime=00:05:00 -q casper -j oe -N esgf_qa $cmdfile


## wait until it finishes, check everything ran correctly
cd $rundir8
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
# Step 9: use globus to move final results from scratch to campaign

# login to globus & transfer files using the web interface

chmod -R ug+rwX o+rX 


################
# Step 10: generate climate indexes for GIS

set idir = $topdir/index

set indir10  = $outdir5
set outdir10 = $idir/data
set cmddir10 = $idir/cmd
set rundir10 = $idir/run

python $post/index.py $indir10 $outdir10 $cmddir10


# launch jobs as dependent chain

$post/launch_multi --chain --run $rundir10 --wall 00:30:00 --mem 50GB\
		   $cmddir10/concat.cmd $cmddir10/minmax.cmd \
		   $cmddir10/pctile.cmd  $cmddir10/indices.cmd \
		   $cmddir10/annual.cmd $cmddir10/merge.cmd

## check everything ran correctly

cd $rundir10

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
