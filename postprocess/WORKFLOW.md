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
whether everything processed correctly, and fix any errors before
moving on to the next one.

Shell variables, loops, etc, are written for tcsh; bash equivalents
are available where noted.

The workflow steps are:

0. Setup
1. Extract
2. Format
3. Aggregate
4. Compress
5. Repack
6. Relocate
7. QA
8. Plot
9. Move
10. Index

___

Before starting, source the appropriate shell variables script to set
up your environment:

```
# tcsh
source shell_vars.csh <id>

# bash
source shell_vars.sh <id>
```

where `<id>` is the simulation id as defined in
`sim_config/sim-info.tsv` (e.g. `era5-eval`).  This sets all the
variables used throughout the workflow.  `$id` gets set as an
envariable visible in other scripts.



```tcsh

################
# Step 0: setup

conda activate nac6

python $post/setup.py $sdir $post/sim_config/$id.yml



################
# Step 1: extract

# $years is set by shell_vars; override here if needed (e.g., testing)

$post/extract.sh $raw $sdir $outdir1 $years $cmddir1

$post/launch_multi --workflow cordex --run $rundir1 $cmddir1/*cmd

## wbgt (+utci) runs extra parallel, else it takes forever

$post/launch_multi --workflow cordex --run $rundir1 --chain \
  $cmddir1/wbgt/wbgt_mon.cmd $cmddir1/wbgt/wbgt_cat.cmd


## wait until it finishes, check everything ran correctly

## this is ridiculous, but it works
alias pad 'awk '"'"'{ for (i=1; i<=NF; i++) printf "%10s", $i; print ""}'"'"''

cd $rundir1
foreach i (*)
  echo -n $i"\t"
  printf "%s\t" `tail -q -n 1 $i/*.o* | cut -f 1 -d : | sort | uniq -c`
  printf "%s\t" `cat $i/stdout*/* | grep mem | datamash -sWR 2 min 4 mean 4 max 4 | pad`
  cat $i/stdout*/* | grep time | datamash -sWR 2 min 3 mean 3 max 3 | pad
end

cd $topdir

# chmod -R -w $outdir1

################
# Step 2: format

$post/format.sh $indir2 $sdir $outdir2 $cmddir2

$post/launch_multi --workflow cordex --run $rundir2 $cmddir2/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir2
cat */stdout*/* | uniq -c
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
cd $topdir



################
# Step 3: aggregate

$post/aggregate.sh $indir3 $sdir $tmpdir3 $outdir3 $cmddir3

$post/launch_multi --workflow cordex --run $rundir3 --chain $cmddir3/avg.cmd $cmddir3/mon.cmd $cmddir3/cat.cmd


## wait until it finishes, check everything ran correctly
cd $rundir3
wc */stdout*/* | tail -1
tail -q -n 1 */*.o* | cut -f 1 -d : | sort | uniq -c
cd $topdir



################
# Step 4: compress

$post/compress.sh $indir4 $sdir $outdir4 $cmddir4

$post/launch_multi --workflow cordex --run $rundir4 $cmddir4/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir4
wc */stdout*/* | tail -1
tail -q -n 1 */*.o* | cut -f 1 -d : | sort | uniq -c
cd $topdir



################
# Step 5: repack

$post/repack.sh $indir5 $sdir $outdir5 $cmddir5

$post/launch_multi --workflow cordex --run $rundir5 $cmddir5/repack.cmd


## wait until it finishes, check everything ran correctly
## symlinks indicate files that ncrepack-cordex did not overwrite (failures)
cd $rundir5
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
find $outdir5 -type l
cd $topdir



################
# Step 6: relocate into DRS tree

$post/relocate.sh --dry-run $indir6 $sdir $outdir6 | tail

$post/relocate.sh $indir6 $sdir $outdir6



################
# Step 7: QA

# indir7drs must be set here (DRS tree created in Step 6)
set indir7drs = `find $topdir/CORDEX-CMIP6 -type d -name v1-r1`

$post/qa.sh $indir7drs $indir7flat $sdir $outdir7 $cmddir7

$post/launch_multi --workflow cordex --run $rundir7 $cmddir7/*.cmd


## wait until it finishes, check everything ran correctly
cd $rundir7
foreach i (nccheck*)
  echo $i
  wc $i/stdout*/* | tail -1
  tail -q -n 1 $i/*.o* | cut -f 1 -d : | uniq -c
end

tail -q -n 1 esgqa/*.o* | cut -f 1 -d : | uniq -c
bash -c 'for f in esgqa/stdout*/*; do tac "$f" | sed "/###/q" | tac; done' |   cut -f1 -d: | sort | uniq -c

## examine QA results

cd $outdir7/ncrepack-check
foreach i (*)
  echo $i
  cat $i/*/* | cut -f1 -d: | sort | uniq -c
end

cd $topdir

## merge cluster.json files so you only need to upload one
python $post/merge_qa.py $qdir/$id.qa.merged.json --find $qdir/qa
echo "scp casper.hpc.ucar.edu:$qdir/$id.qa.merged.json ."

## download results, then check the cluster.json file at:
https://cmiphub.dkrz.de/info/display_qc_results.html

## ERRORs and WARNs to ignore:

## day files get a WARN for chunksize 1 along time, which is necessary
## b/c leap years + xarray can't handle different chunk sizes in a dataset

## Some of the variables in the NCAR-CORDEX-CMIP6_1hr.json files (fzra
## heatidz humidex wchill) aren't in the controlled vocabulary CMOR
## tables, which means that the CORDEX-CMIP6 plugin can't identify the
## main variable, which gives FAILs for [CDXT001] Time Chunking and
## [FILE003] Compression.

## We also get [CDXT001] Time Chunking FAILs for the variables that we
## are providing 6-hourly but are requested 1-hourly (hfls hfss rlus
## fsus [ua,va][50,100,150]m), because it can't find an entry for that
## var+freq in the CMOR tables.

## fzra & humidex don't have a CF standard_name (yet)



################
# Step 8: plot

$post/plot.sh $indir8 $outdir8 $cmddir8

$post/launch_multi --workflow cordex --run $rundir8 $cmddir8/*cmd


## wait until it finishes, check everything ran correctly
cd $rundir8
tail -q -n 1 */stdout*/* | cut -f 1 -d : | uniq -c
tail -q -n 1 */*.o* | cut -f 1 -d : | uniq -c
cd $topdir


## viewing all the plots is probably easier if you download them

echo "scp -r casper.hpc.ucar.edu:$outdir8 cordex-plots/$id"



################
# Step 9: use globus to move final results from scratch to campaign

# login to globus & transfer files using the web interface

# tweak file permissions - files world-read-only, directories inherit
# group & are group-writeable, but no deleting others' files

find . -type f -exec chmod 0444 {} +
find . -type d -exec chmod 3775 {} +


################
# Step 10: generate climate indexes for GIS

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
  wc -l $i/$i.cmd
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
