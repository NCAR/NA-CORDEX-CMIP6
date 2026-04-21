# Postprocessing for NA-CORDEX-CMIP6
----------------------------------


## Current workflow

### Overview

Each step in the workflow generates commandfiles (one per variable), which are
run in parallel using `launch_multi` and `launch_cf`.  Each step reads from its
predecessor's output directory and writes to its own output directory, so input
files are never modified and any step can be partially or fully re-run.

The resulting directory structure looks like:

```
$scratch/             <- shared coordinate files, CSVs, CMOR tables
  extract/
    cmd/              <- commandfiles (one per variable)
    data/             <- per-variable subdirectories of extracted NetCDF files
    run/              <- launch_multi run directories (one per commandfile)
  format/
    data/
    cmd/
    run/
  compress/
    data/
    cmd/
    run/
  aggregate/
    data/             <- CORDEX DRS output tree
    cmd/
    run/
```

Define shell variables for frequently used paths before starting.  For example:

```tcsh
set WRFIN = /glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/ERA5_HIST_E03
set WRFOUT = /glade/campaign/ral/risc/collections/na-cordex-cmip6/raw/ERA5/eval
set scratch = /glade/derecho/scratch/$USER/cordex6/era5
```

The version for the data is the date of simulation completion, in the
format `vYYYYMMDD`.  We can get this programmatically like this:

```
set version = v`find $WRFOUT/*chunk -type f -name wrfout\* -printf "%T+\n" | cut -f 1 -d + | tr -d - | sort | tail -1`
```



### Part 1: Setup

Run `setup.sh` once before any other step.  It creates the WRF coordinate
reference files and downloads the data request CSV and CMOR JSON tables needed
by downstream steps.  All outputs go to a single shared directory.

```bash
setup.sh $WRFOUT/1977_chunk $scratch
```

This must be run before `extract.sh`.  If either the coordinate files or the
downloaded tables are missing, subsequent scripts will halt with an error.


### Part 2: Extract variables from WRF output

1. Run `extract.sh WRFDIR OUTDIR YEARS [CMDDIR]` to generate per-variable
   commandfiles for a range of years, where:
    * `WRFDIR` is the path to a directory containing subdirectories named
      `<year>_chunk` that contain raw WRF output files.
    * `OUTDIR` is the directory where you want extracted data (also the
      directory produced by `setup.sh`, so that coordinate files and CSVs
      are alongside the extracted data).
    * `YEARS` is a year or range of years (e.g., `1980-2023`) to process.
    * `CMDDIR` is the directory where you want the commandfiles.

    ```bash
    set outdir = $scratch/extract
    mkdir -p 
    extract.sh $raw $outdir/data 1980 $outdir/cmd
    mkdir -p $POSTDIR/extract/{data,cmd,run}
    extract.sh $WRFDIR $POSTDIR/setup $POSTDIR/extract/data $YEARS $POSTDIR/extract/cmd
    ```

    Note: `OUTDIR` for `extract.sh` must be the same directory as `OUTDIR`
    for `setup.sh`, so that `postprocess.core.variables.py` can find the
    cached CMOR JSON tables.

2. Inspect the resulting commandfiles and make sure they look sensible.
   For example:

    ```
    python ./postprocess.core.variables.py /path/to/raw/WRF/data/1977_chunk \
      1980 uas /path/to/extract/data
    ```

3. Use `launch_multi` to run the commandfiles:

    ```
    launch_multi --run $POSTDIR/extract/run --workflow cordex $POSTDIR/extract/cmd/*.cmd
    ```

4. After everything has finished, check the captured output:

    ```
    check.sh $POSTDIR/extract/run
    ```

   If all tasks succeeded, `check.sh` exits 0.  Otherwise it lists the
   failing output files for debugging.  Fix any problems and re-run the
   relevant commandfile(s) before proceeding.


### Part 3: Format (CMORize) extracted data

5. Run `format.sh INDIR OUTDIR [CMDDIR]` to generate per-variable
   commandfiles, where:
    * `INDIR` is the `extract/data` directory (contains variable subdirectories
      and the coordinate files and CSV placed there by `setup.sh`).
    * `OUTDIR` is the directory where you want formatted data.
    * `CMDDIR` is the directory where you want the commandfiles.

    ```bash
    mkdir -p $POSTDIR/format/{data,cmd,run}
    format.sh $POSTDIR/extract/data $POSTDIR/format/data $POSTDIR/format/cmd
    launch_multi --run $POSTDIR/format/run --workflow cordex $POSTDIR/format/cmd/*.cmd
    check.sh $POSTDIR/format/run
    ```


### Part 4: Compress formatted data

6. Run `compress.sh INDIR OUTDIR [CMDDIR]` to generate per-variable
   commandfiles, where:
    * `INDIR` is the `format/data` directory.
    * `OUTDIR` is the directory where you want compressed data.
    * `CMDDIR` is the directory where you want the commandfiles.

    ```bash
    mkdir -p $POSTDIR/compress/{data,cmd,run}
    compress.sh $POSTDIR/format/data $POSTDIR/compress/data $POSTDIR/compress/cmd
    launch_multi --run $POSTDIR/compress/run --mem 50GB --wall 2:00:00 $POSTDIR/compress/cmd/*.cmd
    check.sh $POSTDIR/compress/run
    ```

   The compress step has the largest memory and wallclock footprint; adjust
   `--mem` and `--wall` based on observed usage.


### Part 5: Aggregate hourly data to daily and daily to monthly

7. Run `aggregate.sh $POSTDIR/compress/data $POSTDIR/aggregate/data $VERSION [CMDDIR]`
   where `INDIR` is the `compress/data` directory.

   This will generate commandfiles to aggregate hourly variables to daily and
   place them in a directory tree following the CORDEX spec.  (It also
   copies/concatenates input files where appropriate.)

    ```bash
    mkdir -p $POSTDIR/aggregate/{data,cmd_day,run_day,cmd_mon,run_mon}
    aggregate.sh $POSTDIR/compress/data $POSTDIR/aggregate/data $VERSION $POSTDIR/aggregate/cmd_day
    launch_multi --run $POSTDIR/aggregate/run_day $POSTDIR/aggregate/cmd_day/*.cmd
    check.sh $POSTDIR/aggregate/run_day
    ```

8. Run `aggregate.sh` a second time to generate daily → monthly commandfiles
   for the newly aggregated daily data.

    ```bash
    aggregate.sh $POSTDIR/aggregate/data $POSTDIR/aggregate/data $VERSION $POSTDIR/aggregate/cmd_mon
    launch_multi --run $POSTDIR/aggregate/run_mon $POSTDIR/aggregate/cmd_mon/*.cmd
    check.sh $POSTDIR/aggregate/run_mon
    ```


### Part 6: Plot everything to check that the data looks good

9. (See `plot.postprocess.var.py` for usage.  The plot script works on hourly,
   daily, and monthly data.)


### Part 7: QA

10. Run ESGF QA checks on the DRS output tree:

    ```bash
    conda activate esgqa
    esgqa $POSTDIR/aggregate/data
    ```

    Review the report and fix any issues before transferring.


### Part 8: Move the files to their final location

11. Use Globus to copy the final, QA-passed files from scratch to campaign
    storage.  Globus handles the transfer autonomously, in parallel, and with
    error checking.  Detailed instructions can be found in
    [NCAR's HPC documentation](https://ncar-hpc-docs.readthedocs.io/en/latest/storage-systems/data-transfer/globus/#transferring-files-with-the-web-interface).


### Usage Notes

#### `setup.sh`

Takes any WRF chunk directory (containing `wrfout_d01_*` files) and an output
directory.  Creates coordinate reference files and downloads the data request
CSV and CMOR JSON tables.  All downstream scripts look for these files in the
same directory as the extracted data, so use the same directory for `setup.sh`
output and `extract.sh` output.

If `dreq_default.csv` already exists at the default location
(`$WORK/cordex6/dreq_default.csv`), `setup.sh` copies it rather than
downloading it.

#### `extract.sh`

NA-CORDEX simulations are run in decadal chunks with ~3 years of spinup, so
the raw WRF output for 1980-1989 will be in a subdirectory named `1977_chunk`;
if you ask for data for 1988, `extract.sh` knows to use the files in
`1977_chunk` and not the ones in `1987_chunk`, where they are part of the
spinup for 1990-1999.

Extracted data is organized in subdirectories by variable (i.e., all the `uas`
files are in a subdirectory named `uas`, etc.)

If left unspecified, `CMDDIR` defaults to `.`

#### `format.sh` and `cmorize.sh`

`format.sh` reads variable metadata from `dreq_default.csv` and `var_specs.yml`
at commandfile-generation time and embeds it as positional arguments to
`cmorize.sh` in the commandfile.  `cmorize.sh` uses the coordinate files in
`INDIR` (placed there by `setup.sh`).

#### `compress.sh`

Generates one `ncks --ppc` command per file.  Variables with a `qnt` entry in
`var_specs.yml` receive lossy compression at that precision; all others receive
lossless deflate only (`-7 -L1`), as required by the CORDEX spec.

#### `launch_multi`

If you're running lots of tasks in parallel using commandfiles, sometimes
things will go wrong for no particular reason, due to hardware hiccups or
transient software problems.  So you need to check things afterwards to make
sure everything ran correctly, which means you need to capture output from your
processes.

You can also have problems that only affect a subset of the data, so it's a
good idea to have multiple commandfiles batched by something like variable or
year; this makes it easier to track down problems and lets you avoid re-running
the entire workflow.

However, `launch_cf` dumps any captured output into the directory that it is
run from.  This makes it hard to track down problems if you're running multiple
commandfiles at the same time, and creates a mess that will eventually need to
be cleaned up.

The `launch_multi` script iterates through a set of commandfiles, and for each
one, creates a directory in `$rundir` named after the commandfile and runs
`launch_cf` on it from there, so all the captured output is neatly contained,
making it easier to keep track of what's going on and simplifying cleanup when
you're done.

To get access to all the software packages you need in the runtime environment
(for this workflow, that includes CDO and NCO) `launch_cf` needs a script named
`config_env.sh` in the directory where it is run with the appropriate
`module load` commands and `conda activate na_cordex` for the Python scripts.
`launch_multi` will copy `~/config_env.sh` or you can specify it with
`--config`.

`launch_multi` also copies the commandfile into the run directory, as well as
any other needed scripts or files specified with `--copy`.  Making local copies
avoids problems that can be caused by a large number of processes all trying to
read the same file at the same time.

#### Checking job output

Use `check.sh RUNDIR` after all jobs have finished.  It scans each job's
output files for the `Done` line that indicates successful completion,
reports a summary table of succeeded/failed/total counts per commandfile, and
lists failing output files for easier debugging.  Exit code is 0 if all tasks
succeeded, 1 if any failed.

You can also check things manually:

```bash
# Non-empty stdout files may indicate unexpected output
wc */stdout*/*out

# Count how many task outputs end with Done
grep Done */*.o* | cut -f 2 -d : | uniq -c
```

#### `postprocess.core.variables.py`

This script needs `conda activate npl` in the `config_env.sh` for libraries
like `xarray` and `pandas`.  It reads cached CMOR JSON tables from its output
directory (placed there by `setup.sh`).

Note that this script has a large memory footprint and takes a while to run,
especially for precip.


### List of current known problems:

1.  Discrepancy in cell_methods from WRF output and WCRP guidelines

    There are currently disconnects for some variables between the attributes
    specified by the official WCRP JSON tables and the actual attributes of
    that variable as saved by WRF.

    The post-processing script corrects the units of the core variables, but
    the temporal characteristics of the data may not match the specified
    `cell_methods` attribute.  This is important, because it affects the time
    coordinates and bounds for those variables.

    The current problematic variables are:

    a.  clt (cloud fraction)
    b.  evspsbl (evaporation including sublimation and transpiration)
