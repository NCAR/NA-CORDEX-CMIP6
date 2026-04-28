# Postprocessing for NA-CORDEX-CMIP6

The scripts in this directory extract data from raw `wrfout` files and
format them according to the CORDEX-CMIP6 specs, which include CF
compliance.

## Overview

Step-by-step commands for running the post-processing workflow are in
[WORKFLOW.md](WORKFLOW.md).

The workflow runs in parallel using commandfiles.  

For each step, there's a top-level script that generates commandfiles.
Those are are submitted as job arrays to the PBS scheduler using
`launch_multi`, which sets everything up and corrals all the output
into a separate directory for each job.  There's a subdirectory for
each step (with the same name as the script), and each step reads from
its predecessor's output directory and writes to its own output
directory, so input files are not modified and any step can be
partially or fully re-run.

The resulting directory structure looks like this:

```
$scratch/
  CORDEX-CMIP6/     <- DRS output directory tree
  setup/            <- coordinate files, CSVs, CMOR tables
  extract/          <- first step in the workflow
    cmd/            <- commandfiles (one per variable)
    data/           <- per-variable subdirectories of extracted NetCDF files
    run/            <- launch_multi run directories (one per commandfile)
  format/           <- next step in the workflow
    ...             <- same `cmd/ run/ data/` structure as above
  etc.
```



## Getting Started

Run `install.sh` once to create the `nac6` conda environment and install the
ESGF-QA checker.

This script uses `mamba` to build the environment from
`environment.yml` and then configures `esgvoc` for CORDEX-CMIP6.  It
can be computationally intensive (particularly the `esgvoc install`
step), so run it on a Casper interactive node, not a login node.


## Usage Notes

### `launch_multi`

`launch_multi` iterates through a set of commandfiles and, for each one,
creates a subdirectory in `$rundir` named after the commandfile and runs
`launch_cf` from there.  This keeps all captured output neatly contained,
making it easier to track down problems and simplifying cleanup.

`launch_cf` needs a script named `config_env.sh` in its run directory with
the appropriate `module load` commands and `conda activate nac6`.
`launch_multi` will copy `~/config_env.sh` by default, or you can specify
one with `--config`.

`launch_multi` also copies the commandfile into the run directory, along with
any other needed scripts or files specified with `--copy`.  Making local copies
avoids contention when many processes try to read the same file simultaneously.

When running memory- or time-intensive steps, adjust `--mem` and `--wall`
based on observed usage.  The compress step tends to have the largest
footprint.

To run commandfiles as a dependent chain (each step waits for the previous
to finish), use `--chain`:

```
launch_multi --chain --run $rundir file1.cmd file2.cmd file3.cmd
```

### Checking job output

After a step finishes, check for successful completion by scanning job output
files in the run directory.  The exact check varies by step; see WORKFLOW.md
for the commands used at each step.  The general pattern is:

```
# Non-empty stdout files may indicate unexpected output
wc */stdout*/*

# Count how many task outputs end with "Done"
grep Done */*.o* | cut -f 2 -d : | uniq -c
```

### `setup.py`

Takes any WRF chunk directory (containing `wrfout_d01_*` files) and an output
directory.  Creates coordinate reference files and downloads the data request
CSV and CMOR JSON tables needed by downstream steps.

If `dreq_default.csv` already exists at `$WORK/cordex6/dreq_default.csv`,
`setup.py` copies it rather than downloading it.

### `extract.sh`

NA-CORDEX simulations are run in decadal chunks with ~3 years of spinup.  For
example, raw WRF output for 1980--1989 lives in a subdirectory named
`1977_chunk`.  `extract.sh` maps requested years to the correct chunk
directory automatically.

Extracted data is organized in per-variable subdirectories (e.g., all `uas`
files go into a subdirectory named `uas`).

If `CMDDIR` is not specified, it defaults to `.`.

### `format.sh` and `cmorize.sh`

`format.sh` reads variable metadata from `dreq_default.csv` and `var_specs.yml`
at commandfile-generation time and embeds it as positional arguments to
`cmorize.sh` in the commandfile.  `cmorize.sh` uses the coordinate files
produced by `setup.py`.

### `aggregate.sh`

Run twice: first to aggregate hourly data to daily, then again to aggregate
daily to monthly.  Both passes can write to the same output directory.

### `compress.sh`

Generates one `ncks --ppc` command per file.  Variables with a `qnt` entry in
`var_specs.yml` receive lossy compression at that precision; all others receive
lossless deflate only (`-7 -L1`), as required by the CORDEX spec.
