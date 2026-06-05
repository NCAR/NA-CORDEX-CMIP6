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

Generates commandfiles that use the various `postproc_*.py` scripts to
extract data from raw wrfout files into yearly files by variable.
Skips over any existing outputs unless you use `--force`.

Extracted data is organized in per-variable subdirectories (e.g., all `uas`
files go into a subdirectory named `uas`).

NA-CORDEX simulations are run in decadal chunks with 2.5 years of
spinup, so (for example) the simulation for 1980--1989 starts in June
1977, and the raw WRF output files live in a subdirectory named
`1977_chunk`.  `extract.sh` handle mapping the requested years to the
correct chunk directory.


### `format.sh`

Generates commandfiles that use `cmorize.sh` to reformat extracted
data, adding coordinate variables and metadata to meet CF and CORDEX
specs.  Takes the setup directory generates by `setup.py` as an
argument because `cmorize.sh` needs the ancillary files saved there.


### `aggregate.sh`

Generates commandfiles that use CDO & NCO commands to aggregate sub-daily
data to daily and daily to monthly.  Needs to be run twice in a row;
both passes can (should) write to the same output directory.  Also
copies & concatenates files as appropriate if no aggregation is
needed.  Files are organized by variable and frequency.


### `compress.sh`

Generates commandfiles that apply lossy compression to data using
`ncks -L1 --ppc`.  Level of compression is defined in `var_specs.yml`.
Variables are compressed using decimal significant digits (DSD); e.g.,
`-ppc tas=.2` means round to the nearest 0.01 K.

```
type		units	rounded	DSD	vars
temperature	K	0.01	.2	tas* wchill humidex utci wbgt
winds		m s-1	0.01	.2	uas* vas* sfcWind
humidity	(kg/kg)	1e-6	.6	hus*
radiation	W m-2	0.1	.1	r??s hf?s 
pressure	Pa	1	.0	ps psl
geopotential	m	1	.0	zg*
percentage	%	0.1	.1	clt, hurs
convection	J kg-1	0.1	.1	cape cin
hydro flux	kg/m2/s	1e-6	.6	pr fzra snm mrr* evspsbl
column water	kg/m2	0.1	.1	prw snw mrso
snow depth	m	0.001	.3	snd
static		*	--	N/A	orog sftlf

prw,swe,mrso	kg m-2	0.1	.1		= 0.1 mm
snd		m  	0.001	.3		= 1 mm (sub-mm not meaningful)

hus		kg/kg		.6	2?	= 0.1% at low (1e-4) values
pr,fzra		kg/m2/s		.6	2?	~= 0.1 mm/day, NSD=2 @low
snm,mrr*,evs*	kg/m2/s		.6	2?	(same as precip)

```
DSD for hydro flux and column water variables is based on an
observational floor of 0.1 mm = trace precip.


### `plot.sh`

Generates commandfiles that create diagnostic plots of the data using
`plot.postprocess.var.py`.  The plots show the first, last, and middle
timesteps plus a timeseries plot near Boulder, CO; this is generally
enough to spot the kinds of errors created by errors in
postprocessing.  Plots are saved in a `figs` subdirectory rather than
a `data` directory, but otherwise organized in the same way.


### `relocate.sh`

For QA and publication on ESGF, the files must be organized into a DRS
directory tree following the CORDEX spec.  `relocate.sh` does this
using hard links, to avoid duplicating the data.  To copy the DRS tree
to a different filesystem, use Globus, which will do it in parallel
with error-checking.


### `merge_qa.py`

The workflow uses `esgqa` to check the formatting of the output files,
which generates a JSON file for each branch of the dataset.  To avoid
needing to upload dozens of files to the website that interpret them,
`merge_qa.py` combines them into a single JSON file.


### `index.py`

Generates commandfiles that calculate climate indexes (one file per
index) using CDO & NCO, for use in GIS-based climate impacts studies.
A number of commands take other derived quantities as inputs, so the
commandfiles must be run in sequence, rather than in parallel; use the
`--chain` option for `launch_multi` to submit them as dependent jobs.

