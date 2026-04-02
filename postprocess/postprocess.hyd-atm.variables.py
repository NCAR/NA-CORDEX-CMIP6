# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis

# Purpose:
# --------
# This script extracts data for a single variable for 1 year from
# daily WRF output files.  It handles unit conversion and variable
# derivation (e.g. wind rotation, precipitation de-accumulation), but
# further formatting and standards compliance (e.g. adding coordinates
# and metadata) are handled downstream by cmorize.sh.
#
# Variable metadata specifications are taken from:
# https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables

# USAGE NOTES:
# -----

# 1. This script processes a single year of data for one variable,
#    specified via commandline arguments.

# argument 1 : Path to wrfoutput files (chunk directory)
# argument 2 : Year (int)
# argument 3 : Variable (CMORized var name)
# argument 4 : Output directory (where variable subdirs are created)

# 2. It creates variable subdirectories under OUTDIR (argument 4) and
#    writes post-processed output there.  It does not change the
#    working directory; all paths are handled explicitly.

# 3. It is designed for command-file parallelism via launch_cf on
#    Casper HPC at NCAR.  It requires both NCO and CDO; when running
#    in parallel, they need to be made available via `module load`
#    commands in config_env.sh

# 4. 12-km WRF output is very large; be sure to request sufficient memory
#    (~100GB)

# Example execution:
# ------------------------------------------------
# $ python postprocess.core.variables.py {wrfout_path} 1980 tas {outdir}
# ------------------------------------------------

from collections import defaultdict
import xarray as xr
from xarray import ufuncs
from datetime import date
import numpy as np
import pandas as pd
import glob
import sys
import os
import json
import time
import resource

t0 = time.perf_counter()

# -----------------
# keyword arguments

wrfout_path = sys.argv[1]  # path to wrf output
year        = sys.argv[2]  # year
variable    = sys.argv[3]  # variable (cmorized syntax)
outdir      = sys.argv[4]  # output directory (variable subdirs created here)

os.makedirs(outdir, exist_ok=True)

# -------------------------------
# START OF USER DEFINED VARIABLES
# -------------------------------

# Setting to True will overwrite all post-processed data: careful!
do_overwrite_existing = True

# The fixed / static variables orog & sftlf come from variables in a
# WRF input file (HGT and LANDMASK in wrfinput_d01) rather than from
# an output file; wrfinput_path is the authoritative source for these
# input files for the NA-CORDEX simulations.
wrfinput_path     = "/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/ERA5_HIST_E03/input_example/"

wrfout_hour_fname  = "wrfout_hour_d01_"  # prefix for files with hourly output
wrfout_6hour_fname = "wrfout_d01_"  # prefix for files with hourly output
wrfout_fx_fname    = "wrfout_5day_d01_"  # prefix for files with LANDFRAC and HGT

# -------------------------------
# END OF USER DEFINED VARIABLES
# -------------------------------

# Dimension renaming (specified by CORDEX)
#-------------------
dname_map_t = {'Time': 'time'}
dname_map_xy = {'west_east': 'x', 'south_north': 'y'}
dname_map_xyt = dname_map_t | dname_map_xy


# File naming
# -----------
# See CORDEX-CMIP6 archiving specifications for file naming conventions.

dom_id = 'NAM-12'       # domain_id: name assigned to cordex region
drs_id = 'ERA5'         # driving_source_id: ID of driving GCM / reanalysis
dre_id = 'evaluation'   # driving_experiment_id: "evaluation" for ERA5
drv_id = 'r1i1p1f1'     # driving_variant_label: CMIP6 variant id (rxixpxfx)
org_id = 'NCAR'         # institution_id
src_id = 'WRF461S-SN'   # source_id: CORDEX RCM ID
ver_id = 'v1-r1'        # v: version, r: RCM ensemble number

# Base filename string (without leading var and trailing freq/timespan)
fname_base = f'{dom_id}_{drs_id}_{dre_id}_{drv_id}_{org_id}_{src_id}_{ver_id}'

def make_fname(var, cmor_freq):
    """Construct the output filename for a variable at a given frequency.
    For fx variables, appends .nc directly (no timespan component).
    For all others, returns a complete filename with timespan."""
    if cmor_freq == 'fx':
        return f'{var}_{fname_base}_fx.nc'
    elif cmor_freq == '1hr':
        return f'{var}_{fname_base}_1hr_{year}010100-{year}123123.nc'
    elif cmor_freq == '6hr':
       return f'{var}_{fname_base}_6hr_{year}010100-{year}123123.nc'
    elif cmor_freq == 'day':
        return f'{var}_{fname_base}_day_{year}0101-{year}1231.nc'
    elif cmor_freq == 'mon':
        return f'{var}_{fname_base}_mon_{year}01-{year}12.nc'

# Create array of target files
# ----------------------------
start_date   = pd.Timestamp(f'{year}-01-01')
day_before   = start_date - pd.Timedelta(days=1)
end_of_month = start_date + pd.offsets.MonthEnd(0)
end_date     = start_date + pd.offsets.YearEnd(0) + pd.Timedelta(hours=23)
#end_date = start_date + pd.offsets.MonthEnd(0) + pd.Timedelta(hours=23) # Uncomment for quicker testing

time_dim     = pd.date_range(start_date, end_date, freq='h')
acc_time_dim = pd.date_range(day_before, end_date, freq='h')
day_time_dim = pd.date_range(day_before, end_date, freq='d')
six_hr_time_dim = pd.date_range(start_date, end_date, freq='6h')

hr_files     = []
six_hr_files = []
afwa_files   = []
for date in day_time_dim:
    d = str(date)[:10]
    file_str = f'{wrfout_path}/{wrfout_hour_fname}{d}_00:00:00'
    hr_files.append(file_str)

    afwa_file_str = f'{wrfout_path}/wrfout_afwa_d01_{d}_00:00:00'
    afwa_files.append(afwa_file_str)

    six_hr_file_str = f'{wrfout_path}/wrfout_d01_{d}_00:00:00'
    six_hr_files.append(six_hr_file_str)

# ----------------------------

# Load datasets when needed
# -------------------------

fx_glob = f'{wrfout_path}/{wrfout_fx_fname}{year}*'
if not (fx_matches := glob.glob(fx_glob)):
    raise FileNotFoundError(f'No fx files found matching: {fx_glob}')
ds_fx = xr.open_dataset(fx_matches[0])

def load_wrf(files, accumulated=False):
    """Load WRF output files into a dataset and rename dimensions.
    accumulated=True sets mask_and_scale=False, needed for bucket variables
    like precipitation and radiation that use integer overflow accumulation."""
    ds = xr.open_mfdataset(files,
                             concat_dim='Time',
                             combine='nested',
                             chunks={'time':1,'south_north':673,'west_east':707},
                             mask_and_scale=(not accumulated),
                             decode_times=False,
                             decode_coords=False).fillna(1.e20)
    return ds.rename(dname_map_xyt)

# Loader tags:
#   'hr'   : standard hourly files, skip day-before timestep
#   'acc'  : hourly files including day-before timestep, for accumulated vars
#   'afwa' : AFWA diagnostic files, skip day-before timestep
#   'six_hr' : standard six-hourly files, skip day-before timestep
def load_by_tag(tag):
    if tag == 'hr'  : return load_wrf(hr_files[1:])
    if tag == 'acc' : return load_wrf(hr_files,    accumulated=True)
    if tag == 'afwa': return load_wrf(afwa_files[1:])
    if tag == 'six_hr': return load_wrf(six_hr_files[1:])
    raise ValueError(f'Unknown loader tag: {tag}')

# ----------------------

# Output existence check
# ----------------------
def _output_needed(var, fout):
    if os.path.exists(os.path.join(outdir, var, fout)) and not do_overwrite_existing:
        print(f'{var}/{fout} EXISTS : Skipping')
        return False
    return True

# Write extracted variables
# -------------------------
# Writes raw extracted data to outdir/var/fname.nc.
def write_vars(var_da_list):
    for var, cmor_freq, da in var_da_list:
        fout = make_fname(var, cmor_freq)
        if not _output_needed(var, fout):
            continue

        vardir = os.path.join(outdir, var)
        os.makedirs(vardir, exist_ok=True)
        outpath = os.path.join(vardir, fout)

        da.astype(np.float32).to_netcdf(outpath)

        print(f'postproc time: {time.perf_counter() - t0:.1f} sec')
        mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print(f'postproc max memory: {mem / (1024*1024):.1f} GB')


# Clean functions
# ---------------
# Each takes a loaded dataset and returns a list of
# (var, cmor_freq, dataset) tuples for write_vars to process.

# Snow water equivalent - surface snow amount
# ---------------------------------------------------
def clean_snw(ds):
    snw = ds['SNOW']
    snw['time'] = six_hr_time_dim

    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    snw = snw.where(landmask == 1, 1.e20)

    snw = snw.to_dataset(name='snw').drop_attrs()
    return [('snw', '6hr', snw)]
# ---------------------------------------------------

# Snow depth
# ---------------------------------------------------
def clean_snd(ds):
    # SNOWH units : m
    # SNOWH description : PHYSICAL SNOW DEPTH

    snd = ds['SNOWH']
    snd['time'] = six_hr_time_dim
    snd = snd.to_dataset(name='snd').drop_attrs()

    return [('snd', '6hr', snd)]
# ---------------------------------------------------

# Total soil moisture content
# ---------------------------------------------------
def clean_mrso(ds):
    # SMOIS units: m3 m-3 (volumetric)
    # SMOIS description: SOIL MOISTURE
    # Noah-MP soil layer thicknesses (m): 0.10, 0.30, 0.60, 1.00

    da = ds['SMOIS']

    # Integrate volumetric soil moisture over layer thicknesses (m of water),
    # then convert to kg m-2 (* 1000)
    mrso = (da[:, 0] * 0.10 +
            da[:, 1] * 0.30 +
            da[:, 2] * 0.60 +
            da[:, 3] * 1.00) * 1000

    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    mrso = mrso.where(landmask == 1, 1.e20)

    mrso['time'] = six_hr_time_dim
    mrso = mrso.to_dataset(name='mrso').drop_attrs()

    return [('mrso', '6hr', mrso)]
# ---------------------------------------------------

# Surface runoff
# ---------------------------------------------------
def clean_mrros(ds):
    # SFROFF units : mm
    # SFROFF description : SURFACE RUNOFF

    mrros = ds['SFROFF']
    mrros['time'] = six_hr_time_dim
    mrros = mrros.to_dataset(name='mrros').drop_attrs()

    return [('snd', '6hr', snd)]
# ---------------------------------------------------


# Dispatch table
# --------------
# Maps variable names to their clean function and loader tag.
# The default output frequency is '1hr'; exceptions are listed in _OUTFREQ.
# The default loader is 'hr'; exceptions are listed in _LOADER.
#
# Loader tags:
#   'hr'   : standard hourly files, skip day-before timestep
#   'acc'  : hourly files including day-before timestep, for accumulated vars
#   'afwa' : AFWA diagnostic files, skip day-before timestep

_CLEAN = {var: globals()[f'clean_{var}']
          for var in ['snw','snd','mrso',
                      'mrros',
                      ]}

_OUTFREQ = defaultdict(lambda: '1hr', snw='6hr', snd='6hr', 
                       mrso='6hr', mrros='6hr',
                       tasmax='day', tasmin='day')

_LOADER = {
    'snw' : 'six_hr',
    'snd' : 'six_hr',
    'mrso' : 'six_hr',
    'mrros' : 'six_hr',
}

def get_dispatch(var):
    """Return (clean_fn, loader_tag) for a variable,
    applying defaults for anything not explicitly overridden."""
    if var not in _CLEAN:
        return None
    return _CLEAN[var], _LOADER.get(var, 'hr')


# Call functions
# --------------
if variable == 'fx':
    clean_fx()
else:
    entry = get_dispatch(variable)
    if entry is None:
        print(f'Warning: unknown variable {variable}')
    else:
        clean_fn, loader_tag = entry
        if _output_needed(variable, make_fname(variable, _OUTFREQ[variable])):
            write_vars(clean_fn(load_by_tag(loader_tag)))

#print('Done')

# Plotting is handled separately; see plot.postprocess.var.py
