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

wrfout_hour_fname = "wrfout_hour_d01_"  # prefix for files with hourly output
wrfout_fx_fname   = "wrfout_5day_d01_"  # prefix for files with LANDFRAC and HGT

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

hr_files = []
afwa_files = []
for date in day_time_dim:
    d = str(date)[:10]
    file_str = f'{wrfout_path}/{wrfout_hour_fname}{d}_00:00:00'
    hr_files.append(file_str)

    afwa_file_str = f'{wrfout_path}/wrfout_afwa_d01_{d}_00:00:00'
    afwa_files.append(afwa_file_str)

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
def load_by_tag(tag):
    if tag == 'hr'  : return load_wrf(hr_files[1:])
    if tag == 'acc' : return load_wrf(hr_files,    accumulated=True)
    if tag == 'afwa': return load_wrf(afwa_files[1:])
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

# Near-Surface Air Temperature
# ---------------------------------------------------
def clean_tas(ds):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim
    tas = tas.to_dataset(name='tas').drop_attrs()

    return [('tas', '1hr', tas)]
# ---------------------------------------------------

# Daily maximum near-surface air temperature
# ---------------------------------------------------
def clean_tasmax(ds):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tas_max = tas.groupby('time.dayofyear').max()
    tas_max['dayofyear'] = day_time_dim[1:]
    tas_max = tas_max.rename({'dayofyear':'time'}).to_dataset(name='tasmax')

    return [('tasmax', 'day', tas_max)]
# ---------------------------------------------------

# Daily minimum near-surface air temperature
# ---------------------------------------------------
def clean_tasmin(ds):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tas_min = tas.groupby('time.dayofyear').min()
    tas_min['dayofyear'] = day_time_dim[1:]
    tas_min = tas_min.rename({'dayofyear':'time'}).to_dataset(name='tasmin')

    return [('tasmin', 'day', tas_min)]
# ---------------------------------------------------

# Hourly precipitation accumulation
# ---------------------------------------------------
def clean_pr(ds):
    # I_RAINC units : mm
    # I_RAINC description: integer bucket variable for convective precipitation (tips at 100 mm)
    # I_RAINNC units : mm
    # I_RAINNC description: integer bucket variable for non-convective precipitation (tips at 100 mm)
    # RAINC units : mm
    # RAINC description : accumulated convective precipitation
    # RAINNC units : mm
    # RAINNC description : accumulated non-convective precipitation

    pr_vars = ['I_RAINC','I_RAINNC','RAINC','RAINNC']

    da = ds[pr_vars]
    da['time'] = acc_time_dim

    #  / 3600 : mm/hour --> mm/sec == kg s-1 m-2
    tp = ( ((da['I_RAINC']*100.) + da['RAINC']) +
           ((da['I_RAINNC']*100.) + da['RAINNC']) ) / 3600
    pr = tp.diff(dim='time').to_dataset(name='pr').sel(time=time_dim)

    return [('pr', '1hr', pr)]
# ---------------------------------------------------

# Evaporation including sublimation and transpiration
# ---------------------------------------------------
def clean_evspsbl(ds):
    # EDIR units : mm/s
    # EDIR description : ground surface evaporation rate
    # ETRAN units : mm/s
    # ETRAN description : transpiration rate

    da = ds[['EDIR','ETRAN']]
    da['time'] = time_dim
    evspsbl = (da['EDIR'] + da['ETRAN']).to_dataset(name='evspsbl')

    return [('evspsbl', '1hr', evspsbl)]
# ---------------------------------------------------

# Near surface specific humidity
# ---------------------------------------------------
def clean_huss(ds):
    # Q2 units: kg kg-1
    # Q2 description: mixing ratio (QV) at 2 M

    da = ds[['Q2']].rename({'Q2':'huss'})
    da['time'] = time_dim
    huss = (da / (1 + da))  # Convert mixing ratio to specific humidity

    return [('huss', '1hr', huss)]
# ---------------------------------------------------

# Near surface relative humidity
# ---------------------------------------------------
def clean_hurs(ds):
    # Q2 units: kg kg-1
    # Q2 description: mixing ratio (QV) at 2 M
    # T2 units: K
    # T2 description: 2-meter temperature
    # PSFC units: Pa
    # PSFC description: Surface pressure

    # https://glossary.ametsoc.org/wiki/Latent_heat
    # Physical constants - Clausius-Clapeyron
    epsilon = 0.622      # Molecular weight ratio of water/dry air
    Lv = 2.5e6           # Latent heat of vaporization (J/kg)
    Rv = 461.5           # Gas constant for water vapor (J/kg/K)
    T0 = 273.15          # Reference temperature (K)
    e0 = 611.2           # Reference saturation vapor pressure (Pa)

    # Actual vapor pressure from mixing ratio:
    # e = r*P / (epsilon + r)
    e = (ds['Q2'] * ds['PSFC']) / (epsilon + ds['Q2'])

    # Saturation vapor pressure via Clausius-Clapeyron:
    # e_s(T) = e0 * exp[(Lv/Rv)(1/T0 - 1/T)]
    e_s = e0 * ufuncs.exp( (Lv/Rv) * ((1/T0) - (1 / ds['T2'])) )

    hurs = (e / e_s) * 100

    # Sometimes model outputs result in values < 0 or > 100. hurs < 0
    # is invalid; hurs > 100 is sometimes valid (supersaturation
    # conditions at very low temperature), but nobody wants it, so clip.
    hurs = hurs.clip(min=0, max=100)

    hurs = hurs.to_dataset(name='hurs')
    hurs['time'] = time_dim

    return [('hurs', '1hr', hurs)]
# ---------------------------------------------------

# Surface pressure
# ---------------------------------------------------
def clean_ps(ds):
    # PSFC units: Pa
    # PSFC description: Surface pressure

    ps = ds['PSFC']
    ps['time'] = time_dim
    ps = ps.to_dataset(name='ps').drop_attrs()

    return [('ps', '1hr', ps)]
# ---------------------------------------------------

# Mean sea level pressure
# ---------------------------------------------------
def clean_psl(ds):
    # AFWA_MSLP units: Pa
    # AFWA_MSLP description: Mean sea level pressure

    psl = ds['AFWA_MSLP']
    psl['time'] = time_dim
    psl = psl.to_dataset(name='psl').drop_attrs()

    return [('psl', '1hr', psl)]
# ---------------------------------------------------

# Near-surface wind components and speed
# ---------------------------------------------------
# ds_fx is a module-level variable (loaded at startup).
def _wind_components(ds):
    """Shared helper: load U10/V10 and rotate to earth-relative coordinates.
    Returns (uas, vas) as DataArrays."""
    # U10/V10 units: m s-1
    # U10/V10 description: U/V at 10 M
    # Note: U10/V10 are diagnostic and on mass grid; no unstagger needed

    da = ds[['U10','V10']]
    da['time'] = time_dim

    cosa = ds_fx['COSALPHA'].mean(dim='Time').rename(dname_map_xy)
    sina = ds_fx['SINALPHA'].mean(dim='Time').rename(dname_map_xy)

    # Rotate winds to earth relative (lat/lon) coordinates.
    # NOTE: signs on sinalpha are correct as written; some sources
    # have them reversed. Reference:
    # https://www-k12.atmos.washington.edu/~ovens/wrfwinds.html
    uas = (da['U10'] * cosa) - (da['V10'] * sina)
    vas = (da['V10'] * cosa) + (da['U10'] * sina)
    return uas, vas

def clean_sfcWind(ds):
    uas, vas = _wind_components(ds)
    sfcWind = xr.ufuncs.sqrt(uas**2 + vas**2)
    return [('sfcWind', '1hr', sfcWind.to_dataset(name='sfcWind'))]

def clean_uas(ds):
    uas, _ = _wind_components(ds)
    return [('uas', '1hr', uas.to_dataset(name='uas'))]

def clean_vas(ds):
    _, vas = _wind_components(ds)
    return [('vas', '1hr', vas.to_dataset(name='vas'))]
# ---------------------------------------------------

# Surface downwelling shortwave radiation
# ---------------------------------------------------
def clean_rsds(ds):
    # ACSWDNB/I_ACSWDNB units: J m-2
    # ACSWDNB/I_ACSWDNB description: Accumulated downwelling shortwave flux at bottom

    da = ds[['ACSWDNB','I_ACSWDNB']]
    da['time'] = acc_time_dim

    # accumulate J/hour/m-2 to W/m2
    acc_rsds = ( (da['I_ACSWDNB'] * 1e9) + da['ACSWDNB'] ) / 3600
    rsds = acc_rsds.diff(dim='time').sel(time=time_dim).to_dataset(name='rsds')

    rsds['rsds'].attrs['positive'] = 'down'

    return [('rsds', '1hr', rsds)]
# ---------------------------------------------------

# Surface downwelling longwave radiation
# ---------------------------------------------------
def clean_rlds(ds):
    # ACLWDNB/I_ACLWDNB units: J m-2
    # ACLWDNB/I_ACLWDNB description: Accumulated downwelling longwave flux at bottom

    da = ds[['ACLWDNB','I_ACLWDNB']]
    da['time'] = acc_time_dim

    # accumulate J/hour/m-2 to W/m2
    acc_rlds = ( (da['I_ACLWDNB'] * 1e9) + da['ACLWDNB'] ) / 3600
    rlds = acc_rlds.diff(dim='time').sel(time=time_dim).to_dataset(name='rlds')

    rlds['rlds'].attrs['positive'] = 'down'

    return [('rlds', '1hr', rlds)]
# ---------------------------------------------------

# Total cloud cover percentage
# ---------------------------------------------------
def clean_clt(ds):
    # CLDFRAC2D units: %
    # CLDFRAC2D description: 2-D max cloud fraction

    clt = (ds['CLDFRAC2D'] * 100)
    clt['time'] = time_dim
    clt = clt.to_dataset(name='clt').drop_attrs()

    return [('clt', '1hr', clt)]
# ---------------------------------------------------

# Time invariant variables (orog and sftlf)
# ---------------------------------------------------
# orog & sftlf must be produced from a WRF *input* file rather than an output
# file; this function handles both.  Note they have no time dimension.
def clean_fx():

    ds = xr.open_dataset(f'{wrfinput_path}wrfinput_d01', decode_times=False) \
           .rename(_dname_map_xy) \
           .fillna(1.e20)

    # LANDMASK units: 1 (0 = no land, 1 = land; binary)
    # HGT units: m

    sftlf_fout = make_fname('sftlf', 'fx')
    orog_fout  = make_fname('orog',  'fx')

    if os.path.exists(os.path.join(outdir, 'sftlf', sftlf_fout)):
        return

    seaice = ds['SEAICE'].mean(dim='Time')
    sftlf  = ds['LANDMASK'].mean(dim='Time')
    orog   = ds['HGT'].mean(dim='Time')

    seaice = xr.where(seaice!=0, 1, 0)
    sftlf  = (sftlf - seaice) * 100

    sftlf = sftlf.to_dataset(name='sftlf').drop_attrs()
    orog  = orog.to_dataset(name='orog').drop_attrs()

    os.makedirs(os.path.join(outdir, 'sftlf'), exist_ok=True)
    os.makedirs(os.path.join(outdir, 'orog'),  exist_ok=True)
    sftlf.to_netcdf(os.path.join(outdir, 'sftlf', sftlf_fout))
    orog.to_netcdf(os.path.join(outdir, 'orog', orog_fout))

    return()
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
          for var in ['clt','evspsbl','hurs','huss','pr','ps',
                      'psl','rlds','rsds','sfcWind','tas',
                      'tasmax','tasmin','uas','vas']}

_OUTFREQ = defaultdict(lambda: '1hr', tasmax='day', tasmin='day')

_LOADER = {
    'pr'  : 'acc',
    'rsds': 'acc',
    'rlds': 'acc',
    'psl' : 'afwa',
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
