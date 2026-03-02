# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis

# Purpose:
# --------
# This script post-processes WRF output to generate CMORized output
# that conforms to CORDEX-CMIP6 specification (which also entails
# CF-compliance).  Variable metadata specifications are taken from:
# https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables
#
# Additional specifications (near-surface reference height, lossy
# compression precision) are defined in var_specs.yml.

# USAGE NOTES:
# -----

# 1. Ensure that var_specs.yml and cmorize.compress.sh exist in the
#    same directory that this script is running in.

# 2. This script creates variable subdirectories under OUTDIR (argument
#    4) and writes post-processed output there.  It does not change the
#    working directory; all paths are handled explicitly.  cmorize.compress.sh
#    creates coordinate cache files (wrf.xy.coords.nc, etc.) in OUTDIR.

# 3. The script is designed for command-file parallelism via launch_cf
#    on Casper HPC at NCAR.  (Or other such tools on other HPC
#    systems.)  It processes a single year of data for one variable,
#    specified via commandline arguments.

# argument 1 : Path to wrfoutput files (chunk directory)
# argument 2 : Year (int)
# argument 3 : Variable (CMORized var name)
# argument 4 : Output directory (where variable subdirs are created)

# 4. 12-km WRF output is very large; be sure to request sufficient
#    memory for this task (~100GB)

# 5. This workflow requires both NCO and CDO.  On Casper, these must
#    be made available prior to running the script using the
#    appropriate "module load" commands.  When running in parallel
#    with launch_cf, those commands go in a file named
#    `config_env.sh`, which is executed locally for each task.

# 6. Plotting is handled separately; see plot.sh (or README)

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
import subprocess
import yaml
import requests
import json

# -----------------
# keyword arguments

wrfout_path = sys.argv[1]  # path to wrf output 
year        = sys.argv[2]  # year 
variable    = sys.argv[3]  # variable (cmorized syntax)
outdir      = sys.argv[4]  # output directory (variable subdirs created here)

# TODO: fix race condition on wrf.xy.coords.nc / wrf.xy.stagger.coords.nc
# when multiple jobs run concurrently in the same outdir.  One option:
# move coordinate file creation to a separate setup step in the workflow.
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
# TODO: make this a CLI argument
wrfinput_path     = "/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/ERA5_HIST_E03/input_example/"

wrfout_hour_fname = "wrfout_hour_d01_"  # Leading string of wrfout files with hourly output
wrfout_fx_fname   = "wrfout_5day_d01_"  # Leading string of wrfout files with LANDFRAC and HGT

# -------------------------------
# END OF USER DEFINED VARIABLES
# -------------------------------

# Load local variable specs
# -------------------------
_script_dir = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_script_dir, 'var_specs.yml')) as f:
    var_specs = yaml.safe_load(f)

def get_specs(var):
    """Return (levels, refh, qnt) for a variable from var_specs.yml.
    refh and qnt are returned as strings for shell command interpolation,
    or 'None' if not specified."""
    s = var_specs.get(var, {})
    levels = s.get('levels', 'single')
    refh   = str(s['refh']) if 'refh' in s else 'None'
    qnt    = str(s['qnt'])  if 'qnt'  in s else 'None'
    return levels, refh, qnt

# PARSE CMOR Tables from WCRP-CORDEX
# ----------------------------------
def parse_json(url):
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("variable_entry", {})

_cmor_base = "https://raw.githubusercontent.com/WCRP-CORDEX/cordex-cmip6-cmor-tables/main/Tables/CORDEX-CMIP6"
cmor_vars = {freq: parse_json(f"{_cmor_base}_{freq}.json") for freq in ('fx', '1hr', 'day')}
# ----------------------------------

def pull_cmor_specs(var, freq):
    var_info = cmor_vars[freq].get(var)
    freq  = var_info.get('frequency')
    units = var_info.get('units')
    cell  = var_info.get('cell_methods')
    ln    = var_info.get('long_name')
    stdn  = var_info.get('standard_name')
    pos   = var_info.get('positive')
    return [freq, units, cell, stdn, ln, pos]

# File naming
# -----------
# See file:///Users/jsallen/Downloads/CORDEX-CMIP6_archiving_specifications_20250321.pdf
# for file naming conventions. Still need to register WRF 4.6.1

# CORDEX CMOR tables for _id names is here:
# https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables/blob/main/Tables/CORDEX-CMIP6_CV.json

dom_id = 'NAM-12'       # domain_id: name assigned to cordex region
drs_id = 'ERA5'         # driving_source_id:  Identifier of driving data
dre_id = 'evaluation'   # driving_experiment_id: "evaluation" for ERA5 or GCM ID
drv_id = 'r1i1p1f1'     # driving_variant_label: CMIP6 variant id (rxixpxfx)
org_id = 'NCAR'         # institution_id: 
src_id = 'WRF461S-SN'   # source_id: CORDEX RCM id
ver_id = 'v1-r1'        # v: Version of CORDEX dataset | r: RCM ensemble number

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

# Variables 
# ----------------
# tas, tasmax, tasmin, pr, evspsbl, huss, hurs, ps 
# psl, sfcWind, uas, vas, clt, rsds, rlds

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

    afwa_prefix = 'wrfout_afwa_d01'
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
    """Load WRF output files into a dataset.
    accumulated=True sets mask_and_scale=False, needed for bucket variables
    like precipitation and radiation that use integer overflow accumulation."""
    return xr.open_mfdataset(files,
                             concat_dim='Time',
                             combine='nested',
                             chunks={'time':1,'south_north':673,'west_east':707},
                             mask_and_scale=(not accumulated),
                             decode_times=False,
                             decode_coords=False).fillna(1.e20)

# Loader tags:
#   'hr'   : standard hourly files, skip day-before timestep
#   'acc'  : hourly files including day-before timestep, for accumulated vars
#   'afwa' : AFWA diagnostic files, skip day-before timestep
def load_by_tag(tag):
    if tag == 'hr'  : return load_wrf(hr_files[1:])
    if tag == 'acc' : return load_wrf(hr_files,    accumulated=True)
    if tag == 'afwa': return load_wrf(afwa_files[1:])
    raise ValueError(f'Unknown loader tag: {tag}')

ds_fx_inp = xr.open_dataset(f'{wrfinput_path}wrfinput_d01', decode_times=False).fillna(1.e20)

# ----------------------

# Function for cmorizing and editing NETCDF attributes
# ----------------------------------------------------
def cmor_comp_save(wrfout_path, var, fname, qnt, freq, units, lev, refh, cell, ln, stdn):
    cmd = (
    f'bash ./cmorize.compress.sh "{wrfout_path}" "{var}" "{fname}" "{qnt}" "{freq}" '
    f'"{units}" "{lev}" "{refh}" "{cell}" "{ln}" "{stdn}" "{year}" "{outdir}"'
    )

    os.system(cmd)

    return()

# Output existence check
# ----------------------
def _output_needed(var, fout):
    if os.path.exists(os.path.join(outdir, var, fout)) and not do_overwrite_existing:
        print(f'{var}/{fout} EXISTS : Skipping')
        return False
    return True

# Write and cmorize a list of (var, cmor_freq, dataset) tuples
# ------------------------------------------------------------
def write_vars(var_da_list):
    for var, cmor_freq, da in var_da_list:
        fout = make_fname(var, cmor_freq)
        if not _output_needed(var, fout):
            continue
        levels, refh, qnt = get_specs(var)
        info = pull_cmor_specs(var, cmor_freq)
        vardir = os.path.join(outdir, var)
        os.makedirs(vardir, exist_ok=True)
        da.astype(np.float32).to_netcdf(fout)
        
        # bare filename; cmorize.compress.sh reads from cwd,
        # writes to outdir/var/, then removes this file
        cmor_comp_save(wrfout_path, var, fout, qnt, info[0], info[1],
                       levels, refh, info[2], info[3], info[4])
# ------------------------------------------------------------


# Clean functions
# ---------------
# Each takes a loaded dataset and returns a list of
# (var, cmor_freq, dataset) tuples for write_vars to process.

# Near-Surface Air Temperature
# ---------------------------------------------------
def clean_tas(ds):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2'].rename({'Time':'time'})
    tas['time'] = time_dim
    tas = tas.to_dataset(name='tas').drop_attrs()

    return [('tas', '1hr', tas)]
# ---------------------------------------------------

# Daily maximum near-surface air temperature
# ---------------------------------------------------
def clean_tasmax(ds):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2'].rename({'Time':'time'})
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

    tas = ds['T2'].rename({'Time':'time'})
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

    da = ds[pr_vars].rename({'Time':'time'})
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

    da = ds[['EDIR','ETRAN']].rename({'Time':'time'})
    da['time'] = time_dim
    evspsbl = (da['EDIR'] + da['ETRAN']).to_dataset(name='evspsbl')

    return [('evspsbl', '1hr', evspsbl)]
# ---------------------------------------------------

# Near surface specific humidity 
# ---------------------------------------------------
def clean_huss(ds):
    # Q2 units: kg kg-1
    # Q2 description: mixing ratio (QV) at 2 M

    da = ds[['Q2']].rename({'Q2':'huss', 'Time':'time'})
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

    hurs = hurs.to_dataset(name='hurs').rename({'Time':'time'})
    hurs['time'] = time_dim

    return [('hurs', '1hr', hurs)]
# ---------------------------------------------------

# Surface pressure
# ---------------------------------------------------
def clean_ps(ds):
    # PSFC units: Pa
    # PSFC description: Surface pressure

    ps = ds['PSFC'].rename({'Time':'time'})
    ps['time'] = time_dim
    ps = ps.to_dataset(name='ps').drop_attrs()

    return [('ps', '1hr', ps)]
# ---------------------------------------------------

# Mean sea level pressure
# ---------------------------------------------------
def clean_psl(ds):
    # AFWA_MSLP units: Pa
    # AFWA_MSLP description: Mean sea level pressure

    psl = ds['AFWA_MSLP'].rename({'Time':'time'})
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

    da = ds[['U10','V10']].rename({'Time':'time'})
    da['time'] = time_dim

    cosa = ds_fx['COSALPHA'].mean(dim='Time')
    sina = ds_fx['SINALPHA'].mean(dim='Time')

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

    da = ds[['ACSWDNB','I_ACSWDNB']].rename({'Time':'time'})
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

    da = ds[['ACLWDNB','I_ACLWDNB']].rename({'Time':'time'})
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

    clt = (ds['CLDFRAC2D'].rename({'Time':'time'}) * 100)
    clt['time'] = time_dim
    clt = clt.to_dataset(name='clt').drop_attrs()

    return [('clt', '1hr', clt)]
# ---------------------------------------------------

# Time invariant variables (orog and sftlf)
# ---------------------------------------------------
# clean_fx does not follow the standard pattern: it produces two
# outputs from a WRF input file rather than an output file, uses
# a different existence check, and has no time dimension.
def clean_fx(ds):
    # LANDMASK units: 1 (0 = no land, 1 = land ; binary)
    # HGT units: m

    sftlf_fout = make_fname('sftlf', 'fx')
    orog_fout  = make_fname('orog',  'fx')

    if os.path.exists(os.path.join(outdir, 'sftlf', sftlf_fout)):
        return

    sftlf_levels, _, sftlf_qnt = get_specs('sftlf')
    orog_levels,  _, orog_qnt  = get_specs('orog')

    sftlf_info = pull_cmor_specs('sftlf', 'fx')
    orog_info  = pull_cmor_specs('orog',  'fx')

    sftlf  = ds['LANDMASK'].mean(dim='Time')
    seaice = ds['SEAICE'].mean(dim='Time')
    orog   = ds['HGT'].mean(dim='Time')

    seaice = xr.where(seaice!=0, 1, 0)
    sftlf  = (sftlf - seaice) * 100

    sftlf = sftlf.to_dataset(name='sftlf').drop_attrs()
    orog  = orog.to_dataset(name='orog').drop_attrs()

    os.makedirs(os.path.join(outdir, 'sftlf'), exist_ok=True)
    os.makedirs(os.path.join(outdir, 'orog'),  exist_ok=True)
    sftlf.to_netcdf(sftlf_fout)
    orog.to_netcdf(orog_fout)
    # bare filenames; cmorize.compress.sh reads from cwd,
    # writes to outdir/var/, then removes file

    cmor_comp_save(wrfout_path, 'sftlf', sftlf_fout, sftlf_qnt,
                   sftlf_info[0], sftlf_info[1], sftlf_levels, 'None',
                   sftlf_info[2], sftlf_info[3], sftlf_info[4])
    cmor_comp_save(wrfout_path, 'orog', orog_fout, orog_qnt,
                   orog_info[0], orog_info[1], orog_levels, 'None',
                   orog_info[2], orog_info[3], orog_info[4])

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
    clean_fx(ds_fx_inp)
else:
    entry = get_dispatch(variable)
    if entry is None:
        print(f'Warning: unknown variable {variable}')
    else:
        clean_fn, loader_tag = entry
        if _output_needed(variable, make_fname(variable, _OUTFREQ[variable])):
            write_vars(clean_fn(load_by_tag(loader_tag)))

# Plotting is handled separately; see plot.sh (or README)
