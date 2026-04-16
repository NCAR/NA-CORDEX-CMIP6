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
wrfout_pres_fname  = "wrfout_pres_d01_"  # prefix for files with hourly output
wrfout_zlev_fname  = "wrfout_zlev_d01_"  # prefix for files with hourly output
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
        return f'{var}_{fname_base}_6hr_{year}010100-{year}123118.nc'
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
six_hr_acc_time_dim = pd.date_range(day_before, end_date, freq='6h')

hr_files     = []
six_hr_files = []
afwa_files   = []
pres_files   = []
zlev_files   = []
for date in day_time_dim:
    d = str(date)[:10]
    file_str = f'{wrfout_path}/{wrfout_hour_fname}{d}_00:00:00'
    hr_files.append(file_str)

    afwa_file_str = f'{wrfout_path}/wrfout_afwa_d01_{d}_00:00:00'
    afwa_files.append(afwa_file_str)

    six_hr_file_str = f'{wrfout_path}/wrfout_d01_{d}_00:00:00'
    six_hr_files.append(six_hr_file_str)
    
    pres_file_str = f'{wrfout_path}/wrfout_pres_d01_{d}_00:00:00'
    pres_files.append(pres_file_str)

    zlev_file_str = f'{wrfout_path}/wrfout_zlev_d01_{d}_00:00:00'
    zlev_files.append(zlev_file_str)

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
#   'afwa_acc' : AFWA diagnostic files including day-before timestep, for accumulated vars
#   'pres' : Pressure level files, skip day-before timestep
#   'zlev' : Hub-height wind files, skip day-before timestep
#   'six_hr' : standard six-hourly files, skip day-before timestep
#   'six_hr_acc' : six-hourly files including day-before timestep, for accumulated vars

def load_by_tag(tag):
    if tag == 'hr'  : return load_wrf(hr_files[1:])
    if tag == 'acc' : return load_wrf(hr_files,    accumulated=True)
    if tag == 'afwa': return load_wrf(afwa_files[1:])
    if tag == 'afwa_acc': return load_wrf(afwa_files, accumulated=True)
    if tag == 'pres': return load_wrf(pres_files[1:])
    if tag == 'zlev': return load_wrf(zlev_files[1:])
    if tag == 'six_hr': return load_wrf(six_hr_files[1:])
    if tag == 'six_hr_acc': return load_wrf(six_hr_files, accumulated=True)
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

    da = ds['SFROFF'] 
    da['time'] = six_hr_acc_time_dim

    # Differentiate along time axis and convert to kg/m^2/s
    mrros = (da.diff(dim='time') / 21600.0).sel(time=six_hr_time_dim)

    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    mrros = mrros.where(landmask == 1, 1.e20)

    mrros = mrros.to_dataset(name='mrros').drop_attrs()

    return [('mrros', '6hr', mrros)]
# ---------------------------------------------------

# Total runoff
# ---------------------------------------------------
def clean_mrro(ds):
    # SFROFF units : mm
    # SFROFF description : SURFACE RUNOFF
    # UDROFF units : mm
    # UDROFF description : UNDERGROUND RUNOFF

    da = ds[['SFROFF','UDROFF']]
    da['time'] = six_hr_acc_time_dim

    # Differentiate along time axis and convert to kg/m^2/s
    acc_mrro = ( (da['SFROFF']) + da['UDROFF'] ) # mm total
    mrro = (acc_mrro.diff(dim='time')/21600.0).sel(time=six_hr_time_dim)

    # Mask out ocean
    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    mrro = mrro.where(landmask == 1, 1.e20)

    mrro = mrro.to_dataset(name='mrro').drop_attrs()

    return [('mrro', '6hr', mrro)]
# ---------------------------------------------------

# Pressure level data
# ---------------------------------------------------
# Pressure levels in Pa, matching the num_press_levels_stag dimension order
_PRESS_LEVELS_PA = [100000, 92500, 85000, 75000, 70000, 60000, 50000,
                    40000, 30000, 25000, 20000, 15000, 10000, 7000]

# Map level in hPa to dimension index
_PLEV_INDEX = {lev // 100: i for i, lev in enumerate(_PRESS_LEVELS_PA)}

def _make_pres_clean(outvar, wrf_var, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def clean(ds):
        da = ds[wrf_var]
        da['time'] = six_hr_time_dim
        sliced = da.isel(num_press_levels_stag=idx)
        return [(outvar, '6hr', sliced.to_dataset(name=outvar).drop_attrs())]
    return clean

def _make_pres_wind_clean(outvar, level_hPa, component):
    """component: 'u' or 'v'"""
    idx = _PLEV_INDEX[level_hPa]
    def clean(ds):
        u = ds['U_PL'].isel(num_press_levels_stag=idx)
        v = ds['V_PL'].isel(num_press_levels_stag=idx)
        u['time'] = six_hr_time_dim
        v['time'] = six_hr_time_dim

        cosa = ds_fx['COSALPHA'].mean(dim='Time').rename(dname_map_xy)
        sina = ds_fx['SINALPHA'].mean(dim='Time').rename(dname_map_xy)

        if component == 'u':
            rotated = (u * cosa) - (v * sina)
        else:
            rotated = (v * cosa) + (u * sina)

        return [(outvar, '6hr', rotated.to_dataset(name=outvar).drop_attrs())]
    return clean

for _var, _wrf, _levels in [
    ('ta',  'T_PL',   [700, 500, 250]),
    ('zg',  'GHT_PL', [700, 500, 250]),
]:
    for _lev in _levels:
        _name = f'{_var}{_lev}'
        globals()[f'clean_{_name}'] = _make_pres_clean(_name, _wrf, _lev)

for _comp in ['ua', 'va']:
    for _lev in [700, 500, 250]:
        _name = f'{_comp}{_lev}'
        globals()[f'clean_{_name}'] = _make_pres_wind_clean(_name, _lev, _comp[0])

# Specific humidity 
def _make_pres_hus_clean(outvar, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def clean(ds):
        q = ds['Q_PL'].isel(num_press_levels_stag=idx)
        q['time'] = six_hr_time_dim
        # Convert mixing ratio (kg/kg) to specific humidity: hus = q / (1 + q)
        hus = q / (1 + q)
        return [(outvar, '6hr', hus.to_dataset(name=outvar).drop_attrs())]
    return clean

for _lev in [700, 500, 250]:
    _name = f'hus{_lev}'
    globals()[f'clean_{_name}'] = _make_pres_hus_clean(_name, _lev)

# ---------------------------------------------------

# AGL height data
# ---------------------------------------------------
# Height levels in meters (AGL), matching num_z_levels_stag dimension order
_Z_LEVELS_M = [50, 100, 150]
_ZLEV_INDEX = {lev: i for i, lev in enumerate(_Z_LEVELS_M)}

def _make_zlev_wind_clean(outvar, level_m, component):
    """component: 'u' or 'v'"""
    idx = _ZLEV_INDEX[level_m]
    def clean(ds):
        u = ds['U_ZL'].isel(num_z_levels_stag=idx)
        v = ds['V_ZL'].isel(num_z_levels_stag=idx)
        u['time'] = six_hr_time_dim
        v['time'] = six_hr_time_dim

        cosa = ds_fx['COSALPHA'].mean(dim='Time').rename(dname_map_xy)
        sina = ds_fx['SINALPHA'].mean(dim='Time').rename(dname_map_xy)

        if component == 'u':
            rotated = (u * cosa) - (v * sina)
        else:
            rotated = (v * cosa) + (u * sina)

        return [(outvar, '6hr', rotated.to_dataset(name=outvar).drop_attrs())]
    return clean

for _comp in ['ua', 'va']:
    for _lev in _Z_LEVELS_M:
        _name = f'{_comp}{_lev}m'
        globals()[f'clean_{_name}'] = _make_zlev_wind_clean(_name, _lev, _comp[0])


# ---------------------------------------------------

# Wet Bulb Globe Temperature (WBGT)
# ---------------------------------------------------
# Requires the thermofeel package (ECMWF).  The provided na_cordex conda
# environment includes it; create and activate it from the repo with:
#   conda env create -f postprocess/na_cordex.yml
#   conda activate na_cordex
# Alternatively, ensure thermofeel is available in whatever environment is
# specified by config_env.sh.
#
# Processes hourly files one day at a time to manage memory (11 input
# variables at 12km hourly resolution), then concatenates into a single
# annual file consistent with the rest of the pipeline.
#
# References:
#   MRT: Di Napoli et al. (2020), Int J Biometeorol, 64:1233-1245
#   Globe T: Guo et al. (2018), Energy and Buildings
#   Wet bulb T: Stull (2011), J Appl Meteorol Climatol, 50:2267-2269
#   WBGT: ISO 7243 (2017); WBGT = 0.7*Tw + 0.2*Tg + 0.1*Ta
#   thermofeel: Brimicombe et al. (2022), SoftwareX, 18, 101005

_WBGT_WRF_VARS = ['T2', 'Q2', 'PSFC', 'U10', 'V10', 'COSZEN',
                   'SWDOWN', 'SWDDNI', 'GLW', 'LWUPB', 'ALBEDO']


def _vapor_pressure_from_q(q, P):
    """Vapor pressure (Pa) from specific humidity (kg/kg) and pressure (Pa)."""
    eps = 0.62197
    return q * P / (eps + q * (1.0 - eps))


def _dew_point_from_vapor_pressure(e):
    """Dew point (K) from vapor pressure (Pa). Inverse August-Roche-Magnus."""
    e_hPa = np.maximum(e / 100.0, 0.001)
    ln_e = np.log(e_hPa / 6.1078)
    return (237.3 * ln_e) / (17.27 - ln_e) + 273.15


def _compute_wbgt_arrays(ds):
    """Compute WBGT from a single-day WRF dataset. Returns float32 array."""
    import thermofeel

    coszen = np.clip(ds['COSZEN'].values, 0.0, 1.0)
    SWDOWN = np.maximum(np.nan_to_num(ds['SWDOWN'].values, nan=0.0), 0.0)
    SWDDNI = np.maximum(np.nan_to_num(ds['SWDDNI'].values, nan=0.0), 0.0)
    GLW    = np.maximum(np.nan_to_num(ds['GLW'].values, nan=0.0), 0.0)
    LWUPB  = np.maximum(np.nan_to_num(ds['LWUPB'].values, nan=0.0), 0.0)
    ALBEDO = np.nan_to_num(ds['ALBEDO'].values, nan=0.2)
    T2   = ds['T2'].values
    Q2   = ds['Q2'].values
    PSFC = ds['PSFC'].values
    U10  = ds['U10'].values
    V10  = ds['V10'].values

    ssrd = SWDOWN
    fdir = np.minimum(SWDDNI * coszen, ssrd)
    ssr  = SWDOWN * (1.0 - ALBEDO)
    strd = GLW
    strr = GLW - LWUPB

    mrt = thermofeel.calculate_mean_radiant_temperature(
        ssrd=ssrd, ssr=ssr, dsrp=SWDDNI,
        strd=strd, fdir=fdir, strr=strr,
        cossza=coszen,
    )

    va = np.maximum(np.sqrt(U10**2 + V10**2), 0.5)
    Q2_safe = np.maximum(Q2, 0.0)
    e_a = _vapor_pressure_from_q(Q2_safe, PSFC)
    td_k = _dew_point_from_vapor_pressure(e_a)

    wbgt = thermofeel.calculate_wbgt(
        t2_k=T2.astype(np.float64),
        mrt=mrt.astype(np.float64),
        va=va.astype(np.float64),
        td_k=td_k.astype(np.float64),
    )

    return wbgt.astype(np.float32)


def clean_wbgt(_ds_unused):
    """Compute WBGT from hourly WRF output, one day at a time.

    Bypasses the standard load_by_tag mechanism because WBGT needs 11
    simultaneous input variables and loading the full year would exceed
    memory.  Instead, iterates over daily files, computes WBGT for each,
    and concatenates into a single annual dataset.

    The _ds_unused argument is accepted for dispatch-table compatibility
    but ignored.
    """
    daily_results = []

    # hr_files[0] is the day-before file; skip it
    for i, fpath in enumerate(hr_files[1:]):
        if not os.path.exists(fpath):
            raise FileNotFoundError(f'Hourly file not found: {fpath}')

        ds = xr.open_dataset(fpath, engine='netcdf4')
        ds = ds[_WBGT_WRF_VARS]

        wbgt_day = _compute_wbgt_arrays(ds)
        nt_day = wbgt_day.shape[0]

        # Build a DataArray for this day with proper time coords
        day_offset = i * 24
        day_times = time_dim[day_offset : day_offset + nt_day]

        da = xr.DataArray(
            wbgt_day,
            dims=['time', 'y', 'x'],
            coords={'time': day_times},
        )
        daily_results.append(da)
        ds.close()

        if (i + 1) % 30 == 0:
            print(f'  wbgt: processed {i + 1}/{len(hr_files) - 1} days')

    wbgt = xr.concat(daily_results, dim='time').to_dataset(name='wbgt')
    print(f'  wbgt: finished, {len(wbgt.time)} timesteps')

    return [('wbgt', '1hr', wbgt)]
# ---------------------------------------------------

# Moving to AFWA diagnostic ouptuts

# cape
# ---------------------------------------------------
def clean_cape(ds):
    # AFWA_CAPE units : J kg-1
    # AFWA_CAPE description : AFWA Diagnostic: Convective Avail Pot Energy

    cape = ds['AFWA_CAPE']
    cape['time'] = time_dim
    cape = cape.to_dataset(name='cape').drop_attrs()

    return [('cape', '1hr', cape)]
# ---------------------------------------------------

# cin 
# ---------------------------------------------------
def clean_cin(ds):
    # AFWA_CIN units : J kg-1
    # AFWA_CIN description : AFWA Diagnostic: Convective Inhibition

    cin = ds['AFWA_CIN']
    cin['time'] = time_dim
    cin = cin.to_dataset(name='cin').drop_attrs()

    return [('cin', '1hr', cin)]
# ---------------------------------------------------

# prw
# ---------------------------------------------------
def clean_prw(ds):
    # AFWA_PWAT units : kg m-2
    # AFWA_PWAT description : AFWA Diagnostic: Precipitable Water

    prw = ds['AFWA_PWAT']
    prw['time'] = time_dim
    prw = prw.to_dataset(name='prw').drop_attrs()

    return [('prw', '1hr', prw)]
# ---------------------------------------------------

# fzra
# ---------------------------------------------------
def clean_fzra(ds):
    # AFWA_FZRA units : mm (accumulated)
    # Convert to kg m-2 s-1 (mm/s) by differencing and dividing by 3600

    fzra = ds['AFWA_FZRA']
    fzra['time'] = acc_time_dim
    fzra = (fzra / 3600).diff(dim='time').to_dataset(name='fzra').sel(time=time_dim)

    return [('fzra', '1hr', fzra)]
# ---------------------------------------------------


# heatidx
# ---------------------------------------------------
def clean_heatidx(ds):
    # AFWA_HEATIDX units : K
    # AFWA_HEATIDX description : AFWA Diagnostic: Heat index

    heatidx = ds['AFWA_HEATIDX']
    heatidx['time'] = time_dim
    heatidx = heatidx.to_dataset(name='heatidx').drop_attrs()

    return [('heatidx', '1hr', heatidx)]
# ---------------------------------------------------

# wchill
# ---------------------------------------------------
def clean_wchill(ds):
    # AFWA_WCHILL units : K
    # AFWA_WCHILL description : AFWA Diagnostic: Wind chill

    wchill = ds['AFWA_WCHILL']
    wchill['time'] = time_dim
    wchill = wchill.to_dataset(name='wchill').drop_attrs()

    return [('wchill', '1hr', wchill)]
# ---------------------------------------------------



# Dispatch table
# --------------
# Maps variable names to their clean function and loader tag.
# The default output frequency is '6hr'; exceptions are listed in _OUTFREQ.
# The default loader is 'six_hr'; exceptions are listed in _LOADER.
#
# Loader tags:
#   'hr'   : standard hourly files, skip day-before timestep
#   'acc'  : hourly files including day-before timestep, for accumulated vars
#   'afwa' : AFWA diagnostic files, skip day-before timestep
#   'pres' : Pressure level files, skip day-before timestep
#   'zlev' : Hub-height wind files, skip day-before timestep
#   'six_hr' : standard six-hourly files, skip day-before timestep
#   'six_hr_acc' : six-hourly files including day-before timestep
#
# Note: wbgt handles its own file loading internally (see clean_wbgt);
# the 'hr' loader tag is listed for consistency but the dataset passed
# to clean_wbgt is ignored.

_CLEAN = {var: globals()[f'clean_{var}']
          for var in ['snw','snd','mrso','mrros','mrro',
                      'ua700','ua500','ua250',
                      'va700','va500','va250',
                      'ta700','ta500','ta250',
                      'zg700','zg500','zg250',
                      'hus700','hus500','hus250',
                      'ua50m','ua100m','ua150m',
                      'va50m','va100m','va150m',
                      'wbgt', 'cape','cin', 
                      'prw','fzra', 'heatidx',
                      'wchill',
                      ]}

_OUTFREQ = defaultdict(lambda: '6hr',
                       wbgt='1hr',
                       cape='1hr',
                       cin='1hr',
                       prw='1hr',
                       fzra='1hr',
                       heatidx='1hr',
                       wchill='1hr',
                       )

_LOADER = {
    'mrro'   : 'six_hr_acc',
    'mrros'  : 'six_hr_acc',
    'ua700'  : 'pres',
    'ua500'  : 'pres',
    'ua250'  : 'pres',
    'va700'  : 'pres',
    'va500'  : 'pres',
    'va250'  : 'pres',
    'ta700'  : 'pres',
    'ta500'  : 'pres',
    'ta250'  : 'pres',
    'zg700'  : 'pres',
    'zg500'  : 'pres',
    'zg250'  : 'pres',
    'hus250' : 'pres',
    'hus500' : 'pres',
    'hus700' : 'pres',
    'ua50m'  : 'zlev',
    'ua100m' : 'zlev',
    'ua150m' : 'zlev',
    'va50m'  : 'zlev',
    'va100m' : 'zlev',
    'va150m' : 'zlev',
    'wbgt'   : 'hr',
    'cape'   : 'afwa',
    'cin'    : 'afwa',
    'prw'    : 'afwa',
    'fzra'   : 'afwa_acc',
    'heatidx': 'afwa',
    'wchill' : 'afwa',
}

def get_dispatch(var):
    """Return (clean_fn, loader_tag) for a variable,
    applying defaults for anything not explicitly overridden."""
    if var not in _CLEAN:
        return None
    return _CLEAN[var], _LOADER.get(var, 'six_hr')


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
            # wbgt handles its own loading; pass None instead of loading
            # the full hourly dataset into memory
            if loader_tag == 'hr' and variable == 'wbgt':
                write_vars(clean_fn(None))
            else:
                write_vars(clean_fn(load_by_tag(loader_tag)))

#print('Done')

# Plotting is handled separately; see plot.postprocess.var.py
