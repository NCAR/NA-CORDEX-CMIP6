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
# $ python postprocess.hyd-atm.variables.py {wrfout_path} 1980 mrro {outdir}
# ------------------------------------------------

from collections import defaultdict
import xarray as xr
from xarray import ufuncs
import numpy as np
import glob
import sys
import os
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
wrfinput_path = "/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/ERA5_HIST_E03/input_example/"

# filename prefixes for the different types of wrfout files
wrfout_fx_fname   = "wrfout_5day_d01_"  # files that contain fixed vars
wrfout_hour_fname = "wrfout_hour_d01_"  # hourly outputs
wrfout_6hr_fname  = "wrfout_d01_"       # 6-hourly outputs
wrfout_afwa_fname = "wrfout_afwa_d01_"  # AFWA diagnostics
wrfout_pres_fname = "wrfout_pres_d01_"  # pressure-level outputs
wrfout_zlev_fname = "wrfout_zlev_d01_"  # height-AGL outputs

# Calendar and epoch are simulation-specific; both will eventually be read
# from sim_config (future consolidation step).
_cal   = 'standard'
_epoch = '1950-01-01 00:00:00'

# -------------------------------
# END OF USER DEFINED VARIABLES
# -------------------------------

# Dimension renaming (specified by CORDEX)
dname_map_t   = {'Time': 'time'}
dname_map_xy  = {'west_east': 'x', 'south_north': 'y'}
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


# Time coordinate construction
# ----------------------------

# Calendar (_cal) is defined above in the user-defined variables
# section.  Eventually it will come from sim_config.

def _build_time_dim(yr, freq_hours):
    """CFTimeIndex at freq_hours intervals for yr."""
    return xr.cftime_range(start=f'{yr}-01-01',
                           end=f'{int(yr)+1}-01-01',
                           freq=f'{freq_hours}h',
                           closed='left',
                           calendar=_cal,
                           )


# File loading
# ------------
# load_wrf is called at the dispatch site using parameters from _LOADER.
# The day-before file is prepended for accumulated variables so that the
# first diff gives the correct first-interval value.
#
# _day_before_file assumes Dec 31 exists in the prior year (which it
# does in standard and no-leap calendars as represented by WRF).

def _day_before_file(prefix, yr):
    """Return list containing the Dec 31 file of the prior year for prefix."""
    matches = sorted(glob.glob(f'{wrfout_path}/{prefix}{int(yr)-1}-12-31*'))
    if not matches:
        raise FileNotFoundError(
            f'Day-before file not found for year {yr} '
            f'(pattern: {wrfout_path}/{prefix}{int(yr)-1}-12-31*)')
    return matches

def load_wrf(prefix, yr, accumulated=False):
    """Load WRF output files for the given year into a dataset.

    Globs for files matching prefix+yr, sorts lexically, and prepends the
    day-before file when accumulated=True.  Renames WRF dimensions to CORDEX
    conventions."""
    files = sorted(glob.glob(f'{wrfout_path}/{prefix}{yr}-*'))
    if not files:
        raise FileNotFoundError(
            f'No WRF files found for year {yr} '
            f'(pattern: {wrfout_path}/{prefix}{yr}-*)')
    if accumulated:
        files = _day_before_file(prefix, yr) + files

    ds = xr.open_mfdataset(files,
                            concat_dim='Time',
                            combine='nested',
                            chunks={'time': 1, 'south_north': 673, 'west_east': 707},
                            mask_and_scale=False,
                            decode_times=False,
                            decode_coords=False,
                           ).fillna(1.e20)
    return ds.rename(dname_map_xyt)


# fx dataset: loaded once at startup since multiple clean functions use it
fx_glob = f'{wrfout_path}/{wrfout_fx_fname}{year}*'
if not (fx_matches := glob.glob(fx_glob)):
    raise FileNotFoundError(f'No fx files found matching: {fx_glob}')
ds_fx = xr.open_dataset(fx_matches[0])


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
# Each receives a loaded dataset (ds) and a pre-built time coordinate
# (time_dim), amd returns a list of (var, cmor_freq, dataset) tuples
# for write_vars.
#
# wbgt, utci, and humidex are exceptions: they load one file at a time
# for memory reasons and are called directly without ds or time_dim.

# Snow water equivalent - surface snow amount
# ---------------------------------------------------
def clean_snw(ds, time_dim):
    # SNOW units : kg m-2
    # SNOW description : SNOW WATER EQUIVALENT

    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    snw = ds['SNOW'].where(landmask == 1, 1.e20)
    snw['time'] = time_dim

    snw = snw.to_dataset(name='snw').drop_attrs()
    return [('snw', '6hr', snw)]
# ---------------------------------------------------

# Snow depth
# ---------------------------------------------------
def clean_snd(ds, time_dim):
    # SNOWH units : m
    # SNOWH description : PHYSICAL SNOW DEPTH

    snd = ds['SNOWH']
    snd['time'] = time_dim

    snd = snd.to_dataset(name='snd').drop_attrs()
    return [('snd', '6hr', snd)]
# ---------------------------------------------------

# Total soil moisture content
# ---------------------------------------------------
def clean_mrso(ds, time_dim):
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
    mrso['time'] = time_dim

    mrso = mrso.to_dataset(name='mrso').drop_attrs()
    return [('mrso', '6hr', mrso)]
# ---------------------------------------------------

# Surface runoff
# ---------------------------------------------------
def clean_mrros(ds, time_dim):
    # SFROFF units : mm (accumulated)
    # SFROFF description : SURFACE RUNOFF

    da = ds['SFROFF']

    # Differentiate along time axis and convert to kg/m^2/s
    mrros = (da.diff(dim='time') / 21600.0)

    # Mask out ocean
    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    mrros = mrros.where(landmask == 1, 1.e20)
    mrros['time'] = time_dim

    mrros = mrros.to_dataset(name='mrros').drop_attrs()
    return [('mrros', '6hr', mrros)]
# ---------------------------------------------------

# Total runoff
# ---------------------------------------------------
def clean_mrro(ds, time_dim):
    # SFROFF units : mm (accumulated)
    # SFROFF description : SURFACE RUNOFF
    # UDROFF units : mm (accumulated)
    # UDROFF description : UNDERGROUND RUNOFF

    # Differentiate along time axis and convert to kg/m^2/s
    mrro = ((ds['SFROFF'] + ds['UDROFF']).diff(dim='time') / 21600.0)

    # Mask out ocean
    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    mrro = mrro.where(landmask == 1, 1.e20)
    mrro['time'] = time_dim

    mrro = mrro.to_dataset(name='mrro').drop_attrs()
    return [('mrro', '6hr', mrro)]
# ---------------------------------------------------

# Surface upwelling shortwave radiation
# ---------------------------------------------------
def clean_rsus(ds, time_dim):
    # SWUPB units : W m-2
    # SWUPB description : INSTANTANEOUS UPWELLING SHORTWAVE FLUX AT BOTTOM

    rsus = ds['SWUPB']
    rsus['time'] = time_dim

    rsus = rsus.to_dataset(name='rsus').drop_attrs()
    return [('rsus', '6hr', rsus)]
# ---------------------------------------------------

# Surface upwelling longwave radiation
# ---------------------------------------------------
def clean_rlus(ds, time_dim):
    # LWUPB units : W m-2
    # LWUPB description : INSTANTANEOUS UPWELLING LONGWAVE FLUX AT BOTTOM

    rlus = ds['LWUPB']
    rlus['time'] = time_dim

    rlus = rlus.to_dataset(name='rlus').drop_attrs()
    return [('rlus', '6hr', rlus)]
# ---------------------------------------------------

# Surface upward latent heat flux
# ---------------------------------------------------
def clean_hfls(ds, time_dim):
    # LH units : W m-2
    # LH description : LATENT HEAT FLUX AT THE SURFACE

    hfls = ds['LH']
    hfls['time'] = time_dim

    hfls = hfls.to_dataset(name='hfls').drop_attrs()
    return [('hfls', '6hr', hfls)]
# ---------------------------------------------------

# Surface upward sensible heat flux
# ---------------------------------------------------
def clean_hfss(ds, time_dim):
    # HFX units : W m-2
    # HFX description : UPWARD HEAT FLUX AT THE SURFACE

    hfss = ds['HFX']
    hfss['time'] = time_dim

    hfss = hfss.to_dataset(name='hfss').drop_attrs()
    return [('hfss', '6hr', hfss)]
# ---------------------------------------------------

# Surface snow melt
# ---------------------------------------------------
def clean_snm(ds, time_dim):
    # ACSNOM units : kg m-2 (accumulated)
    # ACSNOM description : ACCUMULATED MELTED SNOW

    # Convert to kg m-2 s-1 by differencing and dividing by 21600
    snm = (ds['ACSNOM'].diff(dim='time') / 21600.0)

    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    snm = snm.where(landmask == 1, 1.e20)
    snm['time'] = time_dim

    snm = snm.to_dataset(name='snm').drop_attrs()
    return [('snm', '6hr', snm)]
# ---------------------------------------------------


# Pressure level data
# ---------------------------------------------------
# Pressure levels in Pa, matching the num_press_levels_stag dimension order
_PRESS_LEVELS_PA = [100000, 92500, 85000, 75000, 70000, 60000, 50000,
                    40000, 30000, 25000, 20000, 15000, 10000, 7000]

# Map level in hPa to dimension index
_PLEV_INDEX = {lev // 100: i for i, lev in enumerate(_PRESS_LEVELS_PA)}

# function factory for cleaning generic pressure-level variables
def _make_pres_clean(outvar, wrf_var, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def clean(ds, time_dim):
        da = ds[wrf_var].isel(num_press_levels_stag=idx)
        da['time'] = time_dim

        da = da.to_dataset(name=outvar).drop_attrs()
        return [(outvar, '6hr', da)]
    return clean

# generate clean functions for zg & ta vars
for _var, _wrf, _levels in [
    ('ta',  'T_PL',   [700, 500, 250]),
    ('zg',  'GHT_PL', [700, 500, 250]),
]:
    for _lev in _levels:
        _name = f'{_var}{_lev}'
        globals()[f'clean_{_name}'] = _make_pres_clean(_name, _wrf, _lev)


# function factory for cleaning pressure-level winds
def _make_pres_wind_clean(outvar, level_hPa, component):
    """component: 'u' or 'v'"""
    idx = _PLEV_INDEX[level_hPa]
    def clean(ds, time_dim):
        u = ds['U_PL'].isel(num_press_levels_stag=idx)
        v = ds['V_PL'].isel(num_press_levels_stag=idx)

        cosa = ds_fx['COSALPHA'].mean(dim='Time').rename(dname_map_xy)
        sina = ds_fx['SINALPHA'].mean(dim='Time').rename(dname_map_xy)

        if component == 'u':
            rotated = (u * cosa) - (v * sina)
        else:
            rotated = (v * cosa) + (u * sina)
        rotated['time'] = time_dim

        rotated = rotated.to_dataset(name=outvar).drop_attrs()
        return [(outvar, '6hr', rotated)]
    return clean

# generate clean functions for ua & va vars
for _comp in ['ua', 'va']:
    for _lev in [700, 500, 250]:
        _name = f'{_comp}{_lev}'
        globals()[f'clean_{_name}'] = _make_pres_wind_clean(_name, _lev, _comp[0])

# function factory for cleaning specific humidity
def _make_pres_hus_clean(outvar, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def clean(ds, time_dim):
        q = ds['Q_PL'].isel(num_press_levels_stag=idx)
        # Convert mixing ratio (kg/kg) to specific humidity: hus = q / (1 + q)
        hus = (q / (1 + q))
        hus['time'] = time_dim

        hus = hus.to_dataset(name=outvar).drop_attrs()
        return [(outvar, '6hr', hus)]
    return clean

# generate clean functions for hus vars
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
    def clean(ds, time_dim):
        u = ds['U_ZL'].isel(num_z_levels_stag=idx)
        v = ds['V_ZL'].isel(num_z_levels_stag=idx)

        cosa = ds_fx['COSALPHA'].mean(dim='Time').rename(dname_map_xy)
        sina = ds_fx['SINALPHA'].mean(dim='Time').rename(dname_map_xy)

        if component == 'u':
            rotated = (u * cosa) - (v * sina)
        else:
            rotated = (v * cosa) + (u * sina)
        rotated['time'] = time_dim

        rotated = rotated.to_dataset(name=outvar).drop_attrs()
        return [(outvar, '6hr', rotated)]
    return clean

for _comp in ['ua', 'va']:
    for _lev in _Z_LEVELS_M:
        _name = f'{_comp}{_lev}m'
        globals()[f'clean_{_name}'] = _make_zlev_wind_clean(_name, _lev, _comp[0])

# ---------------------------------------------------

# AFWA diagnostic variables
# ---------------------------------------------------

def clean_cape(ds, time_dim):
    # AFWA_CAPE units : J kg-1
    # AFWA_CAPE description : AFWA Diagnostic: Convective Avail Pot Energy

    cape = ds['AFWA_CAPE'].to_dataset(name='cape').drop_attrs()
    cape['time'] = time_dim
    return [('cape', '1hr', cape)]

def clean_cin(ds, time_dim):
    # AFWA_CIN units : J kg-1
    # AFWA_CIN description : AFWA Diagnostic: Convective Inhibition

    cin = ds['AFWA_CIN'].to_dataset(name='cin').drop_attrs()
    cin['time'] = time_dim
    return [('cin', '1hr', cin)]

def clean_prw(ds, time_dim):
    # AFWA_PWAT units : kg m-2
    # AFWA_PWAT description : AFWA Diagnostic: Precipitable Water

    prw = ds['AFWA_PWAT'].to_dataset(name='prw').drop_attrs()
    prw['time'] = time_dim
    return [('prw', '1hr', prw)]

def clean_fzra(ds, time_dim):
    # AFWA_FZRA units : mm (accumulated)
    # AFWA_FZRA description : AFWA Diagnostic: Freezing rain fall
    # Convert to kg m-2 s-1 by differencing and dividing by 3600

    fzra = (ds['AFWA_FZRA'].diff(dim='time') / 3600.0).to_dataset(name='fzra').drop_attrs()
    fzra['time'] = time_dim
    return [('fzra', '1hr', fzra)]

def clean_heatidx(ds, time_dim):
    # AFWA_HEATIDX units : K
    # AFWA_HEATIDX description : AFWA Diagnostic: Heat index

    heatidx = ds['AFWA_HEATIDX'].to_dataset(name='heatidx').drop_attrs()
    heatidx['time'] = time_dim
    return [('heatidx', '1hr', heatidx)]

def clean_wchill(ds, time_dim):
    # AFWA_WCHILL units : K
    # AFWA_WCHILL description : AFWA Diagnostic: Wind chill
    wchill = ds['AFWA_WCHILL'].to_dataset(name='wchill').drop_attrs()
    wchill['time'] = time_dim
    return [('wchill', '1hr', wchill)]

# ---------------------------------------------------

# Wet Bulb Globe Temperature (WBGT) and
# Universal Thermal Comfort Index (UTCI)
# ---------------------------------------------------
# Requires the thermofeel package (ECMWF).  The provided nac6 conda
# environment includes it; create and activate it from the repo with:
#   conda env create -f postprocess/environment.yml
#   conda activate nac6
#
# Processes hourly files one day at a time to manage memory (11 input
# variables at 12km hourly resolution), writing output incrementally to
# NetCDF via netCDF4.  Both indices are computed together since they share
# the expensive MRT calculation.
#
# These functions do not follow the standard (ds, time_dim) signature;
# they are called directly from the dispatch site as special cases.
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


def _compute_wbgt_utci_arrays(ds):
    """Compute WBGT and UTCI from a single-day WRF dataset.

    Returns (wbgt, utci) as float32 arrays. MRT is computed once and shared
    between both indices to avoid redundant radiation processing.
    """
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

    t2_f64  = T2.astype(np.float64)
    mrt_f64 = mrt.astype(np.float64)
    va_f64  = va.astype(np.float64)
    td_f64  = td_k.astype(np.float64)

    wbgt = thermofeel.calculate_wbgt(
        t2_k=t2_f64, mrt=mrt_f64, va=va_f64, td_k=td_f64,
    )

    utci = thermofeel.calculate_utci(
        t2_k=t2_f64, va=va_f64, mrt=mrt_f64, td_k=td_f64,
    )

    ta_c  = T2 - 273.15
    mrt_c = mrt - 273.15
    valid = (
        (ta_c >= -50) & (ta_c <= 50) &
        (va <= 17) &
        ((mrt_c - ta_c) >= -30) & ((mrt_c - ta_c) <= 70)
    )
    utci = np.where(valid, utci, np.nan).astype(np.float32)

    return wbgt.astype(np.float32), utci


def clean_wbgt_utci():
    """Compute WBGT and UTCI from hourly WRF output, one day at a time.

    Loads one file at a time to avoid accumulating a full year of arrays in
    memory, writing output incrementally to NetCDF via netCDF4.  Returns an
    empty list so the call site has nothing further to do.

    MRT is computed once per day and shared between both indices.
    """
    import netCDF4 as nc
    import cftime as _cftime

    wbgt_fout = os.path.join(outdir, 'wbgt', make_fname('wbgt', '1hr'))
    utci_fout = os.path.join(outdir, 'utci', make_fname('utci', '1hr'))
    os.makedirs(os.path.join(outdir, 'wbgt'), exist_ok=True)
    os.makedirs(os.path.join(outdir, 'utci'), exist_ok=True)

    time_index = _build_time_dim(year, 1)
    time_units = f'hours since {_epoch}'
    time_vals  = _cftime.date2num(list(time_index), time_units, calendar=_cal)

    hr_files = sorted(glob.glob(f'{wrfout_path}/{wrfout_hour_fname}{year}-*'))
    if not hr_files:
        raise FileNotFoundError(
            f'No hourly WRF files found for year {year} in {wrfout_path}')

    t_written = 0
    wbgt_nc = utci_nc = None

    try:
        for i, fpath in enumerate(hr_files):
            ds = xr.open_dataset(fpath, engine='netcdf4')
            ds = ds[_WBGT_WRF_VARS]

            wbgt_day, utci_day = _compute_wbgt_utci_arrays(ds)
            nt_day = wbgt_day.shape[0]
            ds.close()

            if i == 0:
                ny, nx = wbgt_day.shape[1], wbgt_day.shape[2]
                wbgt_nc = nc.Dataset(wbgt_fout, 'w', format='NETCDF4')
                utci_nc = nc.Dataset(utci_fout, 'w', format='NETCDF4')
                for ds_nc, varname in [(wbgt_nc, 'wbgt'), (utci_nc, 'utci')]:
                    ds_nc.createDimension('time', None)
                    ds_nc.createDimension('y', ny)
                    ds_nc.createDimension('x', nx)
                    t_var = ds_nc.createVariable('time', 'f8', ('time',))
                    t_var.units    = time_units
                    t_var.calendar = _cal
                    ds_nc.createVariable(varname, 'f4', ('time', 'y', 'x'),
                                         fill_value=1.e20)

            wbgt_nc['time'][t_written:t_written + nt_day] = time_vals[t_written:t_written + nt_day]
            wbgt_nc['wbgt'][t_written:t_written + nt_day] = wbgt_day
            utci_nc['time'][t_written:t_written + nt_day] = time_vals[t_written:t_written + nt_day]
            utci_nc['utci'][t_written:t_written + nt_day] = utci_day
            t_written += nt_day

            if (i + 1) % 30 == 0:
                wbgt_nc.sync()
                utci_nc.sync()
                print(f'  wbgt/utci: processed {i + 1}/{len(hr_files)} days')

    finally:
        if wbgt_nc: wbgt_nc.close()
        if utci_nc: utci_nc.close()

    print(f'  wbgt/utci: finished, {t_written} timesteps')
    print(f'postproc time: {time.perf_counter() - t0:.1f} sec')
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f'postproc max memory: {mem / (1024*1024):.1f} GB')

    return []
# ---------------------------------------------------

# Humidex
# ---------------------------------------------------
# Requires the thermofeel package (ECMWF).
# Inputs: T2, Q2, PSFC. No radiation fields required.
# Dewpoint is derived from specific humidity using the same helper functions
# used by WBGT/UTCI. Processes one day at a time to limit memory use.
#
# References:
#   Masterton & Richardson (1979), Humidex: a method of quantifying human
#     discomfort due to excessive heat and humidity. Env. Canada CLI 1-79.
#   thermofeel: Brimicombe et al. (2022), SoftwareX, 18, 101005

_HUMIDEX_WRF_VARS = ['T2', 'Q2', 'PSFC']

def clean_humidex():
    """Compute humidex from hourly WRF output, one day at a time.

    Writes output incrementally via xarray append mode to avoid accumulating
    a full year of arrays in memory. Returns an empty list so the call site
    has nothing further to do.
    """
    import thermofeel

    humidex_fout = os.path.join(outdir, 'humidex', make_fname('humidex', '1hr'))
    os.makedirs(os.path.join(outdir, 'humidex'), exist_ok=True)

    time_index = _build_time_dim(year, 1)

    hr_files = sorted(glob.glob(f'{wrfout_path}/{wrfout_hour_fname}{year}-*'))
    if not hr_files:
        raise FileNotFoundError(
            f'No hourly WRF files found for year {year} in {wrfout_path}')

    for i, fpath in enumerate(hr_files):
        ds = xr.open_dataset(fpath, engine='netcdf4')
        ds = ds[_HUMIDEX_WRF_VARS]

        T2   = ds['T2'].values
        Q2   = np.maximum(ds['Q2'].values, 0.0)
        PSFC = ds['PSFC'].values
        ds.close()

        e_a  = _vapor_pressure_from_q(Q2, PSFC)
        td_k = _dew_point_from_vapor_pressure(e_a)

        humidex = thermofeel.calculate_humidex(
            t2_k=T2.astype(np.float64),
            td_k=td_k.astype(np.float64),
        ).astype(np.float32)

        day_times = time_index[i * 24 : i * 24 + humidex.shape[0]]
        mode = 'w' if i == 0 else 'a'

        xr.DataArray(humidex, dims=['time', 'y', 'x'],
                     coords={'time': day_times}) \
          .to_dataset(name='humidex') \
          .to_netcdf(humidex_fout, mode=mode, unlimited_dims=['time'])

        if (i + 1) % 30 == 0:
            print(f'  humidex: processed {i + 1}/{len(hr_files)} days')

    print(f'  humidex: finished')
    print(f'postproc time: {time.perf_counter() - t0:.1f} sec')
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f'postproc max memory: {mem / (1024*1024):.1f} GB')

    return []
# ---------------------------------------------------


# Dispatch table
# --------------
# _CLEAN maps variable names to their clean function.
# _LOADER maps variable names to (prefix, freq_hours, accumulated).
# _OUTFREQ maps variable names to their output frequency (default '6hr').
#
# At the call site, load_wrf and _build_time_dim are called using _LOADER
# parameters, then ds and time_dim are passed into clean_fn.
#
# wbgt and utci both resolve to the combined clean_wbgt_utci function and
# are called directly as special cases (no ds or time_dim).  Submitting
# utci as a standalone job would conflict with wbgt output; use --vars wbgt
# to get both.

_CLEAN = {var: globals()[f'clean_{var}']
          for var in ['snw', 'snd', 'mrso', 'mrros', 'mrro',
                      'rsus', 'rlus', 'hfls', 'hfss', 'snm',
                      'ua700', 'ua500', 'ua250',
                      'va700', 'va500', 'va250',
                      'ta700', 'ta500', 'ta250',
                      'zg700', 'zg500', 'zg250',
                      'hus700', 'hus500', 'hus250',
                      'ua50m', 'ua100m', 'ua150m',
                      'va50m', 'va100m', 'va150m',
                      'cape', 'cin', 'prw', 'fzra', 'heatidx', 'wchill',
                      ]}

_CLEAN['wbgt']    = clean_wbgt_utci
_CLEAN['utci']    = clean_wbgt_utci
_CLEAN['humidex'] = clean_humidex

_OUTFREQ = defaultdict(lambda: '6hr',
                       wbgt='1hr', utci='1hr', humidex='1hr',
                       cape='1hr', cin='1hr', prw='1hr',
                       fzra='1hr', heatidx='1hr', wchill='1hr')

# (prefix, freq_hours, accumulated)
_LOADER = defaultdict(lambda: (wrfout_6hr_fname, 6, False), {
    'mrro'   : (wrfout_6hr_fname,  6, True),
    'mrros'  : (wrfout_6hr_fname,  6, True),
    'snm'    : (wrfout_6hr_fname,  6, True),
    'ua700'  : (wrfout_pres_fname, 6, False),
    'ua500'  : (wrfout_pres_fname, 6, False),
    'ua250'  : (wrfout_pres_fname, 6, False),
    'va700'  : (wrfout_pres_fname, 6, False),
    'va500'  : (wrfout_pres_fname, 6, False),
    'va250'  : (wrfout_pres_fname, 6, False),
    'ta700'  : (wrfout_pres_fname, 6, False),
    'ta500'  : (wrfout_pres_fname, 6, False),
    'ta250'  : (wrfout_pres_fname, 6, False),
    'zg700'  : (wrfout_pres_fname, 6, False),
    'zg500'  : (wrfout_pres_fname, 6, False),
    'zg250'  : (wrfout_pres_fname, 6, False),
    'hus700' : (wrfout_pres_fname, 6, False),
    'hus500' : (wrfout_pres_fname, 6, False),
    'hus250' : (wrfout_pres_fname, 6, False),
    'ua50m'  : (wrfout_zlev_fname, 6, False),
    'ua100m' : (wrfout_zlev_fname, 6, False),
    'ua150m' : (wrfout_zlev_fname, 6, False),
    'va50m'  : (wrfout_zlev_fname, 6, False),
    'va100m' : (wrfout_zlev_fname, 6, False),
    'va150m' : (wrfout_zlev_fname, 6, False),
    'cape'   : (wrfout_afwa_fname, 1, False),
    'cin'    : (wrfout_afwa_fname, 1, False),
    'prw'    : (wrfout_afwa_fname, 1, False),
    'fzra'   : (wrfout_afwa_fname, 1, True),
    'heatidx': (wrfout_afwa_fname, 1, False),
    'wchill' : (wrfout_afwa_fname, 1, False),
})

# Special-case variables that load their own files one at a time
_SELF_LOADING = {'wbgt', 'utci', 'humidex'}


# Call functions
# --------------
if variable == 'fx':
    pass  # fx variables are handled by postprocess.core.variables.py
else:
    clean_fn = _CLEAN.get(variable)
    if clean_fn is None:
        print(f'Warning: unknown variable {variable}')
    else:
        if _output_needed(variable, make_fname(variable, _OUTFREQ[variable])):
            if variable in _SELF_LOADING:
                write_vars(clean_fn())
            else:
                prefix, freq_hours, accumulated = _LOADER[variable]
                ds       = load_wrf(prefix, year, accumulated)
                time_dim = _build_time_dim(year, freq_hours)
                write_vars(clean_fn(ds, time_dim))

print('Done')

# Plotting is handled separately; see plot.postprocess.var.py
