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
    elif cmor_freq == 'day':
        return f'{var}_{fname_base}_day_{year}0101-{year}1231.nc'
    elif cmor_freq == 'mon':
        return f'{var}_{fname_base}_mon_{year}01-{year}12.nc'


# Time coordinate construction
# ----------------------------

# Calendar (_cal) is defined above in the user-defined variables
# section.  Eventually it will come from sim_config.

# Note that although accumulated variables have an extra timestep
# going into the clean_* functions, after differencing they are the
# correct length, so they don't need a different time coordinate.

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
# (time_dim), and returns a list of (var, cmor_freq, dataset) tuples
# for write_vars.

# Near-Surface Air Temperature
# ---------------------------------------------------
def clean_tas(ds, time_dim):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tas = tas.to_dataset(name='tas').drop_attrs()
    return [('tas', '1hr', tas)]
# ---------------------------------------------------

# Daily maximum near-surface air temperature
# ---------------------------------------------------
def clean_tasmax(ds, time_dim):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tasmax = tas.groupby('time.dayofyear').max()
    tasmax = tasmax.rename({'dayofyear': 'time'})
    tasmax['time'] = _build_time_dim(year, 24)

    tasmax = tasmax.to_dataset(name='tasmax').drop_attrs()
    return [('tasmax', 'day', tas_max)]
# ---------------------------------------------------

# Daily minimum near-surface air temperature
# ---------------------------------------------------
def clean_tasmin(ds, time_dim):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tasmin = tas.groupby('time.dayofyear').min()
    tasmin = tasmin.rename({'dayofyear': 'time'})
    tasmin['time'] = _build_time_dim(year, 24)

    tasmin = tasmin.to_dataset(name='tasmin').drop_attrs()
    return [('tasmin', 'day', tas_min)]
# ---------------------------------------------------

# Hourly precipitation accumulation
# ---------------------------------------------------
def clean_pr(ds, time_dim):
    # I_RAINC units : mm
    # I_RAINC description: integer bucket variable for convective precipitation (tips at 100 mm)
    # I_RAINNC units : mm
    # I_RAINNC description: integer bucket variable for non-convective precipitation (tips at 100 mm)
    # RAINC units : mm
    # RAINC description : accumulated convective precipitation
    # RAINNC units : mm
    # RAINNC description : accumulated non-convective precipitation

    da = ds[['I_RAINC', 'I_RAINNC', 'RAINC', 'RAINNC']]

    #  / 3600 : mm/hour --> mm/sec == kg s-1 m-2
    tp = ( ((da['I_RAINC']*100.) + da['RAINC']) +
           ((da['I_RAINNC']*100.) + da['RAINNC']) ) / 3600
    pr = tp.diff(dim='time')
    pr['time'] = time_dim

    pr = pr.to_dataset(name='pr').drop_attrs()
    return [('pr', '1hr', pr)]
# ---------------------------------------------------

# Evaporation including sublimation and transpiration
# ---------------------------------------------------
def clean_evspsbl(ds, time_dim):
    # EDIR units : mm/s
    # EDIR description : ground surface evaporation rate
    # ETRAN units : mm/s
    # ETRAN description : transpiration rate

    evspsbl = (ds['EDIR'] + ds['ETRAN'])
    evspsbl['time'] = time_dim

    evspsbl = evspsbl.to_dataset(name='evspsbl').drop_attrs()
    return [('evspsbl', '1hr', evspsbl)]
# ---------------------------------------------------

# Near surface specific humidity
# ---------------------------------------------------
def clean_huss(ds, time_dim):
    # Q2 units: kg kg-1
    # Q2 description: mixing ratio (QV) at 2 M

    q2   = ds['Q2']
    huss = (q2 / (1 + q2))  # mixing ratio -> specific humidity
    huss['time'] = time_dim

    huss = huss.to_dataset(name='huss').drop_attrs()
    return [('huss', '1hr', huss)]
# ---------------------------------------------------

# Near surface relative humidity
# ---------------------------------------------------
def clean_hurs(ds, time_dim):
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
    hurs['time'] = time_dim

    hurs = hurs.to_dataset(name='hurs').drop_attrs()
    return [('hurs', '1hr', hurs)]
# ---------------------------------------------------

# Surface pressure
# ---------------------------------------------------
def clean_ps(ds, time_dim):
    # PSFC units: Pa
    # PSFC description: Surface pressure

    ps = ds['PSFC']
    ps['time'] = time_dim

    ps = ps.to_dataset(name='ps').drop_attrs()
    return [('ps', '1hr', ps)]
# ---------------------------------------------------

# Mean sea level pressure
# ---------------------------------------------------
def clean_psl(ds, time_dim):
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
    """Shared helper: rotate U10/V10 to earth-relative coordinates.
    Returns (uas, vas) as DataArrays."""
    # U10/V10 units: m s-1
    # U10/V10 description: U/V at 10 M
    # Note: U10/V10 are diagnostic and on mass grid; no unstagger needed

    da = ds[['U10', 'V10']]

    cosa = ds_fx['COSALPHA'].mean(dim='Time').rename(dname_map_xy)
    sina = ds_fx['SINALPHA'].mean(dim='Time').rename(dname_map_xy)

    # Rotate winds to earth relative (lat/lon) coordinates.
    # NOTE: signs on sinalpha are correct as written; some sources
    # have them reversed. Reference:
    # https://www-k12.atmos.washington.edu/~ovens/wrfwinds.html
    uas = (da['U10'] * cosa) - (da['V10'] * sina)
    vas = (da['V10'] * cosa) + (da['U10'] * sina)
    return uas, vas

def clean_sfcWind(ds, time_dim):
    uas, vas = _wind_components(ds)
    sfcWind = xr.ufuncs.sqrt(uas**2 + vas**2)
    sfcWind['time'] = time_dim

    sfcWind = sfcWind.to_dataset(name='sfcWind').drop_attrs()
    return [('sfcWind', '1hr', sfcWind)]

def clean_uas(ds, time_dim):
    uas, _ = _wind_components(ds)
    uas['time'] = time_dim

    uas = uas.to_dataset(name='uas').drop_attrs()
    return [('uas', '1hr', uas)]

def clean_vas(ds, time_dim):
    _, vas = _wind_components(ds)
    vas['time'] = time_dim

    vas = vas.to_dataset(name='vas').drop_attrs()
    return [('vas', '1hr', vas)]
# ---------------------------------------------------

# Surface downwelling shortwave radiation
# ---------------------------------------------------
def clean_rsds(ds, time_dim):
    # ACSWDNB/I_ACSWDNB units: J m-2
    # ACSWDNB/I_ACSWDNB description: Accumulated downwelling shortwave flux at bottom

    da = ds[['ACSWDNB', 'I_ACSWDNB']]

    # accumulate J/hour/m-2 to W/m2
    acc_rsds = ( (da['I_ACSWDNB'] * 1e9) + da['ACSWDNB'] ) / 3600
    rsds = acc_rsds.diff(dim='time')
    rsds['time'] = time_dim

    rsds = rsds.to_dataset(name='rsds').drop_attrs()
    return [('rsds', '1hr', rsds)]
# ---------------------------------------------------

# Surface downwelling longwave radiation
# ---------------------------------------------------
def clean_rlds(ds, time_dim):
    # ACLWDNB/I_ACLWDNB units: J m-2
    # ACLWDNB/I_ACLWDNB description: Accumulated downwelling longwave flux at bottom

    da = ds[['ACLWDNB', 'I_ACLWDNB']]

    # accumulate J/hour/m-2 to W/m2
    acc_rlds = ( (da['I_ACLWDNB'] * 1e9) + da['ACLWDNB'] ) / 3600
    rlds = acc_rlds.diff(dim='time')
    rlds['time'] = time_dim

    rlds = rlds.to_dataset(name='rlds').drop_attrs()
    return [('rlds', '1hr', rlds)]
# ---------------------------------------------------

# Total cloud cover percentage
# ---------------------------------------------------
def clean_clt(ds, time_dim):
    # CLDFRAC2D units: %
    # CLDFRAC2D description: 2-D max cloud fraction

    clt = (ds['CLDFRAC2D'] * 100)
    clt['time'] = time_dim

    # CLDFRAC2D is all-zero on the step after a restart; replace with missing
    zero_timestep = (clt == 0).all(dim=['x', 'y'])
    clt = clt.where(~zero_timestep).fillna(1.e20)
    
    clt = clt.to_dataset(name='clt').drop_attrs()
    return [('clt', '1hr', clt)]
# ---------------------------------------------------

# Time invariant variables (orog and sftlf)
# ---------------------------------------------------
# orog & sftlf must be produced from a WRF *input* file rather than an output
# file; this function handles both.  Note they have no time dimension.
def clean_fx():

    ds = xr.open_dataset(f'{wrfinput_path}wrfinput_d01', decode_times=False) \
           .rename(dname_map_xy) \
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

    seaice = xr.where(seaice != 0, 1, 0)
    sftlf  = (sftlf - seaice) * 100

    sftlf = sftlf.to_dataset(name='sftlf').drop_attrs().astype(np.float32)
    orog  = orog.to_dataset(name='orog').drop_attrs().astype(np.float32)

    os.makedirs(os.path.join(outdir, 'sftlf'), exist_ok=True)
    os.makedirs(os.path.join(outdir, 'orog'),  exist_ok=True)
    sftlf.to_netcdf(os.path.join(outdir, 'sftlf', sftlf_fout))
    orog.to_netcdf(os.path.join(outdir, 'orog', orog_fout))
# ---------------------------------------------------


# Dispatch table
# --------------
# _CLEAN maps variable names to their clean function.
# _LOADER maps variable names to (prefix, freq_hours, accumulated).
# _OUTFREQ maps variable names to their output frequency (default '1hr').
#
# At the call site, load_wrf and _build_time_dim are called using _LOADER
# parameters, then ds and time_dim are passed into clean_fn.

_CLEAN = {var: globals()[f'clean_{var}']
          for var in ['clt', 'evspsbl', 'hurs', 'huss', 'pr', 'ps',
                      'psl', 'rlds', 'rsds', 'sfcWind', 'tas',
                      'tasmax', 'tasmin', 'uas', 'vas']}

_OUTFREQ = defaultdict(lambda: '1hr', tasmax='day', tasmin='day')

# (prefix, freq_hours, accumulated)
_LOADER = defaultdict(lambda: (wrfout_hour_fname, 1, False), {
    'pr'    : (wrfout_hour_fname,  1, True),
    'rsds'  : (wrfout_hour_fname,  1, True),
    'rlds'  : (wrfout_hour_fname,  1, True),
    'psl'   : (wrfout_afwa_fname,  1, False),
})


# Call functions
# --------------
if variable == 'fx':
    clean_fx()
else:
    clean_fn = _CLEAN.get(variable)
    if clean_fn is None:
        print(f'Warning: unknown variable {variable}')
    else:
        if _output_needed(variable, make_fname(variable, _OUTFREQ[variable])):
            prefix, freq_hours, accumulated = _LOADER[variable]
            ds       = load_wrf(prefix, year, accumulated)
            time_dim = _build_time_dim(year, freq_hours)
            write_vars(clean_fn(ds, time_dim))

# Plotting is handled separately; see plot.postprocess.var.py
