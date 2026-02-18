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

# 1. Ensure that var_specs.yml, cmorize.compress.sh, and
#    plot.postprocess.var.py all exist in the same directory that this
#    script is running in

# 2. This script populate the current working directory with
#    additional directories for each variable containing the
#    post-processed output.

# 3. The script is designed for command-file parallelism via launch_cf
#    on Casper HPC at NCAR.  (Or other such tools on other HPC
#    systems.)  It processes a single year of data for one variable,
#    specified via commandline arguments

# argument 1 : Path to wrfoutput files for postproc
# argument 2 : Year (int)
# argument 3 : Variable (cmorized var name) 

# 4. 12-km WRF output is very large; be sure to request sufficient
#    memory for this task (~100GB)

# 5. This workflow requires both NCO and CDO.  On Casper, these must
#    be made available prior to running the script using the
#    appropriate "module load" commands.  When running in parallel
#    with launch_cf, those commands go in a file named
#    `config_env.sh`, which is executed locally for each task.

# Example execution:
# ------------------------------------------------
# $ python postprocess.core.variables.py {wrfout_path} 1980 tas
# ------------------------------------------------

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

# -----------------
# keyword arguments

wrfout_path = sys.argv[1]  # path to wrf output 
year        = sys.argv[2]  # year 
variable    = sys.argv[3]  # variable (cmorized syntax)

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
    """Return (levels, refh, qnt) for a variable from var_specs.yaml.
    refh and qnt are returned as strings for shell command interpolation,
    or 'None' if not specified."""
    s = var_specs.get(var, {})
    levels = s.get('levels', 'single')
    refh   = str(s['refh']) if 'refh' in s else 'None'
    qnt    = str(s['qnt'])  if 'qnt'  in s else 'None'
    return levels, refh, qnt

# PARSE CMOR Tables from WCRP-CORDEX
# ----------------------------------
import requests, json
from pandas import json_normalize

# Load JSON file from CMIP6 CMOR tables
# -------------------------------------
def parse_json(url):
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    variables = data.get("variable_entry", {})
    return(variables)

fx_url  = "https://raw.githubusercontent.com/WCRP-CORDEX/cordex-cmip6-cmor-tables/refs/heads/main/Tables/CORDEX-CMIP6_fx.json"
hr_url  = "https://raw.githubusercontent.com/WCRP-CORDEX/cordex-cmip6-cmor-tables/main/Tables/CORDEX-CMIP6_1hr.json"
day_url = "https://raw.githubusercontent.com/WCRP-CORDEX/cordex-cmip6-cmor-tables/refs/heads/main/Tables/CORDEX-CMIP6_day.json"

fx_cmor_vars  = parse_json(fx_url)
hr_cmor_vars  = parse_json(hr_url)
day_cmor_vars = parse_json(day_url)
# ----------------------------------

# Function to grab CMOR specs from WCRP JSON
# ------------------------------------------
def pull_cmor_specs(var, freq):

    if freq == 'hr':
        variables = hr_cmor_vars
    elif freq == 'day':
        variables = day_cmor_vars
    elif freq == 'fx':
        variables = fx_cmor_vars

    var_info = variables.get(var)

    freq  = var_info.get('frequency')     # 0
    units = var_info.get('units')         # 1
    cell  = var_info.get('cell_methods')  # 2
    ln    = var_info.get('long_name')     # 3
    stdn  = var_info.get('standard_name') # 4
    pos   = var_info.get('positive')      # 5

    out = [freq,units,cell,stdn,ln,pos]

    return(out)
# ------------------------------------------

# Check if postprocessed file already exists
# ------------------------------------------
def check_for_postproc(var, fout):

    if os.path.exists(f'{var}/{fout}') and do_overwrite_existing == False:
        write_netcdf = False
        print(f'{var}/{fout} EXISTS : Skipping')
    else:
        write_netcdf = True

    return(write_netcdf)
# ------------------------------------------

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

fname_hr = f'{dom_id}_{drs_id}_{dre_id}_{drv_id}_{org_id}_{src_id}_{ver_id}_hr'
fname_dd = f'{dom_id}_{drs_id}_{dre_id}_{drv_id}_{org_id}_{src_id}_{ver_id}_day'
fname_mm = f'{dom_id}_{drs_id}_{dre_id}_{drv_id}_{org_id}_{src_id}_{ver_id}_mon'
fname_fx = f'{dom_id}_{drs_id}_{dre_id}_{drv_id}_{org_id}_{src_id}_{ver_id}_fx.nc'

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

# Load datasets when needed:
# -------------------------
print(f'{wrfout_path}{wrfout_fx_fname}{str(start_date)[:8]}*')

fx_fname = glob.glob(f'{wrfout_path}{wrfout_fx_fname}{str(start_date)[:9]}*')[0]
ds_fx = xr.open_dataset(fx_fname)

def load_hr(hr_files):
    ds_hr = xr.open_mfdataset(hr_files[1:], 
                          concat_dim='Time', 
                          combine='nested', 
                          chunks={'time':1,'south_north':673, 'west_east':707}, 
                          decode_times=False, 
                          decode_coords=False).fillna(1.e20) # chunk and fill values
    return(ds_hr)

def load_acc(hr_files):
    ds_acc_hr = xr.open_mfdataset(hr_files, 
                              concat_dim='Time', 
                              combine='nested', 
                              chunks={'time':1,'south_north':673, 'west_east':707}, 
                              mask_and_scale=False,
                              decode_times=False, 
                              decode_coords=False).fillna(1.e20) # chunk and fill values
    return(ds_acc_hr)

def load_afwa(afwa_files):
    ds_afwa_hr = xr.open_mfdataset(afwa_files[1:], 
                              concat_dim='Time', 
                              combine='nested', 
                              chunks={'time':1,'south_north':673, 'west_east':707}, 
                              decode_times=False, 
                              decode_coords=False).fillna(1.e20) # chunk and fill values
    return(ds_afwa_hr)

ds_fx_inp = xr.open_dataset(f'{wrfinput_path}wrfinput_d01', decode_times=False).fillna(1.e20)

# ----------------------

# Function for cmorizing and editing NETCDF attributes
# ----------------------------------------------------
def cmor_comp_save(wrfout_path, var, fname, qnt, freq, units, lev, refh, cell, ln, stdn):

    cmd = (
    f'bash ./cmorize.compress.sh "{wrfout_path}" "{var}" "{fname}" "{qnt}" "{freq}" '
    f'"{units}" "{lev}" "{refh}" "{cell}" "{ln}" "{stdn}" "{year}" '
    )

    os.system(cmd)

    return()


# Near-Surface Air Temperature : Also TMAX / TMIN
# ---------------------------------------------------
def clean_tas(ds):

    print(ds['Time'])

    # T2 units : K
    # T2 description : 2-meter temperature

    tas_fout    = f'tas_{fname_hr}_{year}010100_{year}123123.nc'
    tasmin_fout = f'tasmin_{fname_dd}_{year}0101_{year}1231.nc'
    tasmax_fout = f'tasmax_{fname_dd}_{year}0101_{year}1231.nc'

    levels, refh, qnt = get_specs('tas')  # tasmax/tasmin share these specs

    tas_info    = pull_cmor_specs('tas',    'hr')
    tasmax_info = pull_cmor_specs('tasmax', 'day')
    tasmin_info = pull_cmor_specs('tasmin', 'day')

    tas_vars = 'T2'
    tas = ds[tas_vars].rename({'Time':'time'}).load()

    tas['time'] = time_dim
    tas = tas.to_dataset(name='tas').drop_attrs()

    # Daily maximum from hourly data
    tas_max = tas.groupby('time.dayofyear').max()
    tas_max['dayofyear'] = day_time_dim[1:]
    tas_min = tas.groupby('time.dayofyear').min()
    tas_min['dayofyear'] = day_time_dim[1:]

    tas_min = tas_min.rename({'dayofyear':'time','tas':'tasmin'})
    tas_max = tas_max.rename({'dayofyear':'time','tas':'tasmax'})

    tas_chk    = check_for_postproc('tas',    tas_fout)
    tasmax_chk = check_for_postproc('tasmax', tasmax_fout)
    tasmin_chk = check_for_postproc('tasmin', tasmin_fout)

    if tas_chk == True:
        tas.to_netcdf(tas_fout)
        cmor_comp_save(wrfout_path, 'tas', tas_fout, qnt, tas_info[0], tas_info[1],
                       levels, refh, tas_info[2], tas_info[3], tas_info[4])

    if tasmax_chk == True:
        tas_max.to_netcdf(f'{tasmax_fout}')
        cmor_comp_save(wrfout_path, 'tasmax', tasmax_fout, qnt, tasmax_info[0], tasmax_info[1],
                       levels, refh, tasmax_info[2], tasmax_info[3], tasmax_info[4])

    if tasmin_chk == True:
        tas_min.to_netcdf(f'{tasmin_fout}')
        cmor_comp_save(wrfout_path, 'tasmin', tasmin_fout, qnt, tasmin_info[0], tasmin_info[1],
                       levels, refh, tasmin_info[2], tasmin_info[3], tasmin_info[4])

    return()
# ---------------------------------------------------

# Hourly precipitation accumulation
# ---------------------------------------------------
def clean_pr(ds):
    # I_RAINC units : mm
    # I_RAINC description: integer bucket variable for convective precipitation (tips at 100 mm)
    # I_RAINNC units : mm
    # I_RAINNC description: integer bucket variable for convective precipitation (tips at 100 mm)
    # RAINC units : mm
    # RAINC description : accumulated convective precipitation
    # RAINNC units : mm
    # RAINNC description : accumulated non-convective precipitation

    pr_fout = f'pr_{fname_hr}_{year}010100_{year}123123.nc'
    pr_chk  = check_for_postproc('pr', pr_fout)
    if pr_chk == False: return

    levels, refh, qnt = get_specs('pr')
    pr_info = pull_cmor_specs('pr', 'hr')

    pr_vars = ['XLONG','XLAT','XTIME','I_RAINC','I_RAINNC','RAINC','RAINNC']
    pr_units = 'kg m-2 s-1'

    da = ds[pr_vars].rename({'Time':'time'}).astype(np.float32)
    da['time'] = acc_time_dim

    #  ((I_RAINC * 100) + RAINC) + ((I_RAINNC * 100) + RAINNC) 
    #  / 3600 : mm/hour --> mm/sec
    tp = ( ((da['I_RAINC']*100.) + da['RAINC']) + ((da['I_RAINNC']*100.) + da['RAINNC']) ) / 3600  
    pr = tp.diff(dim='time').to_dataset(name='pr').sel(
            time=time_dim)

    if pr_chk == True:
        pr.to_netcdf(f'{pr_fout}')
        cmor_comp_save(wrfout_path, 'pr', pr_fout, qnt, pr_info[0], pr_info[1],
                       levels, refh, pr_info[2], pr_info[3], pr_info[4])

    return()
# ---------------------------------------------------

# Evaporation including sublimation and transpiration
# ---------------------------------------------------
def clean_evspsbl(ds):
    # EDIR units : mm/s
    # EDIR description : ground surface evaporation rate
    # ETRAN units : mm/s
    # ETRAN description : transpiration rate

    evspsbl_fout = f'evspsbl_{fname_hr}_{year}010100_{year}123123.nc'
    evspsbl_chk  = check_for_postproc('evspsbl', evspsbl_fout)
    if evspsbl_chk == False: return

    levels, refh, qnt = get_specs('evspsbl')
    evspsbl_info = pull_cmor_specs('evspsbl', 'hr')

    evspsbl_vars = ['EDIR','ETRAN']
    evspsbl_units = 'kg m-2 s-1'

    da = ds[evspsbl_vars].rename({'Time':'time'})
    da['time'] = time_dim

    evspsbl = (da['EDIR'] + da['ETRAN']).to_dataset(name='evspsbl')

    evspsbl.to_netcdf(f'{evspsbl_fout}')
    cmor_comp_save(wrfout_path, 'evspsbl', evspsbl_fout, qnt, evspsbl_info[0], evspsbl_info[1],
                   levels, refh, evspsbl_info[2], evspsbl_info[3], evspsbl_info[4])

    return()
# ---------------------------------------------------

# Near surface specific humidity 
# ---------------------------------------------------
def clean_huss(ds):
    # Q2 units: kg kg-1
    # Q2 description: mixing ratio (QV) at 2 M

    huss_fout = f'huss_{fname_hr}_{year}010100_{year}123123.nc'
    huss_chk  = check_for_postproc('huss', huss_fout)
    if huss_chk == False: return

    levels, refh, qnt = get_specs('huss')
    huss_info = pull_cmor_specs('huss', 'hr')

    huss_vars = ['Q2']

    da = ds[huss_vars].rename({'Q2':'huss','Time':'time'}).load()
    da['time'] = time_dim
    huss = (da / (1 + da))

    huss.to_netcdf(f'{huss_fout}')
    cmor_comp_save(wrfout_path, 'huss', huss_fout, qnt, huss_info[0], huss_info[1],
                   levels, refh, huss_info[2], huss_info[3], huss_info[4])

    return()
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

    hurs_fout = f'hurs_{fname_hr}_{year}010100_{year}123123.nc'
    hurs_chk  = check_for_postproc('hurs', hurs_fout)
    if hurs_chk == False: return

    levels, refh, qnt = get_specs('hurs')
    hurs_info = pull_cmor_specs('hurs', 'hr')

    hurs_vars = ['Q2','T2','PSFC']
    hurs_units = '%'

    # https://glossary.ametsoc.org/wiki/Latent_heat
    # Physical constants - Clausius-Clapeyron
    epsilon = 0.622      # Molecular weight ratio of water/dry air
    Lv = 2.5e6           # Latent heat of vaporization (J/kg)
    Rv = 461.5           # Gas constant for water vapor (J/kg/K)
    T0 = 273.15          # Reference temperature (K)
    e0 = 611.2           # Reference saturation vapor pressure (Pa)

    # Actual vapor pressure
    e = (ds['Q2'] * ds['PSFC']) / (epsilon + ds['Q2'])

    # Saturation vapor pressure
    # e_s(T) = e0 * e^[(Lv/Rv)(1/T0 - 1/T)]
    e_s = e0 * ufuncs.exp( (Lv/Rv) * ((1/T0) - (1 / ds['T2'])) )
    
    hurs = (e / e_s) * 100
    hurs = hurs.clip(min=0, max=100)

    # Sometimes model outputs result in values < 0 or > 100. hurs < 0
    # is invalid; hurs > 100 is sometimes valid (supersaturation
    # conditions at very low temperature), but nobody wants it, so clip.
    

    hurs = hurs.to_dataset(name='hurs').rename({'Time':'time'})
    hurs['time'] = time_dim

    hurs.to_netcdf(f'{hurs_fout}')
    cmor_comp_save(wrfout_path, 'hurs', hurs_fout, qnt, hurs_info[0], hurs_info[1],
                   levels, refh, hurs_info[2], hurs_info[3], hurs_info[4])

    return()
# ---------------------------------------------------

# Surface pressure
# ---------------------------------------------------
def clean_ps(ds):
    # PSFC units: Pa
    # PSFC description: Surface pressure

    ps_fout = f'ps_{fname_hr}_{year}010100_{year}123123.nc'
    ps_chk  = check_for_postproc('ps', ps_fout)
    if ps_chk == False: return

    levels, refh, qnt = get_specs('ps')
    ps_info = pull_cmor_specs('ps', 'hr')

    ps_vars = 'PSFC'
    ps = ds[ps_vars].rename({'Time':'time'}).load()
    ps['time'] = time_dim

    ps = ps.to_dataset(name='ps').drop_attrs()
    ps.to_netcdf(f'{ps_fout}')
    cmor_comp_save(wrfout_path, 'ps', ps_fout, qnt, ps_info[0], ps_info[1],
                   levels, refh, ps_info[2], ps_info[3], ps_info[4])

    return()
# ---------------------------------------------------

# Mean sea level pressure
# ---------------------------------------------------
def clean_psl(ds):
    # AFWA_MSLP units: Pa
    # AFWA_MSLP description: Mean sea level pressure

    psl_fout = f'psl_{fname_hr}_{year}010100_{year}123123.nc'
    psl_chk  = check_for_postproc('psl', psl_fout)
    if psl_chk == False: return

    levels, refh, qnt = get_specs('psl')
    psl_info = pull_cmor_specs('psl', 'hr')

    psl_vars = 'AFWA_MSLP'
    psl = ds[psl_vars].rename({'Time':'time'}).load()
    psl['time'] = time_dim

    psl = psl.to_dataset(name='psl').drop_attrs()
    psl.to_netcdf(f'{psl_fout}')
    cmor_comp_save(wrfout_path, 'psl', psl_fout, qnt, psl_info[0], psl_info[1],
                   levels, refh, psl_info[2], psl_info[3], psl_info[4])

    return()
# ---------------------------------------------------

# Near surface wind speed
# ---------------------------------------------------
def clean_sfcWind(ds, dsfx):
    # U10/V10 units: m s-1
    # U10/V10 description: U/V at 10 M

    sfcWind_vars = ['U10','V10']
    sfcWind_units = 'm s-1'

    da = ds[sfcWind_vars].rename({'Time':'time'})
    da['time'] = time_dim

    cosa = dsfx['COSALPHA'].mean(dim='Time')
    sina = dsfx['SINALPHA'].mean(dim='Time')

    # Rotate winds to earth relative (lat/lon) coordinates
    # NOTE: signs on sinalpha are correct as written; some sources
    # have them reversed. Reference:
    # https://www-k12.atmos.washington.edu/~ovens/wrfwinds.html

    uas = (da['U10'] * cosa) - (da['V10'] * sina)
    vas = (da['V10'] * cosa) + (da['U10'] * sina)

    sfcWind = xr.ufuncs.sqrt( (uas**2 + vas**2) )

    sfcWind = sfcWind.to_dataset(name='sfcWind')
    uas = uas.to_dataset(name='uas')
    vas = vas.to_dataset(name='vas')

    sfcWind_fout    = f'sfcWind_{fname_hr}_{year}010100_{year}123123.nc'
    uas_fout = f'uas_{fname_hr}_{year}010100_{year}123123.nc'
    vas_fout = f'vas_{fname_hr}_{year}010100_{year}123123.nc'

    # sfcWind, uas, vas share levels and refh (same physical quantity at same height)
    levels, refh, qnt = get_specs('sfcWind')

    uas_info     = pull_cmor_specs('uas',     'hr')
    vas_info     = pull_cmor_specs('vas',     'hr')
    sfcWind_info = pull_cmor_specs('sfcWind', 'hr')

    sfcWind.to_netcdf(sfcWind_fout)
    uas.to_netcdf(uas_fout)
    vas.to_netcdf(vas_fout)

    cmor_comp_save(wrfout_path, 'uas', uas_fout, qnt, uas_info[0], uas_info[1],
                   levels, refh, uas_info[2], uas_info[3], uas_info[4])

    cmor_comp_save(wrfout_path, 'vas', vas_fout, qnt, vas_info[0], vas_info[1],
                   levels, refh, vas_info[2], vas_info[3], vas_info[4])

    cmor_comp_save(wrfout_path, 'sfcWind', sfcWind_fout, qnt, sfcWind_info[0], sfcWind_info[1],
                   levels, refh, sfcWind_info[2], sfcWind_info[3], sfcWind_info[4])

    return()
# ---------------------------------------------------

# Surface downwelling shortwave radiation 
# ---------------------------------------------------
def clean_rsds(ds):
    # ACSWDNB/I_ACSWDNB units: J m-2
    # ACSWDNB/I_ACSWDNB description: Accumulated downwelling shortwave flux at bottom

    rsds_vars = ['ACSWDNB','I_ACSWDNB']
    rsds_units = 'W m-2'

    da = ds[rsds_vars].rename({'Time':'time'})
    da['time'] = acc_time_dim

    acc_rsds = ( (da['I_ACSWDNB'] * 1e9) + da['ACSWDNB'] ) / 3600  # J/Hour/m-2 accumulation to W/m2
    rsds = acc_rsds.diff(dim='time').sel(time=time_dim).to_dataset(name='rsds')

    rsds_fout = f'rsds_{fname_hr}_{year}010100_{year}123123.nc'
    rsds.to_netcdf(f'{rsds_fout}')

    levels, refh, qnt = get_specs('rsds')
    rsds_info = pull_cmor_specs('rsds', 'hr')

    cmor_comp_save(wrfout_path, 'rsds', rsds_fout, qnt, rsds_info[0], rsds_info[1],
                   levels, refh, rsds_info[2], rsds_info[3], rsds_info[4])

    cmd = f'ncatted -h -a positive,rsds,o,c,down rsds/{rsds_fout}'
    os.system(cmd)

    return()
# ---------------------------------------------------

# Surface downwelling longwave radiation 
# ---------------------------------------------------
def clean_rlds(ds):
    # ACLWDNB/I_ACLWDNB units: J m-2
    # ACLWDNB/I_ACLWDNB description: Accumulated downwelling longwave flux at bottom

    rlds_vars = ['ACLWDNB','I_ACLWDNB']
    rlds_units = 'W m-2'

    da = ds[rlds_vars].rename({'Time':'time'})
    da['time'] = acc_time_dim

    acc_rlds = ( (da['I_ACLWDNB'] * 1e9) + da['ACLWDNB'] ) / 3600  # J/Hour/m-2 accumulation to W/m2
    rlds = acc_rlds.diff(dim='time').sel(time=time_dim).to_dataset(name='rlds')

    rlds_fout = f'rlds_{fname_hr}_{year}010100_{year}123123.nc'
    rlds.to_netcdf(f'{rlds_fout}')

    levels, refh, qnt = get_specs('rlds')
    rlds_info = pull_cmor_specs('rlds', 'hr')

    cmor_comp_save(wrfout_path, 'rlds', rlds_fout, qnt, rlds_info[0], rlds_info[1],
                   levels, refh, rlds_info[2], rlds_info[3], rlds_info[4])

    cmd = f'ncatted -h -a positive,rlds,o,c,down rlds/{rlds_fout}'
    os.system(cmd)

    return()

# ---------------------------------------------------

# Total cloud cover percentage
# ---------------------------------------------------
def clean_clt(ds):
    # CLDFRAC2D units: %
    # CLDFRAC2D description: 2-D max cloud fraction

    clt_vars = 'CLDFRAC2D'
    clt = ds[clt_vars].rename({'Time':'time'}) * 100
    clt['time'] = time_dim

    clt = clt.to_dataset(name='clt').drop_attrs() 
    clt_fout = f'clt_{fname_hr}_{year}010100_{year}123123.nc'
    clt.to_netcdf(f'{clt_fout}')

    levels, refh, qnt = get_specs('clt')
    clt_info = pull_cmor_specs('clt', 'hr')

    cmor_comp_save(wrfout_path, 'clt', clt_fout, qnt, clt_info[0], clt_info[1],
                   levels, refh, clt_info[2], clt_info[3], clt_info[4])

    return()
# ---------------------------------------------------

# Time invariant variables (orog and sftlf)
# ---------------------------------------------------
def clean_fx(ds):
    # LANDMASK units: 1 (0 = no land, 1 = land ; binary)
    # HGT units: m

    sftlf_fout = f'sftlf_{fname_fx}'
    orog_fout = f'orog_{fname_fx}'

    if os.path.exists(f'sftlf/{sftlf_fout}'):
        return 

    sftlf_levels, _, sftlf_qnt = get_specs('sftlf')
    orog_levels,  _, orog_qnt  = get_specs('orog')

    sftlf_info = pull_cmor_specs('sftlf', 'fx')
    orog_info  = pull_cmor_specs('orog',  'fx')

    sftlf = ds['LANDMASK'].mean(dim='Time')
    seaice = ds['SEAICE'].mean(dim='Time')
    orog = ds['HGT'].mean(dim='Time')

    seaice = xr.where(seaice!=0, 1, 0)
    sftlf = (sftlf - seaice) * 100

    sftlf = sftlf.to_dataset(name='sftlf').drop_attrs()
    orog = orog.to_dataset(name='orog').drop_attrs()

    sftlf.to_netcdf(f'{sftlf_fout}')
    orog.to_netcdf(f'{orog_fout}')

    cmor_comp_save(wrfout_path, 'sftlf', sftlf_fout, sftlf_qnt, sftlf_info[0], sftlf_info[1],
                   sftlf_levels, 'None', sftlf_info[2], sftlf_info[3], sftlf_info[4])

    cmor_comp_save(wrfout_path, 'orog', orog_fout, orog_qnt, orog_info[0], orog_info[1],
                   orog_levels, 'None', orog_info[2], orog_info[3], orog_info[4])

    return()
# ---------------------------------------------------

# Call functions
# --------------
# Variables with accumulated quantities
if variable == 'pr'   : clean_pr( load_acc(hr_files) )
if variable == 'rsds' : clean_rsds( load_acc(hr_files) )
if variable == 'rlds' : clean_rlds( load_acc(hr_files) )

# Variables that don't need time-step from previous file
if variable == 'clt'     : clean_clt( load_hr(hr_files) )
if variable == 'evspsbl' : clean_evspsbl( load_hr(hr_files) )
if variable == 'hurs'    : clean_hurs( load_hr(hr_files) )
if variable == 'huss'    : clean_huss( load_hr(hr_files) )
if variable == 'ps'      : clean_ps( load_hr(hr_files) )
if variable == 'psl'     : clean_psl( load_afwa(afwa_files))
if variable == 'sfcWind' : clean_sfcWind(load_hr(hr_files), ds_fx)
if variable == 'tas'     : clean_tas( load_hr(hr_files) )

# Time invariant variables
if variable == 'fx' : clean_fx(ds_fx_inp)

subprocess.run(['python', './plot.postprocess.var.py', year, variable])

if variable == 'sfcWind':
    subprocess.run(['python', './plot.postprocess.var.py', year, 'uas'])
    subprocess.run(['python', './plot.postprocess.var.py', year, 'vas'])

if variable == 'tas':
    subprocess.run(['python', './plot.postprocess.var.py', year, 'tasmax'])
    subprocess.run(['python', './plot.postprocess.var.py', year, 'tasmin'])
