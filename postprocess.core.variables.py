# Author: Jacob Stuivenvolt-Allen
# Last updated: August 19, 2025

# Purpose:
# --------
# This script post-processes WRF output. The resulting
# output will be CMORized. All standardization was done
# by consulting the WCRP-CORDEX CMOR-Tables: 
# https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables

# USAGE:
# -----
# 1. You must have cmorize.compress.sh in the same directory 
# that you are running this script. Note that executing
# this script will populate your current working 
# directory with additional directories for each variable,
# along with the post-processed output.

# 2. You must supply two keyword arguments from the command 
# line when executing this script. This is to allow for
# parallelization with launch_cf on Casper HPC at NCAR. 
# Alternatively, one could submit job arrays using their
# own HPC. 

# Keyword argument 1 : Year (int)
# Keyword argument 2 : Month (int, with no leading 0s)

# 3. 12-km WRF output is large, and a user should
# request sufficient memory for this task 
# (~100GB if possible)

# 4. This workflow requires both NCO and CDO, which are
# currently loaded assuming your HPC uses the Modules 
# system. Change the module load statements as needed
# for your HPC.

# Example execution for one month, January of 1980:
# ------------------------------------------------
# $ python clean.core.variables.py 1980 1

# ------------------------------------------------
import xarray as xr
from xarray import ufuncs
from datetime import date
import numpy as np
import pandas as pd
import glob
import sys
import os

# -------------------------------
# START OF USER DEFINED VARIABLES
# -------------------------------

os.system('module load nco')
os.system('module load cdo')

wrfout_hour_fname = "wrfout_hour_d01_"  # Leading string of wrfout files with hourly output
wrfout_fx_fname   = "wrfout_5day_d01_"  # Leading string of wrfout files with LANDFRAC and HGT

# path to wrfoutput
wrfout_path       = ""
# path to wrfinput for fixed fields (land fraction and terrain height)
wrfinp_path       = "" 

# -------------------------------
# END OF USER DEFINED VARIABLES
# -------------------------------

# -------------
# For launch_cf
# -------------
year = sys.argv[1]
m  = int(sys.argv[2])
mon = f'{m:02d}'
# -------------


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
src_id = 'WRF461'       # source_id: CORDEX RCM id
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
start_date   = pd.Timestamp(f'{year}-{mon}-01')
day_before   = start_date - pd.Timedelta(days=1)
end_of_month = start_date + pd.offsets.MonthEnd(0)
end_date     = start_date + pd.offsets.MonthEnd(0) + pd.Timedelta(hours=23)

time_dim     = pd.date_range(start_date, end_date, freq='h')
acc_time_dim = pd.date_range(day_before, end_date, freq='h')
day_time_dim = pd.date_range(day_before, end_date, freq='d')

hr_files = []
for date in day_time_dim:
    d = str(date)[:10]
    file_str = f'{wrfout_path}/{wrfout_hour_fname}{d}_00:00:00'
    hr_files.append(file_str)
# ----------------------------

# Load dataset one time:
# ----------------------
fx_fname = glob.glob(f'{wrfout_path}/{wrfout_fx_fname}{str(start_date)[:8]}*')[0]
ds_fx = xr.open_dataset(fx_fname)

ds_hr = xr.open_mfdataset(hr_files[1:], 
                          concat_dim='Time', 
                          combine='nested', 
                          chunks={'time':1,'south_north':673, 'west_east':707}, 
                          decode_times=False, 
                          decode_coords=False).fillna(1.e20) # chunk and fill values

ds_acc_hr = xr.open_mfdataset(hr_files, 
                              concat_dim='Time', 
                              combine='nested', 
                              chunks={'time':1,'south_north':673, 'west_east':707}, 
                              decode_times=False, 
                              decode_coords=False).fillna(1.e20) # chunk and fill values

ds_fx_inp = xr.open_dataset(f'{wrfinp_path}/wrfinput_d01', decode_times=False).fillna(1.e20)

# ----------------------

# Function for cmorizing and editing NETCDF attributes
# ----------------------------------------------------
def cmor_comp_save(var, fname, pcc, freq, units, lev, refh, cell, ln, stdn):

    # CMORIZED specs
    # --------------
    #pcc    = "5"
    #freq   = "1hr"
    #units  = "K"
    #levels = "single"
    #refh   = "2"
    #cell   = "None"
    #short_name = "tas"
    #long_name  = "Near-Surface Air Temperature"
    #standard_name = "air_temperature"
    #year = 1980
    # --------------

    cmd = (
    f'bash ./cmorize.compress.sh "{var}" "{fname}" "{pcc}" "{freq}" '
    f'"{units}" "{lev}" "{refh}" "{cell}" "{ln}" "{stdn}" "{year}" {mon}'
    )

    os.system(cmd)

    return()


# Near-Surface Air Temperature : Also TMAX / TMIN
# ---------------------------------------------------
def clean_tas(ds):
    # T2 units : K
    # T2 description : 2-meter temperature

    # tas, tasmax, tasmin CMORIZED specs
    # ----------------------------------
    pcc    = '5'
    levels = 'single'
    refh   = '2'

    tas_info = pull_cmor_specs('tas', 'hr')
    tasmax_info = pull_cmor_specs('tasmax', 'day')
    tasmin_info = pull_cmor_specs('tasmin', 'day')

    freq   = [tas_info[0], tasmax_info[0], tasmin_info[0]]
    units  = [tas_info[1], tasmax_info[1], tasmin_info[1]]
    cell   = [tas_info[2], tasmax_info[2], tasmin_info[2]]
    long_name      = [tas_info[3], tasmax_info[3], tasmin_info[3]]
    standard_name  = [tas_info[4], tasmax_info[4], tasmin_info[4]]
    # ---------------------------------- repeat for each variable

    tas_vars = 'T2'
    tas = ds[tas_vars].rename({'Time':'time'}).load()

    tas['time'] = time_dim
    tas = tas.to_dataset(name='tas').drop_attrs()

    # Daily maximum from hourly data
    tas_max = tas.groupby('time.day').max()
    tas_max['day'] = day_time_dim[1:]
    tas_min = tas.groupby('time.day').min()
    tas_min['day'] = day_time_dim[1:]

    tas_min = tas_min.rename({'day':'time','tas':'tasmin'})
    tas_max = tas_max.rename({'day':'time','tas':'tasmax'})

    tas_fout    = f'tas_{fname_hr}_{year}-{mon}.nc'
    tasmin_fout = f'tasmin_{fname_dd}_{year}-{mon}.nc'
    tasmax_fout = f'tasmax_{fname_dd}_{year}-{mon}.nc'

    tas.to_netcdf(tas_fout)
    tas_min.to_netcdf(f'{tasmin_fout}')
    tas_max.to_netcdf(f'{tasmax_fout}')

    cmor_comp_save('tas', tas_fout, pcc, freq[0], units[0], 
                   levels, refh, cell[0], 
                   long_name[0], standard_name[0])

    cmor_comp_save('tasmax', tasmax_fout, pcc, freq[1], units[1],
                   levels, refh, cell[1],
                   long_name[1], standard_name[1])

    cmor_comp_save('tasmin', tasmin_fout, pcc, freq[2], units[2], 
                   levels, refh, cell[2], 
                   long_name[2], standard_name[2])

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

    # pr cmorized specs
    # ----------------------------------
    pcc    = '8'
    levels = 'single'
    refh   = None

    pr_info = pull_cmor_specs('pr', 'hr')

    freq   = pr_info[0]
    units  = pr_info[1]
    cell   = pr_info[2]
    long_name      = pr_info[3]
    standard_name  = pr_info[4]
    #short_name     = pr_info[3]
    # ---------------------------------- repeat for each variable

    pr_vars = ['XLONG','XLAT','XTIME','I_RAINC','I_RAINNC','RAINC','RAINNC']
    pr_units = 'kg m-2 s-1'

    da = ds[pr_vars].rename({'Time':'time'})
    da['time'] = acc_time_dim

    #  ((I_RAINC * 100) + RAINC) + ((I_RAINNC * 100) + RAINNC) 
    #  / 3600 : mm/hour --> mm/sec
    tp = ((da['I_RAINC']*100.) + da['RAINC']) + ((da['I_RAINNC']*100.) + da['RAINNC']) / 3600  
    pr = tp.diff(dim='time').to_dataset(name='pr').sel(
            time=time_dim)

    pr_fout = f'pr_{fname_hr}_{year}-{mon}.nc'
    pr.to_netcdf(f'{pr_fout}')

    cmor_comp_save('pr', pr_fout, pcc, freq, units, 
                   levels, refh, cell, 
                   long_name, standard_name)

    return()
# ---------------------------------------------------

# Evaporation including sublimation and transpiration
# ---------------------------------------------------
def clean_evspsbl(ds):
    # EDIR units : mm/s
    # EDIR description : ground surface evaporation rate
    # ETRAN units : mm/s
    # ETRAN description : transpiration rate

    # evspsbl cmorized specs
    # ----------------------------------
    pcc    = '8'
    levels = 'single'
    refh   = None

    evspsbl_info = pull_cmor_specs('evspsbl', 'hr')

    freq   = evspsbl_info[0]
    units  = evspsbl_info[1]
    cell   = evspsbl_info[2]
    long_name      = evspsbl_info[3]
    standard_name  = evspsbl_info[4]
    # ---------------------------------- repeat for each variable

    evspsbl_vars = ['EDIR','ETRAN']
    evspsbl_units = 'kg m-2 s-1'

    da = ds[evspsbl_vars].rename({'Time':'time'})
    da['time'] = time_dim

    evspsbl = (da['EDIR'] + da['ETRAN']).to_dataset(name='evspsbl')

    evspsbl_fout = f'evspsbl_{fname_hr}_{year}-{mon}.nc'
    evspsbl.to_netcdf(f'{evspsbl_fout}')

    cmor_comp_save('evspsbl', evspsbl_fout, pcc, freq, units, 
                   levels, refh, cell, 
                   long_name, standard_name)

    return()
# ---------------------------------------------------

# Near surface specific humidity 
# ---------------------------------------------------
def clean_huss(ds):
    # Q2 units: kg kg-1
    # Q2 description: QV at 2 M

    # huss cmorized specs
    # ----------------------------------
    pcc    = '5'
    levels = 'single'
    refh   = '2'

    huss_info = pull_cmor_specs('huss', 'hr')

    freq   = huss_info[0]
    units  = huss_info[1]
    cell   = huss_info[2]
    long_name      = huss_info[3]
    standard_name  = huss_info[4]
    # ---------------------------------- repeat for each variable

    huss_vars = ['Q2']

    da = ds[huss_vars].rename({'Q2':'huss','Time':'time'}).load()
    da['time'] = time_dim
    huss = (da / (1 + da))

    huss_fout = f'huss_{fname_hr}_{year}-{mon}.nc'
    huss.to_netcdf(f'{huss_fout}')

    cmor_comp_save('huss', huss_fout, pcc, freq, units,
                   levels, refh, cell, 
                   long_name, standard_name)

    return()
# ---------------------------------------------------

# Near surface relative humidity
# ---------------------------------------------------
def clean_hurs(ds):
    # Q2 units: kg kg-1
    # Q2 description: QV at 2 M
    # T2 units: K
    # T2 description: 2-meter temperature
    # PSFC units: Pa
    # PSFC description: Surface pressure 

    hurs_vars = ['Q2','T2','PSFC']
    hurs_units = '%'

    # BEG SETH - please check RH calculation below

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
    hurs = hurs.clip(min=0, max=100) # Can't be less than 0 or greater than 100... it is sometimes

    # END SETH 

    hurs = hurs.to_dataset(name='hurs').rename({'Time':'time'})
    hurs['time'] = time_dim

    hurs_fout = f'hurs_{fname_hr}_{year}-{mon}.nc'
    hurs.to_netcdf(f'{hurs_fout}')

    # hurs cmorized specs
    # ----------------------------------
    pcc    = '3'
    levels = 'single'
    refh   = '2'

    hurs_info = pull_cmor_specs('hurs', 'hr')

    freq   = hurs_info[0]
    units  = hurs_info[1]
    cell   = hurs_info[2]
    long_name      = hurs_info[3]
    standard_name  = hurs_info[4]
    # ---------------------------------- repeat for each variable

    cmor_comp_save('hurs', hurs_fout, pcc, freq, units,
                   levels, refh, cell, 
                   long_name, standard_name)

    return()
# ---------------------------------------------------

# Surface pressure
# ---------------------------------------------------
def clean_ps(ds):
    # PSFC units: Pa
    # PSFC description: Surface pressure

    ps_vars = 'PSFC'
    ps = ds[ps_vars].rename({'Time':'time'}).load()
    ps['time'] = time_dim

    ps = ps.to_dataset(name='ps').drop_attrs()
    ps_fout = f'ps_{fname_hr}_{year}-{mon}.nc'
    ps.to_netcdf(f'{ps_fout}')

    # ps cmorized specs
    # ----------------------------------
    pcc    = '3'
    levels = 'single'
    refh   = None

    ps_info = pull_cmor_specs('ps', 'hr')

    freq   = ps_info[0]
    units  = ps_info[1]
    cell   = ps_info[2]
    long_name      = ps_info[3]
    standard_name  = ps_info[4]
    # ---------------------------------- repeat for each variable

    cmor_comp_save('ps', ps_fout, pcc, freq, units,
                   levels, refh, cell,
                   long_name, standard_name)

    return()
# ---------------------------------------------------

# Mean sea level pressure
# ---------------------------------------------------
def clean_psl(ds):
    # AFWA_MSLP units: Pa
    # AFWA MSLP description: Surface pressure

    psl_vars = 'AFWA_MSLP'
    psl = ds[psl_vars].rename({'Time':'time'}).load()
    psl['time'] = time_dim

    psl = psl.to_dataset(name='psl').drop_attrs()
    psl_fout = f'psl_{fname_hr}_{year}-{mon}.nc'
    psl.to_netcdf(f'{psl_fout}')

    # psl cmorized specs
    # ----------------------------------
    pcc    = '5'
    levels = 'single'
    refh   = None
    
    psl_info = pull_cmor_specs('psl', 'hr')

    freq   = psl_info[0]
    units  = psl_info[1]
    cell   = psl_info[2]
    long_name      = psl_info[3]
    standard_name  = psl_info[4]
    # ---------------------------------- repeat for each variable

    cmor_comp_save('psl', psl_fout, pcc, freq, units,
                   levels, refh, cell, 
                   long_name, standard_name)

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

    # BEG SETH - please check wind rotation to earth relative coords below

    # Rotate winds to earth relative (lat/lon) coordinates
    uas = (da['U10'] * cosa) - (da['V10'] * sina)
    vas = (da['V10'] * cosa) + (da['U10'] * sina)

    # Destagger winds with averaging
    uas_destag = (uas[:,:,:-1] + uas[:,:,1:]) / 2.0
    vas_destag = (vas[:,:-1,:] + vas[:,1:,:]) / 2.0

    uas_destag = uas_destag[:,1:,:]
    vas_destag = vas_destag[:,:,1:]

    #uas_destag = wrf.destagger(uas, 2, meta=True)
    #vas_destag = wrf.destagger(vas, 1, meta=True)
    #print(np.shape(uas_destag))
    #print(np.shape(vas_destag))

    sfcWind = xr.ufuncs.sqrt( (uas_destag**2 + vas_destag**2) )

    # END SETH

    sfcWind = sfcWind.to_dataset(name='sfcWind')
    uas = uas_destag.to_dataset(name='uas')
    vas = vas_destag.to_dataset(name='vas')

    sfcWind_fout    = f'sfcWind_{fname_hr}_{year}-{mon}.nc'
    uas_fout = f'uas_{fname_dd}_{year}-{mon}.nc'
    vas_fout = f'vas_{fname_dd}_{year}-{mon}.nc'

    sfcWind.to_netcdf(sfcWind_fout)
    uas.to_netcdf(uas_fout)
    vas.to_netcdf(vas_fout)

    # U10/V10 cmorized specs
    # ----------------------------------
    pcc    = '3'
    levels = 'single'
    refh   = '10'

    uas_info = pull_cmor_specs('uas', 'hr')
    vas_info = pull_cmor_specs('vas', 'hr')
    sfcWind_info = pull_cmor_specs('sfcWind', 'hr')
    
    freq   = [uas_info[0], vas_info[0], sfcWind_info[0]]
    units  = [uas_info[1], vas_info[1], sfcWind_info[1]]
    cell   = [uas_info[2], vas_info[2], sfcWind_info[2]]
    long_name      = [uas_info[3], vas_info[3], sfcWind_info[3]]
    standard_name  = [uas_info[4], vas_info[4], sfcWind_info[4]]
    # ---------------------------------- repeat for each variable

    cmor_comp_save('uas', uas_fout, pcc, freq[0], units[0],
                   levels, refh, cell[0], 
                   long_name[0], standard_name[0])

    cmor_comp_save('vas', vas_fout, pcc, freq, units[1],
                   levels, refh, cell[1],
                   long_name[1], standard_name[1])

    cmor_comp_save('sfcWind', sfcWind_fout, pcc, freq[2], units[2],
                   levels, refh, cell[2],
                   long_name[2], standard_name[2])

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

    rsds_fout = f'rsds_{fname_hr}_{year}-{mon}.nc'
    rsds.to_netcdf(f'{rsds_fout}')

    # rsds cmorized specs
    # ----------------------------------
    pcc    = '7'
    levels = 'single'
    refh   = None

    rsds_info = pull_cmor_specs('rsds', 'hr')

    freq   = rsds_info[0]
    units  = rsds_info[1]
    cell   = rsds_info[2]
    long_name      = rsds_info[3]
    standard_name  = rsds_info[4]
    # ---------------------------------- repeat for each variable
    
    cmor_comp_save('rsds', rsds_fout, pcc, freq, units,
                   levels, refh, cell, 
                   long_name, standard_name)

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

    rlds_fout = f'rlds_{fname_hr}_{year}-{mon}.nc'
    rlds.to_netcdf(f'{rlds_fout}')

    # rlds cmorized specs
    # ----------------------------------
    pcc    = '7'
    levels = 'single'
    refh   = None

    rlds_info = pull_cmor_specs('rlds', 'hr')

    freq   = rlds_info[0]
    units  = rlds_info[1]
    cell   = rlds_info[2]
    long_name      = rlds_info[3]
    standard_name  = rlds_info[4]
    # ---------------------------------- repeat for each variable

    cmor_comp_save('rlds', rlds_fout, pcc, freq, units,
                   levels, refh, cell, 
                   long_name, standard_name)

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
    clt_fout = f'clt_{fname_hr}_{year}-{mon}.nc'
    clt.to_netcdf(f'{clt_fout}')

    # clt cmorized specs
    # ----------------------------------
    pcc    = None
    levels = 'single'
    refh   = None

    clt_info = pull_cmor_specs('clt', 'hr')

    freq   = clt_info[0]
    units  = clt_info[1]
    cell   = clt_info[2]
    long_name      = clt_info[3]
    standard_name  = clt_info[4]
    # ---------------------------------- repeat for each variable

    cmor_comp_save('clt', clt_fout, pcc, freq, units,
                   levels, refh, cell, 
                   long_name, standard_name)

    return()
# ---------------------------------------------------

# Time invariant core variables (orog and sftlf)
# ---------------------------------------------------
def clean_fx(ds):
    # LANDMASK units: 1 (0 = no land, 1 = land ; binary)
    # HGT units: m

    sftlf_fout = f'sftlf_{fname_fx}'
    orog_fout = f'orog_{fname_fx}'

    #if os.path.exists(f'sftlf/{sftlf_fout}'):
    #    return 
    #if os.path.exists(f'sftlf/{sftlf_fout}'):
    #    return 

    sftlf = ds['LANDMASK'].mean(dim='Time')
    orog = ds['HGT'].mean(dim='Time')

    sftlf = sftlf.to_dataset(name='sftlf').drop_attrs()
    orog = orog.to_dataset(name='orog').drop_attrs()

    sftlf.to_netcdf(f'{sftlf_fout}')
    orog.to_netcdf(f'{orog_fout}')

    # sftlf,orog cmorized specs 
    # ----------------------------------
    pcc    = None
    levels = 'fixed'
    refh   = None

    sftlf_info = pull_cmor_specs('sftlf', 'fx')
    orog_info = pull_cmor_specs('orog', 'fx')

    freq   = [sftlf_info[0], orog_info[0]]
    units  = [sftlf_info[1], orog_info[1]]
    cell   = [sftlf_info[2], orog_info[2]]
    long_name      = [sftlf_info[3], orog_info[3]]
    standard_name  = [sftlf_info[4], orog_info[4]]
    # ---------------------------------- repeat for each variable

    cmor_comp_save('sftlf', sftlf_fout, pcc, freq[0], units[0],
                   levels, refh, cell[0], 
                   long_name[0], standard_name[0])

    cmor_comp_save('orog', orog_fout, pcc, freq[1], units[1],
                   levels, refh, cell[1], 
                   long_name[1], standard_name[1])

    return()
# ---------------------------------------------------

# Call functions
# --------------
# Variables with accumulated quantities
#clean_rsds(ds_acc_hr)
#clean_rlds(ds_acc_hr)
#clean_pr(ds_acc_hr)

# Variables that don't need time-step from previous file
#clean_evspsbl(ds_hr)
#clean_tas(ds_hr)
#clean_hurs(ds_hr)
#clean_ps(ds_hr)
#clean_psl(ds_hr)
#clean_huss(ds_hr)
#clean_evspsbl(ds_hr)
#clean_sfcWind(ds_hr, ds_fx)
#clean_clt(ds_hr)

# Time invariant variables
clean_fx(ds_fx_inp)


