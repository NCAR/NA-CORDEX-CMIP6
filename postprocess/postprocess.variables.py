# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis

# Purpose:
# --------
# Extract function definitions for all NA-CORDEX-CMIP6 postprocessing
# variables.  Each function receives a loaded dataset (ds) and a pre-built
# time coordinate (time_dim), and returns a list of
# (var, cmor_freq, dataset) tuples for write_vars.
#
# This file contains only function definitions.  All shared state
# (ds_fx, dname_map_xy, wrfout_* filenames, etc.), dispatch tables,
# and execution logic live in postprocess.machinery.py, which imports
# this file and calls the functions.
#
# Variable metadata specifications are taken from:
# https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables


# Near-Surface Air Temperature
# ---------------------------------------------------
def extract_tas(ds, time_dim):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tas = tas.to_dataset(name='tas').drop_attrs()
    return [('tas', '1hr', tas)]
# ---------------------------------------------------

# Daily maximum near-surface air temperature
# ---------------------------------------------------
def extract_tasmax(ds, time_dim):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tasmax = tas.groupby('time.dayofyear').max()
    tasmax = tasmax.rename({'dayofyear': 'time'})
    tasmax['time'] = _build_time_dim(year, 24)

    tasmax = tasmax.to_dataset(name='tasmax').drop_attrs()
    return [('tasmax', 'day', tasmax)]
# ---------------------------------------------------

# Daily minimum near-surface air temperature
# ---------------------------------------------------
def extract_tasmin(ds, time_dim):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']
    tas['time'] = time_dim

    tasmin = tas.groupby('time.dayofyear').min()
    tasmin = tasmin.rename({'dayofyear': 'time'})
    tasmin['time'] = _build_time_dim(year, 24)

    tasmin = tasmin.to_dataset(name='tasmin').drop_attrs()
    return [('tasmin', 'day', tasmin)]
# ---------------------------------------------------

# Hourly precipitation accumulation
# ---------------------------------------------------
def extract_pr(ds, time_dim):
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
    pr = pr.assign_coords(time=time_dim)
    #pr['time'] = time_dim

    pr = pr.to_dataset(name='pr').drop_attrs()
    return [('pr', '1hr', pr)]
# ---------------------------------------------------

# Evaporation including sublimation and transpiration
# ---------------------------------------------------
def extract_evspsbl(ds, time_dim):
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
def extract_huss(ds, time_dim):
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
def extract_hurs(ds, time_dim):
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
def extract_ps(ds, time_dim):
    # PSFC units: Pa
    # PSFC description: Surface pressure

    ps = ds['PSFC']
    ps['time'] = time_dim

    ps = ps.to_dataset(name='ps').drop_attrs()
    return [('ps', '1hr', ps)]
# ---------------------------------------------------

# Mean sea level pressure
# ---------------------------------------------------
def extract_psl(ds, time_dim):
    # AFWA_MSLP units: Pa
    # AFWA_MSLP description: Mean sea level pressure

    psl = ds['AFWA_MSLP']
    psl['time'] = time_dim

    psl = psl.to_dataset(name='psl').drop_attrs()
    return [('psl', '1hr', psl)]
# ---------------------------------------------------

# Near-surface wind components and speed
# ---------------------------------------------------
# ds_fx is a module-level variable (loaded at startup by machinery).
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

def extract_sfcWind(ds, time_dim):
    uas, vas = _wind_components(ds)
    sfcWind = xr.ufuncs.sqrt(uas**2 + vas**2)
    sfcWind['time'] = time_dim

    sfcWind = sfcWind.to_dataset(name='sfcWind').drop_attrs()
    return [('sfcWind', '1hr', sfcWind)]

def extract_uas(ds, time_dim):
    uas, _ = _wind_components(ds)
    uas['time'] = time_dim

    uas = uas.to_dataset(name='uas').drop_attrs()
    return [('uas', '1hr', uas)]

def extract_vas(ds, time_dim):
    _, vas = _wind_components(ds)
    vas['time'] = time_dim

    vas = vas.to_dataset(name='vas').drop_attrs()
    return [('vas', '1hr', vas)]
# ---------------------------------------------------

# Surface downwelling shortwave radiation
# ---------------------------------------------------
def extract_rsds(ds, time_dim):
    # ACSWDNB/I_ACSWDNB units: J m-2
    # ACSWDNB/I_ACSWDNB description: Accumulated downwelling shortwave flux at bottom

    da = ds[['ACSWDNB', 'I_ACSWDNB']]

    # accumulate J/hour/m-2 to W/m2
    acc_rsds = ( (da['I_ACSWDNB'] * 1e9) + da['ACSWDNB'] ) / 3600
    rsds = acc_rsds.diff(dim='time')
    rsds = rsds.assign_coords(time=time_dim)
    #rsds['time'] = time_dim

    rsds = rsds.to_dataset(name='rsds').drop_attrs()
    return [('rsds', '1hr', rsds)]
# ---------------------------------------------------

# Surface downwelling longwave radiation
# ---------------------------------------------------
def extract_rlds(ds, time_dim):
    # ACLWDNB/I_ACLWDNB units: J m-2
    # ACLWDNB/I_ACLWDNB description: Accumulated downwelling longwave flux at bottom

    da = ds[['ACLWDNB', 'I_ACLWDNB']]

    # accumulate J/hour/m-2 to W/m2
    acc_rlds = ( (da['I_ACLWDNB'] * 1e9) + da['ACLWDNB'] ) / 3600
    rlds = acc_rlds.diff(dim='time')
    rlds = rlds.assign_coords(time=time_dim)
    #rlds['time'] = time_dim

    rlds = rlds.to_dataset(name='rlds').drop_attrs()
    return [('rlds', '1hr', rlds)]
# ---------------------------------------------------

# Total cloud cover percentage
# ---------------------------------------------------
def extract_clt(ds, time_dim):
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

# Snow water equivalent - surface snow amount
# ---------------------------------------------------
def extract_snw(ds, time_dim):
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
def extract_snd(ds, time_dim):
    # SNOWH units : m
    # SNOWH description : PHYSICAL SNOW DEPTH

    snd = ds['SNOWH']
    snd['time'] = time_dim

    snd = snd.to_dataset(name='snd').drop_attrs()
    return [('snd', '6hr', snd)]
# ---------------------------------------------------

# Total soil moisture content
# ---------------------------------------------------
def extract_mrso(ds, time_dim):
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
def extract_mrros(ds, time_dim):
    # SFROFF units : mm (accumulated)
    # SFROFF description : SURFACE RUNOFF

    da = ds['SFROFF']

    # Differentiate along time axis and convert to kg/m^2/s
    mrros = (da.diff(dim='time') / 21600.0)

    # Mask out ocean
    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    mrros = mrros.where(landmask == 1, 1.e20)
    mrros = mrros.assign_coords(time=time_dim)
    #mrros['time'] = time_dim

    mrros = mrros.to_dataset(name='mrros').drop_attrs()
    return [('mrros', '6hr', mrros)]
# ---------------------------------------------------

# Total runoff
# ---------------------------------------------------
def extract_mrro(ds, time_dim):
    # SFROFF units : mm (accumulated)
    # SFROFF description : SURFACE RUNOFF
    # UDROFF units : mm (accumulated)
    # UDROFF description : UNDERGROUND RUNOFF

    # Differentiate along time axis and convert to kg/m^2/s
    mrro = ((ds['SFROFF'] + ds['UDROFF']).diff(dim='time') / 21600.0)

    # Mask out ocean
    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    mrro = mrro.where(landmask == 1, 1.e20)
    mrro = mrro.assign_coords(time=time_dim)

    mrro = mrro.to_dataset(name='mrro').drop_attrs()
    return [('mrro', '6hr', mrro)]
# ---------------------------------------------------

# Surface upwelling shortwave radiation
# ---------------------------------------------------
def extract_rsus(ds, time_dim):
    # SWUPB units : W m-2
    # SWUPB description : INSTANTANEOUS UPWELLING SHORTWAVE FLUX AT BOTTOM

    rsus = ds['SWUPB']
    rsus['time'] = time_dim

    rsus = rsus.to_dataset(name='rsus').drop_attrs()
    return [('rsus', '6hr', rsus)]
# ---------------------------------------------------

# Surface upwelling longwave radiation
# ---------------------------------------------------
def extract_rlus(ds, time_dim):
    # LWUPB units : W m-2
    # LWUPB description : INSTANTANEOUS UPWELLING LONGWAVE FLUX AT BOTTOM

    rlus = ds['LWUPB']
    rlus['time'] = time_dim

    rlus = rlus.to_dataset(name='rlus').drop_attrs()
    return [('rlus', '6hr', rlus)]
# ---------------------------------------------------

# Surface upward latent heat flux
# ---------------------------------------------------
def extract_hfls(ds, time_dim):
    # LH units : W m-2
    # LH description : LATENT HEAT FLUX AT THE SURFACE

    hfls = ds['LH']
    hfls['time'] = time_dim

    hfls = hfls.to_dataset(name='hfls').drop_attrs()
    return [('hfls', '6hr', hfls)]
# ---------------------------------------------------

# Surface upward sensible heat flux
# ---------------------------------------------------
def extract_hfss(ds, time_dim):
    # HFX units : W m-2
    # HFX description : UPWARD HEAT FLUX AT THE SURFACE

    hfss = ds['HFX']
    hfss['time'] = time_dim

    hfss = hfss.to_dataset(name='hfss').drop_attrs()
    return [('hfss', '6hr', hfss)]
# ---------------------------------------------------

# Surface snow melt
# ---------------------------------------------------
def extract_snm(ds, time_dim):
    # ACSNOM units : kg m-2 (accumulated)
    # ACSNOM description : ACCUMULATED MELTED SNOW

    # Convert to kg m-2 s-1 by differencing and dividing by 21600
    snm = (ds['ACSNOM'].diff(dim='time') / 21600.0)

    landmask = ds_fx['LANDMASK'].mean(dim='Time').rename(dname_map_xy)
    snm = snm.where(landmask == 1, 1.e20)
    snm = snm.assign_coords(time=time_dim)
    #snm['time'] = time_dim

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

# function factory for extracting generic pressure-level variables
def _make_pres_extract(outvar, wrf_var, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def extract(ds, time_dim):
        da = ds[wrf_var].isel(num_press_levels_stag=idx)
        da['time'] = time_dim

        da = da.to_dataset(name=outvar).drop_attrs()
        return [(outvar, '6hr', da)]
    return extract

# generate extract functions for zg & ta vars
for _var, _wrf, _levels in [
    ('ta',  'T_PL',   [700, 500, 250]),
    ('zg',  'GHT_PL', [700, 500, 250]),
]:
    for _lev in _levels:
        _name = f'{_var}{_lev}'
        globals()[f'extract_{_name}'] = _make_pres_extract(_name, _wrf, _lev)


# function factory for extracting pressure-level winds
def _make_pres_wind_extract(outvar, level_hPa, component):
    """component: 'u' or 'v'"""
    idx = _PLEV_INDEX[level_hPa]
    def extract(ds, time_dim):
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
    return extract

# generate extract functions for ua & va vars
for _comp in ['ua', 'va']:
    for _lev in [700, 500, 250]:
        _name = f'{_comp}{_lev}'
        globals()[f'extract_{_name}'] = _make_pres_wind_extract(_name, _lev, _comp[0])

# function factory for extracting specific humidity at pressure levels
def _make_pres_hus_extract(outvar, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def extract(ds, time_dim):
        q = ds['Q_PL'].isel(num_press_levels_stag=idx)
        # Convert mixing ratio (kg/kg) to specific humidity: hus = q / (1 + q)
        hus = (q / (1 + q))
        hus['time'] = time_dim

        hus = hus.to_dataset(name=outvar).drop_attrs()
        return [(outvar, '6hr', hus)]
    return extract

# generate extract functions for hus vars
for _lev in [700, 500, 250]:
    _name = f'hus{_lev}'
    globals()[f'extract_{_name}'] = _make_pres_hus_extract(_name, _lev)

# ---------------------------------------------------

# AGL height data
# ---------------------------------------------------
# Height levels in meters (AGL), matching num_z_levels_stag dimension order
_Z_LEVELS_M = [50, 100, 150]
_ZLEV_INDEX = {lev: i for i, lev in enumerate(_Z_LEVELS_M)}

def _make_zlev_wind_extract(outvar, level_m, component):
    """component: 'u' or 'v'"""
    idx = _ZLEV_INDEX[level_m]
    def extract(ds, time_dim):
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
    return extract

for _comp in ['ua', 'va']:
    for _lev in _Z_LEVELS_M:
        _name = f'{_comp}{_lev}m'
        globals()[f'extract_{_name}'] = _make_zlev_wind_extract(_name, _lev, _comp[0])

# ---------------------------------------------------

# AFWA diagnostic variables
# ---------------------------------------------------

def extract_cape(ds, time_dim):
    # AFWA_CAPE units : J kg-1
    # AFWA_CAPE description : AFWA Diagnostic: Convective Avail Pot Energy

    cape = ds['AFWA_CAPE'].to_dataset(name='cape').drop_attrs()
    cape['time'] = time_dim
    return [('cape', '1hr', cape)]

def extract_cin(ds, time_dim):
    # AFWA_CIN units : J kg-1
    # AFWA_CIN description : AFWA Diagnostic: Convective Inhibition

    cin = ds['AFWA_CIN'].to_dataset(name='cin').drop_attrs()
    cin['time'] = time_dim
    return [('cin', '1hr', cin)]

def extract_prw(ds, time_dim):
    # AFWA_PWAT units : kg m-2
    # AFWA_PWAT description : AFWA Diagnostic: Precipitable Water

    prw = ds['AFWA_PWAT'].to_dataset(name='prw').drop_attrs()
    prw['time'] = time_dim
    return [('prw', '1hr', prw)]

def extract_fzra(ds, time_dim):
    # AFWA_FZRA units : mm (accumulated)
    # AFWA_FZRA description : AFWA Diagnostic: Freezing rain fall
    # Convert to kg m-2 s-1 by differencing and dividing by 3600

    fzra = (ds['AFWA_FZRA'].diff(dim='time') / 3600.0)
    fzra = fzra.assign_coords(time=time_dim)

    fzra = fzra.to_dataset(name='fzra').drop_attrs()
    return [('fzra', '1hr', fzra)]

def extract_heatidx(ds, time_dim):
    # AFWA_HEATIDX units : K
    # AFWA_HEATIDX description : AFWA Diagnostic: Heat index

    heatidx = ds['AFWA_HEATIDX'].to_dataset(name='heatidx').drop_attrs()
    heatidx['time'] = time_dim
    return [('heatidx', '1hr', heatidx)]

def extract_wchill(ds, time_dim):
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
# NetCDF via xarray append mode.  Both indices are computed together
# since they share the expensive MRT calculation.
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
    """Vapor pressure (Pa) from mixing ratio (kg/kg) and pressure (Pa)."""
    hus = q / (1 + q)     # mixing ratio -> specific humidity
    eps = 0.62197
    return hus * P / (eps + hus * (1.0 - eps))


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

    wspd = np.maximum(np.sqrt(U10**2 + V10**2), 0.5)
    Q2_safe = np.maximum(Q2, 0.0)
    e_a = _vapor_pressure_from_q(Q2_safe, PSFC)
    tdew = _dew_point_from_vapor_pressure(e_a)

    wbgt = thermofeel.calculate_wbgt(t2_k=T2, mrt=mrt, va=wspd, td_k=tdew,)
    utci = thermofeel.calculate_utci(t2_k=T2, va=wspd, mrt=mrt, td_k=tdew,)

    ta_c  = T2 - 273.15
    mrt_c = mrt - 273.15
    valid = (
        (ta_c >= -50) & (ta_c <= 50) &
        (wspd <= 17) &
        ((mrt_c - ta_c) >= -30) & ((mrt_c - ta_c) <= 70)
    )
    utci = np.where(valid, utci, np.nan)

    return wbgt.astype(np.float32), utci.astype(np.float32)


def extract_wbgt_utci():
    """Compute WBGT and UTCI from hourly WRF output, one day at a time.

    Loads one file at a time to avoid accumulating a full year of arrays in
    memory, writing output incrementally via xarray append mode.  Returns an
    empty list so the call site has nothing further to do.

    MRT is computed once per day and shared between both indices.
    """
    wbgt_fout = os.path.join(outdir, 'wbgt', make_fname('wbgt', '1hr'))
    utci_fout = os.path.join(outdir, 'utci', make_fname('utci', '1hr'))
    os.makedirs(os.path.join(outdir, 'wbgt'), exist_ok=True)
    os.makedirs(os.path.join(outdir, 'utci'), exist_ok=True)

    time_index = _build_time_dim(year, 1)

    hr_files = sorted(glob.glob(f'{wrfout_path}/{wrfout_hour_fname}{year}-*'))
    if not hr_files:
        raise FileNotFoundError(
            f'No hourly WRF files found for year {year} in {wrfout_path}')

#     t_written = 0
#     wbgt_nc = utci_nc = None
#
#     try:
#         for i, fpath in enumerate(hr_files):
#             ds = xr.open_dataset(fpath, engine='netcdf4')
#             ds = ds[_WBGT_WRF_VARS]
#
#             wbgt_day, utci_day = _compute_wbgt_utci_arrays(ds)
#             nt_day = wbgt_day.shape[0]
#             ds.close()
#
#             if i == 0:
#                 ny, nx = wbgt_day.shape[1], wbgt_day.shape[2]
#                 wbgt_nc = nc.Dataset(wbgt_fout, 'w', format='NETCDF4')
#                 utci_nc = nc.Dataset(utci_fout, 'w', format='NETCDF4')
#                 for ds_nc, varname in [(wbgt_nc, 'wbgt'), (utci_nc, 'utci')]:
#                     ds_nc.createDimension('time', None)
#                     ds_nc.createDimension('y', ny)
#                     ds_nc.createDimension('x', nx)
#                     t_var = ds_nc.createVariable('time', 'f8', ('time',))
#                     t_var.units    = time_units
#                     t_var.calendar = _cal
#                     ds_nc.createVariable(varname, 'f4', ('time', 'y', 'x'),
#                                          fill_value=1.e20)
#
#             wbgt_nc['time'][t_written:t_written + nt_day] = time_vals[t_written:t_written + nt_day]
#             wbgt_nc['wbgt'][t_written:t_written + nt_day] = wbgt_day
#             utci_nc['time'][t_written:t_written + nt_day] = time_vals[t_written:t_written + nt_day]
#             utci_nc['utci'][t_written:t_written + nt_day] = utci_day
#             t_written += nt_day
#
#             if (i + 1) % 30 == 0:
#                 wbgt_nc.sync()
#                 utci_nc.sync()
#                 print(f'  wbgt/utci: processed {i + 1}/{len(hr_files)} days')
#
#     finally:
#         if wbgt_nc: wbgt_nc.close()
#         if utci_nc: utci_nc.close()

    for i, fpath in enumerate(hr_files):
        ds = xr.open_dataset(fpath, engine='netcdf4')
        ds = ds[_WBGT_WRF_VARS]

        wbgt_day, utci_day = _compute_wbgt_utci_arrays(ds)
        ds.close()

        day_times = time_index[i * 24 : (i + 1) * 24]
        mode = 'w' if i == 0 else 'a'

        xr.DataArray(wbgt_day, dims=['time', 'y', 'x'],
                     coords={'time': day_times}) \
          .to_dataset(name='wbgt') \
          .to_netcdf(wbgt_fout, mode=mode, unlimited_dims=['time'])

        xr.DataArray(utci_day, dims=['time', 'y', 'x'],
                     coords={'time': day_times}) \
          .to_dataset(name='utci') \
          .to_netcdf(utci_fout, mode=mode, unlimited_dims=['time'])

    print(f'  wbgt/utci: finished')
    print(f'postproc time: {time.perf_counter() - t0:.1f} sec')
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    print(f'postproc max memory: {mem / (1024*1024):.1f} GB')

    return []
# ---------------------------------------------------

# Humidex
# ---------------------------------------------------
def extract_humidex(ds, time_dim):
    # T2 units: K, Q2 units: kg/kg (mixing ratio), PSFC units: Pa
    # Humidex = T2 + 0.5555 * (e_hPa - 10.0)
    # where e_hPa is vapor pressure in hPa

    e_hPa = _vapor_pressure_from_q(ds['Q2'], ds['PSFC']) / 100.0
    humidex = ds['T2'] + 0.5555 * (e_hPa - 10.0)
    humidex['time'] = time_dim

    humidex = humidex.to_dataset(name='humidex').drop_attrs()
    return [('humidex', '1hr', humidex)]
# ---------------------------------------------------
