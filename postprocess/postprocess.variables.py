# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis

# Purpose:
# --------
# Extract function definitions for all NA-CORDEX-CMIP6 postprocessing
# variables.  Each function receives a loaded dataset (ds) and returns
# a list of (var, dataset) tuples.
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
def extract_tas(ds):
    # T2 units : K
    # T2 description : 2-meter temperature

    tas = ds['T2']

    tas = tas.to_dataset(name='tas').drop_attrs()
    return [('tas', tas)]
# ---------------------------------------------------

# tasmin and tasmax are derived from tas after extraction (see
# derive_tasmin / derive_tasmax in postprocess.machinery.py), not
# extracted directly.

# Hourly precipitation accumulation
# ---------------------------------------------------
def extract_pr(ds):
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

    pr = pr.to_dataset(name='pr').drop_attrs()
    return [('pr', pr)]
# ---------------------------------------------------

# Evaporation including sublimation and transpiration
# ---------------------------------------------------
def extract_evspsbl(ds):
    # EDIR units : mm/s
    # EDIR description : ground surface evaporation rate
    # ETRAN units : mm/s
    # ETRAN description : transpiration rate

    evspsbl = (ds['EDIR'] + ds['ETRAN'])

    evspsbl = evspsbl.to_dataset(name='evspsbl').drop_attrs()
    return [('evspsbl', evspsbl)]
# ---------------------------------------------------

# Near surface specific humidity
# ---------------------------------------------------
def extract_huss(ds):
    # Q2 units: kg kg-1
    # Q2 description: mixing ratio (QV) at 2 M

    q2   = ds['Q2']
    huss = (q2 / (1 + q2))  # mixing ratio -> specific humidity

    huss = huss.to_dataset(name='huss').drop_attrs()
    return [('huss', huss)]
# ---------------------------------------------------

# Near surface relative humidity
# ---------------------------------------------------
def extract_hurs(ds):
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

    hurs = hurs.to_dataset(name='hurs').drop_attrs()
    return [('hurs', hurs)]
# ---------------------------------------------------

# Surface pressure
# ---------------------------------------------------
def extract_ps(ds):
    # PSFC units: Pa
    # PSFC description: Surface pressure

    ps = ds['PSFC']

    ps = ps.to_dataset(name='ps').drop_attrs()
    return [('ps', ps)]
# ---------------------------------------------------

# Mean sea level pressure
# ---------------------------------------------------
def extract_psl(ds):
    # AFWA_MSLP units: Pa
    # AFWA_MSLP description: Mean sea level pressure

    psl = ds['AFWA_MSLP']

    psl = psl.to_dataset(name='psl').drop_attrs()
    return [('psl', psl)]
# ---------------------------------------------------

# Shared masking helpers
# ---------------------------------------------------
# ds_fx is a module-level variable (loaded from wrf.fx.nc by machinery).

def _apply_landmask(da):
    """Mask ocean gridcells (LANDMASK == 0) to missing (1e20)."""
    return da.where(ds_fx['LANDMASK'] == 1, 1.e20)

def _wind_components(u, v):
    """Rotate grid-relative u/v wind components to earth-relative coordinates.

    Accepts DataArrays u and v on the mass grid (no unstaggering needed for
    diagnostic wind variables).  Returns (uas, vas) as DataArrays.

    NOTE: signs on sinalpha are correct as written; some sources have them
    reversed. Reference: https://www-k12.atmos.washington.edu/~ovens/wrfwinds.html
    """
    cosa = ds_fx['COSALPHA']
    sina = ds_fx['SINALPHA']
    uas = (u * cosa) - (v * sina)
    vas = (v * cosa) + (u * sina)
    return uas, vas

# Near-surface wind components and speed
# ---------------------------------------------------
def extract_sfcWind(ds):
    # U10/V10 units: m s-1
    # U10/V10 description: U/V at 10 M
    uas, vas = _wind_components(ds['U10'], ds['V10'])
    sfcWind = xr.ufuncs.sqrt(uas**2 + vas**2)

    sfcWind = sfcWind.to_dataset(name='sfcWind').drop_attrs()
    return [('sfcWind', sfcWind)]

def extract_uas(ds):
    # U10/V10 units: m s-1
    # U10/V10 description: U/V at 10 M
    uas, _ = _wind_components(ds['U10'], ds['V10'])

    uas = uas.to_dataset(name='uas').drop_attrs()
    return [('uas', uas)]

def extract_vas(ds):
    # U10/V10 units: m s-1
    # U10/V10 description: U/V at 10 M
    _, vas = _wind_components(ds['U10'], ds['V10'])

    vas = vas.to_dataset(name='vas').drop_attrs()
    return [('vas', vas)]
# ---------------------------------------------------

# Surface downwelling shortwave radiation
# ---------------------------------------------------
def extract_rsds(ds):
    # ACSWDNB/I_ACSWDNB units: J m-2
    # ACSWDNB/I_ACSWDNB description: Accumulated downwelling shortwave flux at bottom

    da = ds[['ACSWDNB', 'I_ACSWDNB']]

    # accumulate J/hour/m-2 to W/m2
    acc_rsds = ( (da['I_ACSWDNB'] * 1e9) + da['ACSWDNB'] ) / 3600
    rsds = acc_rsds.diff(dim='time')

    rsds = rsds.to_dataset(name='rsds').drop_attrs()
    return [('rsds', rsds)]
# ---------------------------------------------------

# Surface downwelling longwave radiation
# ---------------------------------------------------
def extract_rlds(ds):
    # ACLWDNB/I_ACLWDNB units: J m-2
    # ACLWDNB/I_ACLWDNB description: Accumulated downwelling longwave flux at bottom

    da = ds[['ACLWDNB', 'I_ACLWDNB']]

    # accumulate J/hour/m-2 to W/m2
    acc_rlds = ( (da['I_ACLWDNB'] * 1e9) + da['ACLWDNB'] ) / 3600
    rlds = acc_rlds.diff(dim='time')

    rlds = rlds.to_dataset(name='rlds').drop_attrs()
    return [('rlds', rlds)]
# ---------------------------------------------------

# Total cloud cover percentage
# ---------------------------------------------------
def extract_clt(ds):
    # CLDFRAC2D units: %
    # CLDFRAC2D description: 2-D max cloud fraction

    clt = (ds['CLDFRAC2D'] * 100)

    # CLDFRAC2D is all-zero on the step after a restart; replace with missing
    zero_timestep = (clt == 0).all(dim=['x', 'y'])
    clt = clt.where(~zero_timestep).fillna(1.e20)

    clt = clt.to_dataset(name='clt').drop_attrs()
    return [('clt', clt)]
# ---------------------------------------------------

# Snow water equivalent - surface snow amount
# ---------------------------------------------------
def extract_snw(ds):
    # SNOW units : kg m-2
    # SNOW description : SNOW WATER EQUIVALENT

    snw = _apply_landmask(ds['SNOW'])

    snw = snw.to_dataset(name='snw').drop_attrs()
    return [('snw', snw)]
# ---------------------------------------------------

# Snow depth
# ---------------------------------------------------
def extract_snd(ds):
    # SNOWH units : m
    # SNOWH description : PHYSICAL SNOW DEPTH

    snd = ds['SNOWH']

    snd = snd.to_dataset(name='snd').drop_attrs()
    return [('snd', snd)]
# ---------------------------------------------------

# Total soil moisture content
# ---------------------------------------------------
def extract_mrso(ds):
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

    mrso = _apply_landmask(mrso)

    mrso = mrso.to_dataset(name='mrso').drop_attrs()
    return [('mrso', mrso)]
# ---------------------------------------------------

# Surface runoff
# ---------------------------------------------------
def extract_mrros(ds):
    # SFROFF units : mm (accumulated)
    # SFROFF description : SURFACE RUNOFF

    da = ds['SFROFF']

    # Differentiate along time axis and convert to kg/m^2/s
    mrros = (da.diff(dim='time') / 21600.0)

    mrros = _apply_landmask(mrros)

    mrros = mrros.to_dataset(name='mrros').drop_attrs()
    return [('mrros', mrros)]
# ---------------------------------------------------

# Total runoff
# ---------------------------------------------------
def extract_mrro(ds):
    # SFROFF units : mm (accumulated)
    # SFROFF description : SURFACE RUNOFF
    # UDROFF units : mm (accumulated)
    # UDROFF description : UNDERGROUND RUNOFF

    # Differentiate along time axis and convert to kg/m^2/s
    mrro = ((ds['SFROFF'] + ds['UDROFF']).diff(dim='time') / 21600.0)

    mrro = _apply_landmask(mrro)

    mrro = mrro.to_dataset(name='mrro').drop_attrs()
    return [('mrro', mrro)]
# ---------------------------------------------------

# Surface upwelling shortwave radiation
# ---------------------------------------------------
def extract_rsus(ds):
    # SWUPB units : W m-2
    # SWUPB description : INSTANTANEOUS UPWELLING SHORTWAVE FLUX AT BOTTOM

    rsus = ds['SWUPB']

    rsus = rsus.to_dataset(name='rsus').drop_attrs()
    return [('rsus', rsus)]
# ---------------------------------------------------

# Surface upwelling longwave radiation
# ---------------------------------------------------
def extract_rlus(ds):
    # LWUPB units : W m-2
    # LWUPB description : INSTANTANEOUS UPWELLING LONGWAVE FLUX AT BOTTOM

    rlus = ds['LWUPB']

    rlus = rlus.to_dataset(name='rlus').drop_attrs()
    return [('rlus', rlus)]
# ---------------------------------------------------

# Surface upward latent heat flux
# ---------------------------------------------------
def extract_hfls(ds):
    # LH units : W m-2
    # LH description : LATENT HEAT FLUX AT THE SURFACE

    hfls = ds['LH']

    hfls = hfls.to_dataset(name='hfls').drop_attrs()
    return [('hfls', hfls)]
# ---------------------------------------------------

# Surface upward sensible heat flux
# ---------------------------------------------------
def extract_hfss(ds):
    # HFX units : W m-2
    # HFX description : UPWARD HEAT FLUX AT THE SURFACE

    hfss = ds['HFX']

    hfss = hfss.to_dataset(name='hfss').drop_attrs()
    return [('hfss', hfss)]
# ---------------------------------------------------

# Surface snow melt
# ---------------------------------------------------
def extract_snm(ds):
    # ACSNOM units : kg m-2 (accumulated)
    # ACSNOM description : ACCUMULATED MELTED SNOW

    # Convert to kg m-2 s-1 by differencing and dividing by 21600
    snm = (ds['ACSNOM'].diff(dim='time') / 21600.0)

    snm = _apply_landmask(snm)

    snm = snm.to_dataset(name='snm').drop_attrs()
    return [('snm', snm)]
# ---------------------------------------------------

# Humidex
# ---------------------------------------------------
def extract_humidex(ds):
    # T2 units: K, Q2 units: kg/kg (mixing ratio), PSFC units: Pa
    # Humidex = T2 + 0.5555 * (e_hPa - 10.0)
    # where e_hPa is vapor pressure in hPa

    e_hPa = _vapor_pressure_from_q(ds['Q2'], ds['PSFC']) / 100.0
    humidex = ds['T2'] + 0.5555 * (e_hPa - 10.0)

    humidex = humidex.to_dataset(name='humidex').drop_attrs()
    return [('humidex', humidex)]
# ---------------------------------------------------

# Pressure level data
# ---------------------------------------------------
# Pressure levels in Pa, matching the num_press_levels_stag dimension order
_PRESS_LEVELS_PA = [100000, 92500, 85000, 75000, 70000, 60000, 50000,
                    40000, 30000, 25000, 20000, 15000, 10000, 7000]

# Map level in hPa to dimension index
_PLEV_INDEX = {lev // 100: i for i, lev in enumerate(_PRESS_LEVELS_PA)}

# Threshold below which surface pressure masking is applied (Pa).
# Cells where PSFC <= this value are underground at that pressure level.
# Only needed for 700 hPa; 500 hPa and 250 hPa are never underground
# given the model domain's maximum elevation (~3000 m).  

# Pressure-level interpolation operates on the lowest model level,
# which is tens of meters above ground level, so it can differ from
# the surface pressure level by a few hPa, or more over steep
# terrain.  We bump the threshold up to 705 mb to be sure we're
# masking out all the bad pixels.

_PSFC_MASK_THRESHOLD_700 = 70500.0

def _load_psfc(yr):
    """Load surface pressure (PSFC, Pa) from wrfout_d01 files for yr.

    wrfout_d01 is the 6-hourly file, matching the cadence of pressure-level
    outputs.  Filenames correspond exactly to wrfout_pres_d01 except for
    the prefix.
    """
    files = sorted(glob.glob(f'{wrfout_path}/{wrfout_6hr_fname}{yr}-*'))
    if not files:
        raise FileNotFoundError(
            f'No wrfout_d01 files found for year {yr} '
            f'(pattern: {wrfout_path}/{wrfout_6hr_fname}{yr}-*)')
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', message='.*separate the stored chunks.*')
        ds_psfc = xr.open_mfdataset(files,
                                    concat_dim='Time',
                                    combine='nested',
                                    chunks=_CHUNKS,
                                    mask_and_scale=False,
                                    decode_times=False,
                                    decode_coords=False,
                                    )['PSFC']
    # Drop the time coordinate labels before returning. psfc is (time, y, x)
    # and remains 3D after the drop -- only the coordinate values are removed,
    # not the dimension itself.  This prevents the unlabelled time dimension on
    # psfc from conflicting with the CFTimeIndex on the pressure-level data
    # during .where().
    return ds_psfc.rename(dname_map_xyt).drop_vars('time', errors='ignore')

def _mask_underground_700(da, yr):
    """Mask gridcells underground at 700 hPa using surface pressure.

    Sets values to 1e20 where PSFC <= 70000 Pa (i.e., surface is at or
    above 700 hPa).  Called only for 700-hPa variables.
    """
    psfc = _load_psfc(yr)
    return da.where(psfc > _PSFC_MASK_THRESHOLD_700, 1.e20)

# function factory for extracting generic pressure-level variables
def _make_pres_extract(outvar, wrf_var, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def extract(ds):
        da = ds[wrf_var].isel(num_press_levels_stag=idx)
        if level_hPa == 700:
            da = _mask_underground_700(da, year)

        da = da.to_dataset(name=outvar).drop_attrs()
        return [(outvar, da)]
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
    def extract(ds):
        u = ds['U_PL'].isel(num_press_levels_stag=idx)
        v = ds['V_PL'].isel(num_press_levels_stag=idx)
        uas, vas = _wind_components(u, v)
        rotated = uas if component == 'u' else vas
        if level_hPa == 700:
            rotated = _mask_underground_700(rotated, year)

        rotated = rotated.to_dataset(name=outvar).drop_attrs()
        return [(outvar, rotated)]
    return extract

# generate extract functions for ua & va vars
for _comp in ['ua', 'va']:
    for _lev in [700, 500, 250]:
        _name = f'{_comp}{_lev}'
        globals()[f'extract_{_name}'] = _make_pres_wind_extract(_name, _lev, _comp[0])

# function factory for extracting specific humidity at pressure levels
def _make_pres_hus_extract(outvar, level_hPa):
    idx = _PLEV_INDEX[level_hPa]
    def extract(ds):
        q = ds['Q_PL'].isel(num_press_levels_stag=idx)
        # Convert mixing ratio (kg/kg) to specific humidity: hus = q / (1 + q)
        hus = (q / (1 + q))
        if level_hPa == 700:
            hus = _mask_underground_700(hus, year)

        hus = hus.to_dataset(name=outvar).drop_attrs()
        return [(outvar, hus)]
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
    def extract(ds):
        u = ds['U_ZL'].isel(num_z_levels_stag=idx)
        v = ds['V_ZL'].isel(num_z_levels_stag=idx)
        uas, vas = _wind_components(u, v)
        rotated = uas if component == 'u' else vas

        rotated = rotated.to_dataset(name=outvar).drop_attrs()
        return [(outvar, rotated)]
    return extract

for _comp in ['ua', 'va']:
    for _lev in _Z_LEVELS_M:
        _name = f'{_comp}{_lev}m'
        globals()[f'extract_{_name}'] = _make_zlev_wind_extract(_name, _lev, _comp[0])

# ---------------------------------------------------

# AFWA diagnostic variables
# ---------------------------------------------------

def extract_cape(ds):
    # AFWA_CAPE units : J kg-1
    # AFWA_CAPE description : AFWA Diagnostic: Convective Avail Pot Energy

    cape = ds['AFWA_CAPE'].to_dataset(name='cape').drop_attrs()
    return [('cape', cape)]

def extract_cin(ds):
    # AFWA_CIN units : J kg-1
    # AFWA_CIN description : AFWA Diagnostic: Convective Inhibition
    # WRF uses -9.9999e+30 as a flag for "fully inhibited" CIN; mask
    # everything below -1e30 as missing.

    cin = ds['AFWA_CIN'].where(ds['AFWA_CIN'] >= -1.e30, 1.e20)
    cin = cin.to_dataset(name='cin').drop_attrs()
    return [('cin', cin)]

def extract_prw(ds):
    # AFWA_PWAT units : kg m-2
    # AFWA_PWAT description : AFWA Diagnostic: Precipitable Water

    prw = ds['AFWA_PWAT'].to_dataset(name='prw').drop_attrs()
    return [('prw', prw)]

def extract_fzra(ds):
    # AFWA_FZRA units : mm (accumulated)
    # AFWA_FZRA description : AFWA Diagnostic: Freezing rain fall
    # Convert to kg m-2 s-1 by differencing and dividing by 3600

    fzra = (ds['AFWA_FZRA'].diff(dim='time') / 3600.0)

    fzra = fzra.to_dataset(name='fzra').drop_attrs()
    return [('fzra', fzra)]

def extract_heatidx(ds):
    # AFWA_HEATIDX units : K
    # AFWA_HEATIDX description : AFWA Diagnostic: Heat index

    heatidx = ds['AFWA_HEATIDX'].to_dataset(name='heatidx').drop_attrs()
    return [('heatidx', heatidx)]

def extract_wchill(ds):
    # AFWA_WCHILL units : K
    # AFWA_WCHILL description : AFWA Diagnostic: Wind chill

    wchill = ds['AFWA_WCHILL'].to_dataset(name='wchill').drop_attrs()
    return [('wchill', wchill)]

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
# These functions do not follow the standard (ds) signature;
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
    wbgt_fout = os.path.join(outdir, 'wbgt.1hr', make_fname('wbgt', '1hr'))
    utci_fout = os.path.join(outdir, 'utci.1hr', make_fname('utci', '1hr'))
    os.makedirs(os.path.join(outdir, 'wbgt.1hr'), exist_ok=True)
    os.makedirs(os.path.join(outdir, 'utci.1hr'), exist_ok=True)

    time_index, _ = _build_time_coord(year, 1, 'time: point')

    hr_files = sorted(glob.glob(f'{wrfout_path}/{wrfout_hour_fname}{year}-*'))
    if not hr_files:
        raise FileNotFoundError(
            f'No hourly WRF files found for year {year} in {wrfout_path}')

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
