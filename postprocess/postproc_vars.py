# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen

# Purpose:
# --------
# Extract function definitions for all NA-CORDEX-CMIP6 postprocessing
# variables.  Each function receives a loaded dataset (ds) and returns
# a list of (var, dataset) tuples.
#
# This file contains only function definitions.  All shared state
# (ds_fx, dname_map_xy, wrfout_* filenames, etc.), dispatch tables,
# and execution logic live in postproc_engine.py, which imports
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
# derive_tasmin / derive_tasmax in postproc_engine.py), not
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

# Soil moisture content per layer
# ---------------------------------------------------
# Noah-MP layer thicknesses (DZS) and bounds (m below surface).
# Layer midpoints are used as the depth coordinate values.
_SOIL_DZ   = np.array([0.10, 0.30, 0.60, 1.00])
_SOIL_BNDS = np.array([[0.0, 0.1], [0.1, 0.4], [0.4, 1.0], [1.0, 2.0]])
_SOIL_MID  = _SOIL_BNDS.mean(axis=1)

def extract_mrsol(ds):
    # SMOIS units: m3 m-3 (volumetric, includes frozen water)
    # SMOIS description: SOIL MOISTURE
    # Output: mrsol(time, depth, y, x) in kg m-2 per layer, matching
    # CORDEX 6hr mrsol (dimensions: longitude latitude sdepth time1).
    # Coordinate metadata (units, bounds, positive, etc.) is written by
    # cmorize.sh; here we only supply the values.

    smois = ds['SMOIS']
    smois = smois.rename({smois.dims[1]: 'depth'})  # soil_layers_stag -> depth

    dz = xr.DataArray(_SOIL_DZ, dims=['depth'])
    mrsol = smois * dz * 1000  # m3 m-3 * m * kg m-3 -> kg m-2

    mrsol = _apply_landmask(mrsol)

    mrsol = mrsol.to_dataset(name='mrsol').drop_attrs()
    mrsol = mrsol.assign_coords(depth=('depth', _SOIL_MID))
    mrsol['depth_bnds'] = (('depth', 'bnds'), _SOIL_BNDS)
    mrsol['depth'].encoding = {'_FillValue': None, 'dtype': np.float64}
    mrsol['depth_bnds'].encoding = {'_FillValue': None, 'dtype': np.float64}
    return [('mrsol', mrsol)]
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

# Hourly-accumulated radiation with integer bucket (bucket_J = 1e9)
# ---------------------------------------------------
def _bucket_flux(ds, acc, bucket):
    """Mean flux (W m-2) over each hour from an accumulated WRF field
    (J m-2) and its integer bucket counter.  Requires the day-before
    timestep (accumulated=True in the engine), so diff gives full length."""
    total = (ds[bucket] * 1e9) + ds[acc]
    return total.diff(dim='time') / 3600

# Surface upwelling shortwave radiation
# ---------------------------------------------------
def extract_rsus(ds):
    # ACSWUPB/I_ACSWUPB units: J m-2 (hourly file)
    # ACSWUPB description: ACCUMULATED UPWELLING SHORTWAVE FLUX AT BOTTOM
    # Replaces the former 6hr instantaneous SWUPB source.

    rsus = _bucket_flux(ds, 'ACSWUPB', 'I_ACSWUPB')

    rsus = rsus.to_dataset(name='rsus').drop_attrs()
    return [('rsus', rsus)]
# ---------------------------------------------------

# Surface upwelling longwave radiation
# ---------------------------------------------------
def extract_rlus(ds):
    # ACLWUPB/I_ACLWUPB units: J m-2 (hourly file)
    # ACLWUPB description: ACCUMULATED UPWELLING LONGWAVE FLUX AT BOTTOM
    # Replaces the former 6hr instantaneous LWUPB source.

    rlus = _bucket_flux(ds, 'ACLWUPB', 'I_ACLWUPB')

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
    # Now read from the hourly file (was 6hr).  Still instantaneous.
    # Do not switch to ACHFX: float32 accumulator with no bucket loses
    # precision late in long chunks.

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

# TOA radiation
# ---------------------------------------------------
def extract_rsdt(ds):
    # ACSWDNT/I_ACSWDNT units: J m-2 (hourly file)
    # ACSWDNT description: ACCUMULATED DOWNWELLING SHORTWAVE FLUX AT TOP
    rsdt = _bucket_flux(ds, 'ACSWDNT', 'I_ACSWDNT')

    rsdt = rsdt.to_dataset(name='rsdt').drop_attrs()
    return [('rsdt', rsdt)]

def extract_rsut(ds):
    # ACSWUPT/I_ACSWUPT units: J m-2 (hourly file)
    # ACSWUPT description: ACCUMULATED UPWELLING SHORTWAVE FLUX AT TOP
    rsut = _bucket_flux(ds, 'ACSWUPT', 'I_ACSWUPT')

    rsut = rsut.to_dataset(name='rsut').drop_attrs()
    return [('rsut', rsut)]

def extract_rlut(ds):
    # OLR units: W m-2
    # OLR description: TOA OUTGOING LONG WAVE
    # Instantaneous (no ACLWUPT in output); CORDEX labels rlut time: mean.
    rlut = ds['OLR']

    rlut = rlut.to_dataset(name='rlut').drop_attrs()
    return [('rlut', rlut)]
# ---------------------------------------------------

# Surface shortwave components and albedo
# ---------------------------------------------------
def extract_rsdsdiff(ds):
    # SWDDIF units: W m-2
    # SWDDIF description: Shortwave surface downward diffuse irradiance
    rsdsdiff = ds['SWDDIF']

    rsdsdiff = rsdsdiff.to_dataset(name='rsdsdiff').drop_attrs()
    return [('rsdsdiff', rsdsdiff)]

def extract_rsdsdir(ds):
    # SWDOWN, SWDDIF units: W m-2 (instantaneous)
    # CORDEX rsdsdir is direct radiation on a horizontal surface, so use
    # SWDOWN - SWDDIF.  SWDDNI is direct *normal* irradiance and is not
    # used (SWDDNI * COSZEN should approximately equal this result).
    rsdsdir = ds['SWDOWN'] - ds['SWDDIF']

    rsdsdir = rsdsdir.to_dataset(name='rsdsdir').drop_attrs()
    return [('rsdsdir', rsdsdir)]

def extract_alb(ds):
    # ALBEDO units: 1
    # ALBEDO description: ALBEDO
    alb = ds['ALBEDO']

    alb = alb.to_dataset(name='alb').drop_attrs()
    return [('alb', alb)]
# ---------------------------------------------------

# Surface temperature, boundary layer, friction velocity
# ---------------------------------------------------
def extract_ts(ds):
    # TSK units: K
    # TSK description: SURFACE SKIN TEMPERATURE
    ts = ds['TSK']

    ts = ts.to_dataset(name='ts').drop_attrs()
    return [('ts', ts)]

def extract_zmla(ds):
    # PBLH units: m
    # PBLH description: PBL HEIGHT
    zmla = ds['PBLH']

    zmla = zmla.to_dataset(name='zmla').drop_attrs()
    return [('zmla', zmla)]

def extract_ustar(ds):
    # UST units: m s-1
    # UST description: U* IN SIMILARITY THEORY
    ustar = ds['UST']

    ustar = ustar.to_dataset(name='ustar').drop_attrs()
    return [('ustar', ustar)]
# ---------------------------------------------------

# Land surface: ground heat flux, LAI, snow cover, surface soil moisture
# ---------------------------------------------------
def extract_hfso(ds):
    # GRDFLX units: W m-2 (instantaneous, positive downward into soil)
    # GRDFLX description: GROUND HEAT FLUX
    hfso = ds['GRDFLX']

    hfso = hfso.to_dataset(name='hfso').drop_attrs()
    return [('hfso', hfso)]

def extract_lai(ds):
    # LAI units: m2 m-2 (time-varying)
    # LAI description: LEAF AREA INDEX
    lai = ds['LAI']

    lai = lai.to_dataset(name='lai').drop_attrs()
    return [('lai', lai)]

def extract_snc(ds):
    # SNOWC units: 1 (fraction)
    # SNOWC description: FLAG INDICATING SNOW COVERAGE (1 FOR SNOW COVER)
    snc = ds['SNOWC'] * 100  # fraction -> %

    snc = snc.to_dataset(name='snc').drop_attrs()
    return [('snc', snc)]

def extract_mrsos(ds):
    # SMOIS layer 1 (0-0.1 m), m3 m-3, includes frozen water
    mrsos = ds['SMOIS'][:, 0] * _SOIL_DZ[0] * 1000  # -> kg m-2

    mrsos = _apply_landmask(mrsos)

    mrsos = mrsos.to_dataset(name='mrsos').drop_attrs()
    return [('mrsos', mrsos)]
# ---------------------------------------------------

# Cloud ice water path
# ---------------------------------------------------
def extract_clivi(ds):
    # IWP_TOT units: kg m-2
    # IWP_TOT description: ICE CLOUD WATER PATH RESOLVED + UNRESOLVED
    clivi = ds['IWP_TOT']

    clivi = clivi.to_dataset(name='clivi').drop_attrs()
    return [('clivi', clivi)]
# ---------------------------------------------------

# Humidex
# ---------------------------------------------------

def _vapor_pressure_from_q(q, P):
    """Vapor pressure (Pa) from mixing ratio (kg/kg) and pressure (Pa)."""
    hus = q / (1 + q)    # mixing ratio -> specific humidity
    eps = 0.62197
    return hus * P / (eps + hus * (1.0 - eps))

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

# Levels written out (70 hPa is in the file but not requested)
_OUT_LEVELS_HPA = [1000, 925, 850, 750, 700, 600, 500,
                   400, 300, 250, 200, 150, 100]

# WRF fills pressure-level points that are below ground with -999.
# Anything below this threshold is treated as fill.
_WRF_PL_FILL_THRESHOLD = -998.0

# Surface-pressure masking for levels that can intersect terrain.
# 500 hPa and above are never underground given the domain's maximum
# elevation (~3000 m).
#
# Pressure-level interpolation operates on the lowest model level,
# which is tens of meters above ground level, so it can differ from
# the surface pressure level by a few hPa, or more over steep
# terrain.  Cells are masked where PSFC <= level + 5 hPa to be sure we
# mask out all the bad pixels.
_PSFC_MASK_LEVELS_HPA = {1000, 925, 850, 750, 700, 600}
_PSFC_MASK_OFFSET_PA  = 500.0

_psfc_cache = {}

def _load_psfc(yr):
    """Load surface pressure (PSFC, Pa) from wrfout_d01 files for yr.

    wrfout_d01 is the 6-hourly file, matching the cadence of pressure-level
    outputs.  Filenames correspond exactly to wrfout_pres_d01 except for
    the prefix.  Cached per year so grouped jobs load it once.
    """
    if yr in _psfc_cache:
        return _psfc_cache[yr]
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
                                    mask_and_scale=False,
                                    decode_times=False,
                                    decode_coords=False,
                                    )['PSFC']
    # Drop the time coordinate labels before returning. psfc is (time, y, x)
    # and remains 3D after the drop -- only the coordinate values are removed,
    # not the dimension itself.  This prevents the unlabelled time dimension on
    # psfc from conflicting with the CFTimeIndex on the pressure-level data
    # during .where().
    psfc = ds_psfc.rename(dname_map_xyt).drop_vars('time', errors='ignore')
    _psfc_cache[yr] = psfc
    return psfc

def _mask_pres(da, raw_fields, level_hPa, yr):
    """Set da to 1e20 wherever any raw WRF field is -999 (underground fill)
    and, for low levels, wherever PSFC <= level + 5 hPa.

    raw_fields are the unconverted inputs at this level, so fill points
    are identified before conversion or rotation can disguise them.
    """
    valid = raw_fields[0] > _WRF_PL_FILL_THRESHOLD
    for raw in raw_fields[1:]:
        valid = valid & (raw > _WRF_PL_FILL_THRESHOLD)
    if level_hPa in _PSFC_MASK_LEVELS_HPA:
        psfc = _load_psfc(yr)
        valid = valid & (psfc > level_hPa * 100.0 + _PSFC_MASK_OFFSET_PA)
    return da.where(valid, 1.e20)

# function factory for extracting scalar pressure-level variables
# convert: optional function applied to the raw field (e.g. q -> hus)
def _make_pres_extract(outvar, wrf_var, level_hPa, convert=None):
    idx = _PLEV_INDEX[level_hPa]
    def extract(ds):
        raw = ds[wrf_var].isel(num_press_levels_stag=idx)
        da = convert(raw) if convert else raw
        da = _mask_pres(da, [raw], level_hPa, year)

        da = da.to_dataset(name=outvar).drop_attrs()
        return [(outvar, da)]
    return extract

def _q_to_hus(q):
    """Mixing ratio (kg/kg) -> specific humidity."""
    return q / (1 + q)

# generate extract functions for ta, zg, hus & hur vars
for _var, _wrf, _conv in [
    ('ta',  'T_PL',   None),
    ('zg',  'GHT_PL', None),
    ('hus', 'Q_PL',   _q_to_hus),
    ('hur', 'RH_PL',  None),
]:
    for _lev in _OUT_LEVELS_HPA:
        _name = f'{_var}{_lev}'
        globals()[f'extract_{_name}'] = _make_pres_extract(_name, _wrf, _lev, _conv)


# function factory for extracting pressure-level winds
def _make_pres_wind_extract(outvar, level_hPa, component):
    """component: 'u' or 'v'"""
    idx = _PLEV_INDEX[level_hPa]
    def extract(ds):
        u = ds['U_PL'].isel(num_press_levels_stag=idx)
        v = ds['V_PL'].isel(num_press_levels_stag=idx)
        uas, vas = _wind_components(u, v)
        rotated = uas if component == 'u' else vas
        rotated = _mask_pres(rotated, [u, v], level_hPa, year)

        rotated = rotated.to_dataset(name=outvar).drop_attrs()
        return [(outvar, rotated)]
    return extract

# generate extract functions for ua & va vars
for _comp in ['ua', 'va']:
    for _lev in _OUT_LEVELS_HPA:
        _name = f'{_comp}{_lev}'
        globals()[f'extract_{_name}'] = _make_pres_wind_extract(_name, _lev, _comp[0])

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

def extract_capemu(ds):
    # AFWA_CAPE_MU units : J kg-1
    # AFWA_CAPE_MU description : Most unstable CAPE 0-180mb
    # Mask any large-negative flag values defensively (as for CIN).
    capemu = ds['AFWA_CAPE_MU'].where(ds['AFWA_CAPE_MU'] >= -1.e30, 1.e20)
    capemu = capemu.to_dataset(name='capemu').drop_attrs()
    return [('capemu', capemu)]

def extract_cinmu(ds):
    # AFWA_CIN_MU units : J kg-1 (negative, WRF sign kept to match cin)
    # AFWA_CIN_MU description : Most unstable CIN 0-180mb
    # -9.9999e+30 flags "fully inhibited"; mask as missing.
    cinmu = ds['AFWA_CIN_MU'].where(ds['AFWA_CIN_MU'] >= -1.e30, 1.e20)
    cinmu = cinmu.to_dataset(name='cinmu').drop_attrs()
    return [('cinmu', cinmu)]

def extract_li(ds):
    # AFWA_LIDX units : K
    # AFWA_LIDX description : AFWA Diagnostic: Surface Lifted Index
    li = ds['AFWA_LIDX'].to_dataset(name='li').drop_attrs()
    return [('li', li)]

def extract_zlfc(ds):
    # AFWA_ZLFC units : m (above ground level; WRF subtracts HT)
    # AFWA_ZLFC description : AFWA Diagnostic: Level of Free Convection
    # WRF writes -1 where there is no LFC; mask as missing.
    zlfc = ds['AFWA_ZLFC'].where(ds['AFWA_ZLFC'] >= 0, 1.e20)
    zlfc = zlfc.to_dataset(name='zlfc').drop_attrs()
    return [('zlfc', zlfc)]

# ---------------------------------------------------
