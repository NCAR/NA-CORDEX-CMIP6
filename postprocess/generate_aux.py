#!/usr/bin/env python3
# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen
"""
generate_aux.py - Generate auxiliary NetCDF files for postprocessing.

Called from setup.py; not intended to be run standalone.

Produces two auxiliary files in SETUPDIR:

  xy.coords.nc
      lat, lon           (y, x)    -- cell-center geographic coordinates
      lat_bnds, lon_bnds (y, x, 4) -- cell-corner bounds, CCW from SW corner
      x, y               (x), (y)  -- Lambert conformal projection coordinates
      crs                          -- coordinate reference system scalar

  wrf.fx.nc
      LANDMASK   (y, x)  -- land fraction (1=land, 0=ocean)
      COSALPHA   (y, x)  -- wind rotation cosine
      SINALPHA   (y, x)  -- wind rotation sine

Data comes from a geo_em.d01.nc file specified in sim.env as wrfinput_path.
"""

import os
import sys
import subprocess

import numpy as np
import xarray as xr


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Set to True by setup.py when --verbose is passed.
verbose = False

def vprint(*args, **kwargs):
    if verbose:
        print(*args, **kwargs)


def _open_wrfinput(wrfinput_path, varnames):
    """Open WRF input (geo_em.d01.nc) file, loading only varnames.

    WRF input files carry a singleton Time dimension.  We squeeze it out and
    drop every time-related coordinate (Time, XTIME, and anything whose only
    dimension was Time) so none of that leaks into the output files.

    """
    if not os.path.exists(wrfinput_path):
        sys.exit(f"Error: {wrfinput_path} not found")

    ds = xr.open_dataset(wrfinput_path, decode_times=False)[varnames]

    # Squeeze the singleton Time dimension; drop=True removes the scalar
    # coordinate that squeeze would otherwise leave behind.
    ds = ds.squeeze('Time', drop=True)

    # Drop any remaining time-related coordinates xarray may have retained
    # (e.g. XTIME carried as a non-dimension coordinate).
    time_coords = [c for c in ds.coords
                   if c in ('Time', 'XTIME') or 'time' in c.lower()]
    if time_coords:
        ds = ds.drop_vars(time_coords)

    return ds


# ---------------------------------------------------------------------------
# wrf.xy.coords.nc
# ---------------------------------------------------------------------------

def create_coord_file(wrfinput_path, setupdir, force):
    """Create xy.coords.nc with lat/lon, bounds, x/y, and CRS.

    Cell-center lat/lon: XLAT_M / XLONG_M.
    Cell-corner bounds: XLAT_C / XLONG_C (one larger in each dimension)

    Bounds are ordered CCW starting from the SW corner (v=vertex):

    v3: _C(j+1,i)    v2: _C(j+1, i+1)
              +---------+
              |         |
              | _M(j,i) |
              |         |
              +---------+
    v0: _C(j,  i)    v1: _C(j,  i+1)

    i = west_east index   (west_east_stag   for LAT_C & LONG_C)
    j = south_north index (south_north_stag for LAT_C & LONG_C)
    """
    outname = "xy.coords.nc"
    outpath = os.path.join(setupdir, outname)

    if os.path.exists(outpath) and not force:
        print(f"\n=== Coordinate file {outname} already exists (skipping) ===")
        return

    print(f"\n=== Generating coordinate file {outname} ===")
    vprint(f"  Source: {wrfinput_path}")

    ds = _open_wrfinput(wrfinput_path,
                        ['XLAT_M', 'XLONG_M', 'XLAT_C', 'XLONG_C'])

    center_lat = ds['XLAT_M'].values   # (south_north, west_east)
    center_lon = ds['XLONG_M'].values
    corner_lat = ds['XLAT_C'].values   # (south_north_stag, west_east_stag)
    corner_lon = ds['XLONG_C'].values
    ds.close()

    ny, nx = center_lat.shape

    if corner_lat.shape != (ny + 1, nx + 1):
        sys.exit(
            f"Error: XLAT_C shape {corner_lat.shape} does not match expected "
            f"({ny + 1}, {nx + 1}) for center grid ({ny}, {nx})"
        )

    lat_bnds = np.stack([
        corner_lat[ :-1,  :-1],   # SW  vertex 0
        corner_lat[ :-1, 1:  ],   # SE  vertex 1
        corner_lat[1:  , 1:  ],   # NE  vertex 2
        corner_lat[1:  ,  :-1],   # NW  vertex 3
    ], axis=-1)

    lon_bnds = np.stack([
        corner_lon[ :-1,  :-1],
        corner_lon[ :-1, 1:  ],
        corner_lon[1:  , 1:  ],
        corner_lon[1:  ,  :-1],
    ], axis=-1)

    # Longitude monotonicity: shift negative values to 0-360
    center_lon[center_lon < 0] += 360
    lon_bnds[lon_bnds < 0] += 360

    # x/y projection coordinates: 12 km grid spacing, centered on domain
    x_coords = (np.arange(nx) - (nx - 1) / 2.0) * 12000.0
    y_coords = (np.arange(ny) - (ny - 1) / 2.0) * 12000.0

    # x/y bounds: 6 km on either side of each cell center
    x_bnds = np.stack([x_coords - 6000.0, x_coords + 6000.0], axis=-1)
    y_bnds = np.stack([y_coords - 6000.0, y_coords + 6000.0], axis=-1)

    # CRS: Lambert conformal conic (NA-CORDEX parameters)
    # Stored as a scalar integer variable; all projection info is in attributes.
    crs_var = xr.DataArray(
        np.int32(-9999),
        attrs={
            'long_name':                     'coordinate reference system',
            'grid_mapping_name':             'lambert_conformal_conic',
            'standard_parallel':             [np.float32(35.), np.float32(60.)],
            'longitude_of_central_meridian': np.float32(-97.),
            'latitude_of_projection_origin': np.float32(46.),
            'semi_major_axis':               np.float32(6370000.),
            'semi_minor_axis':               np.float32(6370000.),
            'false_easting':                 np.float32(0.),
            'false_northing':                np.float32(0.),
            'units':                         'm',
        }
    )

    out = xr.Dataset(
        {
            'lat': xr.DataArray(
                center_lat.astype(np.float64), dims=['y', 'x'],
                attrs={
                    'units':         'degrees_north',
                    'long_name':     'latitude',
                    'standard_name': 'latitude',
                    'bounds':        'lat_bnds',
                }),
            'lon': xr.DataArray(
                center_lon.astype(np.float64), dims=['y', 'x'],
                attrs={
                    'units':         'degrees_east',
                    'long_name':     'longitude',
                    'standard_name': 'longitude',
                    'bounds':        'lon_bnds',
                }),
            'lat_bnds': xr.DataArray(
                lat_bnds.astype(np.float64), dims=['y', 'x', 'nv'],
                attrs={}),
            'lon_bnds': xr.DataArray(
                lon_bnds.astype(np.float64), dims=['y', 'x', 'nv'],
                attrs={}),
            'crs': crs_var,
            'x': xr.DataArray(
                x_coords, dims=['x'],
                attrs={
                    'units':         'm',
                    'long_name':     'x-coordinate in Cartesian system',
                    'standard_name': 'projection_x_coordinate',
                    'axis':          'X',
                    'bounds':        'x_bnds',
                }),
            'y': xr.DataArray(
                y_coords, dims=['y'],
                attrs={
                    'units':         'm',
                    'long_name':     'y-coordinate in Cartesian system',
                    'standard_name': 'projection_y_coordinate',
                    'axis':          'Y',
                    'bounds':        'y_bnds',
                }),
            'x_bnds': xr.DataArray(
                x_bnds, dims=['x', 'nb'],
                attrs={}),
            'y_bnds': xr.DataArray(
                y_bnds, dims=['y', 'nb'],
                attrs={}),
        }
    )
    out.attrs = {}

    enc = {v: {'_FillValue': None} for v in list(out.data_vars) + list(out.coords)}
    out.to_netcdf(outpath, encoding=enc)
    out.close()

    print(f"  wrote {outpath}")


# ---------------------------------------------------------------------------
# wrf.fx.nc
# ---------------------------------------------------------------------------

# Fields to extract for use during postprocessing.
# LANDMASK: land/sea mask (1=land, 0=ocean); used to mask land-only variables.
# COSALPHA, SINALPHA: wind rotation angles; used to rotate U/V to
#   earth-relative coordinates.
_FX_VARS = ['LANDMASK', 'COSALPHA', 'SINALPHA']


def create_fx_file(wrfinput_path, setupdir, force):
    """
    Extract fixed WRF fields (LANDMASK, COSALPHA, SINALPHA) into wrf.fx.nc.

    The singleton Time dimension is squeezed out, WRF spatial
    dimensions are renamed to CORDEX conventions (x, y), all global
    attributes and all per-variable attributes except 'description'
    are dropped, and the result is written to setupdir/wrf.fx.nc.

    """
    outname = "wrf.fx.nc"
    outpath = os.path.join(setupdir, outname)

    if os.path.exists(outpath) and not force:
        print(f"\n=== Fixed-field file {outname} already exists (skipping) ===")
        return

    print(f"\n=== Generating fixed-field file {outname} ===")
    vprint(f"  Source: {wrfinput_path}")
    vprint(f"  Variables: {', '.join(_FX_VARS)}")

    ds = _open_wrfinput(wrfinput_path, _FX_VARS)
    ds = ds.rename({'south_north': 'y', 'west_east': 'x'})

    ds.attrs = {}
    for var in ds.data_vars:
        ds[var].attrs = {k: v for k, v in ds[var].attrs.items()
                         if k == 'description'}

    # Prevent xarray from writing an unlimited_dims encoding carried over
    # from the source file, which would cause a spurious UserWarning.
    ds.encoding.pop('unlimited_dims', None)

    encoding = {var: {'_FillValue': None} for var in ds.data_vars}
    ds.to_netcdf(outpath, encoding=encoding)
    ds.close()

    # Remove any 'coordinates' attribute xarray may have written on variables,
    # referencing XLAT/XLONG/XTIME that are not present in this file.
    result = subprocess.run(
        f'ncatted -h -a coordinates,,d,, "{outpath}"', shell=True)
    if result.returncode != 0:
        sys.exit(f"Error: ncatted failed on {outpath}")

    print(f"  wrote {outpath}")
