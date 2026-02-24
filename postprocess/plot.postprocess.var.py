# Author: Jacob Stuivenvolt-Allen
# Last updated: February 24, 2025

# Purpose:
# --------
# This script post-processes WRF output. The resulting
# output will be CMORized. All standardization was done
# by consulting the WCRP-CORDEX CMOR-Tables: 
# https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables

# USAGE:
# ----- 

# Example execution for one year of hourly data:
# ------------------------------------------------
# $ python plot.postprocess.var.py 1980 tas
#
# Example with a custom input path:
# ------------------------------------------------
# $ python plot.postprocess.var.py 1980 tas --input-path /path/to/data
#
# Example with a custom output path for figures:
# ------------------------------------------------
# $ python plot.postprocess.var.py 1980 tas --output-path /path/to/figures
#
# The script auto-detects hourly vs monthly data based on the filename.
# For hourly data, it plots Jan 1, Jun 15, and Dec 31.
# For monthly (multi-year) data, it plots the first, middle, and last time steps.

# ------------------------------------------------
import xarray as xr
from xarray import ufuncs
from datetime import date
import numpy as np
import matplotlib.pyplot as plt
import glob
import sys
import os
import argparse

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LongitudeFormatter, LatitudeFormatter
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import matplotlib.ticker as mticker

# --- Argument parsing ---
parser = argparse.ArgumentParser(description='Post-process and plot WRF CORDEX-CMIP6 output.')
parser.add_argument('year', type=str, help='Begininng year in data and filename')
parser.add_argument('var', type=str, help='Variable name (e.g. tas, evspsbl)')
parser.add_argument('--input-path', type=str, default=None,
                    help='Path to input data directory. Default: ./<var>/')
parser.add_argument('--output-path', type=str, default=None,
                    help='Path to output figures directory. Default: ./<var>/figures/')
args = parser.parse_args()

year = args.year
var  = args.var
input_path = args.input_path if args.input_path else f'{var}'
fig_dir = args.output_path if args.output_path else f'{var}/figures'


# Find any file matching the variable and year, regardless of frequency
all_files = glob.glob(f'{input_path}/{var}_NAM-12_ERA5_evaluation_r1i1p1f1_NCAR_WRF461S-SN_v1-r1_*_{year}*.nc')

if not all_files:
    sys.exit(f'ERROR: No files found matching var={var}, year={year} in {input_path}/')

# Check if output path exists
if not os.path.exists(fig_dir):
    os.makedirs(fig_dir)
filename = all_files[0]

# Extract frequency from filename (e.g. hr, day, 6hr, mon)
# Filename pattern: ...v1-r1_{freq}_{timerange}.nc
freq = os.path.basename(filename).split('v1-r1_')[1].split('_')[0]

print(f'Found {freq} file: {filename}')
ds = xr.open_dataset(filename)[var]

# Derive a label for the time range from the filename
# e.g. "198101-199012" for monthly or "1980" for hourly
fname_base = os.path.basename(filename).replace('.nc', '')
time_label = fname_base.split(f'v1-r1_{freq}_')[-1]

# Plot time series - near Boulder
# -------------------------------
target_lat = 40.0
target_lon = 255.75

# Compute distance to each grid point
dist = np.sqrt(
    (ds["lat"] - target_lat)**2 +
    (ds["lon"] - target_lon)**2
)

# Find the indices of the minimum distance 
j, i = np.unravel_index(dist.argmin(), dist.shape)
target_ds = ds[:,j,i]

# Plot
fig = plt.figure(figsize=(12,5))
ax = fig.add_subplot(111)

xax = ds.time
ax.plot(xax, target_ds, color='cadetblue', lw=0.5)

# Adjust rolling window based on frequency
rolling_windows = {'hr': 24 * 5, '6hr': 4 * 5, 'day': 5, 'mon': 12}
rolling_window = rolling_windows.get(freq, 5)

if rolling_window > 1:
    ax.plot(xax, target_ds.rolling(time=rolling_window).mean(), color='darkslategray', lw=1.5)

ax.set_title(f'{var} : {time_label}', loc='left')

units = target_ds.attrs['units']
ax.set_ylabel(units)
plt.savefig(f'{fig_dir}/{var}.timeseries.{time_label}.png', dpi=300)
plt.close()
# -------------------------------

# Plot three time slices
# ----------------------

pcrs = ccrs.LambertConformal(central_longitude=260)
tcrs = ccrs.PlateCarree()

def plot(ax):
    xticks = np.arange(215,310,15)
    yticks = np.arange(5,80,15)

    ax.set_extent([215,310,5,80], crs=ccrs.PlateCarree())
    ax.add_feature(cfeature.COASTLINE.with_scale('110m'), linewidth=0.4)
    ax.add_feature(cfeature.STATES, linewidth=0.5, alpha=0.7)
    ax.add_feature(cfeature.BORDERS, linewidth=0.5, alpha=0.7)

    gl = ax.gridlines(draw_labels=True, crs=ccrs.PlateCarree(),
                    x_inline=False, y_inline=False, color='white', lw=0.3,
                    alpha=0.6)

    gl.xlocator = mticker.FixedLocator(xticks)
    gl.ylocator = mticker.FixedLocator(yticks)
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER

    #gl.ylabels_right = False
    gl.top_labels=False
    gl.right_labels=False
    gl.bottom_labels=False

    return(ax)

def cbar(ax, cf, label):
    pos = ax.get_position()
    x1 = pos.x0 + 0.02
    x2 = pos.x1 - pos.x0 - 0.04
    y1 = pos.y0 - 0.03
    cbar_ax = fig.add_axes([x1, y1, x2, 0.02])
    plt.colorbar(cf, cax=cbar_ax, orientation='horizontal', label=f'{label}')
    return()

# Time slices: for single-year hourly data use specific dates; otherwise first/middle/last
nt = ds.sizes['time']
if freq == 'hr':
    t1 = f'{str(year)}-01-01T00:00:00'
    t2 = f'{str(year)}-06-15T12:00:00'
    t3 = f'{str(year)}-12-31T23:00:00'
    times = [t1, t2, t3]
    time_indices = None
else:
    time_indices = [0, nt // 2, nt - 1]
    times = [str(ds.time.values[idx]) for idx in time_indices]

# ---------
fig, ax = plt.subplots(1,3, figsize=(13,4), subplot_kw={'projection':pcrs})
fig.subplots_adjust(bottom=0.10, top=0.95, left=0.05,
                    right=0.95, wspace=0.15)

for t in range(3):
    if time_indices is not None:
        da = ds.isel(time=time_indices[t])
    else:
        da = ds.sel(time=times[t], method='nearest')

    da = da.where(da != 1e20)

    vmin = da.min().values
    vmax = da.max().values

    lat = ds.lat
    lon = ds.lon

    cf = ax[t].pcolormesh(lon, lat, da,
                      vmin=vmin, vmax=vmax,
                      cmap='nipy_spectral',
                      transform=tcrs)

    # Format title with readable time
    time_str = str(da.time.values)[:10]
    ax[t].set_title(f'{var}: {time_str}', loc='left')
    plot(ax[t])
    cbar(ax[t], cf, label=f'{units}')

plt.savefig(f'{fig_dir}/{var}.2-d.{time_label}.png', dpi=300)
plt.close()
