# Author: Jacob Stuivenvolt-Allen
# Last updated: September 9, 2025

# Purpose:
# --------
# This script post-processes WRF output. The resulting
# output will be CMORized. All standardization was done
# by consulting the WCRP-CORDEX CMOR-Tables: 
# https://github.com/WCRP-CORDEX/cordex-cmip6-cmor-tables
        
# USAGE:
# ----- 

# Example execution for one month, January of 1980:
# ------------------------------------------------
# $ python clean.core.variables.py 1980 tas

# ------------------------------------------------
import xarray as xr
from xarray import ufuncs
from datetime import date
import numpy as np
import matplotlib.pyplot as plt
import glob
import sys
import os    

import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LongitudeFormatter, LatitudeFormatter
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import matplotlib.ticker as mticker

#import colormaps as cmaps

year = sys.argv[1]
var  = sys.argv[2]
os.system(f'mkdir -p {var}/figures')

# Account for filename strings with beginning and start time: 
filename = glob.glob(f'{var}/{var}_NAM-12_ERA5_evaluation_r1i1p1f1_NCAR_WRF461S-SN_v1-r1_hr_{year}*.nc')[0]
ds = xr.open_dataset(filename)[var]

# Plot time series - near boulder
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
ax.plot(xax, target_ds.rolling(time=24*5).mean(), color='darkslategray', lw=1.5)
ax.set_title(f'{var} : {year}', loc='left')

units = target_ds.attrs['units']
ax.set_ylabel(units)
plt.savefig(f'{var}/figures/{var}.timeseries.{year}.png', dpi=300)
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

# Time slices
t1 = f'{str(year)}-01-01T00:00:00'
t2 = f'{str(year)}-06-15T12:00:00'
t3 = f'{str(year)}-12-31T23:00:00'
times = [t1,t2,t3]

# ---------
fig, ax = plt.subplots(1,3, figsize=(13,4), subplot_kw={'projection':pcrs})
fig.subplots_adjust(bottom=0.10, top=0.95, left=0.05,
                    right=0.95, wspace=0.15)

for t, time in enumerate(times):
    da = ds.sel(time=time, method='nearest')
    da = da.where(da != 1e20)

    vmin = da.min().values
    vmax = da.max().values

    lat = ds.lat
    lon = ds.lon

    cf = ax[t].pcolormesh(lon, lat, da, 
                      vmin=vmin, vmax=vmax,
                      cmap='nipy_spectral',
                      transform=tcrs)

    ax[t].set_title(f'{var}: {times[t]}', loc='left')
    plot(ax[t])
    cbar(ax[t], cf, label=f'{units}')

plt.savefig(f'{var}/figures/{var}.2-d.{year}.png', dpi=300)
plt.close()

