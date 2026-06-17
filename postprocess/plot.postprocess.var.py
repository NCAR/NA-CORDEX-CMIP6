# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis
# Last updated: March 2026

# Purpose:
# --------
# Plot a single post-processed CORDEX-CMIP6 NetCDF file for QA purposes.
# Produces a combined figure with a row of 3 map panels (first, middle, last
# timestep) above a full timeseries panel with raw and smoothed overlays.
#
# Maps are plotted in native Lambert Conformal x/y space.
#
# Usage:
#   python plot.postprocess.var.py INFILE OUTDIR
#
#   INFILE   Path to a post-processed NetCDF file
#   OUTDIR   Directory for output figures (created if needed)
#
# Output filename matches the input filename with .png extension.
# Variable name, frequency, and time range are inferred from the filename.

import matplotlib
matplotlib.use('Agg')

import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import sys
import os

# -------------------------
# Arguments
# -------------------------
if len(sys.argv) != 3:
    sys.exit(f'Usage: {sys.argv[0]} INFILE OUTDIR')

infile  = sys.argv[1]
outdir  = sys.argv[2]

os.makedirs(outdir, exist_ok=True)

# -------------------------
# Parse filename
# -------------------------
# CORDEX filename format:
#   var_domain_drvsrc_drvexp_drvvar_inst_src_verreal_freq_timespan.nc
basename = os.path.basename(infile)
stem     = basename[:-3]  # strip .nc
parts    = stem.split('_')
var      = parts[0]
freq     = parts[8]
timespan = parts[9] if len(parts) > 9 else ''

outfile = os.path.join(outdir, stem + '.png')

# -------------------------
# Load data
# -------------------------
print(f'Loading {infile}')
ds = xr.open_dataset(infile)
da = ds[var]

nt = da.sizes['time']
print(f'  var={var}, freq={freq}, ntimes={nt}')

units = da.attrs.get('units', '')

# -------------------------
# Projection
# -------------------------
# Data is on a Lambert Conformal grid; plot in native x/y space by using
# pcrs as both the axes projection and the data transform.

crs_var = ds['crs']
sp = crs_var.attrs['standard_parallel']
pcrs = ccrs.LambertConformal(
    central_longitude=float(crs_var.attrs['longitude_of_central_meridian']),
    central_latitude=float(crs_var.attrs['latitude_of_projection_origin']),
    standard_parallels=(float(sp[0]), float(sp[1])),
)


# -------------------------
# Timeseries near Boulder
# -------------------------
target_lat = 40.0
target_lon = 255.75

dist = np.sqrt((da['lat'] - target_lat)**2 + (da['lon'] - target_lon)**2)
j, i = np.unravel_index(int(dist.argmin()), dist.shape)
ts    = da[:, j, i].values
times = da['time'].values

# Rolling window by frequency; pandas rolling handles edges cleanly
rolling_windows = {'1hr': 24 * 5, '6hr': 4 * 5, 'day': 30, 'mon': 12}
rw = rolling_windows.get(freq, 5)

# -------------------------
# Time slices: first, middle, last
# -------------------------
tidx   = [0, nt // 2, nt - 1]
slices = [da.isel(time=t) for t in tidx]

# Shared color scale across all three slices; masked values skipped automatically
subset = da.isel(time=tidx)
vmin, vmax = subset.min().item(), subset.max().item()

x = ds['x'].values
y = ds['y'].values

# -------------------------
# Figure layout
# -------------------------
fig = plt.figure(figsize=(14, 8))
gs  = gridspec.GridSpec(2, 3, figure=fig,
                        height_ratios=[3, 1],
                        top=0.93, bottom=0.08,
                        left=0.05, right=0.97,
                        hspace=0.40, wspace=0.12)

map_axes = [fig.add_subplot(gs[0, k], projection=pcrs) for k in range(3)]
ts_ax    = fig.add_subplot(gs[1, :])

# -------------------------
# Map panels
# -------------------------
def format_map(ax):
    ax.add_feature(cfeature.COASTLINE.with_scale('10m'), linewidth=0.2)
    ax.add_feature(cfeature.STATES.with_scale('10m'),  linewidth=0.2, alpha=0.5)
    ax.add_feature(cfeature.BORDERS.with_scale('10m'), linewidth=0.2, alpha=0.5)

for k, (ax, t, data) in enumerate(zip(map_axes, tidx, slices)):
    cf = ax.pcolormesh(x, y, data,
                       vmin=vmin, vmax=vmax,
                       cmap='nipy_spectral',
                       shading='nearest',
                       transform=pcrs)
    format_map(ax)
    time_str = str(da.time.values[t])[:16]
    ax.set_title(time_str, fontsize=8, loc='left')

    # Colorbar under each map panel
    pos = ax.get_position()
    cbar_ax = fig.add_axes([pos.x0 + 0.01, pos.y0 - 0.025,
                             pos.width - 0.02, 0.012])
    plt.colorbar(cf, cax=cbar_ax, orientation='horizontal',
                 label=units if k == 1 else '')

# -------------------------
# Timeseries panel
# -------------------------
ts_ax.plot(times, ts, color='cadetblue', lw=0.5, alpha=0.8, label='raw')
if rw > 1 and nt > rw:
    smoothed = pd.Series(ts).rolling(rw, center=True, min_periods=1).mean().values
    ts_ax.plot(times, smoothed,
               color='darkslategray', lw=1.5, label=f'{rw}-step mean')
    ts_ax.legend(fontsize=8, loc='upper right')

ts_ax.set_ylabel(units)
ts_ax.set_title(f'{var} near Boulder ({target_lat}°N, {target_lon}°E)',
                fontsize=9, loc='left')

# -------------------------
# Figure title and save
# -------------------------
fig.suptitle(f'{var}  |  {freq}  |  {timespan}', fontsize=10)
plt.savefig(outfile, dpi=150)
plt.close()
print(f'Saved: {outfile}')
