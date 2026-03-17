#!/bin/bash

# setup.sh - One-time setup for NA-CORDEX-CMIP6 postprocessing workflow.
#
# Creates WRF coordinate reference file and downloads the data request
# CSV needed by later workflow steps.  Run once before running
# extract.sh, format.sh, etc.
#
# All outputs are written to OUTDIR.  Use the same OUTDIR as extract.sh /
# postprocess.core.variables.py so that aggregate.sh and cmorize.sh can find
# the coordinate files and CSV alongside the extracted data.
#
# Usage: setup.sh WRFDIR OUTDIR
#
#   WRFDIR   Any WRF chunk directory containing wrfout_d01_* files
#   OUTDIR   Output directory for coordinate files and downloaded tables

set -euo pipefail

# Default location and URL for the CORDEX data request CSV
DR_CSV_DEFAULT="/glade/work/${USER}/cordex6/dreq_default.csv"
DR_CSV_URL="https://raw.githubusercontent.com/WCRP-CORDEX/data-request-table/main/data-request/dreq_default.csv"

# CORDEX CMOR table base URL and frequencies to cache
CMOR_BASE="https://raw.githubusercontent.com/WCRP-CORDEX/cordex-cmip6-cmor-tables/main/Tables/CORDEX-CMIP6"
CMOR_FREQS="fx 1hr day"

usage() {
    cat >&2 <<EOF
Usage: $(basename "$0") WRFDIR OUTDIR

  WRFDIR   Any WRF chunk directory containing wrfout_d01_* files
  OUTDIR   Output directory for coordinate files and downloaded tables
           (use the same directory as OUTDIR in extract.sh)
EOF
    exit 1
}

[[ $# -ne 2 ]] && usage


WRFDIR="$(realpath "$1")"
mkdir -p $2
OUTDIR="$(realpath "$2")"

[[ ! -d "$WRFDIR" ]] && { echo "Error: WRFDIR not found: $WRFDIR" >&2; exit 1; }

# Get coordinates from first found wrfout_d01 file
for coord_ref_file in "$WRFDIR"/wrfout_d01_*; do break; done

[[ -z "$coord_ref_file" ]] && {
    echo "Error: No wrfout_d01_* files found in $WRFDIR" >&2
    exit 1
}

echo "Using coordinate reference file: $coord_ref_file"



# ---------------------------------------------------------------
# Data request CSV: copy from default location or download
# ---------------------------------------------------------------
DR_CSV="$OUTDIR/dreq_default.csv"
if [[ -f "$DR_CSV" ]]; then
    echo "Data request CSV already exists in OUTDIR (skipping)"
elif [[ -f "$DR_CSV_DEFAULT" ]]; then
    echo "Copying data request CSV from $DR_CSV_DEFAULT"
    cp "$DR_CSV_DEFAULT" "$DR_CSV"
else
    echo "Downloading data request CSV..."
    curl -L -o "$DR_CSV" "$DR_CSV_URL"
    echo "  -> $DR_CSV"
fi


# ---------------------------------------------------------------
# WRF coordinate reference file
# ---------------------------------------------------------------

readonly coord_xy_file="${OUTDIR}/wrf.xy.coords.nc"

if [[ -f "$coord_xy_file" ]]; then
    echo "Coordinate file already exists (skipping)"
else
    echo "Creating coordinate reference file from $coord_ref_file ..."

    ncwa -h -3 -a Time -C -v XLAT,XLONG "$coord_ref_file" "$coord_xy_file"

    # Delete (lots of) unneeded attributes from WRF
    ncatted -h -a ,XLONG,d,, "$coord_xy_file"
    ncatted -h -a ,XLAT,d,, "$coord_xy_file"
    ncatted -h -a '^[A-Z0-9_-]+$',global,d,, "$coord_xy_file"
    ncatted -h -a stagger,,d,, "$coord_xy_file"
    ncatted -h -a coordinates,,d,, "$coord_xy_file"

    ncrename -h -d south_north,y -d west_east,x "$coord_xy_file"
    ncrename -h -v XLAT,lat -v XLONG,lon "$coord_xy_file"

    # Ensure longitudes are monotonic
    # NOTE: this fix is specific to the NAM-12 domain; other domains may differ
    ncap2 -h -O -s 'where(lon < 0) lon = lon + 360' "$coord_xy_file" "$coord_xy_file"

    # Add the projection information
    ncap2 -h -A -s "crs=-9999" "$coord_xy_file"
    ncatted -h -a long_name,crs,o,c,"coordinate reference system" "$coord_xy_file"
    ncatted -h -a grid_mapping_name,crs,o,c,lambert_conformal_conic "$coord_xy_file"
    ncatted -h -a standard_parallel,crs,o,f,"35.,60." "$coord_xy_file"
    ncatted -h -a longitude_of_central_meridian,crs,o,f,-97. "$coord_xy_file"
    ncatted -h -a latitude_of_projection_origin,crs,o,f,46. "$coord_xy_file"
    ncatted -h -a semi_major_axis,crs,o,f,6370000. "$coord_xy_file"
    ncatted -h -a semi_minor_axis,crs,o,f,6370000. "$coord_xy_file"
    ncatted -h -a false_easting,crs,o,f,0. "$coord_xy_file"
    ncatted -h -a false_northing,crs,o,f,0. "$coord_xy_file"
    ncatted -h -a units,crs,o,c,"m" "$coord_xy_file"

    ncap2 -h -A -s 'x=array(-(($x.size-1)/2)*12000.,12000.,$x); y=array(-(($y.size-1)/2)*12000.,12000.,$y)' "$coord_xy_file"
    ncatted -h -a units,y,o,c,m -a units,x,o,c,m "$coord_xy_file"
    ncatted -h -a long_name,y,o,c,"y coordinate in Cartesian system" "$coord_xy_file"
    ncatted -h -a long_name,x,o,c,"x-coordinate in Cartesian system" "$coord_xy_file"
    ncatted -h -a standard_name,y,o,c,projection_y_coordinate "$coord_xy_file"
    ncatted -h -a standard_name,x,o,c,projection_x_coordinate "$coord_xy_file"
    ncatted -h -a axis,x,o,c,X -a axis,y,o,c,Y "$coord_xy_file"

    # Add metadata for lat & lon
    ncatted -h -a units,lat,o,c,degrees_north "$coord_xy_file"
    ncatted -h -a units,lon,o,c,degrees_east "$coord_xy_file"
    ncatted -h -a long_name,lat,o,c,latitude "$coord_xy_file"
    ncatted -h -a long_name,lon,o,c,longitude "$coord_xy_file"
    ncatted -h -a standard_name,lat,o,c,latitude "$coord_xy_file"
    ncatted -h -a standard_name,lon,o,c,longitude "$coord_xy_file"

    ncap2 -O -s 'lon=double(lon)' "$coord_xy_file" "$coord_xy_file"
    ncap2 -O -s 'lat=double(lat)' "$coord_xy_file" "$coord_xy_file"

    # Conventions attribute
    ncatted -h -a Conventions,global,o,c,"CF-1.11" "$coord_xy_file"
    ncatted -h -a institution,global,o,c,"National Center for Atmospheric Research: Research Applications Laboratory" "$coord_xy_file"
    ncatted -h -a source,global,o,c,"Weather Research and Forecasting Model Version 4.6.1" "$coord_xy_file"

    echo "  -> $coord_xy_file"
fi

echo ""
echo "Setup complete. OUTDIR: $OUTDIR"
echo ""
echo "Next step:"
echo "  extract.sh WRFDIR $OUTDIR YEARS [CMDDIR]"
