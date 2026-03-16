#!/bin/bash -x
# Authors: Jacob Stuivenvolt-Allen, Seth McGinnis

# Purpose:
# --------
#
# This script adds the coordinates and metadata (global and variable
# attributes) required by the CF and CORDEX archiving spcs to
# bare-bones netCDF files created in the extraction step of the
# workflow
#
# The coordinate reference files (wrf.xy.coords.nc, wrf.xy.stagger.coords.nc)
# must exist in $outdir; they are created by setup.sh.

# load modules
# ------------
module load nco
module load cdo

wrfout_path=$1 # path to directory containing wrfout files (for XTIME units)
var=$2         # variable name
fname=$3       # wrfout filename
freq=$4        # write frequency
units=$5       # units
lev=$6         # single, pressure, soil, work in progress...
refh=$7        # reference height
cell=$8        # cell_methods
ln=$9          # longname
stdn=${10}     # standard name
year=${11}     # year
outdir=${12}   # output directory (variable subdirs created here)

readonly time_ref_file="${wrfout_path}/wrfout_d01_${year}-12-31_00:00:00"

echo "-------------------------------"
echo "Variable: " $var
echo "Output filename: " $fname
echo "Timestep: " $freq
echo "Units: " $units
echo $lev
echo "Reference Height: " $refh
echo "Cell Methods: " $cell
echo "Longname: " $ln
echo "Standard Name: " $stdn
echo "Year: " $year
echo "Output directory: " $outdir

# Projection info for your WRF grid
# ---------------------------------
readonly cen_lon=$(echo "scale=2; 263.0" | bc)
readonly cen_lat=$(echo "scale=2; 45.0" | bc)

# Global attributes for files
# ---------------------------
readonly activity_id="DD"
readonly contact="na-cordex-admin@ucar.edu"
readonly creation_date=$(date +"%Y-%m-%dT%H:%M:%S")
readonly domain="North America"
readonly domain_id="NAM-12"
readonly driving_experiment="reanalysis simulation of the recent past"
readonly driving_experiment_id="evaluation"
readonly driving_institution_id="ECMWF"
readonly driving_source_id="ERA5"
readonly driving_variant_label="r1i1p1f1"
# frequency set by each var
readonly grid="Lambert conic conformal with 12 km grid spacing"
readonly institution="NSF National Center for Atmospheric Research, Boulder (Colorado), USA"
readonly institution_id="NCAR"
readonly license="https://cordex.org/data-access/cordex-cmip6-data/cordex-cmip6-terms-of-use"
readonly mip_era="CMIP6"
readonly product="model-output"
readonly project_id="CORDEX-CMIP6"
readonly source='Weather Research and Forecasting model version 4.6.1, CORDEX WRF Community configuration S'
readonly source_id='WRF461S-SN'
readonly source_type='ARCM'
readonly version_realization='v1-r1'
readonly references='https://github.com/NCAR/NA-CORDEX-CMIP6 (code and documentation)'
readonly tracking_id="hdl:21.14103/$(uuidgen)"

# ---------------------------------------
# END OF USER DEFINED VARIABLES
# ---------------------------------------

# time units
time_old_units=$(ncdump -h "$time_ref_file" | grep "XTIME:units" | cut -d '"' -f2)
time_start_year="${time_old_units:14:19}"

# Coordinate files are created once by setup.sh and read here.
readonly coord_xy_file="${outdir}/wrf.xy.coords.nc"
readonly coord_xy_stag_file="${outdir}/wrf.xy.stagger.coords.nc"

if [ ! -f "$coord_xy_file" ]; then
    echo "Error: coordinate file not found: $coord_xy_file" >&2
    echo "Run setup.sh before cmorize.sh." >&2
    exit 1
fi

# ------------------------------------------------------
# Function to clean single level variables output by WRF
# ------------------------------------------------------
clean_single_level () {
  # Rename
  ncrename -O -h -d west_east,x -d south_north,y $2 $3

  # Time and calendar attributes: must be set before reference time
  ncatted -h -a long_name,time,o,c,time $3
  ncatted -h -a standard_name,time,o,c,time $3
  ncatted -h -a axis,time,o,c,T $3
  ncatted -h -a calendar,time,o,c,proleptic_gregorian $3

  # Set reftime and correct time units
  cdo -setreftime,1950-01-01,00:00:00,1day $3 ${outdir}/${1}.cdo.tmp.${7}.nc
  mv "${outdir}/${1}.cdo.tmp.${7}.nc" $3
  ncap2 -O -s 'time=double(time)' $3 $3
  ncatted -h -a units,time,o,c,"days since 1950-01-01 00:00:00" $3

  # If ref_height is provided (for example, with 2-m temperature)
  if [[ "${5}" != "None" ]]; then
    ncap2 -h -A -s "height=double(${5})" $3
    ncatted -h -a units,height,o,c,m $3
    ncatted -h -a long_name,height,o,c,height $3
    ncatted -h -a standard_name,height,o,c,height $3
    ncatted -h -a positive,height,o,c,up $3
    ncatted -h -a axis,height,o,c,Z $3
    coords="lat lon height"
  else
    coords="lat lon"
  fi

  # Append in the lat,lon from coordinate file
  # And trim sponge layer
  if [ "$1" = "uas" -o "$1" = "vas" -o "$1" = "sfcWind" ]; then
    ncatted -h -a cell_methods,lat,d,, $coord_xy_stag_file
    ncatted -h -a cell_methods,lon,d,, $coord_xy_stag_file
    ncks -h -A -v crs,lat,lon,y,x $coord_xy_stag_file $3
    ncks -h -O -d x,9,-11 -d y,9,-11 $3 $3
  else
    ncatted -h -a cell_methods,lat,d,, $coord_xy_file
    ncatted -h -a cell_methods,lon,d,, $coord_xy_file
    ncks -h -A -v crs,lat,lon,y,x $coord_xy_file $3
    ncks -h -O -d x,10,-11 -d y,10,-11 $3 $3
  fi

  # Correct lon easting
  ncap2 -h -O -s 'where(lon < 0) lon = lon + 360' $3 $3

  rm $2
}


# -------------------------------------------------------
# Function to clean time invariant variables output by WRF
# -------------------------------------------------------
clean_time_invariant () {
  ncrename -O -h -d west_east,x -d south_north,y $2 $3
  coords="lat lon"

  # Append in the lat,lon from coordinate file
  ncatted -h -a cell_methods,lat,d,, $coord_xy_file
  ncatted -h -a cell_methods,lon,d,, $coord_xy_file
  ncks -h -A -v crs,lat,lon,y,x $coord_xy_file $3

  rm $2

}

# Add global attributes
# ---------------------
function add_global_attrs {

  # Clear existing global attributes
  ncatted -h -a ,$1,d,, -a ,global,d,, $2

  # Global attributes for files
  ncatted -h -a Conventions,global,o,c,"CF-1.11" $2
  ncatted -h -a activity_id,global,o,c,"${activity_id}" $2
  ncatted -h -a contact,global,o,c,"${contact}" $2
  ncatted -h -a creation_date,global,o,c,"${creation_date}" $2
  ncatted -h -a domain,global,o,c,"${domain}" $2
  ncatted -h -a domain_id,global,o,c,"${domain_id}" $2
  ncatted -h -a driving_experiment,global,o,c,"${driving_experiment}" $2
  ncatted -h -a driving_experiment_id,global,o,c,"${driving_experiment_id}" $2
  ncatted -h -a driving_institution_id,global,o,c,"${driving_institution_id}" $2
  ncatted -h -a driving_source_id,global,o,c,"${driving_source_id}" $2
  ncatted -h -a driving_variant_label,global,o,c,"${driving_variant_label}" $2
  ncatted -h -a frequency,global,o,c,"$3" $2
  ncatted -h -a grid,global,o,c,"${grid}" $2
  ncatted -h -a institution,global,o,c,"${institution}" $2
  ncatted -h -a institution_id,global,o,c,"${institution_id}" $2
  ncatted -h -a license,global,o,c,"${license}" $2
  ncatted -h -a mip_era,global,o,c,"${mip_era}" $2
  ncatted -h -a product,global,o,c,"${product}" $2
  ncatted -h -a project_id,global,o,c,"${project_id}" $2
  ncatted -h -a references,global,o,c,"${references}" $2
  ncatted -h -a source,global,o,c,"${source}" $2
  ncatted -h -a source_id,global,o,c,"${source_id}" $2
  ncatted -h -a source_type,global,o,c,"${source_type}" $2
  ncatted -h -a tracking_id,global,o,c,"${tracking_id}" $2
  ncatted -h -a variable_id,global,o,c,"$1" $2
  ncatted -h -a version_realization,global,o,c,"${version_realization}" $2

}

# Add variable attributes
# -----------------------
function add_var_attrs {
  # $1=var, $2=file, $3=units, $4=standard_name, $5=long_name, $6=cell_methods

  # variable attributes
  ncatted -h -a units,"${1}",o,c,"${3}" $2
  ncatted -h -a standard_name,"${1}",o,c,"${4}" $2
  ncatted -h -a long_name,"${1}",o,c,"${5}" $2
  ncatted -h -a coordinates,"${1}",o,c,"${coords}" $2
  ncatted -h -a grid_mapping,"${1}",o,c,"crs" $2

  # If cell_methods is provided:
  if [[ "${6}" != "None" ]]; then
    ncatted -h -a cell_methods,$1,o,c,"${6}" $2
  fi

  # Check cell methods and adjust time coordinate/bnds
  if [[ "${6}" = "area: time: mean" ]]; then

    # Use ncap2 with in-place modification - this should preserve attributes
    ncap2 -h -O -s 'time+=1.0/48.0' $2 $2

    # Add time bounds
    ncap2 -h -A -s 'defdim("nv",2)' $2 $2
    ncap2 -h -A -s 'time_bnds[$time,$nv]=0.0; time_bnds(:,0)=time-1.0/48.0; time_bnds(:,1)=time+1.0/48.0' $2 $2
    ncatted -h -O -a bounds,time,o,c,"time_bnds" $2 $2

  fi

  # Daily aggregated max or min
  if [[ "${6}" = "area: mean time: maximum" || "${6}" = "area: mean time: minimum" ]]; then

    # Use ncap2 with in-place modification - this should preserve attributes
    ncap2 -h -O -s 'time+=0.5' $2 $2

    # Add time bounds
    ncap2 -h -A -s 'defdim("nv",2)' $2 $2
    ncap2 -h -A -s 'time_bnds[$time,$nv]=0.0; time_bnds(:,0)=time-0.5; time_bnds(:,1)=time+0.5' $2 $2
    ncatted -h -O -a bounds,time,o,c,"time_bnds" $2 $2
  fi

}

# Function which leverages yaml data
# -----------------------------------------
function clean_wrf_data {

  wrfout_path=$1
  variable=$2
  wrfout_file=$3
  pcc=$4
  freq=$5
  units=$6
  levels=$7
  ref_height=$8
  cell_methods=$9
  long_name="${10}"
  stan_name="${11}"
  year="${12}"
  outdir="${13}"

  mkdir -p "${outdir}/${variable}"
  out_f="${outdir}/${variable}/${wrfout_file}"

  if [ "$levels" == "single" ]; then
    clean_single_level "${variable}" "${wrfout_file}" "$out_f" "${pcc}" "${ref_height}" "${short_name}" "${year}" "${mon}"

  elif [ "$levels" == "fixed" ]; then
    clean_time_invariant "${variable}" "${wrfout_file}" "$out_f" "${pcc}" "${short_name}" "${year}" "${mon}"
  fi

  add_global_attrs "${variable}" "${out_f}" "${freq}"
  add_var_attrs "${variable}" "${out_f}" "${units}" "${long_name}" "${stan_name}" "${cell_methods}" "${short_name}"

}
# -----------------------------------------
clean_wrf_data "${1}" "${2}" "${3}" "${4}" "${5}" "${6}" "${7}" "${8}" "${9}" "${10}" "${11}" "${12}" "${13}"

#echo "Done"
exit
