#!/bin/bash -l
# Jacob Stuivenvolt-Allen: jsallen@ucar.edu

# Purpose:
# --------
# Generated for the NA-CORDEX post-processing workflow, this script 
# is used by the python wrapper, postprocess.core.variables.py, and
# adds in the necessary/correct metadata and variable attributes for
# CF compliance and WCRP-CORDEX archiving specifications. 

# load modules 
# ------------
module load nco
module load cdo

wrfout_path=$1 # wrfoutput path
var=$2         # Variable name
fname=$3       # wrfout filename
pcc=$4         # compression level
freq=$5        # Write frequency
units=$6       # units
lev=$7         # single, pressure, soil
refh=$8        # reference height
cell=$9        # cell_methods
ln=${10}       # longname
stdn=${11}     # standard name
year=${12}     #  year...

#readonly wrfout_path="/glade/u/home/jsallen/scratch/NA-CORDEX-CMIP6/ERA5_HIST_E01/wrf_d01/1977_chunk/" # where is your data?
readonly coord_ref_file="${wrfout_path}wrfout_d01_${year}-12-31_00:00:00" # Example WRF file for coordinates and time dimension!!

echo "-------------------------------"
echo "Variable: " $var
echo "Output filename: " $fname
echo "Compression level: " $pcc 
echo "Timestep: " $freq
echo "Units: " $units
echo $lev
echo "Reference Height: " $refh
echo "Cell Methods: " $cell
echo "Longname: " $ln
echo "Standard Name: " $stdn
echo "Year: " $year

# Projection info for your WRF grid
# ---------------------------------
readonly cen_lon=$(echo "scale=2; 263.0" | bc)
readonly cen_lat=$(echo "scale=2; 45.0" | bc)

# Global attributes for files
# ---------------------------
readonly activity_id="DD"
readonly contact="jsallen@ucar.edu ; mcginnis@ucar.edu"
readonly creation_date=$(date +"%Y-%m-%d %H:%M:%S")
readonly domain="North America"
readonly domain_id="NAM-12"
readonly driving_experiment="reanalysis simulation of the recent past"
readonly driving_experiment_id="evaluation"
readonly driving_institution_id="ECMWF"
readonly driving_source_id="ERA5"
readonly driving_variant_label="r1i1p1f1"
# frequency set by each var
readonly grid="Lambert conic conformal with 12 km grid spacing"
readonly institution="National Center for Atmospheric Research: Research Applications Laboratory"
readonly institute_id="NCAR"
readonly license="https://cordex.org/data-access/cordex-cmip6-data/cordex-cmip6-terms-of-use"
readonly mip_era="CMIP6"
readonly product="model-output"
readonly project_id="CORDEX-CMIP6"
readonly source='Weather Research and Forecasting Model Version 4.6.1'
readonly source_id='WRF461S-SN'
readonly source_type='ARCM'
readonly version_realization='v1-r1'
readonly references='https://github.com/NCAR/NA-CORDEX-CMIP6 (code and documentation)'
readonly tracking_id=$(uuidgen)

# ---------------------------------------
# END OF USER DEFINED VARIABLES
# ---------------------------------------
# Either move forward with confidence to edit
# program below, or email jsallen@ucar.edu with
# any bugs/issues. 

# time units
time_old_units=$(ncdump -h "$coord_ref_file" | grep "XTIME:units" | cut -d '"' -f2)
time_start_year="${time_old_units:14:19}"


# -----------------------------
# Check for wrf coordinate file 
# -----------------------------
readonly coord_xy_file="./wrf.xy.coords.nc"
readonly coord_xy_stag_file="./wrf.xy.stagger.coords.nc"

if [ ! -f $coord_xy_file ]; then
  echo "WRF coordinate file not found ---------- creating"
  ncks -h -3 --chunk_cache 4000000000 -C -v XLONG,XLAT,XTIME --no_tmp_fl $coord_ref_file $coord_xy_file

  # Delete all uneeded attributes : wrf-holdovers
  ncatted -h -a ,XLONG,d,, $coord_xy_file
  ncatted -h -a ,XLAT,d,, $coord_xy_file
  ncatted -h -a ,XTIME,d,, $coord_xy_file
  ncatted -h -a '^[A-Z0-9_-]+$',global,d,, $coord_xy_file
  ncatted -h -a stagger,,d,, $coord_xy_file
  ncatted -h -a coordinates,,d,, $coord_xy_file

  ncrename -h -d Time,time $coord_xy_file
  ncrename -h -d south_north,y -d west_east,x $coord_xy_file
  ncrename -h -v XLAT,lat -v XLONG,lon $coord_xy_file

  # Add the projection information 
  ncap2 -h -A -s "crs=-9999" $coord_xy_file
  ncatted -h -a grid_mapping_name,crs,o,c,lambert_conformal_conic $coord_xy_file
  ncatted -h -a standard_parallel,crs,o,f,45.0 "$coord_xy_file"
  ncatted -h -a latitude_of_projection_origin,crs,o,f,45.0 $coord_xy_file
  ncatted -h -a longitude_of_central_meridian,crs,o,f,263.0 $coord_xy_file

  # Creating coordinate variables for x and y
  ncap2 -h -A -s 'y=array(0.,12.,$y); x=array(0.,12.,$x)' $coord_xy_file
  ncatted -h -a units,y,o,c,km -a units,x,o,c,km $coord_xy_file
  ncatted -h -a long_name,y,o,c,"y coordinate in Cartesian system" $coord_xy_file
  ncatted -h -a long_name,x,o,c,"x-coordinate in Cartesian system" $coord_xy_file
  ncatted -h -a standard_name,y,o,c,projection_y_coordinate $coord_xy_file
  ncatted -h -a standard_name,x,o,c,projection_x_coordinate $coord_xy_file
  ncatted -h -a axis,x,o,c,X -a axis,y,o,c,Y $coord_xy_file

  # Add all appropriate names to the coordinate variables
  ncatted -h -a ,lat,d,, $coord_xy_file
  ncatted -h -a ,lon,d,, $coord_xy_file

  ncatted -h -a units,lat,o,c,degrees_north $coord_xy_file
  ncatted -h -a units,lon,o,c,degrees_east $coord_xy_file
  ncatted -h -a long_name,lat,o,c,latitude $coord_xy_file
  ncatted -h -a long_name,lon,o,c,longitude $coord_xy_file
  ncatted -h -a standard_name,lat,o,c,latitude $coord_xy_file 
  ncatted -h -a standard_name,lon,o,c,longitude $coord_xy_file

  ncap2 -O -s 'lat=double(lat)' $coord_xy_file $coord_xy_file
  ncap2 -O -s 'lon=double(lon)' $coord_xy_file $coord_xy_file

  # Conventions attribute
  ncatted -h -a Conventions,global,o,c,"CF-1.8" $coord_xy_file
  ncatted -h -a institution,global,o,c,"National Center for Atmospheric Research: Research Applications Laboratory" $coord_xy_file
  ncatted -h -a source,global,o,c,"Weather Research and Forecasting Model Version 4.6.1" $coord_xy_file

  # Get rid of time in spatial coordiantes
  ncwa -h -O -a time -C -v lat,lon,y,x,crs $coord_xy_file $coord_xy_file

  # Add one more file for winds or staggered coordinate vars...
  ncks -h -O -d x,1, -d y,1, $coord_xy_file $coord_xy_stag_file

fi

# ------------------------------------------------------
# Function to clean single level variables output by WRF
# ------------------------------------------------------
clean_single_level () {
  # Rename 
  ncrename -O -h -d west_east,x -d south_north,y $2 $3

  # Time  and calendar attributes : must be set before reference time
  ncatted -h -a long_name,time,o,c,time $3
  ncatted -h -a standard_name,time,o,c,time $3
  ncatted -h -a axis,time,o,c,T $3
  ncatted -h -a calendar,time,o,c,proleptic_gregorian $3

  # Set reftime and corrrect time units
  cdo -setreftime,1950-01-01,00:00:00,1day $3 ${1}.cdo.tmp.${7}.nc
  mv "${1}.cdo.tmp.${7}.nc" $3 # This is probably slow
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

  # Lossy compression
  if [[ "${4}" != "None" ]]; then
    echo "Compressing ${1} --------------------- "
    ncks -h -O -7 -L1 --ppc $1=$4 --chunk_cache 4000000000 --chunk_map rd1 $3 $3 
    ncap2 -O -s 'quantization_info=int(quantization_info)' $3 $3
  fi

  rm $2
}


# -------------------------------------------------------
# Function to clean time invariant variables output by WRF
# -------------------------------------------------------
clean_time_invariant () {
  #echo 'Variable='$1 ', output='$3 ', decimal threshold for compression='$4

  ncrename -O -h -d west_east,x -d south_north,y $2 $3
  coords="lat lon"

  # append in the lat,lon from coordinate file
  ncatted -h -a cell_methods,lat,d,, $coord_xy_file
  ncatted -h -a cell_methods,lon,d,, $coord_xy_file
  ncks -h -A -v crs,lat,lon,y,x $coord_xy_file $3

  # Lossy compression
  if [[ "${4}" != "None" ]]; then
    echo "Compressing ${1} --------------------- "
    ncks -h -O -7 -L1 --ppc $1=$4 --chunk_cache 4000000000 --chunk_map rd1 $3 $3
  fi

  rm $2

}

# Add global attributes
# ---------------------
function add_global_attrs {

  # Clear existing global attributes
  ncatted -h -a ,$1,d,, -a ,global,d,, $2

  # Global attributes for files
  ncatted -h -a Conventions,global,o,c,"CF-1.8" $2
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
  ncatted -h -a grid,global,o,c,"${grid}" $2
  ncatted -h -a institution,global,o,c,"${institution}" $2
  ncatted -h -a institute_id,global,o,c,"${institute_id}" $2
  ncatted -h -a mip_era,global,o,c,"${mip_era}" $2
  ncatted -h -a product,global,o,c,"${product}" $2
  ncatted -h -a project_id,global,o,c,"${project_id}" $2
  ncatted -h -a source,global,o,c,"${source}" $2
  ncatted -h -a source_id,global,o,c,"${source_id}" $2
  ncatted -h -a source_type,global,o,c,"${source_type}" $2
  ncatted -h -a version_realization,global,o,c,"${version_realization}" $2
  ncatted -h -a references,global,o,c,"${references}" $2
  ncatted -h -a tracking_id,global,o,c,"${tracking_id}" $2
  ncatted -h -a license,global,o,c,"${license}" $2

}

# Add variable attributes
# -----------------------
function add_var_attrs {

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

  ncatted -O -h -a standard_parallel,crs,o,f,45.0 $2 $2
  ncatted -O -h -a latitude_of_projection_origin,crs,o,f,45.0 $2 $2
  ncatted -O -h -a longitude_of_central_meridian,crs,o,f,263.0 $2 $2

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

  mkdir -p ${variable}
  out_f="${variable}/${wrfout_file}"

  if [ "$levels" == "single" ]; then
    clean_single_level "${variable}" "${wrfout_file}" "$out_f" "${pcc}" "${ref_height}" "${short_name}" "${year}" "${mon}"

  elif [ "$levels" == "fixed" ]; then
    clean_time_invariant "${variable}" "${wrfout_file}" "$out_f" "${pcc}" "${short_name}" "${year}" "${mon}"
  fi

  add_global_attrs "${variable}" "${out_f}" "${freq}"
  add_var_attrs "${variable}" "${out_f}" "${units}" "${long_name}" "${stan_name}" "${cell_methods}" "${short_name}"

}
# -----------------------------------------
clean_wrf_data "${1}" "${2}" "${3}" "${4}" "${5}" "${6}" "${7}" "${8}" "${9}" "${10}" "${11}" "${12}"
 
exit
