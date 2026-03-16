#!/bin/bash -el
# Jacob Stuivenvolt-Allen: jsallen@ucar.edu

# Purpose:
# --------
# Part of the NA-CORDEX post-processing workflow.  Called by format.sh
# commandfiles to apply CF-compliant metadata and WCRP-CORDEX archiving
# attributes to extracted WRF variables.  Lossy compression is handled
# separately by compress.sh.
#
# Creates output by:
#   1. Copying the appropriate coordinate reference file (tiny)
#   2. Appending the time coordinate and ancillary variables from infile
#   3. Running CDO setreftime on the small file (before data is appended)
#   4. Appending the data variable with the sponge layer trimmed
#   5. Applying CF metadata attributes
#
# Coordinate reference files (wrf.xy.coords.nc, wrf.xy.stagger.coords.nc)
# must exist in INDIR; they are created by setup.sh.

# load modules
# ------------
module load nco
module load cdo

var=$1      # variable name (CMORized)
infile=$2   # input file (from extract step)
freq=$3     # frequency
units=$4    # units
lev=$5      # level type: single or fixed
refh=$6     # reference height (or None)
cell=$7     # cell_methods (or None)
ln=$8       # long_name
stdn=$9     # standard_name
outfile=${10}  # full path to output file
indir=${11}    # extract/data directory (contains coordinate files)

echo "-------------------------------"
echo "Variable:         $var"
echo "Input file:       $infile"
echo "Output file:      $outfile"
echo "Frequency:        $freq"
echo "Units:            $units"
echo "Level type:       $lev"
echo "Reference height: $refh"
echo "Cell methods:     $cell"
echo "Long name:        $ln"
echo "Standard name:    $stdn"

# Global attributes
# -----------------
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

# Coordinate files (created by setup.sh)
readonly coord_xy_file="${indir}/wrf.xy.coords.nc"
readonly coord_xy_stag_file="${indir}/wrf.xy.stagger.coords.nc"

if [ ! -f "$coord_xy_file" ]; then
    echo "Error: coordinate file not found: $coord_xy_file" >&2
    echo "Run setup.sh before cmorize.sh." >&2
    exit 1
fi

mkdir -p "$(dirname "$outfile")"

# Select coordinate file and sponge-layer trim bounds based on variable
if [ "$var" = "uas" -o "$var" = "vas" -o "$var" = "sfcWind" ]; then
    coord_file="$coord_xy_stag_file"
    x_trim="9,-11"
    y_trim="9,-11"
else
    coord_file="$coord_xy_file"
    x_trim="10,-11"
    y_trim="10,-11"
fi

# Remove cell_methods from lat/lon before appending (NCO carries them over)
#ncatted -h -a cell_methods,lat,d,, "$coord_file"
#ncatted -h -a cell_methods,lon,d,, "$coord_file"

# Step 1: Start output file from coordinate reference (tiny; coordinates first
# in file speeds up downstream operations)
cp "$coord_file" "$outfile"

if [ "$lev" = "single" ]; then

    # Step 2: Append time coordinate and ancillary variables from infile
    # (rename WRF spatial dims to CF names first)

    ncks -h -A -v time "$infile" "$outfile"

    # Time and calendar attributes: must be set before setreftime
    ncatted -h -a long_name,time,o,c,time "$outfile"
    ncatted -h -a standard_name,time,o,c,time "$outfile"
    ncatted -h -a axis,time,o,c,T "$outfile"
    ##ncatted -h -a calendar,time,o,c,proleptic_gregorian "$outfile"
    # currently inherited from wrfout, needs to be changed to whatever gcm uses

    
    # Step 3: Set reference time on the small file (coords + time only)
    cdo -setreftime,1950-01-01,00:00:00,1day "$outfile" "${outfile}.cdotmp.nc"
    mv "${outfile}.cdotmp.nc" "$outfile"
    ncap2 -O -s 'time=double(time)' "$outfile" "$outfile"

    ##ncatted -h -a units,time,o,c,"days since 1950-01-01 00:00:00" "$outfile"
    # is this needed?  What does CDO write?
    
    # If ref_height is provided (e.g., 2-m temperature, 10-m wind)
    if [[ "$refh" != "None" ]]; then
        ncap2 -h -A -s "height=double(${refh})" "$outfile"
        ncatted -h -a units,height,o,c,m "$outfile"
        ncatted -h -a long_name,height,o,c,height "$outfile"
        ncatted -h -a standard_name,height,o,c,height "$outfile"
        ncatted -h -a positive,height,o,c,up "$outfile"
        ncatted -h -a axis,height,o,c,Z "$outfile"
        coords="lat lon height"
    else
        coords="lat lon"
    fi

    # Step 4: Append data variable with sponge layer trimmed
    ncks -h -A -d x,$x_trim -d y,$y_trim -v "$var" "$infile" "$outfile"

    # Correct lon easting
    ncap2 -h -O -s 'where(lon < 0) lon = lon + 360' "$outfile" "$outfile"

elif [ "$lev" = "fixed" ]; then

    # fx variables have no time dimension
    ncrename -O -h -d west_east,x -d south_north,y "$infile" "$infile"
    coords="lat lon"

    # Append data variable with sponge layer trimmed
    ncks -h -A -d x,$x_trim -d y,$y_trim -v "$var" "$infile" "$outfile"

fi

# Global attributes
# -----------------
# Clear existing global and variable-level attributes first
ncatted -h -a ,"$var",d,, -a ,global,d,, "$outfile"

ncatted -h -a Conventions,global,o,c,"CF-1.11" "$outfile"
ncatted -h -a activity_id,global,o,c,"${activity_id}" "$outfile"
ncatted -h -a contact,global,o,c,"${contact}" "$outfile"
ncatted -h -a creation_date,global,o,c,"${creation_date}" "$outfile"
ncatted -h -a domain,global,o,c,"${domain}" "$outfile"
ncatted -h -a domain_id,global,o,c,"${domain_id}" "$outfile"
ncatted -h -a driving_experiment,global,o,c,"${driving_experiment}" "$outfile"
ncatted -h -a driving_experiment_id,global,o,c,"${driving_experiment_id}" "$outfile"
ncatted -h -a driving_institution_id,global,o,c,"${driving_institution_id}" "$outfile"
ncatted -h -a driving_source_id,global,o,c,"${driving_source_id}" "$outfile"
ncatted -h -a driving_variant_label,global,o,c,"${driving_variant_label}" "$outfile"
ncatted -h -a frequency,global,o,c,"${freq}" "$outfile"
ncatted -h -a grid,global,o,c,"${grid}" "$outfile"
ncatted -h -a institution,global,o,c,"${institution}" "$outfile"
ncatted -h -a institution_id,global,o,c,"${institution_id}" "$outfile"
ncatted -h -a license,global,o,c,"${license}" "$outfile"
ncatted -h -a mip_era,global,o,c,"${mip_era}" "$outfile"
ncatted -h -a product,global,o,c,"${product}" "$outfile"
ncatted -h -a project_id,global,o,c,"${project_id}" "$outfile"
ncatted -h -a references,global,o,c,"${references}" "$outfile"
ncatted -h -a source,global,o,c,"${source}" "$outfile"
ncatted -h -a source_id,global,o,c,"${source_id}" "$outfile"
ncatted -h -a source_type,global,o,c,"${source_type}" "$outfile"
ncatted -h -a tracking_id,global,o,c,"${tracking_id}" "$outfile"
ncatted -h -a variable_id,global,o,c,"${var}" "$outfile"
ncatted -h -a version_realization,global,o,c,"${version_realization}" "$outfile"

# Variable attributes
# -------------------
ncatted -h -a units,"${var}",o,c,"${units}" "$outfile"
ncatted -h -a standard_name,"${var}",o,c,"${stdn}" "$outfile"
ncatted -h -a long_name,"${var}",o,c,"${ln}" "$outfile"
ncatted -h -a coordinates,"${var}",o,c,"${coords}" "$outfile"
ncatted -h -a grid_mapping,"${var}",o,c,"crs" "$outfile"

if [[ "$cell" != "None" ]]; then
    ncatted -h -a cell_methods,"${var}",o,c,"${cell}" "$outfile"
fi

# Time coordinate adjustments based on cell_methods
if [ "$lev" = "single" ]; then
    if [[ "$cell" = "area: time: mean" ]]; then
        # Shift time to interval midpoint, add bounds
        ncap2 -h -O -s 'time+=1.0/48.0' "$outfile" "$outfile"
        ncap2 -h -A -s 'defdim("nv",2)' "$outfile" "$outfile"
        ncap2 -h -A -s 'time_bnds[$time,$nv]=0.0; time_bnds(:,0)=time-1.0/48.0; time_bnds(:,1)=time+1.0/48.0' "$outfile" "$outfile"
        ncatted -h -O -a bounds,time,o,c,"time_bnds" "$outfile" "$outfile"

    elif [[ "$cell" = "area: mean time: maximum" || "$cell" = "area: mean time: minimum" ]]; then
        # Shift time to noon, add day bounds
        ncap2 -h -O -s 'time+=0.5' "$outfile" "$outfile"
        ncap2 -h -A -s 'defdim("nv",2)' "$outfile" "$outfile"
        ncap2 -h -A -s 'time_bnds[$time,$nv]=0.0; time_bnds(:,0)=time-0.5; time_bnds(:,1)=time+0.5' "$outfile" "$outfile"
        ncatted -h -O -a bounds,time,o,c,"time_bnds" "$outfile" "$outfile"
    fi
fi

echo "Done"
exit
