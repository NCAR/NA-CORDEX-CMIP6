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
#   1. Extracting time coordinate from infile
#   2. Changing the epoch
#   3. Adjusting time coordinates based on cell_methods & frequency
#   4. Appending in the other coordinates from wrf.xy.coords.nc
#   5. Trimming & adding in the data variable from infile
#   6. Adding all the metadata
# Doing is this way avoids rewriting a very large file multiple times,
# which is slow.  The coordinate reference file wrf.xy.coords.nc must
# exist in INDIR; it's created by setup.sh.

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

# echo "-------------------------------"
# echo "Variable:         $var"
# echo "Input file:       $infile"
# echo "Output file:      $outfile"
# echo "Frequency:        $freq"
# echo "Units:            $units"
# echo "Level type:       $lev"
# echo "Reference height: $refh"
# echo "Cell methods:     $cell"
# echo "Long name:        $ln"
# echo "Standard name:    $stdn"

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

if [ ! -f "$coord_xy_file" ]; then
    echo "Error: coordinate file not found: $coord_xy_file" >&2
    echo "Run setup.sh before cmorize.sh." >&2
    exit 1
fi

mkdir -p "$(dirname "$outfile")"

coord_file="$coord_xy_file"
x_trim="10,-11"
y_trim="10,-11"


if [ "$lev" = "single" ]; then

    # This is a little roundabout to avoid rewriting the entire file
    # multiple times, which is slow.  The CDO setreftime command
    # (which is the best option for this operation) requires separate
    # input and output files, so we start by extracting just that,
    # change the epoch, add in the coordinates file, and only then do
    # we append in the data

    # Step 1: Start with the time coordinate from extracted data.  cdo
    # will delete it if there's no data variable, so we also create a
    # dummy variable.  Adding header padding also reduces the odds of
    # file rewrites from header overflow later on

    padding="10000"  #10 KB
    
    tempfile=${outfile}.cmortemp.nc

    ncks -h -A --hdr_pad $padding -v time "$infile" "$tempfile"
    ncatted -h -a history,global,d,, $tempfile
    ncap2 -h -A -s 'dummy[$time]=0.0f' $tempfile $tempfile
    ncap2 -h -O -s 'time=double(time)' $tempfile $tempfile

    # Time and calendar attributes must be set before setreftime
    ncatted -h -a long_name,time,o,c,time "$tempfile"
    ncatted -h -a standard_name,time,o,c,time "$tempfile"
    ncatted -h -a axis,time,o,c,T "$tempfile"
    # #ncatted -h -a calendar,time,o,c,proleptic_gregorian "$outfile"
    # currently calendar is inherited from wrfout
    # needs to be changed to whatever gcm uses


    # Step 2: Set reference time, add leading zeros to month/day, then delete dummy
    cdo -setreftime,1950-01-01,00:00:00,1day "$tempfile" "$outfile"
    ncatted -h -a units,time,o,c,"days since 1950-01-01 00:00:00" "$outfile"
    ncks --no_alphabetize -h -O -x -v dummy $outfile $outfile


    # Step 3: adjust time coordinates based on cell_methods
    if [[ "$cell" = "area: time: mean" ]]; then
        # Shift time to interval midpoint, add bounds
        ncap2 -h -O -s 'time+=1.0/48.0' "$outfile" "$outfile"
        ncap2 -h -A -s 'defdim("bnds",2)' "$outfile" "$outfile"
        ncap2 -h -A -s 'time_bnds[$time,$bnds]=0.0; time_bnds(:,0)=time-1.0/48.0; time_bnds(:,1)=time+1.0/48.0' "$outfile" "$outfile"
        ncatted -h -O -a bounds,time,o,c,"time_bnds" "$outfile" "$outfile"

    elif [[ "$cell" = "area: mean time: maximum" || "$cell" = "area: mean time: minimum" ]]; then
        # Shift time to noon, add day bounds
        ncap2 -h -O -s 'time+=0.5' "$outfile" "$outfile"
        ncap2 -h -A -s 'defdim("bnds",2)' "$outfile" "$outfile"
        ncap2 -h -A -s 'time_bnds[$time,$bnds]=0.0; time_bnds(:,0)=time-0.5; time_bnds(:,1)=time+0.5' "$outfile" "$outfile"
        ncatted -h -O -a bounds,time,o,c,"time_bnds" "$outfile" "$outfile"
    fi


    # Step 4: add in coordinates; this also preserves nice variable ordering

    ncks -h -A -d x,$x_trim -d y,$y_trim $coord_file $outfile

    rm $tempfile


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

    # Step 5: trim sponge zone from data variable and append into outfile
    # N.B. chunking is important here
    ncks -h -A -C --chunk_map rd1 -d x,$x_trim -d y,$y_trim -v "$var" "$infile" "$outfile"


elif [ "$lev" = "fixed" ]; then

    coords="lat lon"
    
    ncks -h -d x,$x_trim -d y,$y_trim "$coord_file" "$outfile"

    ## fx variables have no time dimension
    ##ncrename -O -h -d west_east,x -d south_north,y "$infile" "$infile"

    # Append data variable with sponge layer trimmed
    ncks -h -A -d x,$x_trim -d y,$y_trim -v "$var" "$infile" "$outfile"

fi

# Step 6: add all the metadata


# Global attributes
# -----------------
# Clear existing global and variable-level attributes first
ncatted -h -a ,"$var",d,, -a ,global,d,, "$outfile"

# Combining these all into a single command reduces load on the lustre
# metadata server when running the workflow for the entire dataset

ncatted -h -a Conventions,global,o,c,"CF-1.11" \
           -a activity_id,global,o,c,"${activity_id}" \
           -a contact,global,o,c,"${contact}" \
           -a creation_date,global,o,c,"${creation_date}" \
           -a domain,global,o,c,"${domain}" \
           -a domain_id,global,o,c,"${domain_id}" \
           -a driving_experiment,global,o,c,"${driving_experiment}" \
           -a driving_experiment_id,global,o,c,"${driving_experiment_id}" \
           -a driving_institution_id,global,o,c,"${driving_institution_id}" \
           -a driving_source_id,global,o,c,"${driving_source_id}" \
           -a driving_variant_label,global,o,c,"${driving_variant_label}" \
           -a frequency,global,o,c,"${freq}" \
           -a grid,global,o,c,"${grid}" \
           -a institution,global,o,c,"${institution}" \
           -a institution_id,global,o,c,"${institution_id}" \
           -a license,global,o,c,"${license}" \
           -a mip_era,global,o,c,"${mip_era}" \
           -a product,global,o,c,"${product}" \
           -a project_id,global,o,c,"${project_id}" \
           -a references,global,o,c,"${references}" \
           -a source,global,o,c,"${source}" \
           -a source_id,global,o,c,"${source_id}" \
           -a source_type,global,o,c,"${source_type}" \
           -a tracking_id,global,o,c,"${tracking_id}" \
           -a variable_id,global,o,c,"${var}" \
           -a version_realization,global,o,c,"${version_realization}" \
           $outfile
	
# Variable attributes
# -------------------
ncatted -h -a units,"${var}",o,c,"${units}" \
           -a standard_name,"${var}",o,c,"${stdn}" \
           -a long_name,"${var}",o,c,"${ln}" \
           -a coordinates,"${var}",o,c,"${coords}" \
           -a grid_mapping,"${var}",o,c,"crs" \
	   -a _Fillvalue,"${var}",o,f,1.e20 \
	   -a missing_value,"${var}",o,f,1.e20 \
	   $outfile
	   
if [[ "$cell" != "None" ]]; then
    ncatted -h -a cell_methods,"${var}",o,c,"${cell}" "$outfile"
fi

echo "Done"
exit
