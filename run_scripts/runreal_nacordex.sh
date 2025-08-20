#!/bin/bash

# J. Stuivenvolt-Allen
# July 9, 2025
# MPI SSP370
# For melissa

# Purpose:
# --------
# NA-CORDEX workflow for test configurations.
# Currently only developed for single domain run. 

# The framework: 
# --------------
# year_chunk = 5 / year
# simulation_year = One year of simulation 
# simualation_chunk = Determined by user : currently 13.0 sim years / chunk

# Usage: 
# ------
# https://docs.google.com/document/d/12MllYoc64GtRKNPX6l8w0dba-XulQXFLFYRyHP57KKI/edit?tab=t.ft9y8u8cls8g
# The above google doc, especially the tabs under REAL and RUN workflow describes
# this script and how to use it. Please consult that document when using
# this workflow. I'll hopefully put this on github soon...

readonly do_run_real=false
readonly do_overwrite_nml=true
readonly link_aer_forcing=true

# Toggle $do_run_real to "true" if you would like to submit real.exe to the queue. Toggling "true"
# will use one Derecho node (128 processors) for each year specified by the user:
# This comes from the range of $real_start to $real_end

# ALSO: this script generates the namelists for real and wrf that will be used
# for the whole simulation! If there's a namelist error and you need to override
# the existing namelists, SET $do_run_real TO "false" AND RERUN THIS SCRIPT WHILE
# POINTING TO THE CORRECT NAMELIST!


# ------------------------------------------------- #
# --------- START USER DEFINED VARIALBES ---------- #
# ------------------------------------------------- #

# Scheduler and account
readonly case_name=""    # Change case name to unique experiment name (see 
readonly wrf_nodes="7"                 # Current configuration is built for 7 nodes
readonly job_priority="economy"        # job priority [economy, regular, premium]
readonly pbs_account=""        # 

# Time
readonly real_start="2015" # Beginning year of simluation (must include spin-up years)
readonly real_end="2099"   # End year of target simulation
readonly stop_n='12'       # Number of simulation years per chunk

# For a single continuous sim, select one year (1977)
#readonly wrf_start_years=( 2017 2027 2037 2047 2057 2067 2077 2087 )
readonly wrf_start_years=( 2017 )
readonly enable_leap_years=true  # Set to false to enable 365-day calendar

readonly working_dir="$PWD"            # I have not tested specifing an alternative path, it would probably break
readonly master_nl_dir="$PWD/nml"
readonly met_file_dir="/glade/campaign/ral/risc/trude/cordex/create_met_files/WPS_MPI_usgf/ssp370/met_em_files" # Point to met_files
readonly wrf_master_dir="/glade/campaign/ral/risc/jsallen/CMIP6-NA-CORDEX/wrf_CORDEX/wrfv4.6.1_nac_CBIOM/" # Point to wrf model

# WRF physics schemes : type str
readonly cu_phys='10'          # convection 
readonly mp_phys='8'           # microphysics
readonly ra_lw_phys='4'        # longwave radiation
readonly ra_sw_phys='4'        # longwave radiation
readonly bl_pbl_phys='1'       # planetary boundary layer 
readonly sf_sfclay_phys='1'    # surface layer 
readonly sf_surface_phys='4'   # land surface model 

# nudging : type str 
readonly guv='0.000300'   # u and v 
readonly gt='0.000300'    # temp
readonly gq='0.000000'    # moisture
readonly gph='0.000300'   # geopotential

# wavenumber
readonly xwavenum='8'
readonly ywavenum='8'

echo "Working directory: ${working_dir}"

# ------------------------------------------------- #
# --------- END OF USER DEFINED VARIALBES --------- #
# ------------------------------------------------- #




# ------------------------------------------------- #
# ------- START OF RUNREAL_RUNWRF PROGRAM --------- #
# ------------------------------------------------- #

# Generate an array of years to execute real.exe
real_years=()
for ((real_year=real_start; real_year<=real_end; real_year++)); do
    real_years+=("$real_year")
done

# Print the array for verification
echo "REAL Years: ${real_years[@]}"

# Generate an array of leap years
leap_sy=1900
leap_ey=2104
leap_year_array=()
leap_y=$leap_sy

while [[ "$leap_y" -le "$leap_ey" ]]; do
  leap_year_array+=("$leap_y")
  leap_y=$((leap_y+ 4))
done

if [[ "$wrf_nodes" -eq 7 ]]; then
    #readonly days=(80 80 80 80 45)
    readonly days=(74 74 74 74 69) # Changed
    readonly leap_days=(75 75 75 75 66)
    readonly real_nproc_x=-1
    readonly real_nproc_y=-1
    readonly wrf_nproc_x=21
    readonly wrf_nproc_y=42
    readonly nio_tasks=8
    readonly nio_groups=2
    readonly real_cores=25
    readonly rest_int=( $((1440*37)) $((1440*37)) $((1440*37)) $((1440*37)) $((1440*37)) $((1440*23)) )
    readonly leap_rest_int=( $((1440*15)) $((1440*15)) $((1440*15)) $((1440*15)) $((1440*15)) $((1440*22)) )
    readonly real_walltime="12:00:00"
    readonly wrf_walltime=("12:00:00" "12:00:00" "12:00:00" "12:00:00" "12:00:00")
fi

# Print values for verification
echo "-----------------"
echo "Days: ${days[@]}"
echo "real_cores: $real_cores"
echo "rest_int: ${rest_int[@]}"
echo "wrf_walltime: ${wrf_walltime[@]}"

#    rest_int  = 1440#*61
#    real_walltime  = '00:30:00' 
#    wrf_walltime   = ['12:00:00', '12:00:00' ,'12:00:00' ,'12:00:00' ,'00:45:00' ]

echo "
USING ${wrf_nodes} Derecho Nodes with 128 processors each
Target simulation days per wallclock = ${days[0]}
----------------------------------------------------------
nproc_x/nproc_y     =  ${wrf_nproc_x}/${wrf_nproc_y}
nio_groups          =  ${nio_groups}
nio_tasks_per_group =  ${nio_tasks}
----------------------------------------------------------


"
# Directory structure
# -------------------
# we need one wrf case (source code and exe directory) per chunk. 

chunk_sim=0
for wrf_start_year in "${wrf_start_years[@]}"; do # - - - - - - - - - - - - - - DO 
    chunk_sim=$((chunk_sim+1))
    echo "WRF CHUNK STARTING YEAR $wrf_start_year"


    # Arrays = need to be cleared each sim-chunk
    # ------------------------------------------
    wrf_run_files=()
    target_end_dates=()
    target_beg_dates=()
    # ------------------------------------------


    # Make directories for real and wrf chunks sub_chunks
    # ---------------------------------------------------
    mkdir -p "real_d01/"
    wrf_out_dir="wrf_d01/${wrf_start_year}_chunk"
    echo ${wrf_out_dir}
    mkdir -p "${wrf_out_dir}"
    # ---------------------------------------------------

    # Copy NAC compiled WRF into existing directory
    # ---------------------------------------------
    wrf_chk="${working_dir}/${wrf_out_dir}/wrf"
    if [[ ! -d "$wrf_chk" ]]; then
        echo "-----------------------------------------"
        echo "Staging WRF RUN directory to ./wrf_d01/"
        echo "This can take a few minutes....."
        echo "-----------------------------------------"
        mkdir -p "${wrf_out_dir}/wrf"
        cp -r "${wrf_master_dir}"/* "${wrf_out_dir}/wrf" 
    else
        echo "WARNING: WRF RUN directory already exists and has been staged."
        echo "Delete wrf_run/wrf_real directories and rerun if you want to replace them."
    fi

    wrf_run_dir="${working_dir}/${wrf_out_dir}/wrf/run"
    # ---------------------------------------------

    # PBS file for submitting REAL
    # ----------------------------
    pbs_real_exe="#!/bin/bash
#PBS -N XXXX
#PBS -A ${pbs_account}
#PBS -q main
#PBS -l job_priority=${job_priority}
#PBS -l walltime=${real_walltime}
#PBS -l select=1:ncpus=128:mpiprocs=128:ompthreads=1
#PBS -j oe

module --force purge

ml ncarenv
ml intel
ml ncarcompilers
ml cray-mpich
ml craype
ml hdf5
ml netcdf
ml ncview
ml ncl
ml nco
"
    # --------------------------------------------


    # PBS file for submitting WRF
    # ---------------------------
    pbs_wrf_exe="#!/bin/bash
#PBS -N XXXX
#PBS -A ${pbs_account}
#PBS -q main
#PBS -l job_priority=${job_priority}
#PBS -l walltime=${wrf_walltime}
#PBS -l select=${wrf_nodes}:ncpus=128:mpiprocs=128
#PBS -o log.oe

module --force purge

ml ncarenv
ml intel
ml ncarcompilers
ml cray-mpich
ml craype
ml hdf5
ml netcdf
ml ncview
ml ncl
ml nco

export WRF_CHEM=0
export EM_CORE=1
export WRF_EM_CORE=1
export WRFIO_NCD_LARGE_FILE_SUPPORT=1
export WRF_KPP=0
export YACC='/glade/u/apps/gust/23.04/opt/bin/yacc -d'
export FLEX_LIB_DIR='/glade/u/apps/gust/23.04/opt/lib'
"
    # -----------------------------------------------


    # Chunk simulation years:
    # -----------------------
    wrf_end_year=$(($wrf_start_year+$stop_n))
    chunk_years=()
    for ((chunk_year=${wrf_start_year}; chunk_year<=${wrf_end_year}; chunk_year++)); do
        chunk_years+=("$chunk_year")
    done
    echo "${chunk_years[@]}"

    # -----------------------

    #  Create real and wrf dirs/namelists/sub scripts for each sim
    # -------------------------------------------------------------
    for year in "${chunk_years[@]}"; do # - - - - - - - - - - - - - - - - - DO 

        if [ $year -gt 2024 ]; then
            echo "REACHED END OF SIM PERIOD"
            continue
        fi

        echo " YEAR IN CHUNK YEARS: $year"

        # WRITE .sh files to execute real one time for each year 
        # ------------------------------------------------------
        real_sh_file="${working_dir}/real_d01/${case_name}_batch_real_${year}.sh"
        mkdir -p "$(dirname "$real_sh_file")"
        echo "$pbs_real_exe" > "$real_sh_file"  
        sed -i "/-N/ s/XXXX/REAL_${case_name}_${year}/g" $real_sh_file
        # ------------------------------------------------------


        # Leap year adjust datetimes and sim days
        # ----------------------------------------
        sim_days=(${days[@]})
        sim_rest_int=(${rest_int[@]})

        if [ "$enable_leap_years" = true ] ; then

            is_leap_year=false
            for yyyy in "${leap_year_array[@]}"; do
                if [[ "$yyyy" -eq "$year" ]]; then
                    echo "${yyyy} IS A LEAP YEAR"
                    sim_days=(${leap_days[@]})
                    sim_rest_int=(${leap_rest_int[@]})
                    is_leap_year=true
                    break
                fi
            done
        fi
        # --------------------------------------


        # Populate namelist start and end times for real and wrf
        # ------------------------------------------------------
        jan_one="${year}-01-01"

        # Loop over days and create directories and files
        chunk_year=0
        for idays in "${!sim_days[@]}"; do # - - - - - - - - - - - - - - - - - - - - - DO 
            chunk_year=$((chunk_year+1))
            run_days="${sim_days[$idays]}"

            # Is this a restart? Also, determine the start & end dates for simulation
            # -----------------------------------------------------------------------
            if [[ "$idays" -eq "0" ]]; then
                rest="False"  # Will later set first namelist file as init
            else
                rest="True"   # Will later set rest of namelist to restart
            fi

            if [[ "$idays" -eq 0 ]]; then
                start_date="$jan_one"
            else
                start_date="$wrf_end_date"
            fi

            wrf_end_date=$(date -I -d "${start_date} + ${run_days} days")
            real_end_date=$(date -I -d "${start_date} + $(($run_days+5)) days")
            # -----------------------------------------------------------------------


            # Make real and wrf directories for simulation chunk 
            # --------------------------------------------------
            real_dir="real_d01/${year}_N${chunk_year}"   # JSA STOPPED HERE GOTTA CHANGE SMTHN
            mkdir -p "$real_dir"

            wrf_dir="${wrf_out_dir}/${year}_N${chunk_year}"
            mkdir -p "$wrf_dir"
            # --------------------------------------------------


            # WRITE .sh files to execute WRF one time for each year 
            # ------------------------------------------------------
            wrf_sh_file="${wrf_dir}/${case_name}_batch_wrf_${year}_N${chunk_year}.sh"
            echo "$pbs_wrf_exe" > "$wrf_sh_file"  
            sed -i "/-N/ s/XXXX/WRF_${case_name}_${wrf_start_year}_chunk/g" $wrf_sh_file
            # ------------------------------------------------------


            # Check for WRF input files and break if found : do not want to rerun real
            # ------------------------------------------------------------------------
            if [[ -e "${real_dir}/wrfbdy_d01" || \
                  -e "${real_dir}/wrffdda_d01" || \
                  -e "${real_dir}/wrfinput_d01" || \
                  -e "${real_dir}/wrflowinp_d01" ]] 
            then
                echo "-------------------------------------------------------"
                echo "WARNING: WRF INPUT FILES ALREADY IN real_dir"
                echo "Delete boundary/input files if you want to replace them"
                echo "-------------------------------------------------------\n"
                if [[ "$do_overwrite_nml" == "false" ]]; then
                    echo "-------------------------------------------------------"
                    echo "EXITING: as boundary files already exist and no namelist changes are requested"
                    echo "Set do_overwrite_nml if you'd like to change the namelists"
                    echo "-------------------------------------------------------\n"
                    exit 1
                fi

            fi
            # ------------------------------------------------------------------------


            # Extract month and day from dates
            # --------------------------------
            start_mon=$(date -d "$start_date" +%m)
            real_end_mon=$(date -d "$real_end_date" +%m)
            wrf_end_mon=$(date -d "$wrf_end_date" +%m)

            start_day=$(date -d "$start_date" +%d)
            real_end_day=$(date -d "$real_end_date" +%d)
            wrf_end_day=$(date -d "$wrf_end_date" +%d)
            # --------------------------------

            echo "SETTING UP SIMULATION CHUNK STARTING ON: $start_date"

            # Adjust end month to be january of the next year
            # requires integers : ERROR HERE
            # -----------------------------------------------
            int_start_mon=$((10#$start_mon + 0))
            if [[ "10#$real_end_mon" -eq 1 ]]; then
                int_end_mon=13
            else
                int_end_mon=$((10#$real_end_mon + 0))
            fi
            # -----------------------------------------------


            # Loop through months for real symlinks
            # -------------------------------------
            for imon in $(seq $int_start_mon $int_end_mon); do
                smon=$(printf "%02d" "$imon")
                    if [[ "$imon" -le 12 ]]; then
                        ln -sf ${met_file_dir}/*${year}-${smon}* ${real_dir}
                        end_year=${year}

                    else
                        end_year=$((${year} + 1))
                        smon="01"
                        ln -sf ${met_file_dir}/*${end_year}-${smon}* ${real_dir}
                    fi
            done

            # Create symbolic link for real_dir
            ln -sf ${wrf_run_dir}/* ${real_dir}
            # -------------------------------------

            # Select range of cpus for each iteration of real.exe
            # ---------------------------------------------------
            cpu_bind_1=$((idays * real_cores))
            cpu_bind_2=$((((idays + 1) * real_cores)))

            echo "                                   "
            # ---------------------------------------------------


            # Copy master namelist and make mods
            # ----------------------------------
            [ -f "${real_dir}namelist.input" ] && rm -f "${real_dir}namelist.input"

            tmp_nl="${master_nl_dir}/tmp.namelist.input"
            real_nl="${real_dir}/namelist.input"
            wrf_nl="${wrf_dir}/namelist.input"

            # Check if the master namelist exists
            if [[ ! -f "$tmp_nl" ]]; then
                echo "Error: Master namelist file not found: $tmp_nl"
                exit 1
            fi

            # Copy content to new namelist file
            cp "$tmp_nl" "$real_nl"
            # ----------------------------------


            # REAL NAMELIST: Find and replace strings of importance
            # -----------------------------------------------------
            # Time variables
            sed -i "/run_days/ s/XXXX/$(($run_days+5))/g" $real_nl 
            sed -i "/start_year/ s/XXXX/${year}/g" $real_nl 
            sed -i "/start_mon/ s/XXXX/${start_mon}/g" $real_nl 
            sed -i "/start_day/ s/XXXX/${start_day}/g" $real_nl 

            sed -i "/end_year/ s/XXXX/${end_year}/g" $real_nl
            sed -i "/end_mon/ s/XXXX/${real_end_mon}/g" $real_nl 
            sed -i "/end_day/ s/XXXX/${real_end_day}/g" $real_nl
            sed -i "/restart_interval/ s/XXXX/${sim_rest_int[$chunk_year]}/g" $real_nl 

            # Config variables
            sed -i "/cu_physics/ s/XXXX/${cu_phys}/g" $real_nl
            sed -i "/mp_physics/ s/XXXX/${mp_phys}/g" $real_nl
            sed -i "/ra_lw_physics/ s/XXXX/${ra_lw_phys}/g" $real_nl
            sed -i "/ra_sw_physics/ s/XXXX/${ra_sw_phys}/g" $real_nl
            sed -i "/sf_sfclay_physics/ s/XXXX/${sf_sfclay_phys}/g" $real_nl
            sed -i "/sf_surface_physics/ s/XXXX/${sf_surface_phys}/g" $real_nl
            sed -i "/bl_pbl_physics/ s/XXXX/${bl_pbl_phys}/g" $real_nl

            # Nudging
            sed -i "/guv/ s/XXXX/${guv}/g" $real_nl
            sed -i "/gt/ s/XXXX/${gt}/g" $real_nl
            sed -i "/gq/ s/XXXX/${gq}/g" $real_nl
            sed -i "/gph/ s/XXXX/${gph}/g" $real_nl

            sed -i "/xwavenum/ s/XXXX/${xwavenum}/g" $real_nl
            sed -i "/ywavenum/ s/XXXX/${ywavenum}/g" $real_nl
            # -----------------------------------------------------


            # WRF NAMELIST : Modify slightly from real
            # ----------------------------------------
            cp $real_nl $wrf_nl

            sed -i "/end_day/ s/${real_end_day}/${wrf_end_day}/g" $wrf_nl
            sed -i "/end_hour/ s/18/00/g" $wrf_nl
            sed -i "/run_days/ s/$(($run_days+5))/${run_days}/g" $wrf_nl

            sed -i "/nproc_x/ s/-1/${wrf_nproc_x}/g" $wrf_nl
            sed -i "/nproc_y/ s/-1/${wrf_nproc_y}/g" $wrf_nl

            if [ "$rest" = true ]; then
                sed -i "/restart/ s/.false./.true./g" $wrf_nl
            fi
            # ----------------------------------------


            # Add commands to PBS file for running real.exe
            # ----------------------------------------------
            real_cd_cmd="( cd ${working_dir}/${real_dir}/ ;"
            real_mpi_cmd=" mpiexec -n ${real_cores} --cpu-bind list:${cpu_bind_1}-${cpu_bind_2} ./real.exe  > real.out ) &"
            echo "$real_cd_cmd $real_mpi_cmd" >> "$real_sh_file"
            # ----------------------------------------------


            # Add commands to PBS file for running wrf.exe
            # ----------------------------------------------
            wrf_in_cmd="ln -sf ${working_dir}/${real_dir}/*_d01* ${working_dir}/${wrf_out_dir}/"
            wrf_nl_cmd="cp ${working_dir}/${wrf_dir}/namelist.input ${wrf_run_dir}/"
            wrf_ln_cmd="ln -sf ${wrf_run_dir}/* ${working_dir}/${wrf_out_dir}/"
            wrf_cd_cmd="cd ${working_dir}/${wrf_out_dir}/"
            wrf_mp_cmd="mpiexec ./wrf.exe"

            echo "$wrf_in_cmd" >> "$wrf_sh_file"
            echo "$wrf_nl_cmd" >> "$wrf_sh_file"
            echo "$wrf_ln_cmd" >> "$wrf_sh_file"
            echo "$wrf_cd_cmd" >> "$wrf_sh_file"
            echo "$wrf_mp_cmd" >> "$wrf_sh_file"
            # ----------------------------------------------

            # Append to arrays 
            # ------------------------
            wrf_run_files+=("${working_dir}/${wrf_sh_file}")
            echo "${working_dir}/${wrf_sh_file}"

            target_beg_dates+=("$start_date")
            target_end_dates+=("$wrf_end_date")

            if [[ "$link_aer_forcing" == "true" ]]; then
                #ln -sf $real_sh_file
                sed -i "/auxinput15_inname/ s/XXXX/AOD_${year}_N${chunk_year}.nc/g" $wrf_nl
                sed -i "/auxinput15_inname/ s/XXXX/AOD_${year}_N${chunk_year}.nc/g" $real_nl
            fi

        done # End of do loop for each year_chunk (5/year) # - - - - - - - - - - - - - - - DONE
        echo "wait" >> "$real_sh_file"

        # Submit annual real_sh_file to PBS
        # ---------------------------------
        if [[ "$do_run_real" == "true" ]]; then
            qsub $real_sh_file
        fi
        # ---------------------------------

    done # End of do loop for each simulation_year  - - - - - - - - - - - - - - - - DONE

    # Submission script log for restart execution : chmod 444 so it cannot be deleted
    # -------------------------------------------
    log_file_name="case_submission_scripts.txt"
    log_file_dir="${wrf_out_dir}/run_files"
    mkdir -p "$log_file_dir"
    #touch "${log_file_dir}/${log_file_name}"

    # Create and write to log file
    #log_file_path="${log_file_dir}${log_file_name}" > "$log_file_path" 

    for ((i=0; i<${#target_beg_dates[@]}; i++)); do
        #echo "$i"
        if [[ "$i" -eq 0 ]]; then
            echo "${target_beg_dates[i]} ${target_end_dates[i]} ${wrf_run_files[i]}" > "${log_file_dir}/${log_file_name}"
        else 
            echo "${target_beg_dates[i]} ${target_end_dates[i]} ${wrf_run_files[i]}" >> "${log_file_dir}/${log_file_name}"
        fi
    done

    # Print log string (if needed)
    log_string="Log file created at: $log_file_dir"
    echo "$log_string"

    # JSA - change permissions for safe delete of files
    # -----------------------------------------

done # End of do loop for each simulation_chunk ( STOP_N ) # - - - - - - - - -  DONE



