#!/bin/bash

# J. Stuivenvolt-Allen
# July 9, 2025
# For melissa

# Purpose:
# --------
# NA-CORDEX workflow for test configurations.
# Currently only developed for single domain run. 

# JSA TASKS: 
# ----------
# 1. Add functionality to change calendars (leap, no-leap, 360 day) : Done
# 2. Format as functional bash progamming : Done
# 3. Change log/run/data file permissions for safe-keeping : Done

# The framework: 
# --------------
# year_chunk = 5 / year
# simulation_year = One year of simulation 
# simualation_chunk = Determined by user : currently 13.0 sim years / chunk

# Usage: 
# ------
readonly do_check_real=true
readonly do_run_wrf=true
readonly do_auto_resub=true

# --------------------------------------------- #
# ------- START OF USER INPUT VARIABLES ------- #
# --------------------------------------------- #

# Case name, scheduler, and account
readonly case_name="mpi_ssp370_002"    # Change case name to unique experiment name (see 
readonly wrf_pbs_account="" 

# Time
readonly stop_n='12'       # 2-years spin-up and 10 of simulation : this got confusing
readonly wrf_start_years=( 2017 2027 2037 2047 2057 2067 2077 2087 )

# Directory
readonly working_dir="$PWD"
echo "Working directory: ${working_dir}"

# ------------------------------------------------- #
# --------- END OF USER DEFINED VARIALBES --------- #
# ------------------------------------------------- #
# You probably (?) don't need to change anything below
# ask Jacob or move forward with confidence!



# Submit initial wrf and create log file
# --------------------------------------
# the resubmission will be handled by helper script
chunk_sim=0
n_complete=0
rest_n=$(( (($stop_n-1) * 5) + 3 ))
echo "$rest_n"

for wrf_start_year in "${wrf_start_years[@]}"; do # - - - - - - - - - - - - - - DO 

    ((chunk_sim++))
  
    # Initialization date for each chunk
    start_date="${wrf_start_year}-08-11"
    echo "$start_date"

    # Directory structure
    # -------------------
    wrf_out_dir="${PWD}/wrf_d01/${wrf_start_year}_chunk"
    log_file_dir="${wrf_out_dir}/run_files"
    sub_log_file="${log_file_dir}/case_submission_scripts.txt"

    echo "$sub_log_file"

    # Check for wrf_out directory
    # ---------------------------
    if [ ! -d "$wrf_out_dir" ]; then
        echo "CAN NOT FIND WRF_OUT Directory: Please check case_name"
        echo "-------------------------------------------------"
        echo "Alternatively, check successful completion of runreal_*sh"
        exit 1
    fi
    # ---------------------------

    # Check for submission script log file
    # ------------------------------------
    if [ ! -f "$sub_log_file" ]; then
        echo "CAN NOT FIND WRF LOG FILE: Please check case_name"
        echo "-------------------------------------------------"
        echo "Alternatively, check successful completion of runreal_*sh"
        exit 1
    fi
    # ------------------------------------

    # Check successful completion of real.exe
    # ---------------------------------------
    if [[ "$do_check_real" == "true" ]]; then
        echo "
===========================================================
             CHECKING COMPLETION OF REAL.EXE
===========================================================
"
        real_dir="./real_d01/${wrf_start_year}_N4"   # setting init for the summer (YYYY-08-11)
        search_str="real_em: SUCCESS COMPLETE REAL_EM INIT"
        if grep -q "${search_str}" "${real_dir}/rsl.out.0000"; then
            echo "SUCCESS COMPELTE REAL ---- CONTINUING TO SUBMIT WRF"
            echo "---------------------------------------------------"

        else
            echo "ERROR : CHECK SUCCESSFUL COMPLETION OF REAL.EXE for ${wrf_start_year}_N4"
            exit 1
        fi

    fi
    # ---------------------------------------



    # Submit initial WRF instance
    # ---------------------------
    if [[ "$do_run_wrf" == "true" ]]; then
        echo "
===========================================================
         Submitting WRF initialized at  ${wrf_start_year}-08-11
===========================================================
"

        # Submission log file 
        # -------------------
        line_no=4           # Line number 3 for summer init
        line=$(sed -n "${line_no}p" "$sub_log_file")

        # Grab data from PLACEHOLDER text file
        start_date=$(echo "$line" | awk '{print $1}')  
        end_date=$(echo "$line" | awk '{print $2}')
        export wrf_init_command=$(echo "$line" | awk '{print $3}')

        echo "$start_date"
        echo "$end_date"
        echo "$init_command"
        # -------------------


        # Placeholder file 
        # -------------------
        place_holder_text="CASE_NAME: ${case_name}
CHUNK_ID: ${chunk_sim}
CHUNK_INIT_DATE: ${start_date}
N_COMPLETE: ${n_complete}
N_START_DATE: ${start_date}
N_RESTART_DATE: ${end_date}
N_REST_REM: ${rest_n}
    "
        place_holder_file="${log_file_dir}/PLACEHOLDER.txt"
        echo "$place_holder_text" > "$place_holder_file"

        
        # Check to make sure namelist has rest=false for init
        # ---------------------------------------------------
        wrf_dir="${wrf_out_dir}/${wrf_start_year}_N4"  # setting init for the summer (YYYY-08-11)
        wrf_init_nl="${wrf_dir}/namelist.input"
        sed -i "/restart/ s/.true./.false./g" $wrf_init_nl
        # ---------------------------------------------------

        # Submit 
        JOBID1=$(qsub ${wrf_init_command})
        echo "SUBMITTING FIRST COMMAND IN $PWD"

    fi
    # ---------------------------


    # Submit resubmission script as dependency
    # ----------------------------------------
    if [[ "$do_auto_resub" == "true" ]]; then
        echo "
===========================================================
        Starting automatic resubmission for chunk ${wrf_start_year}
===========================================================
"
        resubmit_f="/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/cordex_share/resub/resubmit.sh"
        cp "${resubmit_f}" "${log_file_dir}/"
        restart_f="/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/cordex_share/resub/restart.sh"
        cp "${restart_f}" "${log_file_dir}/"

        echo "Restart files copied into ${log_file_dir}"

        # Edit PBS accounts and paths in those resubmit/restart scripts
        sed -i "/RESUB_DIR/ s#XXXX#${log_file_dir}#g" ${log_file_dir}/resubmit.sh
        sed -i "/PBS/ s#XXXX#${wrf_pbs_account}#g" ${log_file_dir}/resubmit.sh
        sed -i "/PBS/ s#XXXX#${wrf_pbs_account}#g" ${log_file_dir}/restart.sh

        echo "Replaced paths and strings for restart scripts"

        export RESUB_SH="${log_file_dir}/resubmit.sh"
        export RESUBN="RESUB_WRF_${wrf_start_year}_chunk"

        JOBID2=$(qsub -N ${RESUBN} -q main -l select=1:ncpus=1:mpiprocs=1:mem=1GB -l walltime=00:10:00 -A ${wrf_pbs_account} -W depend=afterok:$JOBID1 ${RESUB_SH})

    fi
    # ----------------------------------------


    done # End of do loop for each simulation chunk



