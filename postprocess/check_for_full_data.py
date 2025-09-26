# ============================*
# ** Copyright UCAR (c) 2025
# ** University Corporation for Atmospheric Research (UCAR)
# ** National Science Foundation National Center for Atmospheric Research (NCAR)
# ** P.O.Box 3000, Boulder, Colorado, 80307-3000, USA
# ============================*

import os
import sys
import glob
import re
import subprocess
import util
import plot

"""
    Monitor the wrf_d01 directory for complete data (one year of data). 
    Check for 365 files for a non leap year,  366 for leap year within each chunk 
    directory (YYYY_chunk).  Check that all the expected YYYY_chunk directories
    are present before checking the number of files.  
    
    Invokes postprocessing script when a complete data set is available (i.e. 
    all expected chunk dirs and associated files).  Save the processed files in a 
    specified directory.
    
    Check for complete post-processed files and invoke plotting script.
    
    
"""

#**************************
# **** Set the following ****
#**************************

#------------------------------------
# Chunk Directory information
# for preprocessed/raw data
#------------------------------------

BASEDIR = '/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/ERA5_HIST_E03/wrf_d01'

# Number of years for the simulation (e.g. simulation runs for 1977, 1987, 1997, 2007
# is a total of 30 years).
TOTAL_YEARS_IN_SIMULATION = 40

# For each simulation, number of years
# e.g. for 1977_chunk, the start year is 1977, last year is 1989 which is 12 years
NUMBER_OF_YEARS_WITHIN_SIMULATION = 13

# collect the seventh year of each decade
SEVENTH_YEAR_OF_DECADE = True

# increment of year, set to 1 for every year, 10 for every decade, etc.
YEAR_INCREMENT = 10

# inside each chunk directory, specify an ordinal year (2 for second, 3 for third, etc. )
# to use as the starting year in determining whether a full complement of data exists.
# e.g. if the actual first year is 1977 and 2 is specified, then use 1978, the second
# year as the starting year
ORDINAL_START_YEAR = 2

#--------------------------
# Plotting Information
#--------------------------
PLOT_INPUT_DIR = '/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/ERA5_HIST_E03/postprocess/tas'

PLOT_CONFIG = '/glade/u/home/minnawin/NA-CORDEX/develop/NA-CORDEX-CMIP6/postprocess/config.yaml'

INPUT_FNAME_TEMPLATE = "tas_NAM-12_ERA5_evaluation_r1i1p1f1_NCAR_WRF461_v1-r1_hr_${YEAR}-${MONTH}"


# the appropriate YEAR is substituted for the ${YEAR}, based on
# the postprocessed data's year
PLOT_FNAME_TEMPLATE = "5day_mean_and_hourly_tas_NAM-12_ERA5_evaluation_r1ip1f1_NCAR_WRF461_v1-r1_hr_${YEAR}"

PLOT_OUTPUT_DIR = '/glade/u/home/minnawin/NA-CORDEX/Output/plots'


#-------------------------------------
# bash script and
# other Python script locations
#--------------------------------------
CMORIZE_SCRIPT = '/glade/u/home/minnawin/NA-CORDEX/NA-CORDEX-CMIP6/postprocess/cmorize.compress.sh'

POST_PROCESS_SCRIPT = '/glade/u/home/minnawin/NA-CORDEX/NA-CORDEX-CMIP6/postprocess/postprocess.core.variables.py'

#*********************************
# **** End of Set the following ****
#*********************************


# CONSTANT values
EXPECTED_NUM_FILES = 365
EXPECTED_NUM_FILES_LEAP_YEAR = 366
EXPECTED_NUM_FILENAME_PATTERNS = 6
DAYS_IN_MONTH = {'01': 30, '02':28, '02_leap': 29, '03':31, '04':30, '05':31,
    '06':30, '07':31, '08':31, '09':30, '10':31, '11':30, '12':31   }

def check_dirs_for_data()-> dict:
    """
         Check for expected chunk directories and complete data within each
         chunk directory. Exit as soon as criteria is unmet.

         Args:
             None

         Returns:

              all_files :       a dictionary of the chunk dirs (keys) and a list of years
                                 (values)  corresponding to the complete data in all chunk
                                  dirs


    """

    complete_chunk_dirs: list = check_for_all_chunk_dirs()

    if complete_chunk_dirs is None:
        sys.exit("Incomplete number of chunk dirs")

    # Check all the files in each chunk directory and determine if we have
    # one year of files (based on the number of files meeting a filename pattern)

    # expected_filename_patterns:
    #     "wrfout_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_hour_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_pres_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_zlev_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_afwa_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfrst_d01_YYYY-MM-DD_hh:mm:SS"

    dir_of_unique_filenames: dict = {}

    for cur_dir in complete_chunk_dirs:
        # keep track of unique filename-date combinations
        unique_filename_dates = set()
        all_files = os.listdir(cur_dir)
        for cur_file in all_files:
            # Make sure we are only considering files with expected filename patterns.
            result = re.search(r'(.*\d{4})-\d{1,2}-\d{1,2}_\d{2}:\d{2}:\d{2}.*',  cur_file)
            if result:
                filename = result.group(1)
                unique_filename_dates.add(filename)

        # Collect a list of unique filenames for each chunk directory.
        dir_of_unique_filenames[cur_dir] = list(unique_filename_dates)

    # Check that all the expected filename patterns have been found for every
    # chunk directory (i.e. all the files named wrfout_d01_<date-time>,
    # wrfout_hour_d01_<date-time>, etc. are in each chunk directory)
    # If any chunk directory has missing files, exit from this script.
    all_files = {}
    keys =  dir_of_unique_filenames.keys()
    for k in keys:
        if len(dir_of_unique_filenames[k]) == EXPECTED_NUM_FILENAME_PATTERNS:
            sys.exit("Insufficient number of filename patterns.")

        # Check for the expected number of files (filename + date + time) in this
        # chunk directory.

        all_files_present:dict  = check_for_all_files(k, dir_of_unique_filenames)

        # build up the final all_files dictionary by appending the all_files_present
        # from each chunk directory
        all_files = {**all_files, **all_files_present}

    return all_files


def check_for_all_files(chunk_dir:str, dir_fnames:dict) -> list:
    """
           PRE-CONDITION:
           ***************
           all the expected filename patterns have been found for every
           chunk directory (i.e. all the files named wrfout_d01_<date-time>,
           wrfout_hour_d01_<date-time>, etc. )

           ***************

            For this chunk directory, check for the expected number of files for each
            filename pattern/type:
            365 for a non Leap Year, 366 for a Leap Year.

           Save the list of filenames as values to the corresponding chunk dir (key) in
           a dictionary.

           Args:
               chunk_dir: the full path to the current chunk dir which is under examination
                               for all files

               dir_fnames: a dictionary containing full path to chunk dirs as key, and
               a list of all the unique filename patterns with year only as values.

           Returns:
               all_files:   a dictionary with the chunk dir as key and
                the list of years (derived from the filenames)

                Exits from the script if there are missing files

    """


    # Patterns for filenames are of the form:
    #              "wrfout_d01_*",
    #              "wrfout_hour_d01_*",
    #              "wrfout_pres_d01_*",
    #              "wrfout_zlev_d01_*",
    #              "wrfout_afwa_d01_*",
    #              "wrfrst_d01_*"

    all_file_patterns_for_chunkdir = dir_fnames[chunk_dir]

    # retrieve all the year information from the files and include only
    # the years beginning from the ordinal year (i.e. nth year) from the first year
    # of the data

    # a list of years of the actual files in this chunk directory
    all_years_all_files = []
    for fp in all_file_patterns_for_chunkdir:
        match = re.match(r'.*_(\d{4})', fp)
        all_years_all_files.append(int(match.group(1)))

    all_years_all_files.sort()
    first_year = int(all_years_all_files[0])
    # determine the last year, so we can ignore it as it only has data for Jan 1
    last_year = first_year + NUMBER_OF_YEARS_WITHIN_SIMULATION
    adjusted_last_year = last_year - 1
    # start with the nth year
    start_year_increment = ORDINAL_START_YEAR - 1
    adjusted_first_year = first_year + start_year_increment
    years_of_interest = [yr for yr in range(adjusted_first_year, adjusted_last_year)]

    missing_years = []
    for expected in years_of_interest:
        if expected not in all_years_all_files:
            missing_years.append(expected)

    #  Search for every filename pattern.
    #  The search is the equivalent to the Unix/Linux command:
    #     find . -name "wrfout_d01_*"  -printf '.'| wc -m
    valid_years = []
    all_files = {}
    missing_files = []
    missing = False

    for cur_file_pattern in all_file_patterns_for_chunkdir:
        #  Extract the year portion of the filename pattern and use to consider only
        #  relevant years (i.e. years that are expected to have 365/366 files
        #  per filename pattern).
        match = re.match(r'.*_(\d{4})', cur_file_pattern)
        file_year = int(match.group(1))
        if file_year in years_of_interest:
            filepath = os.path.join(chunk_dir, cur_file_pattern)
            search_pattern = filepath + "*"

            matched_files_found:list = glob.glob(search_pattern)
            num_files = len(matched_files_found)
            hms: str = "_00:00:00"

            if is_leap_year(file_year):
               if num_files == EXPECTED_NUM_FILES_LEAP_YEAR:
                   # Thus far, criteria is met, store the year as a list
                   valid_years.append(file_year)
               else:
                   missing = True
                   for m in range(1, 13):
                       if m == 2:
                           days_in_month = DAYS_IN_MONTH['02_leap']
                       else:
                           days_in_month = DAYS_IN_MONTH[str(m).zfill(2)]
                       for d in range(1, days_in_month + 1):
                          expected_file = cur_file_pattern + '-' + str(m).zfill(2) + '-' + str(d).zfill(2) + hms
                          expected_full_file = os.path.join(BASEDIR, chunk_dir, expected_file)
                          if expected_full_file not in matched_files_found:
                              missing_files.append(expected_full_file)
            else:
               if num_files == EXPECTED_NUM_FILES:
                   # Thus far, criteria is met, store the year as a list
                   valid_years.append(file_year)
               else:
                   missing = True
                   for m in range(1, 13):
                       days_in_month = DAYS_IN_MONTH[str(m).zfill(2)]
                       for d in range(1, days_in_month + 1):
                           expected_file = cur_file_pattern + '-' + str(m).zfill(2) + '-' + str(d).zfill(2) + hms
                           expected_full_file = os.path.join(BASEDIR, chunk_dir, expected_file)
                           if expected_full_file not in matched_files_found:
                               missing_files.append(expected_full_file)

    if missing:
        print(f"WARNING: ithere are {len(missing_files)} missing files n {chunk_dir}: ")
        for file in missing_files:
           print(f" {file}")
        sys.exit(f"Exiting due to missing files")


    # Thus far, all criteria is met, assign the list of files to the chunk_dir key
    all_files[chunk_dir] = valid_years


    return all_files



def check_for_all_chunk_dirs()  -> bool:
    """
        Search the raw directory, specified by the BASEDIR and check that all
        expected chunk directories are present.
        Chunk directories will look like:
                YYYY_chunk

                or

                YYY7_chunk for chunks corresponding to the 7th year of the
                decade

        Args: None, using global vars  to search for data and relevant scripts

        Returns:
            a list of all the chunk directories (full path)

            if no chunk directories were found, exit from this script


    """

    # Check the directory containing the raw data and get a list of the "chunk"
    # directories: YYYY_chunk (specifically YYY7_chunk).
    # Create a list of the chunk directories with their
    # full path only if ALL the expected dirs are present.
    chunk_dirs_found = os.listdir(BASEDIR)
    chunk_dirs_found.sort()
    chunk_years = []
    for dir in chunk_dirs_found:
        # Verify that this directory has chunk directories
        if SEVENTH_YEAR_OF_DECADE:
            # YYY7_chunk
            match = re.search(r'(\d{4})_chunk', dir)
        else:
            # YYYY_chunk
            match = re.match(r'(\d{3}7)_chunk', dir)

        if not match:
            sys.exit(f"No directories with YEAR_chunk name were found in {BASEDIR}")

        # Collect all the years corresponding to the chunk directories
        chunk_years.append(match.group(1))

    # Verify the chunk years in the BASEDIR are consistent with expected
    # years (based on the TOTAL_YEARS_IN_SIMULATION and YEAR_INCREMENT)
    first_year = int(chunk_years[0])
    expected_last_year:int = first_year + TOTAL_YEARS_IN_SIMULATION
    expected_years:list = [i for i in range(first_year, expected_last_year+1, YEAR_INCREMENT)]
    missing_dirs = []
    for cur_yr in chunk_years:
        if int(cur_yr) not  in expected_years:
            missing_dirs.append(cur_yr)

    if len(missing_dirs) > 0:
        sys.exit(f"Missing the following chunk directories: {missing_dirs}")
    else:
        # Generate a list of the full path to the complete list of chunk directories
        chunk_dir_path: list = [os.path.join(BASEDIR, cur_chunk) for cur_chunk in chunk_dirs_found]
        return chunk_dir_path


def is_leap_year(year:str|int) -> bool:
    """
         Determines whether the input year is a Leap Year using the formula:
         - Evenly divisible by 4 (e.g. 2020, 2024) is a Leap Year,   but if it is evenly divisible by 100,
           then NOT a Leap Year (e.g. 2100, 2200)
          except, if evenly divisible  by 400, then it is a  Leap Year (e.g. 2000, 2400)

         Args:

        @param year:  a string (4 char)  or  integer (4 digit) year

         Returns:
             True if Leap Year, False otherwise
    """

    # Convert the year into an int
    year = int(year)

    if year%4 == 0:
       if year % 100 == 0 :
           if year % 400 == 0:
              # evenly divisible  by 100 and by 400 -> Leap Year
              return True
           # evenly divisible by 100 but NOT by 400 -> Not Leap Year
           return False
       # evenly divisible by 4 -> Leap Year
       return True
    else:
        # not evenly divisible by 4 -> Not Leap Year
        return False


def invoke_postprocessing(all_files: dict) -> list:
    """
     Invoke the postprocess.core.variables.py script
     which expects two arguments: year month (no zero padding)

     Args:
     @param all_files:  dictionary with the list of files (values) for each
                                chunk directory (keys)
     Returns:
         flattened_years: a list of years that correspond to all the postprocessed data

    """
    # verify that the cmorize.compress.sh file is in the same directory as this
    # script

    # Check to make sure necessary scripts exist
    if not os.path.exists(CMORIZE_SCRIPT):
        sys.exit("cmorize.compress.sh script not found")
    if not os.path.exists(POST_PROCESS_SCRIPT):
        sys.exit("postprocess.core.variables.py not found")

    if  os.path.dirname(CMORIZE_SCRIPT) != os.path.dirname(POST_PROCESS_SCRIPT):
        sys.exit("cmorize.compress.sh does not reside in same dir as postprocees.core.variables.py")

    # get the 2D list of all values (years) in the all_files dict
    # and flatten before passing into the invoke_postprocessing function
    all_years_2d: list = all_files.values()
    all_years_flattened = []
    for year_list in all_years_2d:
        for cur_year in year_list:
            all_years_flattened.append(cur_year)

    # for each year, invoke the
    # postprocess.core.variables.py script
    for cur_year in all_years_flattened:
            arguments = [str(cur_year)]
            print(f"Invoking postprocess script for  {cur_year}:")
            print(f"subprocess.run(['python', POST_PROCESS_SCRIPT]+ arguments, capture_output=True, text=True)")

            result = subprocess.run(['python', POST_PROCESS_SCRIPT]+ arguments, capture_output=True, text=True)

    return all_years_flattened

def generate_plots(all_years:list) :
    """
          Generate the plots by invoking methods in plot.py

          Args:
             all_files: The list of all years of data

          Returns:
              0 if no errors were encountered


    """

    # Get the config settings from the YAML config file
    settings:dict = util.parse_config(PLOT_CONFIG)

    # override settings for the filename templates (input and output, etc.),
    # using the user-specified values in the global variable section
    # (at the top of this script) and input param
    settings['input_dir'] = PLOT_INPUT_DIR
    settings['input_filename_template'] = INPUT_FNAME_TEMPLATE
    settings['output_filename_template'] = PLOT_FNAME_TEMPLATE
    settings['output_dir'] = PLOT_OUTPUT_DIR
    settings['years_by_list'] = True
    settings['months_by_list'] = True
    settings['years'] = all_years

    # Retrieve the data variable name from the postprocess filename
    match = re.match(r'(\w+)_.*', INPUT_FNAME_TEMPLATE)
    data_variable = match.group(1)
    settings['data_var'] = data_variable

    # complete data will have data for every month
    all_months = range(1,13)
    zfilled_months = [str(cur).zfill(2) for cur in all_months]
    settings['months_list'] = zfilled_months

    # retrieve input files using updated settings
    all_input_files: list = util.get_input_files(settings)

    # check if plots already exist before invoking plot generation
    if not plots_already_exist(PLOT_FNAME_TEMPLATE, all_years):
        # invoke the plotting function from plot.py
        plot.make_time_series_plots(all_input_files, settings)
    else:
        # Not necessary, but useful information
        print(f"All plots already exist for years: {all_years}, skip creating these plots.")

    return 0


def plots_already_exist(output_filename_expr:str, years:list) -> bool:
    """
       Check if plots were already created for this set of data and
       conditions.

       Args:
         output_filename_expr: the plot filename template
         years:

      Returns:
          True if all existing plots for this data already exist, False otherwise
    """

    # Generate a list of expected plot filenames by getting the static portion of
    # the filename template (i.e. all text except for the '${YEAR}' portion of the template).
    match = re.match(r'(.*)_\$', output_filename_expr)
    partial_expression = match.group(1)

    expected_files = []
    for y in years:
        constructed_fname = partial_expression + "_" + str(y) + ".png"
        expected_files.append(constructed_fname)

    # keep track of expected files that are found in the list of actual files
    actual_plot_files = os.listdir(PLOT_OUTPUT_DIR)
    num_found = 0
    for expected in expected_files:
        if expected in actual_plot_files:
            num_found += 1

    if num_found  == len(expected_files):
        # ALL the plots that are to going to be generated already exist.
        return True

    # Not all plot files were already created
    return False


if __name__ == "__main__":

    # Postprocess and generate plots

    # Checking for expected chunk directories and complete data in all chunk
    # directories.
    all_files:dict =  check_dirs_for_data()
    if bool(all_files):
       print("All data found, invoke postprocessing... ")

       # Postprocessing via postprocess.core.variables.py
       all_years:list =  invoke_postprocessing(all_files)

       # Generate plots via plot.py and config.yaml
       print("Generating plots from postprocessed data...")
       generate_plots(all_years)








