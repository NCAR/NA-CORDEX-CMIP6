import os
import glob
import re

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

# Directory information
# BASEDIR = '/glade/derecho/scratch/jsallen/NA-CORDEX-CMIP6/ERA5_HIST_E03/wrf_d01'
BASEDIR = '/Users/minnawin/NA-CORDEX/Data/wrf_d01'
POST_PROCESS_DIR = '/path/to/NA-CORDEX-CMIP6/postprocess'

# Number of years for the simulation (e.g. simulation runs for 1977, 1987, 1997, 2007
# is a total of 30 years)
TOTAL_YEARS_IN_SIMULATION = 15

# collect the seventh year of each decade
SEVENTH_YEAR_OF_DECADE = False

# increment of year, set to 1 for every year, 10 for every decade, etc.
YEAR_INCREMENT = 15

#--------------------------
# Plotting Information
#--------------------------
PLOT_DIR = '/path/to/NA-CORDEX-CMIP6/visualization/plot.py'
PLOT_CONFIG = '/full/path/to/config.yaml'

#-------------------------------------
# bash script and
# other Python script locations
#--------------------------------------
CMORIZE_SCRIPT = '/path/to/cmorize.compress.sh'
POST_PROCESS_SCRIPT = 'postprocess.core.variables.py'



# CONSTANT values
EXPECTED_NUM_FILES = 365
EXPECTED_NUM_FILES_LEAP_YEAR = 366
EXPECTED_NUM_FILENAME_PATTERNS = 6

def check_dirs_for_data()-> bool:
    """
     Args:
         None
     Returns
        bool: True if all files present to constitute a full year of data for each chunk
                 directory
    """

    complete_chunk_dirs: list = check_for_all_chunk_dirs()
    if complete_chunk_dirs is None:
        return False

    # Check all the files in each chunk directory and determine if we have
    # one year of files (based on the number of files meeting a filename pattern)

    # expected_filename_patterns:
    #     "wrfout_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_hour_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_pres_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_zlev_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfout_afwa_d01_YYYY-MM-DD_hh:mm:SS",
    #     "wrfrst_d01_YYYY-MM-DD_hh:mm:SS"

    dir_to_unique_filenames: dict = {}

    for cur_dir in complete_chunk_dirs:
        # keep track of unique filename-date combinations
        unique_filename_dates = set()
        all_files = os.listdir(cur_dir)
        for cur_file in all_files:
            # Make sure we are only considering files with expected filename patterns.
            unique_expression = "(.*\d{4})-\d{1,2}-\d{1,2}_\d{2}:\d{2}:\d{2}.*"
            result = re.search(unique_expression,  cur_file)
            if result:
                filename = result.group(1)
                unique_filename_dates.add(filename)

        # Collect a list of unique filenames for each chunk directory.
        dir_to_unique_filenames[cur_dir] = list(unique_filename_dates)

    # Check that all the expected filename patterns have been found for every
    # chunk directory (i.e. all the files named wrfout_d01_<date-time>,
    # wrfout_hour_d01_<date-time>, etc. are in each chunk directory)

    keys =  dir_to_unique_filenames.keys()
    for k in keys:
        if len(dir_to_unique_filenames[k]) != EXPECTED_NUM_FILENAME_PATTERNS:
            return False

        # Check for the expected number of files (filename + date + time) in this
        # chunk directory.

        all_files_present = check_for_all_files(k, dir_to_unique_filenames)
        if not all_files_present:
            return False
        else:
            return True


def check_for_all_files(chunk_dir:str, dir_fnames:dict) -> bool:
    """
           ***PRE-CONDITION***:

           all the expected filename patterns have been found for every
           chunk directory (i.e. all the files named wrfout_d01_<date-time>,
           wrfout_hour_d01_<date-time>, etc. )

            For this chunk directory, check for the expected number of files for each
            filename pattern/type:
           365 for a non Leap Year, 366 for a Leap Year.

           Args:
               chunk_dir: the full path to the current chunk dir which is under examination
                               for all files

               dir_fnames: a dictionary containing full path to chunk dirs as key, and
               a list of all the unique filename patterns with year only as values.

           Returns:
               Boolean- True if all the expected files in the chunk directory are present,
               (expected files have specific filename patterns and each pattern should
               have 365 files for a non Leap Year, 366 files for a Leap Year)

    """

    # Patterns for filenames are of the form:
    #              "wrfout_d01_*",
    #              "wrfout_hour_d01_*",
    #              "wrfout_pres_d01_*",
    #              "wrfout_zlev_d01_*",
    #              "wrfout_afwa_d01_*",
    #              "wrfrst_d01_*"

    all_files_for_chunkdir = dir_fnames[chunk_dir]

    # This is the equivalent to the Unix/Linux command:
    #     find . -name "wrfout_d01_*" |wc -l
    #  for every filename pattern
    for cur_file_pattern  in all_files_for_chunkdir:
       filepath = os.path.join(chunk_dir, cur_file_pattern )
       search_pattern = filepath + "*"
       num_files = len(glob.glob(search_pattern))
       year = re.match(r".*_(\d{4})", cur_file_pattern).group(1)
       if is_leap_year(year):
           if num_files != EXPECTED_NUM_FILES:
               return False
       else:
           if num_files != EXPECTED_NUM_FILES_LEAP_YEAR:
               return False

    # if we get here, all criteria met (i.e. all the files for each filename pattern were
    # present).
    return True

def check_for_all_chunk_dirs()  -> bool:
    """
        Search the raw directory, specified by the BASEDIR and check that all
        expected chunk directories are present.
        Chunk directories will look like:
                YYYY_chunk

                or

                YYY7_chunk for chunks corresponding to the 7th year of the
                decade

        Args: None, using global vars above to search for data and relevant scripts

        Returns:
            a list of all the chunk directories


    """

    # Check the directory containing the raw data and get a list of the "chunk"
    # directories: YYYY_chunk (specifically YYY7_chunk).
    # Create a list of the chunk directories with their
    # full path only if ALL the expected dirs are present.
    chunkdir_expression = "(\d{4})_chunk"
    chunkdir7yr_expression = "(\d{3}7)_chunkdir"

    chunk_dirs = os.listdir(BASEDIR)
    chunk_dirs.sort()
    chunk_years = []
    for dir in chunk_dirs:
        if SEVENTH_YEAR_OF_DECADE:
            # YYY7_chunk
            match = re.search(chunkdir7yr_expression, dir)
        else:
            # YYYY_chunk
            match = re.match(chunkdir_expression, dir)
        if match:
            chunk_years.append(match.group(1))
        else:
            return None

    # Verify the chunk years span the same number of years specified in
    # TOTAL_YEARS_IN_SIMULATION
    last_year = len(chunk_years) - 1
    simulation_years = int(chunk_years[last_year]) - int(chunk_years[0])

    if simulation_years ==TOTAL_YEARS_IN_SIMULATION:
        expected_num_dirs = (TOTAL_YEARS_IN_SIMULATION/YEAR_INCREMENT) + 1
        if len(chunk_years) != expected_num_dirs:
            return None

        # Generate a list of the full path to the complete list of chunk directories
        chunk_dir_path: list = [os.path.join(BASEDIR, cur_chunk)  for cur_chunk in chunk_dirs]
        return chunk_dir_path
    else:
        return None


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

if __name__ == "__main__":

    # Testing for Leap Year function
    # list_of_years = ["2000", 2020, 2024, 2028, "1900",1977, 1990, 2100,"2200", "2104"]
    # leaps_or_not = [is_leap_year(yr) for yr in list_of_years]
    # expected = [True, True,True, True, False, False, False, False, False, True]
    # assert leaps_or_not == expected

    if check_dirs_for_data():
        print("All data found, invoke postprocessing... ")

        # invoke postprocessing script

        # invoke plotting
        # set the env variables for the start, end, increment and other plot parameters


    else:
        print("Incomplete data")





