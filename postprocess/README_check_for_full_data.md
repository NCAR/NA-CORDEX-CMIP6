Visual verification of simulated runs
=


Multiple scripts are employed to automate the verification of output. 
A crontab shall be employed to run the check_for_full_data.py script, which
checks for complete data in a specified directory.  When all data is present, 
postprocessing is performed by invoking the postprocess.core.variables.py. 
When postprocessing is complete, time series plots are created by invoking the
plot.py script.  These plots
consist of 3-panels/subplots for the full domain, the Great Lakes region, and the
Boulder Municipal Airport.  Each subplot consists of two curves: the 5day average
temperature and hourly temperature.

All code is located in the NA-CORDEX-CMIP6/postprocess directory


# Overview of Components

## Checking for complete data:

- check_for_full_data.py

   - can be invoked from command line without any arguments or configuration files
   - can be invoked from a cronjob  
   - require loading the conda module and using the npl_2025b conda environment 
   - invokes the postprocess.core.variables.py script when all expected files are found
   - invokes the plot.py script when postprocessing is complete


## Postprocessing scripts:

 - postprocess.core.variables.py
 - cmorize.compress.sh

## Plotting scripts:

- plot.py

  - requires *config.yaml* configuration file
    - for setting input and output locations
      - env variables can be used in defining these
    - for making simple plotting customizations:
      - titles
      - x-axis label
      - y-axis label
      - line color
      - transparency of the 5day average line
      - list of months 
      - list of years
  - can be run from command line
  - can be imported and individual methods can be invoked
     - configuration settings in the config.yaml can be overridden   
     - some settings are overridden in the check_for_full_data.py script
  
- util.py

  -  provides utility methods used in plot.py:
     - parsing the YAML config file
     - retrieving the input data 
     - slicing data
     - converting temperatures from Kelvin to degrees Celsius
      


# Overview of check_for_full_data.py 

### Checking for presence of **ALL** data
- check for all expected chunk directories 
  - determined by the following user-defined values:
     - TOTAL_YEARS_IN_SIMULATION
     - SEVENTH_YEAR_OF_DECADE (True or False)
     - YEAR_INCREMENT

- for every chunk directory, specific filename patterns are expected:
  - wrfout_d01_<year-month_hour:min:sec>
  - wrfout_hour_d01_<year-month_hour:min:sec>
  - wrfout_pres_d01_<year-month_hour:min:sec>
  - wrfout_zlev_d01_<year-month_hour:min:sec>
  - wrfout_afwa_d01_<year-month_hour:min:sec>
      - year is YYYY
      - month is MM 
      - hour is HH
      - min is mm
      - seconds in SS
  -  These **will not** be included for postprocessing:
     - wrfout_5day_d01_<year-month_hour:min:sec> 
     - wrfrst_d01_<year-month_hour:min:sec>

- after above two criteria are met, check for the presence of **all** files in each chunk directory
   - only consider the nth year of data (from the starting year) of the data, as this data typically begins in June
      - this value is specified by the user in the global section of the script by setting the ORDINAL_START_YEAR
   - last year of data only has data for January 1, ignore this file for evaluation
   -  for each chunk directory, search for files containing the expected years
      -  if any years are missing  a list of missing years is printed and the script exits
      - obtain the number of files of each filename pattern within all chunk directories
          - determine the year from the filename and determine if Leap Year
          - performs the Python equivalent of 
  

           find . -name "wrfout_d01_/*  |wc -l " 
  

           - for each filename pattern (including the year)
           - determines if the number of files meets the criteria for a full dataset
           

  - 366 files are expected for Leap Year 
  - 365 files are expected for a non Leap Year
  - generate a dictionary collecting this information:
     - full path of chunk directories (as the key)
     - list of years corresponding to the data (as values)
  

### Postprocessing 

- check that postprocess.core.variables.py and cmortize.compress.sh scripts exist in the same directory as the check_for_full_data.py script
  - exit if they are not
- **NOTE** anticipating that the newer version of the postprocessing.core.variables.py will only require a year, rather that a year and month as command line args 
- use the dictionary created by the check_for_full_data.py to extract the years from the filenames


### Plotting

 - determine if plots already exist for these years of postprocess files
    - exit if **all** expected plots already exist
    - invoke the plot.py script if all plots do not exist
 - override settings in the config.yaml file with values set in the check_for_full_data.py:
   - list of months
   - list of years
   - input directory
   -  input filename pattern
   - output filename pattern
   - data variable (extracted from the input filename template) 
   - exits if expected postprocess files used in plotting are missing 
     - prints a list of missing filenames


# Running check_for_full_data.py

- invoke the check_for_full_data.py 

       python check_for_full_data.py


Before running, set the user-defined values in the global section of the code located between the ***"Set the following"*** and ***"End of Set the following"*** comments:
  - BASEDIR 
     -  the directory where the raw data is located
  - TOTAL_YEARS_IN_SIMULATION 
     - number of years the simulation spans
     - e.g. if chunk dirs are 1977_chunk to 2017_chunk, this spans 40 years
  - SEVENTH_YEAR_OF_DECADE
    -  True or False
    -  True if every decade of data falls on the seventh year:
       - e.g. 1977, 1987, 1997, etc. 
  - YEAR_INCREMENT
     -  number of years between each simulation
     -  e.g. a 10 year increment would result in 1977_chunk, 1987_chunk, etc.
  - ORDINAL_START_YEAR
    -  within each chunk directory,  the nth year of the data to use as a starting date
    -  e.g. if set to 2, then use the second year as the start date
  - PLOT_INPUT_DIR
     -  location of the postprocessed data
  - PLOT_CONFIG 
     - the location of the config.yaml file
     - resides in the same directory as the check_for_full_data.py 
     - can reside elsewhere if desired
  - INPUT_FNAME_TEMPLATE
    - the postprocessed filename pattern
    - e.g. tas_NAM-12_ERA5_evaluation_r1i1p1f1_NCAR_WRF461_v1-r1_hr_${YEAR}-${MONTH}
      - the plotting script will substitute the appropriate YEAR and MONTH based on the config file settings 
        - these config file settings are set in the config.yaml but overridden by the check_for_full_data.py script with the appropriate values
  - PLOT_FNAME_TEMPLATE
    - the output template for the output plot file
    - e.g. 5day_mean_and_hourly_tas_NAM-12_ERA5_evaluation_r1ip1f1_NCAR_WRF461_v1-r1_hr_${YEAR}
      -  creates a file with the above filename, with ${YEAR} substituted with the year corresponding to the postprocessed data
  - PLOT_OUTPUT_DIR
    - location of the directory to save the plot files