Checking for all raw data, invoking postprocessing, and invoking plotting
=

# Overview of Components


## Postprocessing scripts:

 - postprocess.core.variables.py
 - cmorize.compress.sh

## Plotting scripts:

- plot.py

  - requires *config.yaml* configuration file
  - can be run from command line
  - can be imported and individual methods can be invoked
     - configuration settings in the config.yaml can be overridden
- util.py

  -  provides utility methods used in plot.py


# Procedure 

### Checking for presence of **ALL** data
- all expected chunk directories are present
  - as determined by the following user-defined values:
     - TOTAL_YEARS_IN_SIMULATION
     - SEVENTH_YEAR_OF_DECADE (True or False)
     - YEAR_INCREMENT

- for every chunk directory, specific filename patterns are expected:
  - wrfout_d01_<year-month_hour:min:sec>
  - wrfout_hour_d01_<year-month_hour:min:sec>
  - wrfout_pres_d01_<year-month_hour:min:sec>
  - wrfout_zlev_d01_<year-month_hour:min:sec>
  - wrfout_afwa_d01_<year-month_hour:min:sec>
  - wrfrst_d01_<year-month_hour:min:sec>
      - year is YYYY
      - month is MM 
      - hour is HH
      - min is mm
      - seconds in SS

- after above two criteria are met, check for the presence of **all** files obtain the number of files of each filename pattern within all chunk directories
  - determine the year from the filename
  - perform the Python equivalent of 
  
        find . -name "wrfout_d01_/*  |wc -l " 
  
       for each filename pattern and determine if the number of files meets the criteria for required number
  - generate a dictionary collecting this information:
     - full path of chunk directories (as the key)
     - list of years corresponding to the data (as values)
  

### Postprocessing 

### Plotting