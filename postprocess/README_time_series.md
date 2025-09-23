Generating time-series plots for three areas of interest (from the command line)
=

Background
-
Generate three time-series plots for temperature data from netCDF NA-CORDEX post-processed data. Two lines are 
plotted: the 5-day mean temperature (as more prominent, darker line), and the hourly temperature. 

The three areas of interest are:
- the entire data domain
- an area of interest (The Great Lakes)
- a specific point (Boulder Municipal Airport KBDU- Boulder, CO)


Files
- 
- *\__init__.py*
   - to allow importing plot.py and util.py into other Python modules
 
- *config.yaml*
  - example config file
  - user can copy, rename, and modify
  - can be saved anywhere (does not need to reside in the NA-CORDEX-CMAP6/visualization directory)
  - input directory and output directory can be specified explicitly in the config file or set via ENV variable
 
- *util.py*
   - utility functions
   - reading and parsing the YAML config file
   - resampling data
   - converting temperature units in the data
   - slicing data
 
- *plot.py*
   - creates the three time-series plots based on settings in the YAML config file

Configurable settings (YAML config file):
-

- can be renamed to something other than *config.yaml*
- can be saved and invoked from anywhere (refer to instructions for running the plot script below)
  
**Data Related**

- **input_dir**
  - explicitly specify:
    
        input_dir: /glade/u/home/username/project/Data
    
  - specify by ENV var:
    
        input_dir: ${INPUT_DIR}
    
- **input_filename_template**
  - filename format, replacing *year* and *month* with variables that will be substituted with actual values
  - the actual variable values are computed based on the start, end, increment values or from explicit year and month values

        input_filename_template: tas_NAM-12_ERA5_evaluation_r1i1p1f1_NCAR_WRF461_v1-r1_hr_${YEAR}-${MONTH}

    for files that have *tas_NAM-12_ERA5_evaluation_r1i1p1f1_NCAR_WRF461_v1-r1_hr_* in common but differ by YEAR and MONTH

- **input_filename_extension**
  - the filename extension for the input data files
 
        input_filename_extension: nc

- **output_dir**
   - the output directory where the finished plot will be saved, explicit path or via ENV variable

   - explicitly specify:
     
         output_dir: '/glade/u/home/username/project/output/plots'

   - specify by ENV var
     
         output_dir: ${OUTPUT_DIR}

- **output_filename_template**
  - the format for the output filename
  - no extension needs to be specified, the .png extension will be added when saving the plot
 
         output_filename_template: "tas_NAM-12_ERA5_evaluation_r1ip1f1_NCAR_WRF461_v1-r1_hr"

- **data_var**
   - the temperature data variable name
 
         data_var: 'tas'
     
- **convert_to_celsius**
   - Convert temp from K to degrees Celsius
   - If unspecified, default is True (convert to Celsius)
 
         convert_to_celsius: True

     *Note that boolean values ARE NOT surrounded by single or double quotes
     
  **Plotting related**

  - **areas_of_interest**
     - a list of the three areas of interest
     - three keywords are supported:
        - *entire* (for entire domain)
        - *region* (for a specified region, i.e. Great Lakes)
        - *point* (for a specific point, i.e. Boulder Municipal Airport)
     - the order specified will determine the ordering of the subplots
       - for vertical orientation of plots, the first area of interest will be the topmost plot
       - for horizontal orientation of plots, the first area of interest will be the leftmost plot
      
             areas_of_interest:
         
                - 'entire'
                - 'region'
                - 'point'

           As specified, the 'entire' (full domain) plot will be plotted first, followed by the 'region' plot, and finally the 'point' plot
         
      **Plot titles**
      - **region_title**
          - title for the region plot
       
               region_title: 'Great Lakes'
   
      - **entire_title**
         - title for the entire domain plot
       
               entire_title: 'Entire Region'
   
      - **point_title**
         - title for the point plot
       
               point_title: 'KBDU Boulder, CO'
           
      **Defining a region**
    
      The Great Lakes region
    
      - **region_upper_left**
        - lat,lon coordinate for the upper left point defining this region
       
               region_upper_left: [50.2, -89.5]

  
      - **region_lower_left**
         - lat, lon coordinate for the lower left point defining this region
       
                region_lower_left: [43.3, -95.685]
   
      - **region_upper_right**
          - lat, lon coordinate for the upper right point defining this region

                 region_upper_right: [44.0, -75.1]
   
      - **region_lower_right**
          - lat, lon coordinate for the lower right point defininig this region
       
                  region_lower_right: [39.4, -82.6]
   
     **Defining a point**

     Boulder Municipal Airport (KBDU)
    
       - **point_lat**

                  point_lat: 40.04
         
       - **point_lon**

                  point_lon: -105.23

      **Labelling the x- and y-axis**
    
       - **x_axis_label**
         - if unspecified, set to "Time"

                  x_axis_label:

         - specifying a custom label
    
                  x_axis_label: "Year and month"


      - **y_axis_label**
         - if unspecified, use the long name (from data) and the units

                  y_axis_label:

         - specifying a custom label

                  y_axis_label: "Temperature in degrees Celsius"
  
        

       - **x_tick_rotation**
          - rotate the labels for the x-axis
          - if unspecified, the default is 0 (horizontal labels)
        
                  x_tick_rotation: 0

         *Note, plotting is set to minimize overlapping of titles in crowded plots

      **Figure size**

      - **fig_width**
   
        - figure width in inches

                 fig_width: 12

      - **fig_height**
         - figure height in inches
       
                 fig_height: 11

      - **line_color**
        - the color of the line in the time-series plot
        - the color applies to all three subplots/panels
        - values can be specified by:
          - color name
       
                  line_color: "purple"
              
          - hexadecimal value
       
                  line_color: "#4B0092"

            - A hexadecimal value for a colorblind-friendly purple

      - **line_width**
        - the width of the 5-day mean temperature line

                 line_width: 1

      - **alpha_value**
        - the level of transparency of the hourly temperature line
        - values <1 result in more transparency (fainter line)

                 alpha : 0.2
      
Getting started
-

- copy the config.yaml file to your working directory
- rename config.yaml *if desired*, to a more descriptive name
- modify the following for your data:
  - filename_template
  - input_dir
  - output_dir
  - output_filename_template
  - data_var
    - if the temperature variable is different from that specified in the example config file

- set up the INPUT_DIR and OUTPUT_DIR environment variables if not specifying these explicitly
    - for BASH shell, some possible methods
       - .bashrc
       - .bash_profile
       - sourcing a file that defines these
     
         setup_env.bash
     
              #!/bin/bash

              export INPUT_DIR=/glade/u/home/username/project/Data/postprocessed
              export OUTPUT_DIR=/glade/u/home/username/project/Output/plots
  


         and sourcing at the command line:

              source setup_env.bash


 **Generate A Plot**

 - use the npl-2025b conda environment available on the NCAR HPCs
 - clone the code from the Github repository and save it to the appropriate directory (hereafter referred to as $HOME)
 - from the command line:

            python $HOME/NA-CORDEX-CMIP6/visualization/plot.py /path/to/your/config.yaml


     - replace */path/to/your* with the actual path to where the config.yaml file is located
     - *$HOME* refers to the directory where the NA-CORDEX-CMIP6 code was saved
     - the config.yaml file does not need to reside in the same directory as the plot.py module
       

