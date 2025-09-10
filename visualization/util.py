# ============================*
# ** Copyright UCAR (c) 2025
# ** University Corporation for Atmospheric Research (UCAR)
# ** National Center for Atmospheric Research (NCAR)
# ** P.O.Box 3000, Boulder, Colorado, 80307-3000, USA
# ============================*


import os
import sys
import yaml
import xarray as xr
import numpy as np
from collections import namedtuple
import argparse


# Globals
KELVIN_TO_CELSIUS = 273.15

def read_config_from_command_line():
    """
        Read the YAML config file from the command line

        Args:

        Returns:
            The full path to the config file
    """
    # Create Parser
    parser = argparse.ArgumentParser(description='Read in config file')

    # Add arguments
    parser.add_argument('Path', metavar='path', type=str,
                        help='the full path to config file')

    # Execute the parse_args() method
    args = parser.parse_args()
    return args.Path

def parse_config(config_file):
    '''
        Parses the YAML configuration file

        Args:
            config_file: the name of the YAML config file

        Returns:
            settings: a dictionary representation of the settings
    '''

    # Check that the config file exists
    if  not os.path.exists(config_file):
        raise FileExistsError("Config file ", config_file, " does not exist, check your config file.")

    with open(config_file, 'r') as file:
        settings = yaml.safe_load(file)

    return settings

def get_input_files(settings:dict):
    '''
        Create the names of the input filenames based on the list or the start,stop
        information provided in the config file.

        Args:
           @param settings: a dictionary representation of the settings in the config file

        Returns:
           a list of input files including full path
    '''

    # Use the INPUT_DIR env variable
    input_dir = os.getenv('INPUT_DIR')

    # If INPUT_DIR env var not specified, read list from config file
    if input_dir is None:
        if settings['input_dir'] is None:
            sys.exit("No input dir specified as ENV var or in config file.")
        else:
            # use the specified input directory
            input_dir = settings['input_dir']

        # Check for non-existent and empty input directory
        try:
           os.path.exists(input_dir)
           if not os.listdir(input_dir):
               sys.exit(f"ERROR \n {input_dir} is empty. Please check your config file.")
        except  FileNotFoundError:
            print(f"Directory {input_dir} is non-existent. Please check your config file.")

    # Retrieve the years of interest, either by values specified in the YAML config
    # file or by start, end, and increment values specified in the YAML config file.
    if settings['years_by_list']:
        all_years = settings['years']
    else:
        year_start = int(settings['start_year'])
        year_end = int(settings['end_year']) +1
        year_increment = settings['year_increment']
        all_years = [str(cur_yr) for cur_yr  in range(year_start, year_end, year_increment)]

# All the months, either specified in the YAML config or create based on
# start, end, and increment values specified in YAML config file.
    if settings['months_by_list']:
        all_months = settings['months_list']
    else:
        month_start = int(settings['month_start'])
        month_end = int(settings['month_end']) + 1
        month_increment = int(settings['month_increment'])
        all_months = [str(cur).zfill(2) for cur in range(month_start, month_end, month_increment)]

    all_dates:namedtuple = get_dates_by_start_end(settings, all_years, all_months)

# Generate the full-path filenames of input data files
    missing_input_files = []
    all_input_files = []
    for i in range(len(all_dates) ):
        substituted_year_month = (
            settings['input_filename_template'].
            replace('${YEAR}', all_dates[i].year).
            replace('${MONTH}', all_dates[i].month) )
        fname_extension = settings['input_filename_extension']
        if fname_extension:
            filename = substituted_year_month + fname_extension
            full_filename = os.path.join(input_dir, filename)
        else:
            full_filename = os.path.join(input_dir, substituted_year_month)

        # Verify file exists, if not, keep a list of missing files and print them later
        if not os.path.exists(full_filename):
            missing_input_files.append(full_filename)
        else:
            all_input_files.append(full_filename)

    if len(missing_input_files) > 0:
      print(f"WARNING: Missing input files: {missing_input_files}")

    if len(all_input_files) == 0:
      print("ERROR: No input files found for specified time(s), please check your configuration file.")
      sys.exit(1)

    return all_input_files

def get_dates_by_start_end(settings: dict, year_list, month_list) -> list[namedtuple]:
    """
         Determine all the dates (years and months)

    Args:
        Input:
            @param settings: the dictionary representation of the config settings
           @param year_list:  a list of all years of interest
           @param month_list: a list of all months of interest
    Returns:
            a list of named tuples with combination of years
            with months
    """


    YEARMONTH = namedtuple("YEARMONTH", ["year", "month"])
    all_dates = []

    for cur_year in year_list:
        for cur_month in month_list:
            y_m = YEARMONTH(cur_year, cur_month)
            all_dates.append(y_m)

    return all_dates

def extract_and_resample(settings:dict, all_input_files: list) -> namedtuple:
    """
          Extract data as an Xarray DataArray, then resample to the daily mean
          temperature or other specified variable ( variable name specified in the
          netCDF file).

          Args:
          @param settings:  the dictionary representation of the settings in the YAML
                                     config file.
           @param all_input_files:  a list of all the files under consideration

          Returns:
             a named tuple of :
               - an Xarray DataArray containing resampled mean (or median) daily data
             in Kelvin or degrees Celsius
              - the units for temperature as "K" or "C"
              - an Xarray DataArray of lats
              - an Xarray DataArray of lons
              - the original data, before resampling
    """
    # Extract the data from all the input data files
    variable_of_interest = settings['data_var']
    ncdata = xr.open_mfdataset(all_input_files,  concat_dim='time', combine='nested', chunks={'time': 1, 'lat': 236, 'lon': 376}, data_vars='all', )[variable_of_interest]

    # lats and lons
    lat:xr.DataArray = ncdata.lat
    lon:xr.DataArray = ncdata.lon

    # Aggregation, for temperature get the mean
    # Resample the data for daily mean or median temperature
    ncdata_day_K = ncdata.resample(time='D').mean()

    # Convert units from K to degrees C if setting not specified
    if settings['convert_to_celsius'] is None:
        convert_to_celsius = True
    else:
        convert_to_celsius = settings['convert_to_celsius']

    # Define the named tuple containing all the useful information for plotting
    DATA_BY_DAY = namedtuple("DATA_BY_DAY", ["data_day", "units", "lat", "lon", "orig_data"])

    if convert_to_celsius:
        ncdata_day = ncdata_day_K - KELVIN_TO_CELSIUS
        units = "C"
    else:
        # Data in Kelvin
        ncdata_day = ncdata_day_K
        units = "K"

    data = DATA_BY_DAY(ncdata_day, units, lat, lon, ncdata)

    return data


def slice_data(ncdata:namedtuple, settings: dict, criteria='full')->xr.DataArray:
    """
          Subset the data for an area of interest, such as a region or specific point.
          These are specified by a list of lats and lons or a lat,lon pair (for a
          specific point).

          Args:
          @param ncdata: a named tuple containing the resampled data, the units of
                                    temp ('C' or 'K'), the lat (Xarray DataArray) , and the
                                    lon (Xarray DataArray).
          @param settings: dictionary representation of the settings in the config
                                    file
          @param criteria: the area of interest, either "full", "region", or "point"
                                  Default is "full" (full domain), "region'  is a region defined
                                  by points specified in the YAML config file, and
                                  "point" is a specific point (i.e. station,  city, etc.) that
                                  is specified in the YAML config file.
          Returns:
             an Xarray DataArray with the resampled data for the specified domain or point

    """

    # Retrieve the Xarray DataArray data from the named tuple

    # data sampled by day
    data = ncdata.data_day

    # data before sampling
    orig_data = ncdata.orig_data
    lat = orig_data.lat
    lon = orig_data.lon

    if criteria == 'point':
        # get the lat and lon from the config file
        target_lat = settings['point_lat']
        target_lon = settings['point_lon']

        # Distance from each grid point to target location
        dist = np.sqrt(
            (data['lat'] - target_lat)**2 +
            (data['lon'] - target_lon)**2
            )

        # lats and lons are indexed, so get corresponding lat and lon
        # values via unravel_index().
        flat_index = dist.argmin()
        dims = dist.shape

        # indices of the minimum distance value in the distance array, dist
        j,i = np.unravel_index(flat_index, dims)
        temp_point = data[:, j,i]
        return temp_point

    elif criteria == 'region':
        # Create a 'box' from the min and max lons and lats

        # From YAML config
        ul_lat, ul_lon = settings['region_upper_left']
        ll_lat, ll_lon = settings['region_lower_left']
        ur_lat, ur_lon = settings['region_upper_right']
        lr_lat, lr_lon = settings['region_lower_right']

        region_lats = [ul_lat, ll_lat, ur_lat, lr_lat]
        region_lons = [ul_lon, ll_lon, ur_lon, lr_lon]

        max_lat = max(region_lats)
        max_lon = max(region_lons)
        min_lat = min(region_lats)
        min_lon = min(region_lons)

        region_temp = data.where(
            (lat >= min_lat) &
            (lat <= max_lat) &
            (lon >= min_lon) &
            (lon <= max_lon)
            ).mean(dim=('y', 'x'), skipna=True)

        return region_temp

    else:
        # Return the full domain's data
        return data.mean(dim=('y', 'x'), skipna=True)




