import os
import sys
from typing import Iterator

import yaml
from collections import namedtuple
from matplotlib import pyplot as plt
import netCDF4 as nc
import xarray as xr



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
        raise FileExistsError(f"Config file '{config_file}' does not exist, check your config file.")

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
    if not input_dir:
        # use the specified input directory
        input_dir = settings['input_dir']
        print(f"using specified input directory, no environment set: {input_dir}")

        # Check for non-existent and empty input directory
        try:
           os.path.exists(input_dir)
           if not os.listdir(input_dir):
               sys.exit(f"ERROR \n {input_dir} is empty. Please check your config file.")
        except  FileNotFoundError:
           print("Directory is non-existent")

    # All the years either by values specified in the YAML config file or
    # by start,end, increment values specified in the YAML config file.

    if settings['years_by_list']:
        all_years = settings['years']
        print(f" all years, specified in config: {all_years}")
    else:
        print("generate list of years from start, end, increment")
        year_start = int(settings['start_year'])
        year_end = int(settings['end_year']) +1
        year_increment = settings['year_increment']
        all_years = [cur_yr  for cur_yr  in range(year_start, year_end, year_increment)]

# All the months, either specified in the YAML config or create based on
# start,end, increment values specified in YAML config file.
    if settings['months_by_list']:
        print("use specified list of months from config file")
        all_months = settings['months_list']
    else:
        print("generate list of all months based on start,end, increment")
        month_start = int(settings['month_start'])
        month_end = int(settings['month_end']) + 1
        month_increment = int(settings['month_increment'])
        all_months = [cur for cur in range(month_start, month_end, month_increment)]

    all_dates:namedtuple = get_dates_by_start_end(all_years, all_months)

# Generate the full-path filenames of input data files
    missing_input_files = []
    all_input_files = []
    for i in range(len(all_dates) ):
        print(f"year: {all_dates[i].year}, month: {all_dates[i].month}")
        substituted_year_month = settings['input_filename_template'].replace('${YEAR}', all_dates[i].year).replace('${'
                                                                                                             'MONTH}', all_dates[i].month)
        fname_extension = settings['input_filename_extension']
        if fname_extension:
            filename = substituted_year_month + fname_extension
            full_filename = os.path.join(input_dir, filename)
        else:
            full_filename = os.path.join(input_dir, substituted_year_month)

        print(f"full input filename: {full_filename}")
        # Verify file exists, if not, keep a list of missing files and print them later
        if not os.path.exists(full_filename):
            missing_input_files.append(full_filename)
        else:
            all_input_files.append(full_filename)

    if len(missing_input_files) > 0:
      print(f" Missing input files: {missing_input_files}")

    return all_input_files

def get_dates_by_start_end(year_list, month_list) -> list[namedtuple]:
    """
         Determine all the dates (years and months)

    Args:
        Input:
           @param year_list:  a list of all years of interest
           @param month_list: a list of all months of interest
    Returns:
            a list of named tuples containing with all combination of years
            with months
    """


    YEARMONTH = namedtuple("YEARMONTH", ["year", "month"])
    all_dates = []


    for cur_year in year_list:
        for cur_month in month_list:
            y_m = YEARMONTH(cur_year, cur_month)
            all_dates.append(y_m)

    return all_dates


def make_time_series_plots(all_input_files:list, settings:dict) -> None:
    """
         Generate the three time series plots: full domain, Great Lakes region,and Boulder Airport
         in either horizontal (1 row, 3 across) or vertical (1 column, 3 down)

         Args:

         Input:
         @param all_input_files:  A list of the input files (with full filepath)
         @param settings: A dictionary representation of all the settings in the YAML
                                   config file
         Returns:
             None, generates a figure with three time-series plots/panels
    """

    data_by_day: xr.DataArray = extract_data(all_input_files)


def extract_data(all_input_files: list) -> xr.DataArray:
    """
          Extract data as an Xarray DataArray, then resample to the daily mean
          temperature or other specified variable ( variable name specified in the
          netCDF file).

    :param all_input_files:
    :return:

          Args:
          @param all_input_files:  A list of all the data files (full filepath)

          Returns:
             an Xarray DataArray containing resampled mean daily data

    """
    # Extract the data from all the input data files
    variable_of_interest = settings['data_var']
    ncdata = xr.open_mfdataset(all_input_files,
    concat_dim = 'time', combine = 'nested', chunks = {'time': 1, 'lat': 236, 'lon': 376}, data_vars = 'all',)[data_var]
    # aggregation method: mean, median, or sum
    if not settings['aggregation_method']:

        aggregate = 'mean'
    else:
        aggregate = settings['aggregation_method']
    print(f"aggregate by : {aggregate}")

if __name__ == "__main__":
    yaml_file = "config.yaml"
    print(f"Evaluate yaml config file {yaml_file}")
    settings:dict = parse_config(yaml_file)
    all_input_files: list = get_input_files(settings)
    make_time_series_plots(all_input_files, settings)

