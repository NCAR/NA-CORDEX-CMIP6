# ============================*
# ** Copyright UCAR (c) 2025
# ** University Corporation for Atmospheric Research (UCAR)
# ** National Science Foundation  National Center for Atmospheric Research (NCAR)
# ** P.O.Box 3000, Boulder, Colorado, 80307-3000, USA
# ============================*

import os
import sys
from matplotlib import pyplot as plt
import util
import argparse


def make_time_series_plots(all_input_files: list, settings: dict) -> None:
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

    ncdata_tuple = util.extract_and_resample(settings, all_input_files)
    # Axis labels and x-tick orientations are the same for all subplots
    if settings['x_axis_label'] is None:
        x_axis_label = 'Time'
    else:
        x_axis_label = settings['x_axis_label']

    if settings['y_axis_label'] is None:
        if ncdata_tuple.units == 'C':
            y_axis_label = ncdata_tuple.orig_data.long_name + " (degrees " + ncdata_tuple.units + ")"
        else:
            y_axis_label = ncdata_tuple.orig_data.long_name + " (" + ncdata_tuple.units + ")"

    else:
        y_axis_label = settings['y_axis_label']

    # x-tick label rotation (90 for easier reading, 0 for horizontal labels).
    # If unspecified, use default of horizontal labels.
    if settings['x_tick_rotation'] is None:
        x_tick_rotation = 0
    else:
        x_tick_rotation = settings['x_tick_rotation']

    # Set the orientation of the three time-series plots: horizontal or
    # vertical.  Vertical is the default.
    panel_orientation = settings['panel_orientation']

    # three axes as a list
    ax = [0, 1, 2]

    if panel_orientation == 'horizontal':
        # 1 row, 3 columns
        fig, (ax[0], ax[1], ax[2]) = plt.subplots(1, 3)
    else:
        # 3 rows, 1 column (vertically stacked)
        fig, (ax[0], ax[1], ax[2]) = plt.subplots(3)

    # figure size
    fig.set_figwidth(settings['fig_width'])
    fig.set_figheight(settings['fig_height'])

    # Generate plots in the order specified in the config file
    # If the entire region is first, then it will be the top plot (vertical orientation)
    # or left-most (horizontal orientation).
    line_color = settings['line_color']
    line_width = settings['line_width']
    alpha_value = settings['alpha']
    areas_of_interest = settings['areas_of_interest']
    for idx, area in enumerate(areas_of_interest):
        if area == 'region':
            region_of_interest_5day, region_of_interest_by_hour = util.slice_data(
                ncdata_tuple, settings,  criteria="region", )

            ax[idx].set_title(settings['region_title'])
            ax[idx].set_ylabel(y_axis_label)
            ax[idx].set_xlabel(x_axis_label)
            plt.sca(ax[idx])
            plt.xticks(rotation=x_tick_rotation)
            ax[idx].plot(
                region_of_interest_5day.time, region_of_interest_5day, color=line_color,
                linewidth=line_width,
                )
            ax[idx].plot(
                region_of_interest_by_hour.time, region_of_interest_by_hour, color=line_color, linewidth=1,
                alpha=alpha_value,
                )

        elif area == 'entire':
            entire_domain_5d, entire_domain_by_hour = util.slice_data(ncdata_tuple, settings, criteria="all")
            ax[idx].set_title(settings['entire_title'])
            ax[idx].set_ylabel(y_axis_label)
            ax[idx].set_xlabel(x_axis_label)
            plt.sca(ax[idx])
            plt.xticks(rotation=x_tick_rotation)
            ax[idx].plot(entire_domain_5d.time, entire_domain_5d, color=line_color, linewidth=line_width)
            ax[idx].plot(
                entire_domain_by_hour.time, entire_domain_by_hour, color=line_color, linewidth=1,
                alpha=alpha_value,
                )

        elif area == 'point':
            point_of_interest_5d, point_of_interest_by_hour = util.slice_data(
                ncdata_tuple, settings, criteria="point", )
            ax[idx].set_title(settings['point_title'])
            ax[idx].set_ylabel(y_axis_label)
            ax[idx].set_xlabel(x_axis_label)
            plt.sca(ax[idx])
            plt.xticks(rotation=x_tick_rotation)
            ax[idx].plot(point_of_interest_5d.time, point_of_interest_5d, color=line_color, linewidth=line_width)
            ax[idx].plot( point_of_interest_by_hour.time, point_of_interest_by_hour, color=line_color, linewidth=1,
                alpha=alpha_value, )

    # Reduce overlapping titles
    plt.tight_layout()

    # Save the plot
    output_name = create_output_name(settings)
    plt.savefig(output_name)

    # plt.show()


def create_output_name(settings: dict) -> str:
    ''''
        Create the output filename with full path

        Args:
            @param settings: The dictionary representation of the settings in the
                                       configuration file.
        Returns:
            the name of the output file (with full path)
    '''

    # Use the OUTPUT_DIR env variable if set
    output_dir = os.getenv('OUTPUT_DIR')

    # If OUTPUT_DIR env var not specified, read list from config file
    if output_dir is None:

        # if the output directory setting is unspecified, exit
        if settings['output_dir'] is None:
            sys.exit("No output directory specified as an ENV var or in the config file.")
        else:
            output_dir = settings['output_dir']
            # If output directory does not exist, create it
            os.makedirs(output_dir, exist_ok=True)


    # create the plot filename from the output_filename_template setting
    # including the full path
    fname = settings['output_filename_template']

    # All the months, either specified in the YAML config or create based on
    # start, end, and increment values specified in YAML config file.

    # Retrieve the years of interest, either by values specified in the YAML config
    # file or by start, end, and increment values specified in the YAML config file.
    if settings['years_by_list']:
        all_years = settings['years']
        all_years = [str(cur_yr) for cur_yr in all_years]
    else:
        year_start = int(settings['start_year'])
        year_end = int(settings['end_year']) + 1
        year_increment = settings['year_increment']
        all_years = [str(cur_yr) for cur_yr in range(year_start, year_end, year_increment)]

    if settings['months_by_list']:
        all_months = settings['months_list']
    else:
        month_start = int(settings['month_start'])
        month_end = int(settings['month_end']) + 1
        month_increment = int(settings['month_increment'])
        all_months = [str(cur).zfill(2) for cur in range(month_start, month_end, month_increment)]

    # substitute the year and month in the output file
    for y in all_years:
        for m in all_months:
            substituted_year_month  = (fname.replace('${YEAR}', y).replace('${MONTH}', m))

    plot_name = substituted_year_month  + '.png'
    print(f" plot name from output template: {plot_name}")
    full_plot_filename = os.path.join(output_dir, plot_name)

    return full_plot_filename


if __name__ == "__main__":
    # Get the YAML config file path
    parser = argparse.ArgumentParser(description='Parsing YAML config')
    parser.add_argument(
        'config_file', type=str,
        help='the full path to config file',
        )
    config_file = parser.parse_args().config_file

    settings: dict = util.parse_config(config_file)

    # Get input data
    all_input_files: list = util.get_input_files(settings)

    # Generate time-series plots
    make_time_series_plots(all_input_files, settings)

    print("Plotting complete.")
