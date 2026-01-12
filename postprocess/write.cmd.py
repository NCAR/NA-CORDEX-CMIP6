import os 

# USER DEFINED VARIABLES
# ----------------------
start_year      = 1980
end_year        = 1980 # INCLUSIVE : EDIT THIS
wrf_d01_out_dir = "/glade/campaign/ral/risc/collections/na-cordex-cmip6/raw/ERA5/eval/"

varlist = [
        'rsds','rlds','pr',
        'evspsbl','tas','hurs',
        'ps','psl','huss',
        'sfcWind','fx'
        ]

#varlist = [ 'tas' ]

# ----------------------
# END OF USER DEFINED VARIABLES

cwd = os.getcwd()
cmdfile = f"{cwd}/cmdfile"

if os.path.exists(cmdfile):
    os.system(f'rm {cmdfile}')

for var in varlist:
    os.system(f'mkdir -p {var}')

    if var == 'tas':
        os.system('mkdir -p tasmax')
        os.system('mkdir -p tasmin')

    if var == 'sfcWind':
        os.system('mkdir -p uas')
        os.system('mkdir -p vas')

    for iy, year in enumerate(range(start_year,end_year+1)):

        # Specify the starting year of the simulation chunk for paths
        sim_start_year = ((year // 10) * 10) - 3
        dat_dir = f'{wrf_d01_out_dir}{sim_start_year}_chunk/'

        cmd1 = f'python {cwd}/postprocess.core.variables.py {dat_dir} {year} {var}'
        cmd2 = cmd1 + ' > ' + var + '/out.${step}.log 2>&1'

        with open(cmdfile, "a") as file:
            file.write(cmd2 + "\n")


