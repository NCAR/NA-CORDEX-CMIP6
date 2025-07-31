# NA-CORDEX-CMIP6

### High-Resolution Climate Projections for North America

As global climate models (GCMs) continue to advance, they remain limited in spatial detail—capturing patterns at scales too coarse to inform decisions about local infrastructure, water resources, or extreme event risks. By applying regional climate models (RCMs) over limited domains, we can translate global-scale projections into actionable insights at the community and watershed level, capturing critical features like mountain ranges, coastlines, and land–atmosphere interactions.

### The NA-CORDEX Framework

NA-CORDEX (North American Coordinated Regional Downscaling Experiment) is part of a global initiative to produce high-resolution climate data tailored to regional needs. Originally based on CMIP5-era GCMs, the project generated simulations over North America at 25–50 km resolution from 1950–2100. These datasets have been widely used in climate research and impacts studies, offering daily-to-monthly variables in standard netCDF format to support diverse applications from agriculture to urban planning. More information on NA-CORDEX-CMIP5 can be found at https://na-cordex.org/index.html.

### Next-Generation Simulations: CMIP6 at 12 km

We are now expanding NA-CORDEX by downscaling CMIP6 global models at 12 km resolution, providing a new level of detail for North American climate projections. These simulations improve the representation of localized extremes and better resolve topographic and land-use influences on regional climate. Data will be openly available and formatted for compatibility with common analysis tools, supporting both scientific research and climate resilience planning across sectors.

This repository contains general information, workflow scripts, and post-processing utilities for the National Center of Atmospheric Research's contributions to NA-CORDEX for CMIP6.


### Simulation Goals:
A historical simulation for 70 years (1950-2020).
Two future simulations : SSP2-4.5 and SSP3-7.0 (2020-2100)
Target is 7-10 GCMs : (700-1000 total years)
MPI-ESM2-1-HR
NorESM2-MM
CESM2
TBD

### WRF Version: 4.6.1
This model was compiled on NCAR's Derecho high-performance computing machine, and can be found at the following location (/glade/u/home/wrfhelp/derecho_pre_compiled_code/). Namelists for ongoing simulations are provided in this repo and will be updated.

### WRF Physics Configuration: 
Cumulus : Kain-Fritsch (10)
Microphysics : Thompson (8) 
LW/SW Radiation : RRTMG (4) 
PBL : Yonsei University (1) 
Surface : Revised MM5 (1) 
Land Surface Model : Noaa-MP (4) 

### Spectral Nudging: 
GUV: 3E-04
GT: 3E-04
GPH: 3E-04
GQ: 0.0

### Other physics/dynamics options:
Fractional sea ice = 1
Seaice_thickness_default: 1
RADT : 12
