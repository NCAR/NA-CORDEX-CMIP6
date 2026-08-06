## Known Issues

### Clouds (clt)

WRF's 2-dimensional cloud fraction field is missing data for the first
time-step of each simulation segment; as a result, the variable 'clt'
contains 5—6 hourly time-steps per year with missing values.

### Missing Timesteps in MPI-370

The following timesteps are missing from the SSP3-7.0 simulation of
MPI-ESM1-2-HR:

* 2075-08-16 19:00:00 through 2075-08-17 00:00:00 UTC
* 2099-12-31 19:00:00 through 2099-12-31 23:00:00 UTC.

### Runaway Snow in Greenland
Snow accumulates indefinitely over Greenland.  (This is a common problem in 
many simulations.)  Don't use simulation results over Greenland.
