#!/bin/bash
# Authors: Seth McGinnis, Jacob Stuivenvolt-Allen

mamba env create -f environment.yml

export PYTHONIOENCODING=utf-8

conda run -n nac6 esgvoc use universe@latest
conda run -n nac6 esgvoc use cordex-cmip6@latest
