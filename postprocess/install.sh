#!/bin/bash

mamba env create -f environment.yml

export PYTHONIOENCODING=utf-8

conda run -n nac6 esgvoc config set universe:branch=esgvoc_dev
conda run -n nac6 esgvoc config add cordex-cmip6
conda run -n nac6 esgvoc install
