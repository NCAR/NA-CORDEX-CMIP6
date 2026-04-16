module load ncl
module load nco
module load cdo
module load conda
conda activate na_cordex

which python
python -c "import sys; print(sys.executable)"
python -c "import thermofeel; print(thermofeel.__file__)" || echo "NO THERMOFEEL"
