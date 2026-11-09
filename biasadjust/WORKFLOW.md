# NA-CORDEX-CMIP6 Bias-Adjusted Output: Reformatting Workflow

Reformats ISIMIP3BASD bias-adjusted output (annual daily files on the
0.125 degree ERA5-Land grid) into CF/CORDEX-style files: rebuilt
coordinates, cleaned global attributes, 5-year daily chunks, compressed,
repacked, and placed in a DRS tree.

Same pattern as `postprocess/WORKFLOW.md`: each step has `cmd`, `run`, and
`data` directories; a script writes commandfiles, `launch_multi` runs them,
and you check results before moving on.  Commands are bash.

One run of this workflow = one driving model = one `ba_config/<model>.yml`
(`ece`, `mpi`, `era`; `nor` and `cesm` are stubs until their output exists).
Each config covers every experiment bias-adjusted for that model;
historical is processed once and shared by all scenarios.  To add a
scenario later, add its sources to the model's config and re-run from
Step 0; files that already exist are skipped (except by repack.sh, which
repacks everything again).

Naming follows the CORDEX-CMIP5 Adjust convention (CORDEX_adjust_drs.pdf;
no CORDEX-CMIP6 bias-adjustment spec exists yet):

- The BA ID, `<BCname>-<OBSname>-<REFperiod>` with BCname =
  institution-method, is appended to version_realization in the filename
  and the DRS path: `v1-r1` becomes `v1-r1-PIK-ISIMIP3-ERA5Land-1985-2014`.
  No extra DRS level, so the tree has the same depth as the raw archive.
- DRS activity `DD` becomes `bias-adjusted-output` (also the `activity_id`
  attribute).
- Historical years adjusted in a combined historical-ssp* run (2010-2014)
  are labeled with that scenario and stored in its DRS branch, so
  historical ends in 2009.

Steps:

0. Setup
1. Format (metadata fix + concatenation into 5-year files)
2. Compress
3. Repack
4. Relocate
5. QA

Not yet written: physical-consistency QA (tasmin <= tas <= tasmax, hurs
range, pr >= 0, sfcWind vs uas/vas, land-mask fill consistency, seams at
simf boundaries), plot, move, index.

## Files

New in `biasadjust/`:

| File | Purpose |
|---|---|
| `ba_config/<model>.yml` | Per-model config: source dirs, year-to-experiment mapping, filename components |
| `ba_attrs.yml` | Global attribute keep/drop/set rules and templates |
| `ba_setup.py` | Step 0: builds SETUPDIR |
| `ba_format.py` | Step 1: inventory, mapping, chunking, commandfiles, manifest |
| `ba_cmorize.py` | Step 1 worker (run by launch_multi) |
| `relocate_ba.sh` | Step 4 (copy of `postprocess/relocate.sh`, see below) |
| `shell_vars.bash` | Environment variables for each step |

Copied from `postprocess/` and modified (diff against the original when
upstream changes):

| Copy | Original | Copied at | Changes |
|---|---|---|---|
| `relocate_ba.sh` | `relocate.sh` | c8d7a95 | DRS project/activity/version from `ba.env` |

Used from `postprocess/` unmodified: `launch_multi`, `config_env.sh`,
`compress.sh`, `repack.sh`, `qa.sh`, `sim_config/exp-info.tsv`.  Also reads
`var_table.tsv` and the `ncrepack-cordex` scripts from the raw simulation's
setup directory (`raw_setupdir` in the config).

___

```bash
source biasadjust/shell_vars.bash <model>    # e.g. ece
conda activate nac6


################
# Step 0: setup

python $ba/ba_setup.py $sdir $baconfig

# Re-run after editing the config or ba_attrs.yml.  creation_date (and so
# the DRS version) is kept unless --new-date is given.


################
# Step 1: format

python $ba/ba_format.py $sdir $outdir1 $cmddir1

# ba_format.py stops without writing commandfiles if any listed year is
# missing, any year comes from two sources, or an experiment has a gap.
# Check its summary (experiment ranges, mislabeled-file counts) and
# $cmddir1/manifest.tsv before launching.

$post/launch_multi --workflow cordex --run $rundir1 \
  --copy $ba/ba_cmorize.py $cmddir1/*cmd

## wait until it finishes, check everything ran correctly
## (WARNING lines for relabeled driving_experiment_id and
## driving_institution_id are expected)
cd $rundir1
for i in *; do
  printf "%s\t" "$i"
  cat $i/stdout*/* | grep -v -E '^(WARNING|NOTE):' | uniq -c | tr '\n' ' '
  printf "\t"
  tail -q -n 1 $i/*.o* | cut -f 1 -d : | uniq -c
done
grep -h -E '^(Error|NOTE):' */stdout*/* | sort | uniq -c
ls $outdir1/*/*.tmp 2>/dev/null    # leftover .tmp files = failed tasks
cd $topdir


################
# Step 2: compress

$post/compress.sh $indir2 $sdir $outdir2 $cmddir2
# Check that each <var>Adjust.day line shows a quant value (from the
# Adjust rows ba_setup.py added to var_table.tsv), not quant=--.

$post/launch_multi --workflow cordex --run $rundir2 $cmddir2/*cmd


################
# Step 3: repack

$post/repack.sh $indir3 $sdir $outdir3 $cmddir3

$post/launch_multi --workflow cordex --run $rundir3 $cmddir3/*cmd

## any file still a symlink failed
find $outdir3 -type l


################
# Step 4: relocate into DRS tree

$ba/relocate_ba.sh --dry-run $indir4 $sdir $outdir4 | head
$ba/relocate_ba.sh $indir4 $sdir $outdir4


################
# Step 5: QA

# qa.sh takes one DRS node (the level above frequency), so run it once
# per experiment.
drsroot=$outdir4/$(grep '^drs_project=' $sdir/ba.env | cut -d= -f2)
for node in $(find $drsroot -type d -name day -printf '%h\n' | sort -u); do
  exp=$(echo $node | awk -F/ '{print $(NF-3)}')
  $post/qa.sh $node $indir5flat $sdir $outdir5/$exp $cmddir5/$exp
done

for c in $cmddir5/*/; do
  $post/launch_multi --workflow cordex --run $rundir5/$(basename $c) $c/*cmd
done

# Expected: the CORDEX-CMIP6 checker flags the *Adjust variable names,
# NAM-12i, the CORDEX-Adjust project, bias-adjusted-output, and the long
# version_realization element, since they are not in the CORDEX-CMIP6
# controlled vocabulary.
```

## Open items

- CORDEX-CMIP6 has no published bias-adjustment spec yet.  The `Adjust`
  suffix, `project_id`, `bc_info`, and the DRS layout follow the
  CORDEX-CMIP5 Adjust convention and are set in the config and
  `ba_attrs.yml` so they are easy to change.
- The `version_realization` attribute stays `v1-r1`; the long string is in
  `bc_info`, the filename, and the DRS path.  Check against the CMIP5
  Adjust spec.
- `time_units` in `ba_attrs.yml` should match the raw daily files exactly.
- ERA5 evaluation: confirm `driving_variant_label`, and whether the
  alternate calibrations (simh-1953-1979, simh-1980-2009) are published.
- `raw_setupdir` paths in the configs are unverified.
