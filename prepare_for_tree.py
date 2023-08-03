# assumes "tba3_all_samples_no_dupes.tsv" has this as the first line:
# BioSample       randrun_id      VCF     diff    TBProf  mapped  mednCov lineage
#
# Duplicate samples already had one sample selected in process_datasets.py

from tqdm import tqdm
import pandas
import subprocess
rands=pandas.read_csv("tba3_all_samples_no_dupes.tsv", sep="\t") # 75311 rows
rands_with_diffs = rands.loc[rands["diff"] == True]              # 64875 rows
rands_with_diffs.reset_index(drop=True, inplace=True)
print(rands_with_diffs)

# append dataframe with coverage report file information
has_diff_report = []
print("Processing diffs and coverage reports, and copying them to TREE directory...")
with tqdm(total=rands_with_diffs.shape[0]) as progress:
    for index, row in rands_with_diffs.iterrows():
        progress.update(1)
        try:
            subprocess.check_output(f"cp rand_runs/{row['randrun_id']}/{row['BioSample']}.diff ./TREE/", shell=True)
            subprocess.check_output(f"cp rand_runs/{row['randrun_id']}/{row['BioSample']}.report ./TREE/ 2> /dev/null", shell=True)
            has_diff_report.append(True)
        except subprocess.CalledProcessError as e:
            #print(f"No coverage file for {row['BioSample']}")
            #if row['mednCov'] != "" and row['mednCov'] > 10:
                # todo: maybe actually do something with this information?
                #print("Found TBProfiler JSON though, and median coverage is above 10.")
            #else:
                #pass
            has_diff_report.append(False)
    rands_with_diffs['report'] = has_diff_report
print(rands_with_diffs)

# does NOT account for whether these diffs have good enough coverage!
# ex: SAMN18146291 will show up even though only 8% of it maps and its coverage is 5x
print("Writing files...")
with open("TREE/tba3_have_diffs.txt", "a") as diffs_to_include_file:
    for index, row in rands_with_diffs.iterrows():
        diffs_to_include_file.write(f"{row['randrun_id']}/{row['BioSample']}.diff")

with open("TREE/tba3_have_diffs_and_reports.txt", "a") as diffs_and_coverage_file:
    for index, row in rands_with_diffs.iterrows():
        if row['report'] is True:
            diffs_and_coverage_file.write(f"{row['randrun_id']}/{row['BioSample']}.report")
        else:
            pass

#with open("TREE/tba3_diffs_but_no_report.txt", "a"):
# todo: this file might be helpful for knowing which samples to get tbprofiler coverage from...
# but I kind of doubt there's any runs that do have tbprof but lack coverage reports.

with open("TREE/tba3_strains.tsv", "a") as strain_metadata_file:
    for index, row in rands_with_diffs.iterrows():
        strain_metadata_file.write(f"{row['BioSample']}\t{row['lineage']}")

# this is NOT the coverage as determined by Lily's script, this is the tbprofiler median coverage!
with open("TREE/tba3_median_coverage.tsv", "a") as coverage_metadata_file:
    for index, row in rands_with_diffs.iterrows():
        coverage_metadata_file.write(f"{row['BioSample']}\t{row['mednCov']}")

