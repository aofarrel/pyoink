skip_dupe_check = False

import os
# clean up files from old runs, supressing errors as we do so since those files might not exist yet
os.system("rm klr_lineages klr_lineages_sorted_alphabetically klr_dupes klr_samples_only 2> /dev/null") # klr
os.system("rm klr_samples_not_in_tba3 klr_samples_also_in_tba3 tba3_samples_not_in_klr 2> /dev/null") # comparisons
os.system("rm rand_runs/*/*_GitHub.txt 2> /dev/null")
os.system("rm tba3_all_samples.tsv tba3_all_samples_no_dupes.tsv 2> /dev/null")
os.system("rm process.log 2> /dev/null") # do this BEFORE calling logger
import logging
logging.basicConfig(filename="process.log", level=logging.INFO, force=True)
import json
import filecmp
import subprocess
from tabulate import tabulate
from tqdm import tqdm

class Sample():
# attributes:
#     String biosample
#     Boolean pulled
#     Boolean decontaminated
#     Boolean varcalled
#     Boolean diff
#     Boolean coverage
#     Boolean tbprof
#     Int depth
#     String randrun_id
#     String known_lineage_id
#     String tbprofiler_lineage
#     String usher_lineage


    def __init__(self, biosample, known_lineage_id=None, randrun_id=None):
        self.biosample = biosample.strip("(").strip(")").strip(",")
        if known_lineage_id is not None:
            self.known_lineage_id = known_lineage_id.strip("(").strip(")").strip(",")
            self.randrun_id = None
        if randrun_id is not None:
            self.randrun_id = randrun_id.strip("(").strip(")").strip(",")
            self.known_lineage_id = None
        self.varcalled = None
        self.diff = None
        self.report = None
    
    def hasVCF(self, vcfs):
        if f"{self.biosample}_final.vcf" in vcfs:
            self.varcalled = True
        elif f"{self.biosample}.vcf" in vcfs:
            self.varcalled = True
        else:
            self.varcalled = False
    
    def hasDiff(self, diffs):
        if f"{self.biosample}.diff" in diffs:
            self.diff = True
        else:
            self.diff = False
    
    def hasCoverage(self, reports):
        if f"{self.biosample}.report" in reports:
            self.report = True
        else:
            self.report = False
    
    def hasTBProf(self, jsons):
        # todo: this could be made simplier with regex
        # todo: adjust so this could take in klrs
        for some_json in jsons:
            if some_json.startswith(f"{self.biosample}"): # json filename not entirely consistent
                self.tbprof = True
                with open(f"rand_runs/{self.randrun_id}/{some_json}", "r") as tbprof:
                    report = json.load(tbprof)
                    
                    # lineage
                    apparent_lineage = (str(report["sublin"]))
                    if apparent_lineage == "":
                        self.tbprofiler_lineage = "Unknown"
                        logging.info(f'{self.biosample} has a TBProfiler JSON but no lineage information.')
                    elif apparent_lineage == " ":
                        self.tbprofiler_lineage = "Unknown"
                        logging.info(f'{self.biosample} has a TBProfiler JSON but no lineage information.')
                    else:
                        self.tbprofiler_lineage = apparent_lineage
                    
                    # QC
                    self.tbprof_pct_reads_mapped = (str(report["qc"]["pct_reads_mapped"]))
                    self.tbprof_median_coverage = (str(report["qc"]["median_coverage"]))
                    
                    return
        # no TBProf JSON found
        self.tbprof = False
        self.tbprofiler_lineage = None
        self.tbprof_pct_reads_mapped = None
        self.tbprof_median_coverage = None
    
    def stats(self):
        print(f"\033[0m{self.biosample}\t{self.known_lineage_id}", end="\t")
        for item in [self.varcalled, self.diff]:
            if item == False:
                print("\033[91m N", end="\t")
            else:
                print("\033[32m Y", end="\t")
        print("\033[0m")
    
    def to_file(self, file):
        with open(file, "a") as output:
            if self.known_lineage_id is not None:
                output.write(f"{self.biosample}\t{self.known_lineage_id}\t{self.varcalled}\t{self.diff}\n")
            elif self.randrun_id is not None:
                output.write(f"{self.biosample}\t{self.randrun_id}\t{self.varcalled}\t{self.diff}\t{self.tbprof}\t{self.tbprof_pct_reads_mapped}\t{self.tbprof_median_coverage}\t{self.tbprofiler_lineage}\n")
            else:
                print(f"ERROR - {self.biosample} has neither randrun nor klr id")

#### KLR dataset ####

# get KLR inputs from GitHub
#klr_runs = [directory for directory in os.listdir("./lineage_runs")]
#for klr_run in klr_runs:
#        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/lineage/{klr_run}.txt >> ./lineage_runs/{klr_run}/{klr_run}.txt")

# make lists of known lineages
all_lineages_samples = []
all_lineages_information = []
subdirectories = [x[1] for x in os.walk("./lineage_runs")]
lineages = subdirectories[0] # this may not be robust...
for lineage in lineages:

    # make arrays of files
    files = [item for sublist in [file[2] for file in os.walk(f"./lineage_runs/{lineage}")] for item in sublist]
    vcfs = [filename for filename in files if filename.endswith(".vcf")]
    diffs = [filename for filename in files if filename.endswith(".diff")]
    input_file = [filename for filename in files if filename.startswith("L")][0] # should only ever be one per lineage
    
    # get a list of sample IDs from the input file
    with open(f"./lineage_runs/{lineage}/{input_file}", "r") as sample_file:
        samples = [x.strip("\n") for x in sample_file.readlines()]
    
    # add per-lineage info for a pretty table later
    all_lineages_information.append([f"{lineage}", len(samples), len(vcfs), len(diffs), 100*len(vcfs)/len(samples)])
    
    # check each sample ID for vcfs and diffs
    this_lineages_samples = []
    for sample in samples:
        # set up sample object
        this_sample = Sample(sample, known_lineage_id=f"{lineage}")
        this_sample.hasVCF(vcfs)
        this_sample.hasDiff(diffs)
        
        # add to this lineage's list of samples
        this_lineages_samples.append(this_sample)
        
        # debug: print stats
        #this_sample.stats()
    
    # check for orphan files (files that don't belong to any sample)
    known_vcfs = []
    for sample in this_lineages_samples:
        if sample.varcalled is True:
            known_vcfs.append(f"{sample.biosample}_final.vcf")
    for vcf in vcfs: # defined earlier as files that end with vcf in this folder
        if vcf not in known_vcfs:
            print(f"WARNING: {vcf} does not appear to be associated with any sample!")
    
    known_diffs = []
    for sample in this_lineages_samples:
        if sample.diff is True:
            known_diffs.append(f"{sample.biosample}.diff")
    for diff in diffs: # defined earlier as files that end with vcf in this folder
        if diff not in known_diffs:
            print(f"WARNING: {diff} does not appear to be associated with any sample!")
    
    # add to array
    all_lineages_samples.append(this_lineages_samples)
all_klr_samples = [sample for lineages in all_lineages_samples for sample in lineages] # flattened list

# check for dupes
print("Checking for duplicates...")
for i in tqdm(range(len(all_klr_samples))):
    for j in range(len(all_klr_samples)):
        if all_klr_samples[i].biosample == all_klr_samples[j].biosample:
            if all_klr_samples[i].known_lineage_id != all_klr_samples[j].known_lineage_id:
                logging.warning(f"Found {all_klr_samples[i].biosample} in {all_klr_samples[i].known_lineage_id} and {all_klr_samples[j].known_lineage_id}")
 
# write per-lineage outputs
print(tabulate(all_lineages_information, headers=["lineage", "samples", "VCFs", "diffs", "% VCF"]))

# write per-sample outputs
for sample in all_klr_samples:
    sample.to_file("klr_all_samples_information.tsv")

#### tba3 dataset ####

# download mirrors from GitHub (will be checked against later)
print("Downloading GitHub mirrors...")
tba3_runs = [directory for directory in os.listdir("./rand_runs")]
for tba3_run in tba3_runs:
    if tba3_run.endswith("partial_redo"):
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/{tba3_run}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
    elif tba3_run == "rescue_rescue_redo":
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/main/inputs/tb_accessions/tb_a3/rescue_rescue_samples.txt >> ./rand_runs/{tba3_run}/rescue_rescue_redo_GitHub.txt")
    elif tba3_run.endswith("_redo"):
        tba3_run_no_redo = tba3_run.replace("_redo", "")
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/exclusive_subsets/tb_a3_{tba3_run_no_redo}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
    elif tba3_run == "rescue_run":
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/rescue_samples.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
    elif tba3_run == "party_time🎉":
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/exclusive_subsets/tb_a3_pool.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt") 
    elif tba3_run.startswith("rand13000_"):
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/main/inputs/tb_accessions/tb_a3/{tba3_run}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
    else:
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/exclusive_subsets/tb_a3_{tba3_run}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
        

all_randruns_samples = []
all_randruns_information = []
subdirectories = [x[1] for x in os.walk("./rand_runs")]
randruns = subdirectories[0] # this may not be robust...
for randrun in tqdm(randruns):
    # make arrays
    files = [item for sublist in [file[2] for file in os.walk(f"./rand_runs/{randrun}")] for item in sublist]
    vcfs = [filename for filename in files if filename.endswith(".vcf")]
    diffs = [filename for filename in files if filename.endswith(".diff")]
    coverage = [filename for filename in files if filename.endswith(".report")]
    #bams = [filename for filename in files if filename.endswith(".bam")]
    tbprf = [filename for filename in files if filename.endswith(".json")]
    if randrun == "rescue_rescue_redo":
        print("Note: No input file for rescue_rescue_redo. Will be using the GitHub mirror for all checks.")
        with open("./rand_runs/rescue_rescue_redo/rescue_rescue_redo_GitHub.txt", "r") as sample_file:
            samples = [x.strip("\n") for x in sample_file.readlines()]
    else:
        try:
            input_file = f"./rand_runs/{randrun}/inputs/tb_a3_{randrun}.txt"
            with open(input_file, "r") as sample_file:
                samples = [x.strip("\n") for x in sample_file.readlines()]
        except FileNotFoundError:
            if randrun == "rescue_run":
                with open("./rand_runs/rescue_run/rescue_samples.txt", "r") as sample_file:
                    samples = [x.strip("\n") for x in sample_file.readlines()]
            elif randrun == "party_time🎉":
                with open("./rand_runs/party_time🎉/tb_a3_pool.txt", "r") as sample_file:
                    samples = [x.strip("\n") for x in sample_file.readlines()]
            else:
                tba3_run_no_redo = randrun.replace("_redo", "")
                input_file = f"./rand_runs/{randrun}/inputs/tb_a3_{tba3_run_no_redo}.txt"
                with open(input_file, "r") as sample_file:
                    samples = [x.strip("\n") for x in sample_file.readlines()]
        
        # check input file against github file
        with open(f"./rand_runs/{randrun}/{randrun}_GitHub.txt", "r") as other_sample_file:
            supposed_samples = [x.strip("\n") for x in other_sample_file.readlines()]
        if len(samples) != len(supposed_samples):
            print(f"WARNING: Input file has {len(samples)} samples but GitHub mirror has {len(supposed_samples)} samples!")
        for i in range(len(supposed_samples)):
            if supposed_samples[i] not in samples:
                print(f"WARNING: Found {supposed_samples[i]} on the GitHub mirror, but not the input file!")
            try:
                if samples[i] not in supposed_samples:
                    print(f"WARNING: Found {supposed_samples[i]} in the input file, but not the GitHub mirror!")
            except IndexError:
                pass
    
    # add per-lineage info for a pretty table later
    all_randruns_information.append([f"{randrun}", len(samples), len(vcfs), len(diffs), len(tbprf), 100*len(vcfs)/len(samples)])
    
    this_randruns_samples = []
    for sample in samples:
        # set up sample object
        this_sample = Sample(sample, randrun_id=f"{randrun}")
        this_sample.hasVCF(vcfs)
        this_sample.hasDiff(diffs)
        this_sample.hasCoverage(coverage)
        this_sample.hasTBProf(tbprf)
        
        # add to this lineage's list of samples
        this_randruns_samples.append(this_sample)
    
    all_randruns_samples.append(this_randruns_samples)
    
    # check for orphan files (files that don't belong to any sample)
    known_vcfs = []
    for sample in this_randruns_samples:
        if sample.varcalled is True:
            known_vcfs.append(f"{sample.biosample}.vcf")
    for vcf in vcfs: # defined earlier as files that end with vcf in this folder
        if vcf not in known_vcfs:
            print(f"WARNING: {vcf} does not appear to be associated with any sample!")
    
    known_diffs = []
    for sample in this_randruns_samples:
        if sample.diff is True:
            known_diffs.append(f"{sample.biosample}.diff")
    for diff in diffs: # defined earlier as files that end with diff in this folder
        if diff not in known_diffs:
            print(f"WARNING: {diff} does not appear to be associated with any sample!")
            
all_rand_samples = [sample for randruns in all_randruns_samples for sample in randruns] # flattened list

# check for dupes

if not skip_dupe_check: 
    rand_dupes_to_delete = []
    print("Checking for duplicates (this will take at least ten minutes)...")
    for i in tqdm(range(len(all_rand_samples)), bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}, {elapsed} elapsed"):
        for j in range(len(all_rand_samples)):
            if all_rand_samples[i].biosample == all_rand_samples[j].biosample:
                if all_rand_samples[i].randrun_id != all_rand_samples[j].randrun_id:
                    # these are duplicates -- find out which one is better
                    if all_rand_samples[i].varcalled is True and all_rand_samples[j].varcalled is False:
                        logging.info(f"Found {all_rand_samples[i].biosample} in {all_rand_samples[i].randrun_id} and {all_rand_samples[j].randrun_id}")
                        logging.info(f"\tChose {all_rand_samples[i].randrun_id} because it has a VCF and the other one does not")
                        rand_dupes_to_delete.append(all_rand_samples[j])
                    elif all_rand_samples[i].varcalled is False and all_rand_samples[j].varcalled is True:
                        logging.info(f"Found {all_rand_samples[i].biosample} in {all_rand_samples[i].randrun_id} and {all_rand_samples[j].randrun_id}")
                        logging.info(f"\tChose {all_rand_samples[j].randrun_id} because it has a VCF and the other one does not")
                        rand_dupes_to_delete.append(all_rand_samples[i])
                    elif all_rand_samples[i].tbprof is True and all_rand_samples[j].tbprof is False:
                        logging.info(f"Found {all_rand_samples[i].biosample} in {all_rand_samples[i].randrun_id} and {all_rand_samples[j].randrun_id}")
                        logging.info(f"\tChose {all_rand_samples[i].randrun_id} because it has TBProfiler information and the other one does not")
                        rand_dupes_to_delete.append(all_rand_samples[j])
                    elif all_rand_samples[i].tbprof is False and all_rand_samples[j].tbprof is True:
                        logging.info(f"Found {all_rand_samples[i].biosample} in {all_rand_samples[i].randrun_id} and {all_rand_samples[j].randrun_id}")
                        logging.info(f"\tChose {all_rand_samples[j].randrun_id} because it has TBProfiler information and the other one does not")
                        rand_dupes_to_delete.append(all_rand_samples[i])
                    else:
                        logging.info(f"Found {all_rand_samples[i].biosample} in {all_rand_samples[i].randrun_id} and {all_rand_samples[j].randrun_id} but they seem very similar... deleting one at random")
                        rand_dupes_to_delete.append(all_rand_samples[j]) # delete one at random
    
    unique_rand_samples = []
    for sample in all_rand_samples:
        if sample not in rand_dupes_to_delete:
            unique_rand_samples.append(sample)
            
    # write sample-level outputs
    with open("tba3_all_samples_no_dupes.tsv", "w") as thingy:
        thingy.write("BioSample\trandrun_id\t\tVCF?\tdiff?\tTBProf?\tmapped\tmednCov\tlineage\n")
    for sample in tqdm(unique_rand_samples):
        sample.to_file("tba3_all_samples_no_dupes.tsv")
else:
    # write sample-level outputs
    with open("tba3_all_samples.tsv", "w") as thingy:
        thingy.write("BioSample\trandrun_id\t\tVCF?\tdiff?\tTBProf?\tmapped\tmednCov\tlineage\n")
    for sample in tqdm(all_rand_samples):
        sample.to_file("tba3_all_samples.tsv")
    

# write run-level outputs -- this will include dupes unfortunately
print(tabulate(all_randruns_information, headers=["lineage", "samples", "VCFs", "diffs", "TBProf strain", "% VCF"]))