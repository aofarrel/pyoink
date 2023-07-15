# compare datasets
import os
import json
import filecmp
import subprocess

class Sample():
# attributes:
#     String biosample
#     Boolean pulled
#     Boolean decontaminated
#     Boolean varcalled
#     Boolean diff
#     Boolean report
#     Float depth
#     String randrun_id
#     String known_lineage_id
#     String tbprofiler_lineage
#     String usher_lineage

    def __init__(self, biosample, known_lineage_id=None, randrun_id=None):
        self.biosample = biosample.strip("(").strip(")").strip(",")
        if known_lineage_id is not None:
            self.known_lineage = known_lineage_id.strip("(").strip(")").strip(",")
        if randrun_id is not None:
            self.randrun = randrun_id.strip("(").strip(")").strip(",")
        self.varcalled = None
        self.diff = None
        self.report = None
    
    def hasVCF(self, vcfs):
        if f"{self.biosample}_final.vcf" in vcfs:
            self.varcalled = True
        else:
            self.varcalled = False
    
    def hasDiff(self, diffs):
        if f"{self.biosample}.diff" in diffs:
            self.diff = True
        else:
            self.diff = False
    
    def hasReport(self, reports):
        if f"{self.biosample}.report" in reports:
            self.report = True
        else:
            self.report = False
    
    def stats(self):
        print(f"\033[0m{self.biosample}\t{self.known_lineage}", end="\t")
        for item in [self.varcalled, self.diff]:
            if item == False:
                print("\033[91m N", end="\t")
            else:
                print("\033[32m Y", end="\t")
        print("\033[0m")
    
    def write(self, file):
        with open(file, "a") as output:
            output.write(f"{self.biosample}\t{self.known_lineage}\t{self.varcalled}\t{self.diff}\n")
        


# clean up files from old runs, supressing errors as we do so since those files might not exist yet
os.system("rm klr_lineages klr_lineages_sorted_alphabetically klr_dupes klr_samples_only 2> /dev/null") # klr
os.system("rm klr_samples_not_in_tba3 klr_samples_also_in_tba3 tba3_samples_not_in_klr 2> /dev/null") # comparisons
os.system("rm rand_runs/*/*_GitHub.txt")

#### KLR dataset ####

# get KLR inputs from GitHub
#klr_runs = [directory for directory in os.listdir("./lineage_runs")]
#for klr_run in klr_runs:
#        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/lineage/{klr_run}.txt >> ./lineage_runs/{klr_run}/{klr_run}.txt")

# make lists of known lineages
all_lineages_samples = []
subdirectories = [x[1] for x in os.walk("./lineage_runs")]
lineages = subdirectories[0] # this may not be robust...
for lineage in lineages:

    # make arrays
    #this_lineage = {f"{lineage}"}
    files = [item for sublist in [file[2] for file in os.walk(f"./lineage_runs/{lineage}")] for item in sublist]
    vcfs = [filename for filename in files if filename.endswith(".vcf")]
    diffs = [filename for filename in files if filename.endswith(".diff")]
    input_file = [filename for filename in files if filename.startswith("L")][0] # should only ever be one per lineage
    with open(f"./lineage_runs/{lineage}/{input_file}", "r") as sample_file:
        samples = [x.strip("\n") for x in sample_file.readlines()]
    print(f"{lineage} input {len(samples)} BioSamples, and ended up with {len(vcfs)} VCFs and {len(diffs)} diffs")
    
    # look per sample
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
    
    all_lineages_samples.append(this_lineages_samples)
all_klr_samples = [sample for lineages in all_lineages_samples for sample in lineages] # flattened list

# check for dupes
print("Checking for duplicates...")
for i in range(len(all_klr_samples)):
    for j in range(len(all_klr_samples)):
        if all_klr_samples[i].biosample == all_klr_samples[j].biosample:
            if all_klr_samples[i].known_lineage != all_klr_samples[j].known_lineage:
                print(f"WARNING: Found {all_klr_samples[i].biosample} in {all_klr_samples[i].known_lineage} and {all_klr_samples[j].known_lineage}")

 
# write outputs 
for sample in all_klr_samples:
    sample.write("klr_all_samples_information.tsv")

#### tba3 dataset ####

# download mirrors from GitHub (will be checked against later)
tba3_runs = [directory for directory in os.listdir("./rand_runs")]
for tba3_run in tba3_runs:
    print(tba3_run)
    if tba3_run.endswith("partial_redo"):
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/{tba3_run}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
    elif tba3_run.endswith("_redo"):
        tba3_run_no_redo = tba3_run.replace("_redo", "")
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/exclusive_subsets/{tba3_run_no_redo}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
    elif tba3_run == "rescue_samples.txt":
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/{tba3_run}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")
    else:
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/tb_a3/exclusive_subsets/tb_a3_{tba3_run}.txt >> ./rand_runs/{tba3_run}/{tba3_run}_GitHub.txt")

all_randruns_samples = []
subdirectories = [x[1] for x in os.walk("./rand_runs")]
randruns = subdirectories[0] # this may not be robust...
for randrun in randruns:
    # make arrays
    files = [item for sublist in [file[2] for file in os.walk(f"./rand_runs/{randrun}")] for item in sublist]
    bams = [filename for filename in files if filename.endswith(".bam")]
    vcfs = [filename for filename in files if filename.endswith(".vcf")]
    diffs = [filename for filename in files if filename.endswith(".diff")]
    coverage = [filename for filename in files if filename.endswith(".report")]
    #tbprf = [filename for filename in files if filename.endswith("uhhhhhhhhh")]
    input_file = f"./rand_runs/{randrun}/inputs/tb_a3_{randrun}.txt"
    with open(input_file, "r") as sample_file:
        samples = [x.strip("\n") for x in sample_file.readlines()]
    print(f"{randrun} input {len(samples)} BioSamples, and ended up with {len(vcfs)} VCFs, {len(diffs)} diffs, {len(bams)} BAMs, {len(coverage)} coverage reports.")
    
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
     
    
    # look per sample
    this_randruns_samples = []
    for sample in samples:
        # set up sample object
        this_sample = Sample(sample, randrun_id=f"{randrun}")
        this_sample.hasVCF(vcfs)
        this_sample.hasDiff(diffs)
        
        # add to this lineage's list of samples
        this_randruns_samples.append(this_sample)
    
    all_randruns_samples.append(this_lineages_samples)
all_rand_samples = [sample for randruns in all_randruns_samples for sample in randruns] # flattened list

# check for dupes
print("Checking for duplicates (this will take a while)...")
for i in range(len(all_rand_samples)):
    for j in range(len(all_rand_samples)):
        if all_rand_samples[i].biosample == all_rand_samples[j].biosample:
            if all_rand_samples[i].randrun != all_rand_samples[j].randrun:
                print(f"WARNING: Found {all_rand_samples[i].biosample} in {all_rand_samples[i].randrun} and {all_rand_samples[j].randrun}")
 
# write outputs 
for sample in all_rand_samples:
    sample.write("tba3_all_samples.tsv")




# process lineages from JSONs
""" lazy_array_of_strings_to_write = []
for file in os.listdir("./rand_runs/rand12344"):
    if file.endswith(".json"):
        sample = file.rstrip("._to_Ref.H37Rv.bam.results.json")
        with open(f"rand12344/{file}") as thisjson:
            data = json.load(thisjson)
            strain = data["sublin"]
            lazy_array_of_strings_to_write.append(f"{sample}\t{strain}\n")

with open("strains_from_jsons.tsv", "w") as outfile:
    for thingy in lazy_array_of_strings_to_write:
        outfile.write(thingy) """