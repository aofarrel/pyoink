# compare datasets
import os
import json
import filecmp
import subprocess

class Sample():
# attributes:
#     String biosample
#     Boolean klr # true if known lineage run, false if tba3 run
#     Boolean pulled
#     Boolean decontaminated
#     Boolean varcalled
#     Boolean diff
#     Float depth
#     String known_lineage
#     String tbprofiler_lineage
#     String usher_lineage

    def __init__(self, biosample, known_lineage):
        self.biosample = biosample.strip("(").strip(")").strip(",")
        self.known_lineage = known_lineage.strip("(").strip(")").strip(",")
        self.varcalled = None
        self.diff = None
    
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
    
    def stats(self):
        print(f"\033[0m{self.biosample}\t{self.known_lineage}", end="\t")
        for item in [self.varcalled, self.diff]:
            if bool(item) == False:
                print("\033[91m N", end="\t")
            else:
                print("\033[32m Y", end="\t")
        print("\033[0m")
        


# clean up files from old runs, supressing errors as we do so since those files might not exist yet
os.system("rm klr_lineages klr_lineages_sorted_alphabetically klr_dupes klr_samples_only 2> /dev/null") # klr
os.system("rm klr_samples_not_in_tba3 klr_samples_also_in_tba3 tba3_samples_not_in_klr 2> /dev/null") # comparisons

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
    bams = [filename for filename in files if filename.endswith(".bam")]
    vcfs = [filename for filename in files if filename.endswith(".vcf")]
    diffs = [filename for filename in files if filename.endswith(".diff")]
    coverage = [filename for filename in files if filename.endswith(".report")]
    #tbprf = [filename for filename in files if filename.endswith("uhhhhhhhhh")]
    input_file = [filename for filename in files if filename.startswith("L")][0] # should only ever be one per lineage
    with open(f"./lineage_runs/{lineage}/{input_file}", "r") as sample_file:
        samples = [x.strip("\n") for x in sample_file.readlines()]
        print(samples[0])
    print(f"{lineage} input {len(samples)} BioSamples, and ended up with {len(vcfs)} VCFs and {len(diffs)} diffs ({len(bams)} BAMs {len(coverage)} coverage reports).")
    
    # look per sample
    this_lineages_samples = []
    for sample in samples:
        # set up sample object
        this_sample = Sample(sample, f"{lineage}")
        this_sample.hasVCF(vcfs)
        this_sample.hasDiff(diffs)
        
        # add to this lineage's list of samples
        this_lineages_samples.append(sample)
        
        # debug: print stats
        this_sample.stats()
    
    all_lineages_samples.append(this_lineages_samples)


# check for duplicates by cat'ing all known lineage biosamples into one file
all_files = [item for sublist in [file[2] for file in os.walk(".")] for item in sublist]
all_inputs = [filename for filename in all_files if filename.startswith("L")]
for file in all_inputs:
    with open(file, "r") as lineagefile:
        for line in lineagefile.readlines():
            line = line.strip("\n")
            with open("klr_lineages", "a") as catfile:
                catfile.write(f"{line}\t{file.strip('.txt')}\n")
number_of_klr_samples = os.popen("wc -l klr_lineages | awk '{print $1}'").read().strip('\n')
print(f'Combined {number_of_klr_samples} samples.')
os.system("sort klr_lineages >> klr_lineages_sorted_alphabetically")
os.system("uniq -D klr_lineages_sorted_alphabetically >> klr_dupes")
dupes = os.popen("wc -l klr_dupes | awk '{print $1}'").read().strip('\n')
print(f'{dupes} samples might be duplicates!')
os.system("cut -f1 klr_lineages_sorted_alphabetically >> klr_samples_only")
os.system("rm klr_lineages")


 
#### tba3 dataset ####

# make sure input files match what's on GitHub
tba3_runs = [directory for directory in os.listdir("./rand_runs")]
for tba3_run in tba3_runs:
        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/lineage/{tba3_run}.txt >> {tba3_run}/{tba3_run}.txt")


# compare KLR to the tba3 dataset
os.system("comm -13 ../tb_a3.txt klr_samples_only >> klr_samples_not_in_tba3.txt")
os.system("comm -12 ../tb_a3.txt klr_samples_only >> klr_samples_also_in_tba3.txt")
os.system("comm -23 ../tb_a3.txt klr_samples_only >> tba3_samples_not_in_klr.txt")




# process lineages from JSONs
lazy_array_of_strings_to_write = []
for file in os.listdir("rand12344"):
    if file.endswith(".json"):
        sample = file.rstrip("._to_Ref.H37Rv.bam.results.json")
        with open(f"rand12344/{file}") as thisjson:
            data = json.load(thisjson)
            strain = data["sublin"]
            lazy_array_of_strings_to_write.append(f"{sample}\t{strain}\n")

with open("strains_from_jsons.tsv", "w") as outfile:
    for thingy in lazy_array_of_strings_to_write:
        outfile.write(thingy)