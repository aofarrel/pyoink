# compare datasets
import os
import json
import filecmp

class sample {
    String biosample
    Boolean klr # true if known lineage run, false if tba3 run
    Boolean pulled
    Boolean decontaminated
    Boolean varcalled
    Boolean diff
    Float depth
    String known_lineage
    String tbprofiler_lineage
    String usher_lineage
}

# clean up files from old runs, supressing errors as we do so since those files might not exist yet
os.system("rm klr_lineages klr_lineages_sorted_alphabetically klr_dupes klr_samples_only 2> /dev/null") # klr
os.system("rm klr_samples_not_in_tba3 klr_samples_also_in_tba3 tba3_samples_not_in_klr 2> /dev/null") # comparisons

#### KLR dataset ####

# get KLR inputs from GitHub
#klr_runs = [directory for directory in os.listdir("./lineage_runs")]
#for klr_run in klr_runs:
#        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/lineage/{klr_run}.txt >> ./lineage_runs/{klr_run}/{klr_run}.txt")

# make lists of samples by known lineage
L1_1 = []
L1_1_1 = []
L1_1_1_1 = []
L1_1_2 = []
L1_1_3 = []
L1_2_1 = []
L1_2_2 = []
L2 = []
L3 = []
L3_1_1 = []
L3_1_2 = []
L3_1_2_1 = []
L3_1_2_2 = []
L4_1 = []
L4_2 = []
L4_3 = []
L4_4 = []
L4_5 = []
L4_6 = []
L4_7 = []
L4_8 = []
L4_9 = []
L5 = []
L5_1_1 = []
L5_1_2 = []
L5_1_3 = []
L5_1_4 = []
L5_1_5 = []
L5_2 = []
L5_3 = []
L6 = []
L6_1_1 = []
L6_1_2 = []
L6_1_3 = []
L6_2_1 = []
L6_2_2 = []
L6_2_3 = []
L6_3_1 = []
L6_3_2 = []
L6_3_3 = []
L9 = []

# fill lists, and cat all known lineage biosamples into one file
os.chdir("./lineage_runs")
all_files = [item for sublist in [file[2] for file in os.walk(".")] for item in sublist]
all_bams = [filename for filenmae in all_files if filename.endswith(".bam")]
all_vcfs = [filename for filenmae in all_files if filename.endswith(".vcf")]
all_diffs = [filename for filenmae in all_files if filename.endswith(".diff")]
all_coverage = [filename for filenmae in all_files if filename.endswith(".report")]
#all_tbprf = [filename for filenmae in all_files if filename.endswith("uhhhhhhhhh")]
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


# go through directories and cross-check against sample lists
os.chdir("..")
for directory1 in [x[0] for x in os.walk("./lineage_runs")]:
    for directory2 in [x[0] for x in os.walk("./lineage_runs")]:
            if directory1 != directory2:
                    print("Checking {directory1} and {directory2}")
                    filecmp.cmpfiles()




#  
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