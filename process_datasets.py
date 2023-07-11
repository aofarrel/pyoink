# compare datasets
import os
import json
import filecmp
import subprocess

# class sample {
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
# }

# clean up files from old runs, supressing errors as we do so since those files might not exist yet
os.system("rm klr_lineages klr_lineages_sorted_alphabetically klr_dupes klr_samples_only 2> /dev/null") # klr
os.system("rm klr_samples_not_in_tba3 klr_samples_also_in_tba3 tba3_samples_not_in_klr 2> /dev/null") # comparisons

#### KLR dataset ####

# get KLR inputs from GitHub
#klr_runs = [directory for directory in os.listdir("./lineage_runs")]
#for klr_run in klr_runs:
#        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/lineage/{klr_run}.txt >> ./lineage_runs/{klr_run}/{klr_run}.txt")

# make lists of known lineages
all_lineages = []
subdirectories = [x[1] for x in os.walk("./lineage_runs")]
lineages = subdirectories[0] # this may not be robust...
for lineage_as_subdirectory in lineages:
    print(f"Checking {lineage_as_subdirectory}")
    #this_lineage = f"{lineage_as_subdirectory}".replace(".", "_") # not needed right now
    files = [item for sublist in [file[2] for file in os.walk(f"./lineage_runs/{lineage_as_subdirectory}")] for item in sublist]
    bams = [filename for filename in files if filename.endswith(".bam")]
    vcfs = [filename for filename in files if filename.endswith(".vcf")]
    diffs = [filename for filename in files if filename.endswith(".diff")]
    coverage = [filename for filename in files if filename.endswith(".report")]
    #tbprf = [filename for filename in files if filename.endswith("uhhhhhhhhh")]
    input_file = [filename for filename in files if filename.startswith("L")][0] # should only ever be one per lineage
    first_half = f"wc -l ./lineage_runs/{lineage_as_subdirectory}/{input_file}"
    second_half = " | awk '{print $1}'"
    count_inputs = subprocess.Popen(first_half+second_half, shell=True, stdout=subprocess.PIPE)
    number_of_inputs = count_inputs.stdout.read()
    print(f"{lineage_as_subdirectory} input {number_of_inputs} BioSamples, and ended up with {len(vcfs)} VCFs, {len(bams)} BAMs, {len(diffs)} diffs, {len(coverage)} coverage reports.")
exit(0)


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