# compare datasets
import os
import json
import filecmp

# clean up files from old runs, supressing errors as we do so since those files might not exist yet
os.system("rm klr_lineages klr_lineages_sorted_alphabetically klr_dupes klr_samples_only 2> /dev/null") # klr
os.system("rm klr_samples_not_in_tba3 klr_samples_also_in_tba3 tba3_samples_not_in_klr 2> /dev/null") # comparisons

#### KLR dataset ####

# get KLR inputs from GitHub
#klr_runs = [directory for directory in os.listdir("./lineage_runs")]
#for klr_run in klr_runs:
#        os.system(f"curl https://raw.githubusercontent.com/aofarrel/SRANWRP/v1.1.14/inputs/tb_accessions/lineage/{klr_run}.txt >> ./lineage_runs/{klr_run}/{klr_run}.txt")

# cat all known lineage files into one file
os.chdir("./lineage_runs")
with open("klr_lineages", "w") as catfile:
        files = [item for sublist in [file[2] for file in os.walk(".")] for item in sublist]
        lineage_files = [filename for filename in files if filename.startswith("L")]
        print(lineage_files)
        for file in files:
                with open(file, "r") as lineagefile:
                        for line in lineagefile.readlines():
                                line = line.strip("\n")
                                catfile.write(f"{line}\t{file.strip('.txt')}\n")
number_of_klr_samples = os.popen("wc -l klr_lineages | awk '{print $1}'").read().strip('\n')
print(f'Combined {number_of_klr_samples} samples.')
os.system("sort klr_lineages >> klr_lineages_sorted_alphabetically")
os.system("uniq -D klr_lineages_sorted_alphabetically >> klr_dupes")
dupes = os.popen("wc -l klr_dupes | awk '{print $1}'").read().strip('\n')
print(f'{dupes} samples might be duplicates!')
os.system("cut -f1 klr_lineages_sorted_alphabetically >> klr_samples_only")
os.system("rm klr_lineages")


# go through every sample and see what we have for that sample
# first, make sure each sample only appears once - if there are no duplicates this shouldn't be necessary
# but it's possible for a copy error to have occurred
# THIS IS EXTREMELY INEFFECIENT AND BAD
os.chdir("..")
with open("./lineage_run/klr_samples_only", "r") as klr_samples_file:
        for sample in klr_samples_file.readlines():
                print(sample)
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