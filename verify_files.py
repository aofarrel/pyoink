import os
import argparse

arg = argparse.ArgumentParser(prog = 'pyoink verifier', 
	description = "Verify you downloaded every sample's bits and pieces.")
#arg.add_argument('-n', '--number_of_inputs', type=int)
arg.add_argument('--vcf', type=bool, default=True)
arg.add_argument('--diff', type=bool, default=True)
arg.add_argument('--bedgraph', type=bool, default=True)
arg.add_argument('--fastqc', type=bool, default=False)
arg.add_argument('-e', '--exclude_these_samples', type=str, required=False)
arg.add_argument('-i', '--input_samples_file', type=str)
args = arg.parse_args()

vcfs = []
diffs = []
bgs = []
exclude = []
all_samples = []
samples_vcf = []
samples_bgs = []
samples_dif = []
samples_missing = []
ls = os.listdir()

with open(f"{args.input_samples_file}", "r") as biosamples:
	for line in biosamples.readlines():
		all_samples.append(line)

if args.exclude_these_samples:
	with open(f"{args.exclude_these_samples}", "r") as nope:
		for line in nope.readlines():
			exclude.append(line)

n_samples = len(all_samples) - len(exclude)
samples = [sample for sample in all_samples if sample not in exclude]

for file in ls:
	if file.endswith(".vcf") and args.vcf is True:
		vcfs.append(file)
		samples_vcf.append(file.rstrip(".vcf"))
	elif file.endswith(".diff") and args.diff is True:
		diffs.append(file)
		samples_dif.append(file.rstrip(".diff"))
	elif file.endswith(".bedgraph") and args.bedgraph is True:
		bgs.append(file)
		samples_bgs.append(file.rstrip(".bedgraph"))
if args.fastqc is True:
	htmls = []
	ls = os.listdir("additional_outputs/")
	for file in ls:
		if file.endswith(".html"):
			htmls.append(file)

samples.sort()
samples_vcf.sort()
samples_dif.sort()
samples_bgs.sort()

print(f"Total of {len(diffs)} diffs, {len(vcfs)} vcfs, {len(bgs)} bedgraphs.")
if args.vcf is True and len(vcfs) != len(diffs):
	print(f"WARNING: {len(vcfs)} VCFs, {len(diffs)} diffs")
if args.bedgraph is True and len(diffs) != len(bgs):
	print(f"WARNING: {len(bgs)} bedGraphs, {len(diffs)} diffs")
if args.fastqc is True and args.number_of_inputs - len(htmls) != len(diffs):
	print(f"WARNING: Input {args.number_of_inputs}, but that minus {len(htmls)} doesn't match number of diffs ({len(diffs)})")

for sample in samples:
	if sample.strip('\n') in samples_dif:
		pass
	else:
		samples_missing.append(sample)

print(f"Total bioSample accessions: {len(all_samples)}")
print(f"Samples (minus excluded): {len(samples)}")
#print(f"Samples which can be excluded: {len(exclude)}")
print(f"These are the {len(samples_missing)} samples are missing diff files:")
for sample in samples_missing:
	print(sample.strip('\n'))

