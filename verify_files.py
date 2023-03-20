import os
import argparse

arg = argparse.ArgumentParser(prog = 'pyoink verifier', 
	description = "Verify you downloaded every sample's bits and pieces.")
arg.add_argument('--vcf', type=bool, default=True)
arg.add_argument('--diff', type=bool, default=True)
arg.add_argument('--bedgraph', type=bool, default=True)
arg.add_argument('--fastqc', type=bool, default=False)
arg.add_argument('--number_of_inputs', type=int, default=1000)
args = arg.parse_args()

vcfs = []
diffs = []
bgs = []
ls = os.listdir()
for file in ls:
	if file.endswith(".vcf") and args.vcf is True:
		vcfs.append(file)
	elif file.endswith(".diff") and args.diff is True:
		diffs.append(file)
	elif file.endswith(".bedgraph") and args.bedgraph is True:
		bgs.append(file)
if args.fastqc is True:
	htmls = []
	ls = os.listdir("additional_outputs/")
	for file in ls:
		if file.endswith(".html"):
			htmls.append(file)

print(f"Total of {len(diffs)} diff files found.")
if args.vcf is True and len(vcfs) != len(diffs):
	print(f"WARNING: {len(vcfs)} VCFs, {len(diffs)} diffs")
if args.bedgraph is True and len(diffs) != len(bgs):
	print(f"WARNING: {len(bgs)} bedGraphs, {len(diffs)} diffs")
if args.fastqc is True and args.number_of_inputs - len(htmls) != len(diffs):
	print(f"WARNING: Input {args.number_of_inputs}, but that minus {len(htmls)} doesn't match number of diffs ({len(diffs)})")