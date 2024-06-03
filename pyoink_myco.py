#!/usr/bin/python3

# assumes files get copied to workdir and pyoink.py is in workdir too
# hardcoded for myco_sra

import os 
import sys
import argparse
import subprocess

parser = argparse.ArgumentParser(prog="pyoink", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                                description="""Download workflow outputs 
                                                from Terra painlessly. Automatically splits
                                                very large downloads into batches of 998. 
                                                Has two modes of functioning, depending
                                                on whether or not you can load Job Manager
                                                (which tends to break if you scatter >2000x).
                                                REQUIRES you have gsutil set up and authenticated.""")
parser.add_argument('--submission_id', help="submission ID as it appears in Terra", required=True)
parser.add_argument('--workflow_id', help="ID of workflow as it appears in Terra", required=True)
parser.add_argument('--bucket', default="fc-057366ca-c3a6-4847-86b9-82f941bda80c")
parser.add_argument('--reportprefix', default="001")
args = parser.parse_args()

subprocess.run('touch gs_info.txt', shell=True)
subprocess.run(f'echo \"bucket: {args.bucket}\nsubmission_id: {args.submission_id}\nworkflow_id: {args.workflow_id}\n\" >> gs_info.txt', shell=True)

# this is done in order of the smallest downloads first

#### download reports ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "pull_reports.txt" --task "merge_reports" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run(f'mv pull_reports.txt {args.reportprefix}_pull_reports.txt', shell=True)
print("Finished pulling the SRA pull report file.")

subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "strain_reports.tsv" --task "collate_bam_strains" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run(f'mv strain_reports.tsv {args.reportprefix}_strain_reports.tsv', shell=True)
print("Finished pulling the strain report file.")

subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "resistance_reports.tsv" --task "collate_bam_resistance" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run(f'mv resistance_reports.tsv {args.reportprefix}_resistance_reports.tsv', shell=True)
print("Finished pulling the resistance report file.")

### download diff reports ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "*.report"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
print("Finished pulling the diff report files.")

#### download tbprofiler jsons ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "results/*.json" --task "profile_bam"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
print("Finished pulling TBProfiler JSONs.")

#### download tbprofiler txts ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "results/*.txt" --task "profile_bam"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
print("Finished pulling TBProfiler text files.")

#### download diffs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "*.diff"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
print("Finished pulling diffs.")

#### download bedgraphs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "*.bedgraph"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
print("Finished pulling bedgraphs.")

#### download vcfs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "*.vcf" --task "variant_calling"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
print("Finished pulling vcfs.")

subprocess.run('rm downloaded_successfully.txt', shell=True)
subprocess.run('rm failed_to_download.txt', shell=True)
