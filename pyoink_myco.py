#!/usr/bin/python3

# assumes files get copied to workdir and pyoink.py is in ../helper_scripts
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
parser.add_argument('--bucket', default="fc-caa84e5a-8ef7-434e-af9c-feaf6366a042")
args = parser.parse_args()

subprocess.run('touch gs_info.txt', shell=True)
subprocess.run(f'echo \"bucket: {args.bucket}\nsubmission_id: {args.submission_id}\nworkflow_id: {args.workflow_id}\n\" >> gs_info.txt', shell=True)

# this is done in order of the smallest downloads first
#### download reports ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "pull_reports.txt" --task "cat_reports" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_pull.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_pull.txt', shell=True)
print("Finished pulling the SRA pull report file.")

subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "strain_reports.txt" --task "cat_strains" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_strain.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_strain.txt', shell=True)
print("Finished pulling the strain report file.")

subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "resistance_reports.txt" --task "cat_resistance" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_resistance.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_resistance.txt', shell=True)
print("Finished pulling the resistance report file.")

### download diff reports ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "*.report"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_diffreports.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_diffreports.txt', shell=True)
print("Finished pulling the diff report files.")

#### download tbprofiler jsons ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "results/*.json" --task "profile"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_tbprf.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_tbprf.txt', shell=True)
print("Finished pulling TBProfiler JSONs.")

#### download tbprofiler txts ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "results/*.txt" --task "profile"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_tbprftxt.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_tbprftxt.txt', shell=True)
print("Finished pulling TBProfiler text files.")

#### download diffs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id}', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_diff.txt', shell=True)
print("Finished pulling diffs.")

#### download bedgraphs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "*.bedgraph"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_bdgrph.txt', shell=True)
print("Finished pulling bedgraphs.")

#### download vcfs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --bucket {args.bucket} --workflow_id {args.workflow_id} --file "*.vcf" --task "varcall_with_array"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_vcf.txt', shell=True)
print("Finished pulling vcfs.")