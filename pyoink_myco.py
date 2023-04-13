# assumes pyoink.py is in the workdir
# hardcoded for myco_sra

import os 
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
parser.add_argument('--submission_id', help="submission ID as it appears in Terra")
parser.add_argument('--workflow_id', help="ID of workflow as it appears in Terra")
args = parser.parse_args()

# this is done in order of the smallest downloads first

#### download pull reports ####
with subprocess.Popen(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "pull_reports.txt" --task "cat_reports"', 
                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as pyoink_tbprf:
    for output in pyoink_tbprf.stdout:
        print(f'pyoink_report-->{output.decode("UTF-8")}')
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_report.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_report.txt', shell=True)
print("Finished pulling the SRA pull report file.")

#### download tbprofiler jsons ####
with subprocess.Popen(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "results/*.json" --task "profile"', 
                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as pyoink_tbprf:
    for output in pyoink_tbprf.stdout:
        print(f'pyoink_tbprf-->{output.decode("UTF-8")}')
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_tbprf.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_tbprf.txt', shell=True)
print("Finished pulling TBProfiler JSONs.")

#### download diffs ####
with subprocess.Popen(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id}', 
                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as pyoink_diff:
    for output in pyoink_diff.stdout:
        print(f'pyoink_diff-->{output.decode("UTF-8")}')
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_diff.txt', shell=True)
print("Finished pulling diffs.")

#### download bedgraphs ####
with subprocess.Popen(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "*.bedgraph""', 
                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as pyoink_bdgrph:
    for output in pyoink_bdgrph.stdout:
        print(f'pyoink_bdgrph-->{output.decode("UTF-8")}')
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_bdgrph.txt', shell=True)
print("Finished pulling bedgraphs.")

#### download vcfs ####
with subprocess.Popen(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "*.vcf" --task "varcall_with_array"', 
                        shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as pyoink_vcf:
    for output in pyoink_vcf.stdout:
        print(f'pyoink_vcf-->{output.decode("UTF-8")}')
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_vcf.txt', shell=True)
print("Finished pulling vcfs.")