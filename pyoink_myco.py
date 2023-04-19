# assumes pyoink.py is in the workdir
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
args = parser.parse_args()

# this is done in order of the smallest downloads first
#### download pull report ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "pull_reports.txt" --task "cat_reports" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_pull.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_pull.txt', shell=True)
print("Finished pulling the SRA pull report file.")

subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "strain_reports.txt" --task "cat_strains" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_strain.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_strain.txt', shell=True)
print("Finished pulling the SRA pull report file.")

subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "resistance_reports.txt" --task "resistance_reports" --not_scattered', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_resistance.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_resistance.txt', shell=True)
print("Finished pulling the SRA pull report file.")

#### download tbprofiler jsons ####
#subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "results/*.json" --task "profile"', 
#                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
#subprocess.run('mv downloaded_successfully.txt downloaded_successfully_tbprf.txt', shell=True)
#subprocess.run('mv failed_to_download.txt failed_to_download_tbprf.txt', shell=True)
#print("Finished pulling TBProfiler JSONs.")

#### download tbprofiler txts ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "results/*.txt" --task "profile"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_tbprftxt.txt', shell=True)
subprocess.run('mv failed_to_download.txt failed_to_download_tbprftxt.txt', shell=True)
print("Finished pulling TBProfiler JSONs.")

#### download diffs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id}', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_diff.txt', shell=True)
print("Finished pulling diffs.")

#### download bedgraphs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "*.bedgraph"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_bdgrph.txt', shell=True)
print("Finished pulling bedgraphs.")

#### download vcfs ####
subprocess.check_call(f'python3 pyoink.py --submission_id {args.submission_id} --workflow_id {args.workflow_id} --file "*.vcf" --task "varcall_with_array"', 
                        shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
subprocess.run('mv downloaded_successfully.txt downloaded_successfully_vcf.txt', shell=True)
print("Finished pulling vcfs.")