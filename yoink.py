import os 
import re
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-gs', '--gs_addresses', required=True, type=str, \
    help='path to text file containing all gs addresses, one line per array')
parser.add_argument('-od', '--output_directory', required=False, default=".", type=str, \
    help='directory for all outputs (make sure this directory will have enough space!!!!)')

args = parser.parse_args()

gs = args.gs_addresses
od = args.output_directory

if od[-1] != '/':
    od = od+'/'

def retrieve_data(gs):
    with open(gs) as f:
        for line in f:
            line = line.strip()
            if line.startswith('['):
                line = line[1:]
            if line.endswith(']'):
                line = line[:-1]
            line = re.sub(',', ' ', line)
            try:
                # this is easier than using the subprocess module
                # because the resulting command has a ton of
                # spaces, but generally subprocess is better practice
                if os.system(f'gsutil -m cp {line} {od}') != 0:
                    raise Exception('Darn!')
            except:
                print(f'{line} did not download')
                

retrieve_data(gs)