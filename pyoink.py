import os 
import re
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
parser.add_argument('-od', '--output_directory', required=False, default=".", type=str, \
    help='directory for all outputs (make sure it has enough space!)')
parser.add_argument('-v', '--verbose', required=False, default=False, type=bool, \
    help='print out gsutil download commands to stdout before running them')
parser.add_argument('-e', '--exclude', required=False, \
    help='file of gs URIs (one per line) to exclude when downloading')

option_a = parser.add_argument_group("""\n
                                            Option A:
                                    Pulling from Job Manager
                                    ************************""", 
                                    """If you can load Job Manager in Terra, copy and paste 
                                    the outputs you want to download into a text file. This
                                    will prevent you from having to use gsutil ls commands.""")
option_a.add_argument('-jm', '--job_manager_arrays_file', required=False, type=str, \
    help='path to text file containing all gs addresses, one line per array, copied from Terra Job Manager')

option_b = parser.add_argument_group("""\n
                                            Option B:
                                    Pulling without Job Manager
                                    ***************************""",
                                    """If you cannot load Job Manager, define the listed arguments
                                    to pull your outputs. Unlike option A, only one task's outputs 
                                    at a time are supported.""")
option_b.add_argument('--bucket', default="fc-caa84e5a-8ef7-434e-af9c-feaf6366a042", help="workspace bucket")
option_b.add_argument('--submission_id', help="submission ID as it appears in Terra")
option_b.add_argument('--workflow_name', default="myco", help="""name of workflow as it appears in the WDL
                                                    on the line that starts with 'workflow'. this
                                                    name might not match Dockstore name or filename
                                                    of the WDL.""")
option_b.add_argument('--workflow_id', help="ID of workflow as it appears in Terra")
option_b.add_argument('--task', default="make_mask_and_diff", help="name of WDL task")
option_b.add_argument('--file', default="*.diff", help="filename (asterisks are supported)")
option_b.add_argument('--attempt2', type=bool, default=False, help="(bool) is this output from a second attempt of the task?")
option_b.add_argument('--shards', type=bool, default=True, help="(bool) is this output from a scattered task? if true, gsutil ls will be run to find the number of shards")
option_b.add_argument('--cacheCopy', type=bool, default=False, help="(bool) is this output cached from a previous run?")
option_b.add_argument('--glob', type=bool, default=False, help="(bool) does this output make use of WDL's glob()?")

args = parser.parse_args()
od = args.output_directory
jm = args.job_manager_arrays_file

def read_file(thingy):
    gs_addresses = []
    with open(thingy) as f:
        for line in f:
            line = line.strip()
            if line.startswith('['):
                line = line[1:]
            if line.endswith(']'):
                line = line[:-1]
            line = re.sub(',', ' ', line)
            gs_addresses.append(line)
    return gs_addresses

def retrieve_data(gs_addresses: list):
    print(len(gs_addresses))
    print(type(gs_addresses))
    # first check for gsutil's 1000 argument limit
    if len(gs_addresses) > 998:
        print("Approaching gsutil's limit on number of arguments, splitting into smaller downloads...")
        list_of_smallish_lists_of_uris = [gs_addresses[i:i + 499] for i in range(0, len(gs_addresses), 499)]
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            #if args.verbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris)
    
    # then, convert to string and check for MAX_ARG_STRLEN
    uris_as_string = " ".join(gs_addresses)
    if len(uris_as_string) > 131000: # MAX_ARG_STRLEN minus 72
        print("Approaching MAX_ARG_STRLEN, splitting into smaller downloads...")
        list_of_smallish_lists_of_uris = [download_me[i:i + 499] for i in range(0, len(download_me), 499)]
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            #if args.verbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris)
    command = f"gsutil -m cp {uris_as_string} {od}"
    if args.verbose: print(f"Attempting the following command:\n {command}\n\n")
    #subprocess.run(f'gsutil -m cp {uris_as_string} {od}', shell=True, capture_output=True, encoding="UTF-8")

if __name__ == '__main__':
    if jm is not None:
        # option A
        # make sure the user isn't trying to use options A and B
        if args.submission_id is not None or args.workflow_id is not None:
            raise Exception("jm was passed in, but so was submission and/or workflow ids (see --help)")
        else:
            print(args.submission_id, args.workflow_id)
            gs_addresses = read_file(jm)
    else:
        # option B
        # make sure all non-default option B args are set
        if None in (args.submission_id, args.workflow_id):
            raise Exception("no jm file was passed in, but we dont have enough arguments for option B (see --help)")
        else:
            path = f"gs://{args.bucket}/submissions/{args.submission_id}/{args.workflow_name}/{args.workflow_id}/call-{args.task}/"
            base = []
            if args.cacheCopy == True: base.append("cacheCopy/")
            if args.attempt2 == True: base.append("attempt-2/")
            if args.glob == True: base.append("glob*/")
            base.append(f"{args.file}")
            print(f'Constructed path:\n{path}shard-(whatever)/{"".join(base)}')
            shards = list(os.popen(f"gsutil ls {path}"))
            gs_addresses = []
            for shard in shards:
                uri = shard[:-1] + "".join(base)
                gs_addresses.append(f'{uri}')

    # get rid of anything that should be excluded
    all = set(gs_addresses)
    exclude = set(read_file(args.exclude))
    include = all - exclude
    #if args.verbose:
        #print(f"Full set of URIs:\n {all}")
        #print(f"URIs to exclude:\n {exclude}")
        #print(f"URIs that will be downloaded:\n {include}")
    
    # check if we need multiple gsutil calls to fall within the limits gsutil -m cp 
    # additional checks for MAX_ARG_STRLEN etc are in retrieve_data())
    download_me = list(include)
    retrieve_data(download_me)

