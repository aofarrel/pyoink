import os 
import re
import argparse

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

option_a = parser.add_argument_group("""************************\n
                                    Option A\n
                                    Pulling from Job Manager\n
                                    ************************""", 
                                    """If you can load Job Manager in Terra, copy and paste 
                                    the outputs you want to download into a text file. This
                                    will prevent you from having to use gsutil ls commands.""")
option_a.add_argument('-jm', '--job_manager_arrays_file', required=False, type=str, \
    help='path to text file containing all gs addresses, one line per array, copied from Terra Job Manager')

option_b = parser.add_argument_group("""***************************\n
                                    Option B\n
                                    Pulling without Job Manager\n
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
option_b.add_argument('--shards', type=bool, default=True, help="(bool) is this output from a scattered task? if true, gsutil ls will be run to find the number of shards")
option_b.add_argument('--cacheCopy', type=bool, default=False, help="(bool) is this output cached from a previous run?")
option_b.add_argument('--glob', type=bool, default=False, help="(bool) does this output make use of WDL's glob()?")

args = parser.parse_args()
od = args.output_directory
if od[-1] != '/': od = od+'/'
jm = args.job_manager_arrays_file

def read_file(jm):
    with open(gs) as f:
    for line in f:
        line = line.strip()
        if line.startswith('['):
            line = line[1:]
        if line.endswith(']'):
            line = line[:-1]
        line = re.sub(',', ' ', line)
    return gs_addresses

def retrieve_data(gs_addresses):
    uris_as_string = " ".join(gs_addresses)
    try:
        # this is easier than using the subprocess module
        # because the resulting command has a ton of
        # spaces, but generally subprocess is better practice
        command = f"gsutil -m cp {uris_as_string} {od}"
        print(f"Attempting the following command:\n {command}\n\n") if args.verbose else pass
        if os.system(f'gsutil -m cp {uris_as_string} {od}') != 0:
            raise Exception('Failed to download at least one file.')
    except:
        print(f'{line} did not download')



if __name__ == '__main__':
    if jm is not None:
        # option A
        # make sure the user isn't trying to use options A and B
        if None not in (args.submission_id, args.workflow_id):
            raise Exception("jm was passed in, but so was submission and/or workflow ids (see --help)")
        else:
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
            if args.glob == True: base.append("glob*/")
            base.append(f"{args.file}")
            print(f'Constructed path:\n{path}shard-(whatever)/{"".join(base)}')
            shards = list(os.popen(f"gsutil ls {path}"))
            gs_addresses = []
            for shard in shards:
                uri = shard[:-1] + "".join(base)
                gs_addresses.append(f'\"{uri}\"')

    # check if we need multiple gsutil calls
    if len(gs_addresses) > 998:
        print("Splitting into smaller downloads...")
        list_of_smallish_lists_of_uris = [uris[i:i + 998] for i in range(0, len(uris), 998)]
        for smallish_list_of_uris in list_of_lists_of_uris:
            retrieve_data(smallish_list_of_uris)
    else:
        retrieve_data(gs_addresses)

