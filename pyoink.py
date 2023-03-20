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
parser.add_argument('--small-steps', required=False, default=False, type=bool, \
    help='download files in batches of fifty (not recommended if you are downloading more than about 300 files in total)')

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
option_b.add_argument('--try_attempt2_on_failure', type=bool, default=True, help="(bool) if a file can't be downloaded, should we try looking for a second attempt? (useful for preempted tasks)")

args = parser.parse_args()
od = args.output_directory
jm = args.job_manager_arrays_file
verbose = args.verbose

def list_to_set_consistently(some_list: list):
    '''Surely the built-in methods should handle this... hmmm'''
    sorted_list = sorted(some_list, reverse=True)
    uniques = []
    for thingy in sorted_list:
        if thingy not in uniques:
            uniques.append(thingy)
    return {thing for thing in uniques}

def grab_gs_address(single_line_string):
    '''Extract gs:// address from lines that probably contain one.
    This isn't perfect -- you can't pass gsutil's progress bars and
    returning None might cause issues down the line.'''
    #pattern = re.compile("gs:\/\/.+\b") # this works on Regexr but not here
    if single_line_string.endswith("could not be transferred."):
        return None
    else:
        pattern = re.compile("gs:\/\/.+[^...]")
        try:
            result = str(pattern.findall(single_line_string)[0])
        except IndexError:
            print(f"Warning -- could not extract gs address from this line: {single_line_string}")
            return None
        if result is None or result == "":
            print(f"Warning -- could not extract gs address from this line: {single_line_string}")
            return None
        else:
            return result

def determine_what_downloaded(stderr_as_multi_line_string):
    '''Returns a list of lists. The first list is the gs URIs that successfully DL'd.
    The second list is the gs URIs that failed. Both will be written to files.'''
    # called by parse_gsutil_stderr and main
    exceptions = []
    probable_successes = []
    for line in (stderr_as_multi_line_string.splitlines()):
        if line.startswith("CommandException"):
            print(f"gsutil reported {line}")
            gs = grab_gs_address(line)
            if gs is not None:
                exceptions.append(gs)
        elif line.startswith("Copying "):
            probable_successes.append(grab_gs_address(line))
        else: # progress bars, etc
            pass
    # if no Copying... lines, probably 
    # compare gs addresses in both arrays -- NoURLMatched already covered since that
    # doesn't generate a Copying... line, but code below should handle other cases
    exceptions_set = list_to_set_consistently(exceptions)
    probable_successes_set = list_to_set_consistently(probable_successes)
    known_successes_set = probable_successes_set - exceptions_set
    with open("failed_to_download.txt", "a") as f:
        f.writelines(f"{uri}\n" for uri in list(exceptions_set))
    with open("downloaded_successfully.txt", "a") as f:
        f.writelines(f"{uri}\n" for uri in list(known_successes_set))
    return [list(known_successes_set), exceptions]

def parse_gsutil_stderr(stderr_file):
    with open(stderr_file) as f:
        list_of_lines = f.readlines()
        stderr_as_multiline_string = "\n".join([line for line in list_of_lines])
        return determine_what_downloaded(stderr_as_multiline_string)

def read_file(thingy):
    '''Reads input file for the JM (option A) use case, and the exclusion file for both use cases.'''
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
    ''' Actually pull gs:// addresses, after checking it's not too many at a time. This function
    is recursive if a lot of files need to be downloaded.'''
    uris_as_string = " ".join(gs_addresses)

    # first check for gsutil's 1000 argument limit (checks LIST length, not string length)
    if len(gs_addresses) > 998:
        if verbose: print("Approaching gsutil's limit on number of arguments, splitting into smaller batches...")
        list_of_smallish_lists_of_uris = [gs_addresses[i:i + 499] for i in range(0, len(gs_addresses), 499)]
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            #if verbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris)
    
    # now check for the debugging small_steps argument (checks LIST length, not string length)
    if len(gs_addresses) > 50 and args.small_steps is True:
        if verbose: print("small-steps was passed in, so we'll be downloading only 50 files at a time...")
        list_of_smallish_lists_of_uris = [gs_addresses[i:i + 50] for i in range(0, len(gs_addresses), 50)]
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            #if verbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris)
    
    # then check for MAX_ARG_STRLEN (checks STRING length, not list length)
    elif len(uris_as_string) > 131000: # MAX_ARG_STRLEN minus 72
        if verbose: print("Approaching MAX_ARG_STRLEN, splitting into smaller batches...")
        list_of_smallish_lists_of_uris = [download_me[i:i + 499] for i in range(0, len(download_me), 499)]
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            #if verbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris)
   
    # iff all checks pass, actually download (we do this in an else block to avoid downloading twice when recursing) 
    else:
        command = f"gsutil -m cp {uris_as_string} {od}"
        if verbose:
            print(f"Attempting to download {len(gs_addresses)} files via the following command:\n {command}\n\n")
        else:
            print(f"Downloading {len(gs_addresses)} files, please wait...")
        this_download = subprocess.run(f'gsutil -m cp {uris_as_string} {od}', shell=True, capture_output=True, encoding="UTF-8")
        # for some reason gsutil puts everything in stderr and nothing in stdout, so we have to do a lot of parsing to find CommandExceptions
        successes_and_exceptions = determine_what_downloaded(this_download.stderr)
        successes = successes_and_exceptions[0]
        exceptions = successes_and_exceptions[1]
        if args.try_attempt2_on_failure and args.job_manager_arrays_file != "attempt2.tmp":
            print(f"Attempted {len(gs_addresses)} downloads: {len(successes)} succeeded, {len(exceptions)} failed, gsutil returned {this_download.returncode}.")
            print(f"Looking for files in attempt-2/ folders...")
            with open("attempt2.tmp", "a") as f:
                for failed_gs_uri in exceptions:
                    possible_output_after_preempting = failed_gs_uri.removesuffix(args.file) + "attempt-2/" + args.file + "\n"
                    f.write(possible_output_after_preempting)
            # this call mixes a group A and a group B input variable -- but we'll allow that because it helps stop us from recursing infinitely
            with subprocess.Popen(f'python3 foo.py --job_manager_arrays_file attempt2.tmp --small-steps', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as recurse:
                for output in recurse.stdout:
                    print(output.decode("UTF-8"))
                print("for")
            print("with")
            subprocess.run('rm attempt2.tmp', shell=True)
            print("Finished checking for attempt 2.")
        else:
            print(f"Attempted {len(gs_addresses)} downloads: {len(successes)} succeeded, {len(exceptions)} failed, gsutil returned {this_download.returncode}.")


if __name__ == '__main__':
    if jm is not None:
        # option A
        # make sure the user isn't trying to use options A and B
        if args.submission_id is not None or args.workflow_id is not None:
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
    if args.exclude is not None:
        all = set(gs_addresses)
        exclude = set(read_file(args.exclude))
        include = all - exclude
        if verbose:
            print(f"{len(all)} possible URIs detected.")
            print(f"{len(exclude)} URIs in the exclusion file.")
        print(f"{len(include)} URIs will be downloaded -- but maybe not all at once.")
        download_me = list(include)
    else:
        download_me = gs_addresses
    retrieve_data(download_me)

