#!/usr/bin/python3

import os 
import re
import argparse
import subprocess

parser = argparse.ArgumentParser(prog="pyoink", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                                description="""Download workflow outputs 
                                                from Terra painlessly. Automatically splits
                                                very large downloads into smaller batches. Gets your
                                                scattered task outputs even when Terra's UI breaks.
                                                Keeps track of what downloaded and what didn't.
                                                Restart functionality (via --exclude and a previous
                                                run's downloaded_successfully file) so you don't need
                                                to start over if your computer disconnects from the net.

                                                Pyoink can download files by either inputting a
                                                text file full of gs URIs (useful if you can load Job
                                                Manager), or by inputting information about the task
                                                you want to pull outputs from (useful when Job Manager's
                                                UI breaks, which tends to happen if a workflow launches
                                                more than about 4000 VMs). In the second case, pyoink
                                                will automatically find scattered task outputs and
                                                outputs from preempted VMs.

                                                Pyoink REQUIRES you have gsutil set up, authenticated,
                                                and on your path.""",
                                                epilog="Author: Ash O'Farrell (UCSC Pathogen Genomics)")
parser.add_argument('-od', '--output_directory', required=False, default=".", type=str, \
    help='directory for all outputs (make sure it has enough space!)')
parser.add_argument('-v', '--verbose', required=False, action='store_true', \
    help='print out status of each call to gsutil cp')
parser.add_argument('-vv', '--veryverbose', required=False, action='store_true', \
    help='print out gsutil download commands to stdout before running them, and status of each call to gsutil cp')
parser.add_argument('-e', '--exclude', required=False, \
    help='file of gs URIs (one per line) to exclude when downloading')
parser.add_argument('--small-steps', required=False, action='store_true', \
    help='download files in batches of fifty (not recommended if you are downloading more than about 300 files in total)')
parser.add_argument('-f', '--exceptions_file', required=False, default="failed_to_download.txt", \
    help='log file listing all files that failured to download (may be inaccurate if recursing)')

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
                                    at a time are supported.\n
                                    
                                    Let's say pyoink detects 5000 shards in a task and is looking for 
                                    a file with the extension bam. After getting the first 499 shards 
                                    (50 if small-steps), and assuming not_scattered, cacheCopy, glob,
                                    and do_not_attempt2_on_failure are all on their default false, 
                                    pyoink will search for outputs in this order:\n
                                    1. Regular outputs:\n
                                    gs://bucket/submissions/submission_id/workflow_name/workflow_id/call_task/shard-xxx/*.bam\n
                                    2. Outputs after an initial failure (common if using preemptibles):\n
                                    gs://bucket/submissions/submission_id/workflow_name/workflow_id/call_task/shard-xxx/attempt-2/*.bam\n
                                    3. Call cached outputs:\n
                                    gs://bucket/submissions/submission_id/workflow_name/workflow_id/call_task/shard-xxx/cacheCopy/*.bam\n
                                    
                                    If #1 successfully download 490 files, then the 10 failures will move on to #2. 
                                    If 5 files fail #2, then those 5 failures will move on to #3.
                                    
                                    It's simple! (Kind of.)""")
option_b.add_argument('--bucket', default="fc-d11c2e78-5175-400e-b517-6b070fad43b6", help="workspace bucket")
option_b.add_argument('--submission_id', help="submission ID as it appears in Terra")
option_b.add_argument('--workflow_name', default="myco", help="""name of workflow as it appears in the WDL
                                                    on the line that starts with 'workflow'. this
                                                    name might not match Dockstore name or filename
                                                    of the WDL.""")
option_b.add_argument('--workflow_id', help="ID of workflow as it appears in Terra")
option_b.add_argument('--task', default="make_mask_and_diff_no_covstats", help="name of WDL task")
option_b.add_argument('--file', default="*.diff", help="filename (wildcards and subfolders below the workdir are supported, e.g. outputs/*.bam)")

option_b.add_argument('--not_scattered', action="store_true", help="outputs are not from a scattered task (skips running gsutil ls)")
option_b.add_argument('--cacheCopy', action="store_true", help="""(do not use unless you know what you're doing) are *all* outputs definitely
                                                                in callCache folders? if only some of your outputs are in callCache folders,
                                                                leave this variable alone.""")
option_b.add_argument('--glob', action="store_true", help="does this output make use of WDL's glob()?")
option_b.add_argument('--do_not_attempt2_on_failure', action="store_true", help="(do not use unless you know what you're doing) do not check for attempt-2 folders (ie, assume your task is not using preemptibles) nor callCache")

args = parser.parse_args()
od = args.output_directory
jm = args.job_manager_arrays_file
if args.veryverbose is True:
    veryverbose = True
    verbose = True
elif args.verbose is True:
    veryverbose = False
    verbose = True
else:
    veryverbose = False
    verbose = False

global_successes = []

def list_to_set_consistently(some_list: list):
    '''Surely the built-in methods should handle this... hmmm'''
    sorted_list = sorted(some_list, reverse=True)
    uniques = []
    for thingy in sorted_list:
        if thingy not in uniques:
            uniques.append(thingy)
    return {thing for thing in uniques}

def grab_gs_address(single_line_string):
    '''Extract gs:// address from gsutil stderr line that probably contain one.
    This isn't perfect -- you can't pass gsutil's progress bars and
    returning None might cause issues down the line.'''
    if single_line_string.endswith("could not be transferred."):
        return None
    else:
        #pattern = re.compile("gs:\/\/.+\b") # this works on Regexr but not here
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

def determine_what_downloaded(stderr_as_multi_line_string, exceptions_file):
    '''Returns a list of lists. The first list is the gs URIs that successfully DL'd.
    The second list is the gs URIs that failed. Both will be written to files.'''
    # called by parse_gsutil_stderr and main
    exceptions = []
    probable_successes = []
    for line in (stderr_as_multi_line_string.splitlines()):
        if line.startswith("CommandException"):
            if veryverbose: print(f"gsutil reported {line}")
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
    try:
        with open(exceptions_file, "a") as f:
            f.writelines(f"{uri}\n" for uri in list(exceptions_set))
    except FileNotFoundError:
        with open(exceptions_file, "w") as f:
            f.writelines(f"{uri}\n" for uri in list(exceptions_set))
    with open("downloaded_successfully.txt", "a") as f:
        f.writelines(f"{uri}\n" for uri in list(known_successes_set))
    return [list(known_successes_set), exceptions]

def indent(depth, batch):
    return f"{fill_with('    ', depth)}{fill_with('+', batch)}"

def fill_with(char, number):
    '''goofy indentation-for-prints thingy'''
    soon_to_be_string = []
    for i in range(0, number):
        soon_to_be_string.append(char)
    return "".join(soon_to_be_string)

def debug_count_lines(some_file, notes):
    '''debug function that counts lines in a file and prints other info'''
    try:
        with open(some_file, 'r') as some_file_handler:
            contents = some_file_handler.readlines()
            print(f'{some_file}: {len(contents)} lines -- {notes}')
            print(f'first line: {contents[0]}')
    except FileNotFoundError:
        print(f"Tried to read {some_file} w/ circumstances {notes} but file wasn't found")

def parse_gsutil_stderr(stderr_file):
    with open(stderr_file) as f:
        list_of_lines = f.readlines()
        stderr_as_multiline_string = "\n".join([line for line in list_of_lines])
        return determine_what_downloaded(stderr_as_multiline_string)

def read_jm_file(thingy):
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
            list_of_files = line.split('  ')  # Terra puts two spaces between each URI
            gs_addresses.append(list_of_files)
    gs_addresses_flat = [uri for line_of_uris in gs_addresses for uri in line_of_uris]
    return gs_addresses_flat

def retrieve_data(gs_addresses: list, depth=0, batch=0):
    ''' Actually pull gs:// addresses, after checking it's not too many at a time. This function
    is recursive if a lot of files need to be downloaded.'''
    uris_as_string = " ".join(gs_addresses)

    if verbose: print(f"{indent(depth,batch)}Processing {len(gs_addresses)} gs_addresses")

    # first check for gsutil's 1000 argument limit (checks LIST length, not string length)
    if len(gs_addresses) > 998:
        if verbose: print("Approaching gsutil's limit on number of arguments, splitting into smaller batches...")
        list_of_smallish_lists_of_uris = [gs_addresses[i:i + 499] for i in range(0, len(gs_addresses), 499)]
        batch = 0
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            batch += 1
            if verbose: print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Batch {batch} out of {len(list_of_smallish_lists_of_uris)}:")
            if veryverbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris, depth=depth+1, batch=batch)
    
    # now check for the debugging small_steps argument (checks LIST length, not string length)
    elif len(gs_addresses) > 50 and args.small_steps is True:
        if verbose: print("small-steps was passed in, so we'll be downloading only 50 files at a time...")
        list_of_smallish_lists_of_uris = [gs_addresses[i:i + 50] for i in range(0, len(gs_addresses), 50)]
        batch = 0
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            batch += 1
            if verbose: print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Batch {batch} out of {len(list_of_smallish_lists_of_uris)}:")
            if veryverbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris, depth=depth+1, batch=batch)
    
    # then check for MAX_ARG_STRLEN (checks STRING length, not list length)
    elif len(uris_as_string) > 131000: # MAX_ARG_STRLEN minus 72
        if verbose: print("Approaching MAX_ARG_STRLEN, splitting into smaller batches...")
        list_of_smallish_lists_of_uris = [download_me[i:i + 499] for i in range(0, len(download_me), 499)]
        batch = 0
        for smallish_list_of_uris in list_of_smallish_lists_of_uris:
            batch += 1
            if verbose: print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Batch {batch} out of {len(list_of_smallish_lists_of_uris)}:")
            if veryverbose: print(f"Downloading this subset:\n {smallish_list_of_uris}")
            retrieve_data(smallish_list_of_uris, depth=depth+1, batch=batch)
   
    # iff all checks pass, actually download (we do this in an else block to avoid downloading twice when recursing) 
    else:
        command = f"gsutil -m cp {uris_as_string} {od}"
        if veryverbose:
            print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Attempting to download {len(gs_addresses)} files via the following command:\n {command}\n\n")
        else:
            print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Downloading {len(gs_addresses)} files, please wait...")
        this_download = subprocess.run(f'gsutil -m cp {uris_as_string} {od}', shell=True, capture_output=True, encoding="UTF-8")
        # for some reason gsutil puts everything in stderr and nothing in stdout, so we have to do a lot of parsing to find CommandExceptions
        # todo: we could do even more parsing and subprocess.check_call to maybe get gsutil's progress bars!
        # see: https://stackoverflow.com/questions/33028298/prevent-string-being-printed-python
        if veryverbose: debug_count_lines("gsutil.stderr", "just before overwrite in the big else")

        # write gsutil stderr to a file, so we can parse it when recursing
        with open("gsutil.stderr", "w") as f:
            for line in this_download.stderr:
                f.write(line)
        if veryverbose: debug_count_lines("gsutil.stderr", "just after overwrite in the big else")

        # we don't need to parse the file we just wrote here since we still have this_download.stderr in memory
        successes_and_exceptions = determine_what_downloaded(this_download.stderr, exceptions_file=args.exceptions_file)
        successes = successes_and_exceptions[0]
        exceptions = successes_and_exceptions[1]
        global_successes.append(successes)
        print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Attempted {len(gs_addresses)} downloads: {len(successes)} succeeded, {len(exceptions)} failed, gsutil returned {this_download.returncode}.")

        # check for attempt-2/ if any downloads failed
        if len(exceptions) > 0 and args.job_manager_arrays_file != "attempt2.tmp" and not args.do_not_attempt2_on_failure:
            print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Looking for {len(exceptions)} files in attempt-2/ folders...")
            if veryverbose: debug_count_lines("attempt2.tmp", "just before overwrite")

            with open("attempt2.tmp", "w") as f:
                for failed_gs_uri in exceptions:
                    possible_output_after_preempting = failed_gs_uri.removesuffix(args.file) + "attempt-2/" + args.file + "\n"
                    f.write(possible_output_after_preempting)
            if veryverbose: debug_count_lines("attempt2.tmp", "just after overwrite")
            
            # this call mixes a group A and a group B input variable -- but we'll allow that because it helps stop us from recursing infinitely
            with subprocess.Popen(f'python3 pyoink.py -f "attempt-2-exceptions.txt" --job_manager_arrays_file "attempt2.tmp" --do_not_attempt2_on_failure', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as recurse:
                for output in recurse.stdout:
                    print(f'{fill_with("    ", depth+1)}{fill_with("+", batch)}{output.decode("UTF-8")}')
                if veryverbose: debug_count_lines("gsutil.stderr", "just before read in attempt-2 block")
                
                # get success and exceptions from attempt-2
                with open("gsutil.stderr", "r") as f:
                    attempt_2_stderr = f.read()
                attempt_2_successes_and_exceptions = determine_what_downloaded(attempt_2_stderr, exceptions_file="attempt-2-exceptions.txt")
                attempt_2_successes = attempt_2_successes_and_exceptions[0]
                attempt_2_exceptions = attempt_2_successes_and_exceptions[1]
                global_successes.append(attempt_2_successes)
            if veryverbose: debug_count_lines("attempt2.tmp", "just before deletion")
            subprocess.run('rm attempt2.tmp', shell=True)

            # check for call-cache/ if any attempt-2/ downloads failed
            if len(attempt_2_exceptions) > 0:
                print(f"{fill_with('    ', depth)}{fill_with('+', batch)}Looking for {len(attempt_2_exceptions)} files in cacheCopy/ folders...")
                with open("cache_copy.tmp", "a") as f:
                    for failed_gs_uri in attempt_2_exceptions:
                        possible_output_if_call_cached = failed_gs_uri.removesuffix(args.file).removesuffix("attempt-2/") + "cacheCopy/" + args.file + "\n"
                        f.write(possible_output_if_call_cached)
                # this call mixes a group A and a group B input variable -- but we'll allow that because it helps stop us from recursing infinitely
                with subprocess.Popen(f'python3 pyoink.py --job_manager_arrays_file cache_copy.tmp --do_not_attempt2_on_failure -v', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as recurse:
                    for output in recurse.stdout:
                        print(f'{fill_with("    ", depth+1)}{fill_with("+", batch)}{output.decode("UTF-8")}')
                subprocess.run('rm cache_copy.tmp', shell=True)

        else:
            pass
            #print(f"Attempted {len(gs_addresses)} downloads: {len(successes)} succeeded, {len(exceptions)} failed, gsutil returned {this_download.returncode}.")



if __name__ == '__main__':
    if verbose: "Welcome to pyoink!"
    if jm is not None:
        # option A
        # make sure the user isn't trying to use options A and B
        if args.submission_id is not None or args.workflow_id is not None:
            raise Exception("jm was passed in, but so was submission and/or workflow ids (see --help)")
        else:
            gs_addresses = read_jm_file(jm)
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
            if args.not_scattered == False:
                print(f'Constructed path:\n{path}shard-(whatever)/{"".join(base)}')
                shards = list(os.popen(f"gsutil ls {path}"))
                gs_addresses = []
                for shard in shards:
                    uri = shard[:-1] + "".join(base)
                    gs_addresses.append(f'{uri}')
            else:
                gs_addresses = [f'{path}{"".join(base)}']
                print(f'Constructed path:\n {gs_addresses[0]}')

    # get rid of anything that should be excluded
    if args.exclude is not None:
        all = set(gs_addresses)
        exclude = set(read_jm_file(args.exclude))
        include = all - exclude
        if verbose:
            print(f"{len(all)} possible URIs detected.")
            print(f"{len(exclude)} URIs in the exclusion file.")
        print(f"{len(include)} URIs will be downloaded -- but maybe not all at once.")
        download_me = list(include)
    else:
        download_me = gs_addresses
    #global expected_number_of_downloads = len(download_me)
    retrieve_data(download_me)

