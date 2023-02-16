import os
import argparse

arg = argparse.ArgumentParser(prog = 'grab scattered terra outs', 
	description = "Download scattered workflow outputs when Terra's UI "
	"isn't cooperating by directly evoking gsutil (REQUIRES gsutil to be set up and authenticated).")
arg.add_argument('--bucket', default="fc-9f0bdb6d-155d-41bc-90af-35ef59252ff8", required=False)
arg.add_argument('--submission_id', default="793a7888-6d9e-468d-a16c-e73a43562c91", required=False)
arg.add_argument('--workflow_name', default="myco", required=False)
arg.add_argument('--workflow_id', default="21dab191-dc30-4897-bec8-f5a6e8bfd462", required=False)
arg.add_argument('--task', default="make_mask_and_diff_", required=False)
arg.add_argument('--file', default="*.sh", required=False)
args = arg.parse_args()


path = f"gs://{args.bucket}/submissions/{args.submission_id}/{args.workflow_name}/{args.workflow_id}/call-{args.task}/"
shards = list(os.popen(f"gsutil ls {path}"))
uris = []
for shard in shards:
	uri = shard[:-1] + f"{args.file}"
	uris.append(uri)

if len(uris) > 998:
	print("Splitting into smaller downloads...")
	list_of_smallish_lists_of_uris = [uris[i:i + 998] for i in range(0, len(uris), 998)]
	for smallish_list_of_uris in list_of_lists_of_uris:
		uris_as_string = " ".join(smallish_list_of_uris)
		command = f"gsutil -m cp {uris_as_string} ."
		print(f"Running {command}\n\n")
		os.system(command)
else:
	uris_as_string = " ".join(uris)
	command = f"gsutil -m cp {uris_as_string} ."
	print(f"Running {command}\n\n")
	os.system(command)