# pyoink
 Terra's UI makes it near-impossible to move your workflow outputs out of a workspace bucket en masse. It doesn't help that gsutil only wants to download 999 files in parallel, and that call cacheing and retries can put output files in unexpected folders. This Python script handles all that nonsense for you. Make sure you already have [gcloud CLI](https://cloud.google.com/sdk/gcloud) set up on whatever machine is facilitating the transfer, logged in as a user with write access to the Terra workspace.

 A wrapper specifically for [myco](github.com/aofarrel/myco) (aka "the first part of TB-D") is provided to make Option B easier.

 ## option A: copy-paste from Job Manager

 `python3 pyoink.py -jm [textfile_of_gs_addresses]`
 
 If you can get into Job Manager, you'll want this option. Just copy-paste as many output strings/arrays as you'd like into a text file, then feed that into pyoink. You can include any arbitrary files in the workspace bucket, even across different tasks or workflow runs.

![screenshot of job history page on terra with mouse hovering over job manager link](./terra_job_history.png)

![screenshot of job manager on terra with an array of outputs highlighted](./terra_job_manager.png)

## option B: search bucket recursively
If Job Manager refuses to load, you can enter the `--bucket`, `--submission_id`, `--task`, `--workflow_id`, and `--workflow_name` to sniff out the `--file` you're looking for, across any and all shards. If output basenames vary by shard, you can use wildcards, such as `results/*.json` or `*.vcf`. However, if outputs make use of WDL's `glob()` feature, make sure to also set `--glob`. By default it will be assumed your task is scattered, if not, use the `--notscattered` flag.

Because option B uses a lot of `gsutil ls` commands, it is slower than option A and may incur some additional costs.

## caveats
If you have multiple files with the same basename, they will simply overwrite each other. [This is a limitation of gsutil itself](https://github.com/GoogleCloudPlatform/gsutil/issues/372).

There are rare circumstances where some output is generated, but then the VM gets prempted during delocalization. This may result in a structure like this:
📁 shard-64
|-SAMEA10029809.decontam.counts.tsv
|-SAMEA10029809_2.decontam.fq.gz
|-fastp_decontam_check-64.log
|-📁 attempt-2
|--SAMEA10029809.decontam.counts.tsv
|--SAMEA10029809_1.decontam.fq.gz
|--SAMEA10029809_2.decontam.fq.gz
|--fastp_decontam_check-64.log

If you were to glob on `*decontam*` as your `--file`, you would end up with only SAMEA10029809.decontam.counts.tsv and SAMEA10029809_2.decontam.fq.gz, never picking up SAMEA10029809_1.decontam.fq.gz as pyoink found at least one file to satisfy the glob and would not attempt to search for an attempt-2 folder.


