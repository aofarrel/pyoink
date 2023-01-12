# yoink
 Small Python 3 script to convert outputs from how they look in Terra's UI to something you can actually paste into the command line.

 The files in example.txt are private, it's just an example of how your text file should look. `gsutil cp` can only handle up to 999 files at a time, so if any of your output arrays have > 999 files, you'll need to break it down into a new array on a new line in your input text file.

## where do I find my outputs in Terra's UI?

![screenshot of job history page on terra with mouse hovering over job manager link](./terra_job_history.png)

![screenshot of job manager on terra with an array of outputs highlighted](./terra_job_manager.png)