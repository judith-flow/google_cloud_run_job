# Google Cloud Run Job
Cloud run job on Google Cloud for running ML model automatically

Machine learning model is generated by Pycaret and saved in a pkl file

main.py tries to load input data from BigQuery tables and upload prediction results to Cloud Storage bucket. 

## Why Cloud Run

It is most cost efficient for low frequency App deployment, and performance is in some perspectives even better than the App Engine.

Because Cloud Run can shut down completely, but App Engine needs at least one instance to run.

Reference:

https://dev.to/pcraig3/cloud-run-vs-app-engine-a-head-to-head-comparison-using-facts-and-science-1225

## Deploy
details in file: deploy_steps_powershell.ps1
