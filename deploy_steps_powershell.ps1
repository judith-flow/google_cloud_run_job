# create Google Cloud Run job and schedule them: 
# 1. create environment
# Open power shell, enable the API

gcloud services enable \
artifactregistry.googleapis.com \
cloudbuild.googleapis.com \
run.googleapis.com


# 2. create repository
# upload files through Cloud Editor
# Dockerfile is needed
mkdir ~/my-folder-name
cd ~/my-folder-name


# 3. Deploy
# create environment variables (only valid for one clou session 
export PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NAME=$(gcloud projects describe $PROJECT_ID --format='value(name)')
export REGION=us-central1

# build an image (name should contain dash, no underscore) 
gcloud builds submit --tag gcr.io/$PROJECT_ID/my-image-name

# create cloud run job
gcloud beta run jobs create my-job-name --image gcr.io/$PROJECT_ID/my-image-name --region $REGION

# udpate cloud run job to always use the latest version of image
gcloud beta run jobs update my-job-name --image gcr.io/$PROJECT_ID/my-image-name:latest


# 4. create service account
gcloud iam service-accounts create my-servie-account-name  --display-name "my bigquery service account"

# if cloud run service 
gcloud run services add-iam-policy-binding my-service-name      --role='roles/run.invoker'        --region=$REGION       --member=serviceAccount:my-service-account-namen@$PROJECT_ID.iam.gserviceaccount.com

# if cloud run job
gcloud beta run jobs add-iam-policy-binding my-job-name      --role='roles/run.invoker'        --region=$REGION       --member=serviceAccount:my-service-account-namen@$PROJECT_ID.iam.gserviceaccount.com


# 5. Test run
gcloud beta run jobs execute my-job-name 

# Tips: debug by test run error
# very often it is due to memory limitation, then go to Yaml file to change CPU setting in case of memory limitation.


# 6. Create a Cloud Scheduler Job to trigger the cloud run job
# enable scheduler api service
gcloud services enable cloudscheduler.googleapis.com


# e.g. Create Cloud Scheduler Job for every day at midnight (0:00 UTC time):
# "my-job" will be the one showing on cloud scheduler

gcloud scheduler jobs create http my-scheduler-job-name    
--schedule="0 0 * * *" \    
--uri="https://$REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/my-cloud-run-job-name:run"\
--http-method=POST \
--oauth-service-account-email=my-service-account-name@$PROJECT_ID.iam.gserviceaccount.com\
--location $REGION


