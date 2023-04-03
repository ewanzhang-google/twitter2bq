https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
# Twitter2BQ
Twitter2BQ is a Google Cloud Dataflow pipeline that can fetch recent tweets and store them in BigQuery. It reads from the [Twitter API v2 Search Tweet](https://developer.twitter.com/en/docs/twitter-api/tweets/search/introduction) functionality by leveraging the [`searchtweets-v2`](https://pypi.org/project/searchtweets-v2/) Python library from Twitter. The pipeline is batched (bounded) and intended to be run on a recurring schedule.

## ToDo
- [Done] Add instructions on how to schedule the pipeline
- [Done] Extend examples of how to use searchtweets api

## Installation

### Before you begin
1. Create or select a Google Cloud project.
1. Make sure that billing is enabled for your Cloud project. Learn how to [check if billing is enabled on a project](https://cloud.google.com/billing/docs/how-to/verify-billing-enabled). 
1. Open Cloud Shell by clicking the "Activate Cloud Shell" button at the top of the Google Cloud console. 
1. Enable required Cloud APIs:
    ```
    gcloud services enable dataflow compute_component logging storage_component storage_api bigquery pubsub datastore.googleapis.com cloudresourcemanager.googleapis.com
    ```
1. Grant roles to your Google Account. Run the following command once for each of the following IAM roles: ```roles/iam.serviceAccountUser```
    ```
    gcloud projects add-iam-policy-binding PROJECT_ID --member="user:EMAIL_ADDRESS" --role=ROLE
    ```
    * Replace ```PROJECT_ID``` with your project ID.
    * Replace ```EMAIL_ADDRESS``` with your email address.
    * Replace ```ROLE``` with each role listed above.
1. Create a Cloud Storage Bucket and configure it as follows:
    * Set the Storage Class to `S` (Standard)
    * Set the Storage location to the following: `US` (United States)
    * Replace `BUCKET_NAME` with a unique bucket name. Don't include sensitive information in the bucket name because the bucket namespace is global and publicly visible. 
    ```
    gsutil mb -c STANDARD -l US gs://BUCKET_NAME
    ```
1. Grant roles to your Compute Engine default service account. Run the following command once for each of the following IAM roles: `roles/dataflow.admin`, `roles/dataflow.worker`, and `roles/storage.objectAdmin`.
    ```
    gcloud projects add-iam-policy-binding PROJECT_ID --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" --role=SERVICE_ACCOUNT_ROLE
    ```
    * Replace `PROJECT_ID` with your project ID.
    * Replace `PROJECT_NUMBER` with your project number. To find your project number, see [Identify projects](https://cloud.google.com/resource-manager/docs/creating-managing-projects#identifying_projects) or use the ```gcloud projects describe``` command.
    * Replace `SERVICE_ACCOUNT_ROLE` with each individual role listed above. 

### Configure Twitter access
1. Gain access to the Twitter API and generate a API Key and Secret by following [these steps on the Twitter Developer's Console](https://developer.twitter.com/en/docs/twitter-api/getting-started/getting-access-to-the-twitter-api).
1. Create a `credentials.yaml` file in the following format
    ```
    search_tweets_v2:
        endpoint: https://api.twitter.com/2/tweets/search/recent
        consumer_key: API_KEY
        consumer_secret: API_SECRET
    ```
    * Replace `API_KEY` with your API KEY from the Twitter Developer's Console.
    * Replace `API_SECRET` with your API Secret from the Twitter Developer's Console.
1. In Google Cloud Storage bucket you created earlier (`BUCKET_NAME`), create a folder called `twitter2bq`.
1. Upload the `credentials.yaml` file to your Google Cloud Storage bucket in the `twitter2bq` folder.
### Configure your Twitter Search queries
1. Create a `queries.txt` file. Each line should represent a query that you'd like to make on Twitter. Follow the Twitter documentation on [building queries for search tweets](https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query) for more inforamtion. 

    For example, the following file would return Tweets where @Google is mentioned, while excluding retweets:
    ```
    @Google -is:retweet
    ```

    For another example, the following file would return Tweets where @Google or @GoogleCloud or @YouTube or @Alphabet is mentioned in english language, while excluding retweets and any keywords that involve 'holidays':
    ```
    (@Google OR @GoogleCloud OR @YouTube OR @Alphabet) lang:en -is:retweet -holidays
    ```
1. Upload the `queries.txt` file to your Google Cloud Storage bucket in the `twitter2bq` folder. 
## Run the pipeline
1. Open Cloud Shell by clicking the "Activate Cloud Shell" button at the top of the Google Cloud console. 
1. Clone this repository to your Cloud Shell environment.
    ```
    git clone https://github.com/TODO-UPDATE-URL.git
    ```
1. Move into the cloned repository.
    ```
    cd ./data-ingestion/twitter2bq
    ```
1. Install prerequisites Python packages to your Cloud Shell environment.
    ```
    pip install -r requirements.txt
    ```
1. You can choose to run the pipeline either locally:
    ```
    python twitter2bq.py \
    --gcp_project_id PROJECT_ID \
    --gcs_bucket_id BUCKET_ID \
    --bq_project_id BQ_PROJECT_ID \
    --bq_dataset_name BQ_DATASET_NAME
    ```
    
    or remotely on GCP:
    ```
    python -m twitter2bq \
    --region GCP_REGION \
    --runner DataflowRunner \
    --project PROJECT_ID \
    --temp_location gs://BUCKET_ID/dataflow_tmp/ \
    --requirements_file requirements.txt \
    --gcp_project_id PROJECT_ID \
    --gcs_bucket_id BUCKET_ID \
    --bq_project_id BQ_PROJECT_ID \
    --bq_dataset_name BQ_DATASET_NAME
    ```

    * Replace ```PROJECT_ID``` with your project ID.
    * Replace ```BUCKET_ID``` with your Cloud Storage bucket ID.
    * Replace ```BQ_PROJECT_ID``` with the project ID of the BigQuery project you'd like to store the pipeline's output.
    * Replace ```BQ_DATASET_NAME``` with the name of the BigQuery dataset you'd like to store the pipeline's output. 
    * Replace ```GCP_REGION``` with the name of the GCP region you'd like to operate in. (Example: `us-central1`)
    * You can optionally add --max_tweets ```NUMBER_BETWEEN_10_AND_100``` towards the end of the parameters list to specify the number of tweets you want to return per call (default 10; max 100)

## Convert to Flex template and run on schedule
### Build custom Docker image and template spec file
1. Create a Dockerfile
    ```
    FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

    ARG WORKDIR=/dataflow/template
    RUN mkdir -p ${WORKDIR}
    WORKDIR ${WORKDIR}
  
    RUN apt-get update && apt-get install -y libffi-dev && rm -rf /var/lib/apt/lists/*

    COPY requirements.txt .
    COPY twitter2bq.py .

    ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
    ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/twitter2bq.py"

    RUN pip install -U -r ./requirements.txt
    ```
1. Create an empty template spec json file in the desired GCS bucket
    * replace ```BUCKET_NAME``` with your Cloud Storage bucket name
    * replace ```BUCKET_LOCATION``` with your bucket location. (Example: `us-central1`)
    ```
    gcloud storage buckets create gs://BUCKET_NAME --location=BUCKET_LOCATION
    
    touch twitter2bq_template.json
    gsutil cp twitter2bq_template.json gs://BUCKET_NAME/templates/
    ```

1. Build and push container image
    * replace ```BUCKET_NAME``` with your Cloud Storage bucket name
    * replace ```PROJECT_NAME``` with your GCP project name
    * Replace ```GCP_REGION``` with the name of the GCP region you'd like to operate in. (Example: `us-central1`)
    ```
    export TEMPLATE_PATH=gs://BUCKET_NAME/templates/twitter2bq_template.json
    export TEMPLATE_IMAGE=gcr.io/PROJECT_NAME/GCP_REGION/twitter2bq_template:latest
    
    gcloud auth configure-docker
    docker image build -t $TEMPLATE_IMAGE .
    docker push $TEMPLATE_IMAGE
    ```
### Configure the Flex template
1. Create template
    ```
    gcloud dataflow flex-template build $TEMPLATE_PATH --image "$TEMPLATE_IMAGE" --sdk-language PYTHON
    ```
1. Run template (optional)
    ```
    gcloud dataflow flex-template run JOB_NAME --template-file-gcs-location=gs://BUCKET_NAME/templates/twitter2bq_template.json --region=GCP_REGION --parameters gcp_project_id=PROJECT_ID, --parameters gcs_bucket_id=BUCKET_ID, --parameters bq_project_id=BQ_PROJECT_ID, --parameters bq_dataset_name=BQ_DATASET_NAME
    ```
    * Replace ```JOB_NAME``` with your desired Dataflow job name.
    * Replace ```PROJECT_ID``` with your project ID.
    * Replace ```BUCKET_ID``` with your Cloud Storage bucket ID.
    * Replace ```BUCKET_NAME``` with your Cloud Storage bucket name.
    * Replace ```BQ_PROJECT_ID``` with the project ID of the BigQuery project you'd like to store the pipeline's output.
    * Replace ```BQ_DATASET_NAME``` with the name of the BigQuery dataset you'd like to store the pipeline's output. 
    * Replace ```GCP_REGION``` with the name of the GCP region you'd like to operate in. (Example: `us-central1`)
    * You can optionally add --max_tweets ```NUMBER_BETWEEN_10_AND_100``` towards the end of the parameters list to specify the number of tweets you want to return per call (default 10; max 100)
### Scheduling the pipeline
1. Create Cloud Scheduler to invoke the Flex template job
    ```
    gcloud scheduler jobs create http twitter2bq_scheduler --schedule "CUSTOM_FREQUENCY" --uri "https://dataflow.googleapis.com/v1b3/projects/PROJECT_ID/locations/GCP_REGION/flexTemplates:launch" --http-method POST --location GCP_REGION --message-body "{                       
    \"launch_parameter\": {
        \"jobName\": \"JOB_NAME\",
        \"containerSpecGcsPath\": \"gs://BUCKET_NAME/templates/twitter2bq_template.json\",
        \"parameters\": {
        \"gcp_project_id\": \"PROJECT_ID\",
        \"gcs_bucket_id\": \"BUCKET_ID\",
        \"bq_project_id\": \"BQ_PROJECT_ID\",
        \"bq_dataset_name\": \"BQ_DATASET_NAME\"
            }
        }
    }" --oauth-service-account-email "DEFAULT_COMPUTE_SERVICE_ACCOUNT" --headers "Content-Type=application/json" 
    ```
    * Replace ```CUSTOM_FRQUENCY``` with the job frequency of your choice (Example: every 3 hours: `"0 */3 * * *"`).
    * Replace ```JOB_NAME``` with your desired Dataflow job name.
    * Replace ```PROJECT_ID``` with your project ID.
    * Replace ```BUCKET_ID``` with your Cloud Storage bucket ID.
    * Replace ```BUCKET_NAME``` with your Cloud Storage bucket name.
    * Replace ```BQ_PROJECT_ID``` with the project ID of the BigQuery project you'd like to store the pipeline's output.
    * Replace ```BQ_DATASET_NAME``` with the name of the BigQuery dataset you'd like to store the pipeline's output. 
    * Replace ```GCP_REGION``` with the name of the GCP region you'd like to operate in. (Example: `us-central1`, however the region in your uri should match with your flex template region)
    * Replace ```DEFAULT_COMPUTE_SERVICE_ACCOUNT``` with your default compute service account name
    * You can optionally add --max_tweets ```NUMBER_BETWEEN_10_AND_100``` towards the end of the parameters list to specify the number of tweets you want to return per call (default 10; max 100)

## License
    Copyright 2022 Google LLC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
