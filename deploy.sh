#!/usr/bin/env bash

set -e

GCP_PROJECT="data-ingestion-and-storage"
REGION="us-central1"
SERVICE_ACCOUNT="dataflow@data-ingestion-and-storage.iam.gserviceaccount.com"
SHARED_VPC="https://www.googleapis.com/compute/v1/projects/networking-351216/regions/us-central1/subnetworks/default"
DF_GCS_BUCKET="ovg-dataflow"

INPUT_FILE="gs://ovg-data-ingestion/nyc-yellow-taxi-trips/*.csv"
OUTPUT_BQ_TABLE="${GCP_PROJECT}:scratch.nyc_yellow_taxi_trips"

export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64/

# Setup gcloud
gcloud config set project ${GCP_PROJECT}

# Build jar
(cd java/bq-write-api && gradle clean shadowJar)

# Start pipeline
python src/df_bq_write_api.py \
    --job_name df-bq-write-api \
    --runner DataflowRunner \
    --project ${GCP_PROJECT} \
    --region ${REGION} \
    --service_account_email ${SERVICE_ACCOUNT} \
    --subnetwork ${SHARED_VPC} \
    --no_use_public_ips \
    --temp_location gs://${DF_GCS_BUCKET}/temp/ \
    --staging_location gs://${DF_GCS_BUCKET}/staging/ \
    --save_main_session true \
    --input ${INPUT_FILE} \
    --output ${OUTPUT_BQ_TABLE} \
    --requirements_file requirements.txt \
    --dataflow_service_options=enable_prime \
    --dataflow_service_options=enable_hot_key_logging \
    --dataflow_service_options=enable_google_cloud_profiler \
    --prebuild_sdk_container_engine=cloud_build \
    --classpath ./java/bq-write-api/build/libs/bq-write-api-0.1-all.jar
