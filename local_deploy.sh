#!/usr/bin/env bash

set -e

GCP_PROJECT="data-ingestion-and-storage"
DF_GCS_BUCKET="ovg-dataflow"

INPUT_FILE="gs://ovg-data-ingestion/nyc-yellow-taxi-trips/*.csv"
OUTPUT_BQ_TABLE="${GCP_PROJECT}:scratch.nyc_yellow_taxi_trips"

export JAVA_HOME=/usr/lib/jvm/java-1.17.0-openjdk-amd64/

# Setup gcloud
gcloud config set project ${GCP_PROJECT}

# Build jar
(cd java/bq-write-api && gradle clean shadowJar)

# Start update pipeline
python src/df_bq_write_api.py \
    --job_name df-bq-write-api \
    --runner DirectRunner \
    --save_main_session true \
    --temp_location gs://${DF_GCS_BUCKET}/temp/ \
    --input ${INPUT_FILE} \
    --output ${OUTPUT_BQ_TABLE} \
    --requirements_file requirements.txt \
    --classpath ./java/bq-write-api/build/libs/bq-write-api-0.1-all.jar
