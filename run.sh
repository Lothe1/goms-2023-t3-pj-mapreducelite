#!/bin/bash

# Configuration
MINIO_BUCKET="mrl-lite"
INPUT_FOLDER="Input"
INTERMEDIATE_FOLDER="temp"
OUTPUT_FOLDER="Output"

# MinIO server configuration
MINIO_SERVER="http://localhost:9000"
MINIO_ACCESS_KEY="ROOTNAME"
MINIO_SECRET_KEY="CHANGEME123"

# Function to check if MinIO bucket exists, create if not
function ensure_bucket_exists() {
    bucket_name=$1
    exists=$(minio-cli ls ${MINIO_SERVER} --access-key ${MINIO_ACCESS_KEY} --secret-key ${MINIO_SECRET_KEY} | grep "${bucket_name}")
    if [ -z "${exists}" ]; then
        echo "Creating bucket ${bucket_name}..."
        minio-cli mb ${MINIO_SERVER}/${bucket_name} --access-key ${MINIO_ACCESS_KEY} --secret-key ${MINIO_SECRET_KEY}
    else
        echo "Bucket ${bucket_name} already exists."
    fi
}

# Function to upload files to MinIO
function upload_to_minio() {
    local source_folder=$1
    local destination_folder=$2

    echo "Uploading files from ${source_folder} to MinIO bucket ${MINIO_BUCKET}/${destination_folder}..."

    for file in ${source_folder}/*; do
        filename=$(basename ${file})
        echo "Uploading ${filename}..."
        minio-cli cp ${file} ${MINIO_SERVER}/${MINIO_BUCKET}/${destination_folder}/${filename} --access-key ${MINIO_ACCESS_KEY} --secret-key ${MINIO_SECRET_KEY}
    done

    echo "Upload complete."
}

# Stage 1: Mapping and Reducing
echo "Starting Stage 1: Mapping and Reducing..."

# Ensure MinIO bucket exists
ensure_bucket_exists ${MINIO_BUCKET}

# Upload input files to MinIO
upload_to_minio ${INPUT_FOLDER} ${INPUT_FOLDER}

# Execute Stage 1 MapReduce job
echo "Running Stage 1 MapReduce job..."
./mrlite -map matrix-multiply-stage-one -input ${MINIO_BUCKET}/${INPUT_FOLDER} -output ${MINIO_BUCKET}/${INTERMEDIATE_FOLDER}

# Stage 2: Mapping and Reducing
echo "Starting Stage 2: Mapping and Reducing..."

# Execute Stage 2 MapReduce job
echo "Running Stage 2 MapReduce job..."
./mrlite -map matrix-multiply-stage-two -input ${MINIO_BUCKET}/${INTERMEDIATE_FOLDER} -output ${MINIO_BUCKET}/${OUTPUT_FOLDER}

# Upload output files to MinIO
upload_to_minio ${INTERMEDIATE_FOLDER} ${INTERMEDIATE_FOLDER}
upload_to_minio ${OUTPUT_FOLDER} ${OUTPUT_FOLDER}

echo "MapReduce job completed successfully."