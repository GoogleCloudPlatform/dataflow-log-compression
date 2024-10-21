# Log Compression and Decompression Pipelines

This repository contains two Apache Beam pipelines designed to manage the compression, storage, and cleanup of log files in Google Cloud Storage (GCS). These pipelines help efficiently manage storage costs and access speed for logs by compressing files into Nearline/Coldline/Archival storage and decompressing them into Standard storage with TTL metadata for automated cleanup.

## Compress and Clean Pipeline

### Overview
This pipeline compresses log files using Zstandard ([Zstandard](https://github.com/facebook/zstd)) and moves them to Nearline, Coldline, or Archive storage in GCS. It substitutes GCS lifecycle management for compressing and storing files while also cleaning up decompressed files by checking their TTL metadata. **Important:** GCS lifecycle management will still be needed to delete compressed files in Nearline/Coldline/Archival after a certain period.

### Disclaimer
This pipeline moves logs in a manner similar to Google Cloud Storage (GCS) lifecycle management. It is important to thoroughly test the code in a non-production logging bucket before deploying it in any production environment. The pipeline logic and parameters should be validated against your specific use case, as individual requirements and behaviors may vary. Users are responsible for fully testing, understanding, and verifying the code to ensure it works as expected before implementing it in a production system.

### Key Variables
The following variables should be adjusted across the pipeline files. Some are mandatory, while others are optional.

#### Mandatory Variables:
- **`PROJECT_ID`**: Google Cloud project ID where Dataflow and GCS are set up.
- **`BUCKET_NAME`**: GCS bucket name where the logs are stored.
- **`REGION`**: GCS region where the bucket is located.
- **`SUBNETWORK`**: Full subnetwork URL for the GCS network configuration.
- **`DATAFLOW-BUCKET`**: Bucket where Dataflow artifacts like templates, logs, and temporary files are stored.
- **`GAR-PROJECT-ID`**: Google Artifact Registry (GAR) project ID to host your Docker container image.
- **`SERVICE_ACCOUNT_EMAIL`**: The email of the service account that will run the Dataflow job.

#### Optional Variables:
- **`EXCLUDED_FOLDERS`**: List of folders to exclude from compression and deletion (e.g., temp directories).
- **`DAYS_BEFORE_COMPRESSION`**: The number of days that a file must remain unmodified before it’s eligible for compression (default: 29).
- **`SIZE_THRESHOLD_BYTES`**: Minimum file size (in bytes) for compression. Files smaller than this will be moved to Nearline/Coldline/Archive without compression (default: 250 bytes).
- **`STORAGE_CLASS`**: The GCS storage class where the compressed files will be moved. Options: `"NEARLINE"`, `"COLDLINE"`, `"ARCHIVE"` (default: `"COLDLINE"`).


### Clone and Setup
Clone this repository to your local machine and CD into the compress_and_clean directory:

```
git clone <repo>
cd compress_and_clean
```

### Adjust the Variables
Go through the following files and adjust the mandatory and optional variables as needed:

1. **`compress_and_clean_logs.py`**: Modify the following:
   - `PROJECT_ID`
   - `BUCKET_NAME`
   - `REGION`
   - `SUBNETWORK`
   - `DAYS_BEFORE_COMPRESSION` (optional)
   - `SIZE_THRESHOLD_BYTES` (optional)
   - `STORAGE_CLASS` (optional)

2. **`cloudbuild.yaml`**: Replace `<GAR-PROJECT-ID>` with your actual project ID.

3. **`metadata.json`**: Update:
   - `<GAR-PROJECT-ID>`: The project where the Docker image is hosted.
   - `<DATAFLOW-BUCKET>`: The bucket for Dataflow.

### Dockerfile and Cloud Build Setup
You will use `cloudbuild.yaml` to build and deploy the Docker image using Google Cloud Build.

1. Build the Docker image:
```
gcloud builds submit --config cloudbuild.yaml .
```

### Configuring the `metadata.json`
Before deploying the pipeline, update the `metadata.json` file with the correct paths and network configurations.

### Build and Run the Flex Template

#### Step 1: Build the Flex Template
```
gcloud dataflow flex-template build "gs://<DATAFLOW-BUCKET>/templates/compress_and_clean_logs.json" \
  --image "gcr.io/<GAR-PROJECT-ID>/compress-clean-container:latest" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json" \
  --service-account-email "<YOUR_SERVICE_ACCOUNT_EMAIL>"
```

#### Step 2: Run the Dataflow Job
```
gcloud dataflow flex-template run "compress-and-clean-logs-job" \
  --template-file-gcs-location "gs://your-bucket-name/templates/compress_and_clean_logs.json" \
  --region "your-region" \
  --parameters subnetwork="https://www.googleapis.com/compute/v1/projects/your-project-id/regions/your-region/subnetworks/your-subnet"
```

Ensure that the Docker container is built and running before building and running the Flex Template.

### Note:
- The Docker image is hosted in Google Artifact Registry (GAR).
- GCS lifecycle management should be configured separately to delete files after a retention period.

## Decompress Pipeline

### Overview
The decompress pipeline is used to extract logs from Nearline/Coldline/Archive storage, decompress them, and store them back into Standard storage with an updated TTL. This allows for efficient reads and enables flexible management of logs within specified date ranges.

### Key Variables
The following variables should be adjusted across the pipeline files. Some are mandatory, while others are optional.

#### Mandatory Variables:
- **`PROJECT_ID`**: Google Cloud project ID where Dataflow and GCS are set up.
- **`BUCKET_NAME`**: GCS bucket name where the logs are stored.
- **`REGION`**: GCS region where the bucket is located.
- **`SUBNETWORK`**: Full subnetwork URL for the GCS network configuration.
- **`DATAFLOW-BUCKET`**: Bucket where Dataflow artifacts like templates, logs, and temporary files are stored.
- **`GAR-PROJECT-ID`**: Google Artifact Registry (GAR) project ID to host your Docker container image.
- **`SERVICE_ACCOUNT_EMAIL`**: The email of the service account that will run the Dataflow job.

#### Optional Variables:
- **`EXCLUDED_FOLDERS`**: List of folders to exclude from processing (e.g., temp directories).
- **`DAYS_TO_KEEP`**: Number of days to keep the decompressed files before they are eligible for deletion (default: 3 days).
- **`START_DATE`** & **`END_DATE`**: Define the date range for which logs will be decompressed (mandatory).
- **`FOLDERS`**: Array of top-level folder names to process. Use `"*"` to process all folders (default: `*`).

### Clone and Setup
Change directory to the decompress directory.

```
cd decompress
```

### Adjust the Variables
Go through the following files and adjust the mandatory and optional variables as needed:

1. **`decompress_logs.py`**: Modify the following:
   - `PROJECT_ID`
   - `BUCKET_NAME`
   - `REGION`
   - `SUBNETWORK`
   - `DAYS_TO_KEEP` (optional)
   - `START_DATE` & `END_DATE` (mandatory)
   - `FOLDERS` (optional)

2. **`cloudbuild.yaml`**: Replace `<GAR-PROJECT-ID>` with your actual project ID.

3. **`metadata.json`**: Update:
   - `<GAR-PROJECT-ID>`: The project where the Docker image is hosted.
   - `<DATAFLOW-BUCKET>`: The bucket for Dataflow.

### Dockerfile and Cloud Build Setup
You will use `cloudbuild.yaml` to build and deploy the Docker image using Google Cloud Build.

Build the Docker image:
```
gcloud builds submit --config cloudbuild.yaml .
```

### Configuring the `metadata.json`
Before deploying the pipeline, update the `metadata.json` file with the correct paths and network configurations.

### Build and Run the Flex Template

#### Step 1: Build the Flex Template
```
gcloud dataflow flex-template build "gs://<DATAFLOW-BUCKET>/templates/decompress_logs.json" \
  --image "gcr.io/<GAR-PROJECT-ID>/decompress-container:latest" \
  --sdk-language "PYTHON" \
  --metadata-file "metadata.json" \
  --service-account-email "<YOUR_SERVICE_ACCOUNT_EMAIL>"
```

#### Step 2: Run the Dataflow Job
```
gcloud dataflow flex-template run "decompress-logs-job" \
  --template-file-gcs-location "gs://<YOUR_BUCKET_NAME>/templates/decompress_logs.json" \
  --region "your-region" \
  --parameters startDate="2023-01-01",endDate="2023-02-04",folders="*",daysToKeep="3",subnetwork="<YOUR_SUBNETWORK_URL>"
```

### Notes
- Replace `<YOUR_BUCKET_NAME>`, `<YOUR_PROJECT_ID>`, `<YOUR_SERVICE_ACCOUNT_EMAIL>`, and `<YOUR_SUBNETWORK_URL>` with your specific values.
- The `folders` parameter allows for specifying individual folders or `"*"` to process all folders.

"""

## Future Improvements

#### 1. Automating Compression Job with Cloud Scheduler
To ensure that log files are compressed on a regular cadence, you can automate the **compress-and-clean pipeline** using Google Cloud Scheduler. This allows you to set up a scheduled job that triggers the compression process at regular intervals, such as daily, weekly, or monthly.

Here’s how you can set it up:
1. **Create a Cloud Scheduler Job**:
   - Go to the **Cloud Scheduler** in your Google Cloud Console.
   - Create a new job with a custom schedule (e.g., every 24 hours).
   - Set the **target** as an HTTP-based **Pub/Sub** endpoint to trigger your Dataflow job.

2. **Publish to a Pub/Sub Topic**:
   - Your Cloud Scheduler job can publish a message to a Pub/Sub topic that triggers the Dataflow compression pipeline.
   - Ensure that the **compress_and_clean_logs.py** is configured to listen to this Pub/Sub trigger.

3. **Configure the Pipeline for Automated Triggers**:
   - Modify the pipeline to allow it to be triggered by Pub/Sub messages. This can include setting up the start and end date dynamically or processing logs as soon as they are available.

Example of creating a Cloud Scheduler job:
```
gcloud scheduler jobs create pubsub compress-clean-scheduler \
    --schedule="0 2 * * *" \
    --topic=<YOUR-PUBSUB-TOPIC> \
    --message-body="{}" \
    --time-zone="America/Chicago"
```

This job will run the compression job every day at 2 AM (as per the schedule). You can adjust the schedule based on your use case.

#### 2. Performance Improvements
- **Parallelization**: Depending on the volume of logs, you may consider parallelizing the decompression/compression processes to improve the overall performance.
- **Error Handling**: Enhance error logging and retries for any failed decompression or compression operations to ensure that no data is missed during the process.
- **Monitoring and Alerts**: Integrate monitoring with Google Cloud’s operations suite to trigger alerts if there are issues with the Dataflow pipeline, such as failed jobs or long processing times.

#### 3. Dynamic Input Parameters
Consider enhancing the flexibility of the pipelines by allowing dynamic parameters to be passed in, such as specific log folders or custom retention periods, depending on preferences or business requirements.

#### 4. Lifecycle Management for Decompressed Files
You may want to implement automatic lifecycle management policies in GCS to delete old decompressed files after a certain time period, to ensure efficient use and cost savings.

## FAQ

### Is this production-ready code?
No, this is not production-ready. It should be tested thoroughly and adjusted based on specific use cases prior to production use.

### What is the purpose of the `SIZE_THRESHOLD_BYTES` variable?
This variable ensures that small files, which may not benefit from compression, are moved directly to Nearline/Coldline/Archive storage without being compressed.

### Why is lifecycle management still needed?
While this pipeline handles the compression and TTL-based deletion of decompressed files, lifecycle management is still required to handle the deletion of compressed files in Nearline/Coldline/Archive storage.

### Can I specify which folders to decompress?
Yes, the `FOLDER` variable allows you to specify an array of folder names to be processed. Using `"*"` will process all folders in the bucket.

### How does the TTL deletion work?
TTL is managed using custom metadata `x-goog-meta-ttl`. The compress and clean pipeline checks this metadata and deletes files if the TTL has expired.

### What are Flex Templates, and why are they used?
Flex Templates allow for more flexibility and control over the Dataflow jobs by letting you define custom pipelines with specific Docker images, environment settings, and parameters. They are used when the standard templates do not meet the unique requirements of your pipeline.

### How do I configure and use the metadata.json file?
The metadata.json file is used to define the configuration and parameters for your Flex Templates. It includes information about the Docker image, environment variables, and custom parameters that the pipeline will use. When building and deploying your Flex Template, this file is referenced to ensure all the correct settings are applied.