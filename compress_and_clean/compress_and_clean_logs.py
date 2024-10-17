# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches
from apache_beam.io.filesystems import FileSystems
import zstandard as zstd
from google.cloud import storage
from datetime import datetime, timedelta, timezone

# Commonly changed variables
PROJECT_ID = "<DATAFLOW-PROJECT-ID>"
BUCKET_NAME = "<LOG-BUCKET-ID>"
REGION = "<SUBNET-REGION>"
SUBNETWORK = f"https://www.googleapis.com/compute/v1/projects/<NETWORK-PROJECT-ID>/regions/{REGION}/subnetworks/<SUBNET-NAME>"
EXCLUDED_FOLDERS = ["dataflow/"]  # Folders that should be ignored by compression. Dataflow temp folder is recommended.
GENERATE_LOG_FILE = True  # If log files should be generated within dataflow folder
DAYS_BEFORE_COMPRESSION = 29  # Set the number of days before a file should be compressed and moved to Nearline/Coldline/Archive storage
SIZE_THRESHOLD_BYTES = 250  # Set the size threshold (In Bytes) for compression
STORAGE_CLASS = "COLDLINE"  # Options: "NEARLINE", "COLDLINE", "ARCHIVE"

# Less commonly changed variables
JOB_NAME = "compress-clean-logs"
TEMP_LOCATION = f"gs://{BUCKET_NAME}/dataflow/temp/{JOB_NAME}"
STAGING_LOCATION = f"gs://{BUCKET_NAME}/dataflow/staging/{JOB_NAME}"
INPUT_PATTERN = f"gs://{BUCKET_NAME}/**"
OUTPUT_LOG_FILE = f"gs://{BUCKET_NAME}/dataflow/last_dataflow_compress_job_log.txt"

# Function to clean up temp and staging directories before the pipeline run
def clean_up_directories(bucket_name, job_name, client):
    bucket = client.bucket(bucket_name)
    for path in [f"dataflow/temp/{job_name}", f"dataflow/staging/{job_name}"]:
        blobs = bucket.list_blobs(prefix=path)
        for blob in blobs:
            blob.delete()

# Function to change newly compressed objects to the specified storage class (Nearline/Coldline/Archive)
def update_storage_class_if_needed(bucket_name, blob_name, client, storage_class=STORAGE_CLASS):
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.reload()
        if blob.storage_class != storage_class:
            blob.update_storage_class(storage_class)
            return f"Successfully updated storage class for {blob_name} to {storage_class}"
        else:
            return f"File {blob_name} is already in {storage_class} storage"
    except Exception as e:
        return f"Failed to update storage class for {blob_name}: {str(e)}"

# Function to check and delete decompressed files if TTL has expired
def check_and_delete_if_ttl_expired(blob):
    ttl_str = blob.metadata.get('x-goog-meta-ttl')
    if ttl_str:
        ttl_time = datetime.fromisoformat(ttl_str.rstrip('Z')).replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > ttl_time:
            blob.delete()
            return f"Deleted due to TTL expiry"
        else:
            return f"Skipped (TTL not expired)"
    return f"Skipped (No TTL set)"

class CompressFiles(beam.DoFn):
    def start_bundle(self):
        self.client = storage.Client()

    def process(self, element):
        file_path = element.metadata.path

        if any(
            file_path.startswith(f"gs://{BUCKET_NAME}/{folder}")
            for folder in EXCLUDED_FOLDERS
        ):
            yield beam.pvalue.TaggedOutput(
                "log", f"File: {file_path}, Action: Skipped (Excluded Folder)"
            )
            return

        try:
            if '_decompressed' in file_path:
                bucket_name = file_path.split("/")[2]
                blob_name = "/".join(file_path.split("/")[3:])
                bucket = self.client.bucket(bucket_name)
                blob = bucket.blob(blob_name)
                blob.reload()

                ttl_check_result = check_and_delete_if_ttl_expired(blob)
                yield beam.pvalue.TaggedOutput(
                    "log", f"File: {file_path}, Action: {ttl_check_result}"
                )
                return
            
            bucket_name = file_path.split("/")[2]
            blob_name = "/".join(file_path.split("/")[3:])
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            blob.reload()

            if blob.storage_class == STORAGE_CLASS:
                yield beam.pvalue.TaggedOutput(
                    "log", f"File: {file_path}, Action: Skipped (Already in {STORAGE_CLASS})"
                )
                return

            # Proceed with compression
            updated_time = blob.updated
            file_size = blob.size

            # Check if the file is older than the specified number of days
            cutoff_time = datetime.now(timezone.utc) - timedelta(
                days=DAYS_BEFORE_COMPRESSION
            )
            if updated_time <= cutoff_time:
                if file_size >= SIZE_THRESHOLD_BYTES:
                    # Compress the file and move it to specified storage class
                    compressed_path = file_path + ".zst"
                    with FileSystems.open(file_path) as f_in:
                        with FileSystems.create(compressed_path) as f_out:
                            cctx = zstd.ZstdCompressor(write_content_size=True)
                            with cctx.stream_writer(f_out, closefd=False) as compressor:
                                while True:
                                    chunk = f_in.read(16384)
                                    if not chunk:
                                        break
                                    compressor.write(chunk)

                    FileSystems.delete([file_path])

                    new_blob_name = compressed_path.split(f"gs://{BUCKET_NAME}/")[1]
                    update_result = update_storage_class_if_needed(
                        bucket_name, new_blob_name, self.client, STORAGE_CLASS
                    )
                    log_message = f"File: {file_path}, Action: Compressed and Moved to {STORAGE_CLASS}, {update_result}"
                    yield compressed_path

                    yield beam.pvalue.TaggedOutput("log", log_message)
                else:
                    # Move file to specified storage class without compression
                    update_result = update_storage_class_if_needed(
                        bucket_name, blob_name, self.client, STORAGE_CLASS
                    )
                    yield beam.pvalue.TaggedOutput(
                        "log", f"File: {file_path}, Action: Moved to {STORAGE_CLASS}, {update_result}"
                    )
            else:
                yield beam.pvalue.TaggedOutput(
                    "log", f"File: {file_path}, Action: Skipped (Not Old Enough)"
                )

        except Exception as e:
            log_message = f"File: {file_path}, Error: {str(e)}"
            yield beam.pvalue.TaggedOutput("log", log_message)
            yield beam.pvalue.TaggedOutput("failures", (file_path, str(e)))

def run_compression(input_pattern, output_log_file, pipeline_options, job_name):
    client = storage.Client()

    clean_up_directories(BUCKET_NAME, job_name, client)

    with beam.Pipeline(options=pipeline_options) as p:
        files = (
            p
            | "Match Files" >> MatchFiles(input_pattern)
            | "Read Matches" >> ReadMatches()
        )

        compressed_files = files | "Compress Files" >> beam.ParDo(
            CompressFiles()
        ).with_outputs("failures", "log", main="main")

        if GENERATE_LOG_FILE:
            compressed_files.log | "Write Log File" >> beam.io.WriteToText(
                output_log_file, append_trailing_newlines=True
            )

if __name__ == "__main__":
    pipeline_options_dict = {
        "project": PROJECT_ID,
        "runner": "DataflowRunner",
        "region": REGION,
        "temp_location": TEMP_LOCATION,
        "staging_location": STAGING_LOCATION,
        "subnetwork": SUBNETWORK,
        "save_main_session": True,
        "job_name": JOB_NAME
    }

    pipeline_options = PipelineOptions(**pipeline_options_dict)
    run_compression(INPUT_PATTERN, OUTPUT_LOG_FILE, pipeline_options, JOB_NAME)
