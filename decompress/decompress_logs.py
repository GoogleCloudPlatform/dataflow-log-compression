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

# Define custom pipeline options
class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--startDate', type=str, help="Start date to decompress logs: yyyy-mm-dd")
        parser.add_value_provider_argument('--endDate', type=str, help="End date to decompress logs: yyyy-mm-dd")
        parser.add_value_provider_argument('--folders', type=str, default="*", help="Array of top level folder names or use '*' to process all folders")
        parser.add_value_provider_argument('--daysToKeep', type=int, default=3, help="Number of days to keep the decompressed files to be cleaned up by compress_and_clean")

# Commonly changed variables
PROJECT_ID = "<DATAFLOW-PROJECT-ID>"
BUCKET_NAME = "<LOG-BUCKET-ID>"
REGION = "<SUBNET-REGION>"
SUBNETWORK = f"https://www.googleapis.com/compute/v1/projects/<NETWORK-PROJECT-ID>/regions/{REGION}/subnetworks/<SUBNET-NAME>"
EXCLUDED_FOLDERS = ["dataflow/"]  # Folders that should be ignored by compression. Dataflow temp folder is recommended.
GENERATE_LOG_FILE = True  # If log files should be generated within dataflow folder

# Less commonly changed variables
JOB_NAME = "decompress-logs"
TEMP_LOCATION = f"gs://{BUCKET_NAME}/dataflow/temp/{JOB_NAME}"
STAGING_LOCATION = f"gs://{BUCKET_NAME}/dataflow/staging/{JOB_NAME}"
INPUT_PATTERN = f"gs://{BUCKET_NAME}/**"
OUTPUT_LOG_FILE = f"gs://{BUCKET_NAME}/dataflow/last_dataflow_decompress_job_log.txt"

# Function to clean up temp and staging directories before the pipeline run
def clean_up_directories(bucket_name, job_name, client):
    bucket = client.bucket(bucket_name)
    for path in [f"dataflow/temp/{job_name}", f"dataflow/staging/{job_name}"]:
        blobs = bucket.list_blobs(prefix=path)
        for blob in blobs:
            blob.delete()

# Function to update TTL of decompressed files using custom metadata
def set_ttl(blob, days_to_keep):
    ttl_date = datetime.now(timezone.utc) + timedelta(days=days_to_keep)
    ttl_timestamp = ttl_date.isoformat()
    metageneration_match_precondition = blob.metageneration
    metadata = {'x-goog-meta-ttl': ttl_timestamp}
    blob.metadata = metadata
    blob.patch(if_metageneration_match=metageneration_match_precondition)

class DecompressFiles(beam.DoFn):
    def start_bundle(self):
        self.client = storage.Client()

    def process(self, element, startDate, endDate, folders, daysToKeep):
        file_path = element.metadata.path
        log_messages = []

        def log_message(message):
            log_messages.append(f"File: {file_path}, {message}")

        # Check for excluded folders
        if any(file_path.startswith(f"gs://{BUCKET_NAME}/{folder}") for folder in EXCLUDED_FOLDERS):
            log_message("Skipped (Excluded Folder)")
            yield beam.pvalue.TaggedOutput("log", "\n".join(log_messages))
            return

        # Check if the file is in one of the specified folders
        folder_name = file_path.split("/")[3]  # Get the top-level folder name
        if "*" not in folders.get().split(",") and folder_name not in folders.get().split(","):
            log_message("Skipped (Not in Specified Folders)")
            yield beam.pvalue.TaggedOutput("log", "\n".join(log_messages))
            return

        try:
            bucket_name = file_path.split("/")[2]
            blob_name = "/".join(file_path.split("/")[3:])
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)

            blob.reload()

            # Extract and parse the date from the file path
            file_date_str = "/".join(blob_name.split("/")[-4:-1])
            file_date = datetime.strptime(file_date_str, "%Y/%m/%d")
            start_date = datetime.strptime(startDate.get(), "%Y-%m-%d")
            end_date = datetime.strptime(endDate.get(), "%Y-%m-%d")

            # Skip files outside the specified date range
            if not (start_date <= file_date <= end_date):
                log_message("Skipped (Outside Date Range)")
                yield beam.pvalue.TaggedOutput("log", "\n".join(log_messages))
                return

            # Ensure _decompressed is only added once and handle all cases
            if "_decompressed" in file_path:
                # Case 1: Already decompressed file
                decompressed_path = file_path  # It's already decompressed, no further action needed
            elif file_path.endswith(".zst"):
                # Case 2: Compressed by zst
                # Remove .zst and add _decompressed before the file extension
                decompressed_path = file_path.replace(".zst", "")
                decompressed_path = decompressed_path.replace(".", "_decompressed.")
            elif "STANDARD" in blob.storage_class:
                # Case 4: Normal file in standard storage, skip decompression
                log_message(f"Skipped: {file_path} is already in STANDARD storage and has not been compressed yet.")
                yield beam.pvalue.TaggedOutput("log", "\n".join(log_messages))
                return
            else:
                # Case 3: Normal file in nearline/coldline/archive, decompress and add _decompressed
                decompressed_path = file_path.replace(".", "_decompressed.")

            # Check if the corresponding decompressed file already exists
            decompressed_blob = bucket.blob(decompressed_path.split(f"gs://{BUCKET_NAME}/")[1])
            if decompressed_blob.exists():
                log_message("Updating TTL for Already Decompressed File")
                set_ttl(decompressed_blob, daysToKeep.get())
                log_message("TTL updated")
                yield beam.pvalue.TaggedOutput("log", "\n".join(log_messages))
                return

            # Proceed with decompression or copy
            if file_path.endswith(".zst"):
                log_message("Decompressing")
                with FileSystems.open(file_path) as f_in:
                    dctx = zstd.ZstdDecompressor()
                    with dctx.stream_reader(f_in) as reader:
                        with FileSystems.create(decompressed_path) as f_out:
                            f_out.write(reader.read())
            else:
                log_message("Copying without decompression")
                with FileSystems.open(file_path) as f_in:
                    with FileSystems.create(decompressed_path) as f_out:
                        f_out.write(f_in.read())

            # Set TTL and update storage class
            set_ttl(decompressed_blob, daysToKeep.get())
            log_message("TTL set for file")
            decompressed_blob.update_storage_class("STANDARD")
            log_message("Storage class updated to STANDARD")

            log_message(f"Processed and saved to {decompressed_path}")
            yield decompressed_path
            yield beam.pvalue.TaggedOutput("log", "\n".join(log_messages))

        except Exception as e:
            log_message(f"Error: {str(e)}")
            yield beam.pvalue.TaggedOutput("log", "\n".join(log_messages))
            yield beam.pvalue.TaggedOutput("failures", (file_path, str(e)))


def run_decompression(input_pattern, output_log_file, pipeline_options, job_name):
    custom_options = pipeline_options.view_as(CustomOptions)
    client = storage.Client()

    clean_up_directories(BUCKET_NAME, job_name, client)

    with beam.Pipeline(options=pipeline_options) as p:
        files = (
            p
            | "Match Files" >> MatchFiles(input_pattern)
            | "Read Matches" >> ReadMatches()
        )

        decompressed_files = files | "Decompress Files" >> beam.ParDo(
            DecompressFiles(),
            startDate=custom_options.startDate,
            endDate=custom_options.endDate,
            folders=custom_options.folders,
            daysToKeep=custom_options.daysToKeep
        ).with_outputs("failures", "log", main="main")

        if GENERATE_LOG_FILE:
            decompressed_files.log | "Write Log File" >> beam.io.WriteToText(
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
    run_decompression(INPUT_PATTERN, OUTPUT_LOG_FILE, pipeline_options, JOB_NAME)
