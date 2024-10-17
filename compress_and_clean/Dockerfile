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

# Use the official Python 3.11 slim image as the base
FROM python:3.11-slim

# Copy the SDK entrypoint binary from the Apache Beam image
COPY --from=apache/beam_python3.11_sdk:2.58.0 /opt/apache/beam /opt/apache/beam

# Copy the Flex Template launcher binary
COPY --from=gcr.io/dataflow-templates-base/python311-template-launcher-base:20230622_RC00 /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Set the working directory
ARG WORKDIR=/template
WORKDIR ${WORKDIR}

# Copy your requirements.txt and Python script into the working directory
COPY requirements.txt .
COPY compress_and_clean_logs.py .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Specify the Python file to run as part of the Flex Template
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/compress_and_clean_logs.py"

# Set the entrypoint to the Dataflow Python template launcher
ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
