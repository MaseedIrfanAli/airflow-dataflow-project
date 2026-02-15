# REQUIRED base image for Flex Templates
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Set working directory
WORKDIR /template

# Copy your pipeline code
COPY . /template

# Install dependencies
RUN pip install --no-cache-dir apache-beam[gcp]==2.53.0

# Required environment variables for Flex Templates
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=""
