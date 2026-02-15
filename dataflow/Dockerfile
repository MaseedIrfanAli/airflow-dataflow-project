# Correct versioned base image (VERY IMPORTANT)
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base:latest

# Set working directory
WORKDIR /template

# Copy only required files
COPY main.py /template/main.py
COPY metadata.json /template/metadata.json

# Install dependencies
RUN pip install --no-cache-dir apache-beam[gcp]==2.53.0

# Required environment variables
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=""
