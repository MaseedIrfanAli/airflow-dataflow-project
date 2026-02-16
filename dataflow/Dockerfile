FROM gcr.io/dataflow-templates-base/python3-template-launcher-base:latest

WORKDIR /template

COPY main.py /template/main.py
COPY metadata.json /template/metadata.json

RUN pip install --no-cache-dir apache-beam[gcp]==2.53.0

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="/template/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=""

ENTRYPOINT ["/opt/google/dataflow/python_template_launcher"]
