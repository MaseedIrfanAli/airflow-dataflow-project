FROM apache/beam_python3.10_sdk:2.53.0
COPY . /pipeline
WORKDIR /pipeline
Run pip install apache-beam[gcp]==2.53.0

ENTRYPOINT ["python", [main.py]
