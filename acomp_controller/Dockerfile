# Dockerfile
# ACOMP controller image.
# Build: az acr build --registry acompregistry --image acomp-controller:v1 --platform linux/amd64 .

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY acomp/ ./acomp/
COPY main.py .

# Config is mounted as a ConfigMap at /config/alomp_config.yaml
# by the Kubernetes Deployment (see k8s-manifests.yaml)
RUN mkdir -p /config

ENV ACOMP_CONFIG_PATH=/config/alomp_config.yaml
ENV ACOMP_NAMESPACE=default
ENV ACOMP_ENTRY_POINT=frontend
ENV ACOMP_POLL_INTERVAL=15
ENV ACOMP_POLL_INTERVAL_FAST=5
ENV ACOMP_POLL_INTERVAL_SLOW=30
ENV ACOMP_SLO_VIOLATION_WINDOW=3
ENV ACOMP_DRY_RUN=false

ENTRYPOINT ["python", "-u", "main.py"]
