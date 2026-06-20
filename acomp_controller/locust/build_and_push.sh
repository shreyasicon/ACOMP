#!/bin/bash
# locust/build_and_push.sh
#
# Builds the ACOMP load generator image and pushes it to your Azure
# Container Registry (acompregistry). Run this from Azure Cloud Shell or
# any machine with Docker + Azure CLI logged in.
#
# Usage: bash build_and_push.sh

set -e

ACR_NAME="acompregistry"
IMAGE_NAME="acomp-loadgenerator"
IMAGE_TAG="v1"

echo "==> Logging into ACR: $ACR_NAME"
az acr login --name "$ACR_NAME"

echo "==> Building image (linux/amd64 -- must match x86pool nodes)"
docker build --platform linux/amd64 -t "${ACR_NAME}.azurecr.io/${IMAGE_NAME}:${IMAGE_TAG}" .

echo "==> Pushing image"
docker push "${ACR_NAME}.azurecr.io/${IMAGE_NAME}:${IMAGE_TAG}"

echo "==> Done. Image available at: ${ACR_NAME}.azurecr.io/${IMAGE_NAME}:${IMAGE_TAG}"
echo "==> Next: kubectl apply -f k8s-manifests.yaml"
