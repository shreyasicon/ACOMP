#!/bin/bash
# build_and_push.sh
#
# Builds the ACOMP controller image and pushes it to Azure Container Registry.
# Run from the acomp_controller directory in Azure Cloud Shell.
#
# Usage: bash build_and_push.sh

set -e

ACR_NAME="acompregistry"
IMAGE_NAME="acomp-controller"
IMAGE_TAG="v1"

echo "==> Logging into ACR: $ACR_NAME"
az acr login --name "$ACR_NAME"

echo "==> Building image (linux/amd64 -- matches x86pool nodes)"
az acr build \
  --registry "$ACR_NAME" \
  --image "${IMAGE_NAME}:${IMAGE_TAG}" \
  --platform linux/amd64 \
  .

echo "==> Done. Image available at: ${ACR_NAME}.azurecr.io/${IMAGE_NAME}:${IMAGE_TAG}"
echo "==> Next: kubectl apply -f k8s-manifests.yaml"
