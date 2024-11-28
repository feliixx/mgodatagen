#!/bin/bash

IMAGE_NAME="mgodatagen"
VERSION=$(git describe --tags --always)
IMAGE_REPO="ghcr.io/feliixx"
PLATEFORMS="linux/amd64,linux/arm64"

docker buildx create --name multiarch-builder --use

docker buildx build \
  --platform ${PLATEFORMS} \
  --tag ${IMAGE_REPO}/${IMAGE_NAME}:${VERSION} \
  --tag ${IMAGE_REPO}/${IMAGE_NAME}:latest \
  --push \
  .
