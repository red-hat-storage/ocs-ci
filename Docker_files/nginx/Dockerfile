# Use a base image with QEMU for multi-architecture support.
FROM --platform=$BUILDPLATFORM nginx:alpine

RUN apk update
RUN apk upgrade
RUN apk add fio bash
RUN apk add --no-cache util-linux
RUN apk add coreutils

# Build and Push the multi-architecture Docker image using Docker Buildx:
# docker buildx build --platform linux/amd64,linux/ppc64le,linux/s390x,linux/arm64 -t quay.io/ocsci/nginx:<tag> . --push
