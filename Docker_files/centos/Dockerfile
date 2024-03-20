# Use a base image with QEMU for multi-architecture support.
FROM --platform=$BUILDPLATFORM quay.io/centos/centos:stream9

RUN yum update -y
RUN yum upgrade -y
RUN yum install -y bash
RUN yum install -y util-linux
RUN yum install -y fio
RUN yum install -y --allowerasing coreutils

# Build and Push the multi-architecture Docker image using Docker Buildx:
# docker buildx build --platform linux/amd64,linux/ppc64le,linux/s390x,linux/arm64 -t quay.io/ocsci/centos:<tag> . --push
