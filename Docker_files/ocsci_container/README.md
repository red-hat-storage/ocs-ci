# ocs-ci Containers for OpenShift Data Foundation

This project builds containers for [ocs-ci].
The primary purpose for doing so was to be able to run [ocs-ci] tests with container

The containers are expected to run with a service account that has admin credentials.

### Build Image

**Change directory to Docker_files/ocsci_container [$ cd Docker_files/ocsci_container]**

**docker/podman build -t <image-name> -f Dockerfile_ocsci . --build-arg BRANCH_ID_ARG=<branch-id/master/stable>**

BRANCH_ID_ARG:
There are 3 options:
1. BRANCH_ID_ARG is empty -> checkout to stable branch
2. BRANCH_ID_ARG=master -> checkout to master branch
3. BRANCH_ID_ARG=PR-ID [1234] -> checkout to relevant branch based on pr-id

### Run Container
```commandline
docker/podman run -v <kubeconfig-path>:/opt/cluster <image-name> run-ci --cluster-path /opt/cluster_path
--ocp-version <ocp-version> --ocs-version <ocs-version> --cluster-name <cluster-name> <test-path>
```
*Add Params:
```
kubeconfig-path: Path to kubeconfig on your local machine
image-name: Image name
run-ci params

```

Example:

```commandline
docker run -v ~/ClusterPath:/opt/cluster ocsci_image run-ci --cluster-path /opt/cluster_path
--ocp-version 4.12 --ocs-version 4.12 --cluster-name cluster-name tests/manage/z_cluster/test_must_gather.py
```

### Upload image to quay.io
**Login to quay.io**
docker/podman login -u <user-name> quay.io

**Tag Image**

docker/podman image tag ocsci-container quay.io/ocsci/ocs-ci-container:stable

**Push Image to quay registry**

podman push ocsci-container quay.io/ocsci/ocs-ci-container:stable

### Download image from quay.io

**docker/podman pull quay.io/ocsci/ocs-ci-container:stable**
