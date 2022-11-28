# ocs-ci Containers for OpenShift Data Foundation

This project builds containers for [ocs-ci].
The primary purpose for doing so was to be able to run [ocs-ci] tests with container

The containers are expected to run with a service account that has admin credentials.

### Build Image

**Change directory to Docker_files/ocsci_container [$ cd Docker_files/ocsci_container]**

**docker/podman build -t Dockerfile_ocsci -f Dockerfile_ocsci . --build-arg BRANCH_ID_ARG=<branch-id/master/stable>**

BRANCH_ID_ARG:
There are 3 options:
1. BRANCH_ID_ARG is empty -> checkout to stable branch
2. BRANCH_ID_ARG=master -> checkout to master branch
3. BRANCH_ID_ARG=PR-ID [1234] -> checkout to relevant branch based on pr-id
### Run Container
```commandline
docker/podman run -v <kubeconfig-path>:/opt/cluster/auth -e MARKER_PYTEST=<marker>
-e OCP_VERSION=<ocp-version> -e OCS_VERSION=<ocs-version> -e  CLUSTER_NAME=<cluster-path>
-e TEST_PATH=<test-path> -it <image-name> /bin/bash
```
*Add Params:
```
kubeconfig-path: Path to kubeconfig on your local machine
MARKER_PYTEST: Pytest marker ["tier1", "acceptance" ..]
OCP_VERSION: OCP Version ["4.11", "4.12" ..]
OCS_VERSION: ODF Version ["4.11", "4.12" ..]
CLUSTER_NAME: Cluster name [optional]
TEST_PATH: test path [tests/manage/z_cluster/test_must_gather.py]

```

Example:

```
docker run -v /home/odedviner/ClusterPath/auth:/opt/cluster/auth -e MARKER_PYTEST=tier1
-e OCP_VERSION=4.12 -e OCS_VERSION=4.11 -e  CLUSTER_NAME=cluster-aws
-e TEST_PATH=tests/manage/z_cluster/test_must_gather.py -it oded_image111 /bin/bash
```

### Upload image to quay.io
**Login to quay.io**
docker login -u <user-name> quay.io

**Tag Image**

docker image tag ocs-ci-stable quay.io/<user-name>/ocs-ci:ocs-ci-stable

**Push Image to quay registry**

docker image push quay.io/<user-name>/ocs-ci:ocs-ci-stable


### Download image from quay.io

**docker pull quay.io/<user-name>/ocs-ci:ocs-ci-stable**
