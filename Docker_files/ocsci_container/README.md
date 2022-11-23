# ocs-ci Containers for OpenShift Data Foundation

This project builds containers for [ocs-ci].
The primary purpose for doing so was to be able to run [ocs-ci] tests with container

The containers are expected to run with a service account that has admin credentials.

### Procedure

**Copy kubeconfig to Docker_files/ocsci_container/scripts dir**

**Change directory to Docker_files/ocsci_container [$ cd Docker_files/ocsci_container]**

**Run Build cmd**
```commandline
docker/podman build -t <image-name> -f Dockerfile_ocsci . --build-arg TEST_PATH_ARG=<test-path> --build-arg OCP_VERSION_ARG=<ocp-version> --build-arg OCS_VERSION_ARG=<ocs-version> --build-arg MARKER_PYTEST_ARG=<marker-pytest>

```
*Add Params:
```
TEST_PATH_ARG: Test Path ["tests/xyz"]
OCP_VERSION_ARG: OCP Version ["4.11", "4.12" ..]
OCS_VERSION_ARG: ODF Version ["4.11", "4.12" ..]
MARKER_PYTEST_ARG: Pytest marker ["tier1", "acceptance" ..]

```

Example:

```
docker build -t ocsci-image -f Dockerfile_ocsci . --build-arg TEST_PATH_ARG="tests/manage/z_cluster/test_must_gather.py" --build-arg OCP_VERSION_ARG="4.12" --build-arg OCS_VERSION_ARG="4.12" --build-arg MARKER_PYTEST_ARG="tier1"
```
