# ocs-ci Containers for OpenShift Dedicated Test Suite

This is a repository that builds containers for [ocs-ci].
The primary purpose for doing so was to be able to run [ocs-ci] tests

The containers are expected to run with a service account that has admin credentials.

### Procedure

*Copy kubeconfig to Docker_files/ocsci_container/scripts dir

*Add Params:
```
TEST_PATH_ARG: Test Path ["tests/xyz"]
OCP_VERSION_ARG: OCP Version ["4.11", "4.12" ..]
OCS_VERSION_ARG: ODF Version ["4.11", "4.12" ..]
MARKER_PYTEST_ARG: Pytest marker ["tier1", "acceptance" ..]

```

Example:

```
docker build -t <image_name> -f Dockerfile_ocsci . --build-arg TEST_PATH_ARG="tests/manage/z_cluster/test_must_gather.py" --build-arg OCP_VERSION_ARG="4.12" --build-arg OCS_VERSION_ARG="4.12" --build-arg MARKER_PYTEST_ARG="tier1"
```
