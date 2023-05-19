# ocs-ci Containers for OpenShift Data Foundation

This project builds containers for [ocs-ci].
The primary purpose for doing so was to be able to run [ocs-ci] tests with container
The containers are expected to run with a service account that has admin credentials.

### Build Image
* Build Image on your local machine
* PARAMS:
  * IMAGE_NAME - image name [e.g. quay.io/ocsci/ocs-ci-container:release-4.12]
  * ENGINE - engine name [podman/docker]
* Example:
  ```
  make build-image \
  IMAGE_NAME=quay.io/ocsci/ocs-ci-container:stable \
  ENGINE=podman
  ```

### Running ODF tests
* Running ODF tests
* PARAMS:
  * CLUSTER_PATH - path to kubeconfig on your local machine [e.g: ~/cluster_path]
  * RUN_CI - run-ci cmd for more info https://github.com/red-hat-storage/ocs-ci/tree/master/docs
    ```
    run-ci --cluster-path /opt/cluster \
    --ocp-version 4.12 \
    --ocs-version 4.12 \
    --cluster-name cluster-name \
     tests/manage/z_cluster/test_osd_heap_profile.py
    ```
  * ENGINE - engine name [podman/docker] [optional, by default docker]
  * PULL_IMAGE - there is option to pull the image from `quay.io/ocsci/ocs-ci-container`
  * IMAGE_NAME - image name [e.g. quay.io/ocsci/ocs-ci-container:release-4.12]
  * AWS_PATH - PATH to AWS credintials [e.g = ~/.aws] [optional]
* Example:
  ```
  make run-odf CLUSTER_PATH=~/ClusterPath \
  RUN_CI="run-ci ..." \
  ENGINE=podman \
  PULL_IMAGE=quay.io/ocsci/ocs-ci-container:release-4.12 \
  IMAGE_NAME=quay.io/ocsci/ocs-ci-container:release-4.12 \
  AWS_PATH=~/.aws
  ```

### Run Managed Service
* Running Managed Service deployment/tests
* PARAMS:
  * CLUSTER_PATH - path to kubeconfig on your local machine [e.g: ~/cluster_path]
  * RUN_CI - run-ci cmd for more info https://github.com/red-hat-storage/ocs-ci/tree/master/docs
    ```
    run-ci --color=yes ./tests/ \
    -m deployment \
    --ocs-version 4.10 \
    --ocp-version 4.10 \
    --ocsci-conf=conf/deployment/rosa/managed_3az_provider_qe_3m_3w_m54x.yaml \
    --ocsci-conf=/opt/cluster/ocm-credentials-stage \
    --cluster-name cluster-provider \
    --cluster-path /opt/cluster/p1 \
    --deploy
    ```
  * ENGINE - engine name [podman/docker] [optional, by default docker]
  * PULL_IMAGE - there is option to pull the image from `quay.io/ocsci/ocs-ci-container`
  * IMAGE_NAME - image name [e.g. quay.io/ocsci/ocs-ci-container:release-4.12]
  * AWS_PATH - PATH to AWS credintials [e.g: ~/.aws]
  * OCM_CONFIG - PATH to ocm configuration file [e.g: ~/.config/ocm]
  * PROVIDER_NAME - Provider Name [we don't need PROVIDER_NAME param for consumer cluster]
* Example:
  ```
  make run-managed-service CLUSTER_PATH=~/ClusterPath \
  RUN_CI="run-ci ..." \
  ENGINE=podman \
  PULL_IMAGE=quay.io/ocsci/ocs-ci-container:release-4.12 \
  IMAGE_NAME=quay.io/ocsci/ocs-ci-container:release-4.12 \
  AWS_PATH=~/.aws \
  OCM_CONFIG= ~/.config/ocm \
  PROVIDER_NAME = cluster-provider
  ```

### Upload image to quay.io
**Login to quay.io**
```
docker/podman login -u <user-name> quay.io
```

**Tag Image**
```
docker/podman image tag <IMAGE_NAME> quay.io/ocsci/ocs-ci-container:<tag_name>
```

**Push Image to quay registry**
```
docker/podman push ocsci-container quay.io/ocsci/ocs-ci-container:<tag_name>
```

**Pull Image from quay registry**
```
docker/podman pull quay.io/ocsci/ocs-ci-container:<tag_name>
```

**Running OCS-CI container**
```
podman/docker run
-v <cluster-path>:/opt/cluster:Z
-v <repo-path>:ocs-ci/data:/opt/ocs-ci/data:Z
quay.io/ocsci/ocs-ci-container:<tag_name>
run-ci --cluster-path /opt/cluster --ocp-version 4.13 --ocs-version 4.13 --cluster-name <cluster-name>
<test_path>
```
******************************************************************************
**We can add `--ocsci-conf /opt/cluster/logs.yaml` and we will get logs**

* $ cat logs.yaml
```
RUN:
  log_dir: "/opt/cluster"
```
