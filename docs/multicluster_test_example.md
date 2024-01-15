# Multi-cluster test example

The following example and notes are based on the main
[PR#4875](https://github.com/red-hat-storage/ocs-ci/pull/4875) for this feature.

## Example test

Following example is simple test which do basically nothing, just shows how to
switch between cluster configuration contexts and how to access configuration
of particular cluster without switching context to it.

```python
import logging
import os

from ocs_ci.utility.utils import exec_cmd

from ocs_ci.framework import config

log = logging.getLogger(__name__)


def test_multicluster_dahorak():
    # print number of clusters to the log
    log.info(f"Number of clusters: {config.nclusters}")

    # iterate over the clusters and perform some actions on each cluster
    for i in range(config.nclusters):
        # switch context to the i-th cluster
        config.switch_ctx(i)
        # print the cluster name of the selected cluster
        log.info(f"CLUSTER_NAME: {config.ENV_DATA['cluster_name']}")
        # run oc command against the selected cluster
        log.info(exec_cmd("oc version").stdout)
        # print kubeconfig path of the selected cluster
        log.info(os.environ["KUBECONFIG"])

    # access configuration of particular cluster without switching context to it
    log.info(f"Cluster name of first cluster: {config.clusters[0].ENV_DATA['cluster_name']}")
    log.info(f"Cluster name of second cluster: {config.clusters[1].ENV_DATA['cluster_name']}")
```

## Execution of the example test

See also [Running tests on multicluster environment section in Usage
doc](usage.md).

I saved the above example test into `tests/test_multicluster_dahorak.py` file.
Additionally I have two clusters and related cluster dirs:

* cluster: `dahorak-test1`, cluster path: `../clusters/dahorak-test1/`
* cluster: `dahorak-test2`, cluster path: `../clusters/dahorak-test2/`

(Each cluster dir contains only `auth/kubeconfig` directory/file.)

Now I'll run the test via following command:

```bash
$ run-ci multicluster 2 tests/test_multicluster_dahorak.py \
    --ocsci-conf ocsci_conf.yaml \
    --cluster1 --cluster-path ../clusters/dahorak-test1/ --cluster-name dahorak-test1 --ocp-version 4.10 \
    --cluster2 --cluster-path ../clusters/dahorak-test2/ --cluster-name dahorak-test2 --ocp-version 4.9 --ocsci-conf config_for_cluster2.yaml
```
* On the second line, I provided configuration file common for both clusters (beside other parameters I also set `skip_ocs_deployment: True`, to avoid additional checks and log messages in the console output).
* Third and fourth line is specific for particular cluster (cluster path, cluster name and additional configuration variables and files).

The output (truncated for better readability) looks like this:
```
$ run-ci multicluster 2 \
    tests/test_multicluster_dahorak.py \
    --ocsci-conf ocsci_conf.yaml \
    --cluster1 --cluster-path ../clusters/dahorak-test1/ --cluster-name dahorak-test1 --ocp-version 4.10 \
    --cluster2 --cluster-path ../clusters/dahorak-test2/ --cluster-name dahorak-test2 --ocp-version 4.9 --ocsci-conf config_for_cluster2.yaml
2022-02-02 16:19:21,790 - MainThread - ocs_ci.framework.pytest_customization.ocscilib - INFO - Pytest configure switching to: cluster=0
2022-02-02 16:19:22,410 - MainThread - ocs_ci.framework.pytest_customization.ocscilib - INFO - Dump of the consolidated config file is located here: /tmp/run-1643815158-cl0-config.yaml
2022-02-02 16:19:22,411 - MainThread - ocs_ci.framework.pytest_customization.ocscilib - INFO - Skipping version collection because we skipped the OCS deployment
2022-02-02 16:19:22,411 - MainThread - ocs_ci.framework.pytest_customization.ocscilib - INFO - Pytest configure switching to: cluster=1
2022-02-02 16:19:22,994 - MainThread - ocs_ci.framework.pytest_customization.ocscilib - INFO - Dump of the consolidated config file is located here: /tmp/run-1643815158-cl1-config.yaml
2022-02-02 16:19:22,994 - MainThread - ocs_ci.framework.pytest_customization.ocscilib - INFO - Skipping version collection because we skipped the OCS deployment
================================== test session starts ==================================
platform linux -- Python 3.9.7, pytest-5.3.5, py-1.11.0, pluggy-0.13.1
rootdir: ocs-ci, inifile: pytest.ini
plugins: metadata-1.11.0, ordering-0.6, marker-bugzilla-0.9.4, logger-0.5.1, html-2.1.1, flaky-3.7.0, repeat-0.9.1, profiling-1.7.0
collected 1 item

tests/test_multicluster_dahorak.py::test_multicluster_dahorak
------------------------------------ live log setup -------------------------------------
  ...
------------------------------------- live log call -------------------------------------
16:19:23 - MainThread - tests.test_multicluster_dahorak - INFO - Number of clusters: 2
16:19:23 - MainThread - tests.test_multicluster_dahorak - INFO - CLUSTER_NAME: dahorak-test1
16:19:23 - MainThread - ocs_ci.utility.utils - INFO - Executing command: oc version
16:19:24 - MainThread - tests.test_multicluster_dahorak - INFO - b'Client Version: 4.10.0-0.nightly-2022-02-02-000921\nServer Version: 4.10.0-0.nightly-2022-02-02-000921\nKubernetes Version: v1.23.3+b63be7f\n'
16:19:24 - MainThread - tests.test_multicluster_dahorak - INFO - ../clusters/dahorak-test1/auth/kubeconfig
16:19:24 - MainThread - tests.test_multicluster_dahorak - INFO - CLUSTER_NAME: dahorak-test2
16:19:24 - MainThread - ocs_ci.utility.utils - INFO - Executing command: oc version
16:19:25 - MainThread - tests.test_multicluster_dahorak - INFO - b'Client Version: 4.10.0-0.nightly-2022-02-02-000921\nServer Version: 4.9.17\nKubernetes Version: v1.22.3+e790d7f\n'
16:19:25 - MainThread - tests.test_multicluster_dahorak - INFO - ../clusters/dahorak-test2/auth/kubeconfig
16:19:25 - MainThread - tests.test_multicluster_dahorak - INFO - Cluster name of first cluster: dahorak-test1
16:19:25 - MainThread - tests.test_multicluster_dahorak - INFO - Cluster name of second cluster: dahorak-test2
PASSED                                                                            [100%]

============================= 1 passed in 60.92s (0:01:00) ==============================
```

Just few notes about the output:
* in the beginning of the output, configuration for first and second cluster is
  dumped to `/tmp/run-1643815158-cl0-config.yaml` and
  `/tmp/run-1643815158-cl1-config.yaml` files respectively.
* Then in the test section (`--- live log call ---`), it prints cluster name,
  output of `oc version` command and path to `kubeconfig` for the first cluster
  and then the same for the second cluster.
