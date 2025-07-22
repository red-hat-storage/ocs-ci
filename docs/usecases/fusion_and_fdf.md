# Fusion and Fusion Data Foundation

## Requirements

Fusion deployments require an OCP cluster.

FDF deployments require an OCP cluster with Fusion deployed.

In addition to the typical ocs-ci requirements for execution, we also require [skopeo](https://github.com/containers/skopeo/blob/main/install.md#installing-skopeo) for retrieving image data from the pre-release registry.

## Fusion

We support the deployment of Fusion on top of OCP deployments by using the `deploy-fusion` entry-point. You can see more info about this entry-point with `deploy-fusion --help`.

You can deploy Fusion with the following command:

`deploy-fusion --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH`

Note these are the same `CLUSTER_NAME` and `CLUSTER_PATH` you passed to `run-ci` to deploy OCP.

By default this will deploy the latest supported GA version of Fusion.

For pre-release versions of Fusion, you can use the same command with an additional config for the version you wish to install.

To install previous GA versions, use the appropriate GA config file present in [fusion_version](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/framework/conf/fusion_version)
```
deploy-fusion --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH --conf ocs_ci/framework/conf/fusion_version/fusion-2.8.yaml
```

To specify the location of the generated junit report, use the `--report` argument:
```
deploy-fusion --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH --report path/to/report.xml
```

## Fusion Data Foundation

We also support the deployment of Fusion Data Foundation as an alternative to ODF when Fusion is installed. Similar to Fusion there is an entry-point created for FDF deployments, `deploy-fdf`. You can see more info about this entry-point with `deploy-fdf --help`.

You can deploy FDF with the following command:

`deploy-fdf --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH --conf CLUSTER_CONF`

Note these are the same `CLUSTER_NAME` and `CLUSTER_PATH` you passed to `run-ci` to deploy OCP. In addition, `--conf` is a repeatable argument similar to `--ocsci-conf`. Generally just pass the same files you did to `run-ci` to deploy OCP. We need certain info about the cluster (such as platform) in order to configure storage properly.

By default this will deploy the latest supported GA version of FDF.

For pre-release versions of FDF, you can use the same command with an additional config for the version you wish to install.

```
deploy-fdf --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH --conf CLUSTER_CONF --conf ocs_ci/framework/conf/fdf_version/fdf-4.18.yaml
```

Note for pre-release deployments: you will need to add a section to your `pull-secret` which contains your credentials to the registry where pre-release images are stored. Please reach out to the ecosystem team for more information.

To specify the location of the generated junit report, use the `--report` argument:
```
deploy-fdf --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH --conf CLUSTER_CONF --report path/to/report.xml
```
