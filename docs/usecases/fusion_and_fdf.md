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

## Fusion Data Foundation

We also support the deployment of Fusion Data Foundation as an alternative to ODF when Fusion is installed. Similar to Fusion there is an entry-point created for FDF deployments, `deploy-fdf`. You can see more info about this entry-point with `deploy-fdf --help`.

You can deploy FDF with the following command:

`deploy-fdf --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH`

Note these are the same `CLUSTER_NAME` and `CLUSTER_PATH` you passed to `run-ci` to deploy OCP.

By default this will deploy the latest supported GA version of FDF.

For pre-release versions of FDF, you can use the same command with an additional config for the version you wish to install.

```
deploy-fdf --cluster-name CLUSTER_NAME --cluster-path CLUSTER_PATH --conf ocs_ci/framework/conf/fdf_version/fdf-4.18.yaml
```
