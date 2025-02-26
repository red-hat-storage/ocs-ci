# Fusion and Fusion Data Foundation

## Fusion

We support the deployment of Fusion on top of OCP deployments.

The following config file should be passed when deploying Fusion:

`conf/ocsci/fusion_deployment.yaml`

For pre-release versions of Fusion, you can use this config (or any in this directory depending on the version you need):

`ocs_ci/framework/conf/fusion_version/fusion-2.8.yaml`


## Fusion Data Foundation

We also support the deployment of Fusion Data Foundation as an alternative to ODF when Fusion is installed.

The following config file should be passed when deploying FDF:

`conf/ocsci/fdf_deployment.yaml`

For pre-release versions of FDF, you can use this config (or any in this directory depending on the version you need):

`ocs_ci/framework/conf/fdf_version/fdf-4.18.yaml`

We also require [skopeo](https://github.com/containers/skopeo/blob/main/install.md#installing-skopeo) for retrieving image data from the pre-release registry.
