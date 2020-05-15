# Using ocs-ci to deploy OCP cluster without default OCS installation

When you need to perform OCS installation manually (eg. to verify installation
docs or to use not yet automated deployment options), but still would like to
use ocs-ci to prepare the cluster as usual (eg. via deploy cluster jenkins
job), there is an option to skip OCS deployment.

## Option skip_ocs_deployment

Installation of OCS will be skipped when in `ENV_DATA` section of ocs-ci
configuration file, option `skip_ocs_deployment` is set to `true`.

If you don't need to change anything else in ocs-ci configuration file, you
can just use `conf/ocsci/skip_ocs_deploy.yaml` file instead of creating your
own config file.

## Skipping OCS installation in deploy cluster jenkins job

Add `conf/ocsci/skip_ocs_deploy.yaml` to `CLUSTER_CONF` parameter of your
deployment job.

If you need to use a particular deployment configuration (to select
particular platform, in particular way), you can specify multiple config file
names separated by a space in `CLUSTER_CONF` field. Eg.:

```
conf/deployment/aws/ipi_1az_rhcos_lso_3m_3w.yaml conf/ocsci/skip_ocs_deploy.yaml
```

## Enabling catalog source with development builds of OCS

When you skip OCS installation, you will be able to install already released
(so called live) version of OCS only.

If you need to use dev. builds of OCS, you need to create a catalog source
first:

```
$ oc create -f ocs_ci/templates/ocs-deployment/catalog-source.yaml
```

If you need to use particular CI build of OCS, edit image specification of
ocs-catalogsource and relplace tag `latest` with a version of your choice, eg.
`4.4.0-420.ci`.
