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

## Catalog sources with CI builds

Note that this section assumes you have your environment configured as if you
were running ocs-ci tests (`oc` can connect to your cluster, pull secret
provided to `openshift-installer` by ocs-ci during OCP deployment included one
for `quay.io/rhceph-dev` and `brew.registry.redhat.io`).
See also [Usage](/usage.md) guide.

### Enabling catalog source with development builds of LSO

If you are using CI builds of OCP and plan to use OCS with LSO (local storage
operator), you will need to create a catalog source so that CI version of LSO
will be available in OCP operatorhub.

To do that, you can reuse `local-storage-optional-operators.yaml` file from
ocs-ci repository:

```
$ oc create -f ocs_ci/templates/ocs-deployment/local-storage-optional-operators.yaml
```

### Enabling catalog source with development builds of OCS

When you skip OCS installation, you will be able to install already released
(so called live) version of OCS only.

**From ODF 4.9 (build: 4.9.0-166.ci) catalog source has to be named redhat-operators, hence before
creating one from template, you should disable the default one by following
command:**

```console
oc patch operatorhub.config.openshift.io/cluster -p='{"spec":{"sources":[{"disabled":true,"name":"redhat-operators"}]}}' --type=merge
```

If you need to use dev. builds of OCS, you need to create a catalog source for
these images first using
``ocs_ci/templates/ocs-deployment/catalog-source.yaml`` template.

In the template, you have to edit image specification of ocs-catalogsource
and replace tag `latest` with either a version of your choice, eg.
`4.4.0-420.ci` if you need to use particular CI build of OCS, or use one of
release specific tags such as `latest-4.4` or `latest-stable-4.4`.
There is no `latest` tag assigned to these `quay.io/rhceph-dev` dev container
images.

When you specify the tag in ``catalog-source.yaml`` file, you can create the
catalog source via:

```
$ oc create -f ocs_ci/templates/ocs-deployment/catalog-source.yaml
```
