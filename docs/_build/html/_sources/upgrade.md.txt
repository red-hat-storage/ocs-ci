# Upgrade

Upgrade can be performed to the:

* latest one DS build (by default)
* next one Y version, for example current `4.2` -> latest `4.3`.
    (version passed by `--upgrade-ocs-version` param)
* specific image:tag specified by configuration file (see the example below)
    or cli param `--upgrade-ocs-registry-image`.
* next one DS build from current one - `upgrade_to_latest: false` has to be
  set, see here: [upgrade_to_next_build.yaml](https://github.com/red-hat-storage/ocs-ci/tree/master/conf/ocsci/upgrade_to_next_build.yaml)
  for more details.

## Deployment

To be able to upgrade your cluster you cannot deploy the latest downstream
build, because then you won't have any next build available for upgrade.

For this purpose we have this config file: [upgrade.yaml](https://github.com/red-hat-storage/ocs-ci/tree/master/conf/ocsci/upgrade.yaml)
which you can pass via `--ocsci-conf` when you are running the deployment. This will
automatically take not latest build but one before.

Use `--live-deploy` parameter in the case you would like to deploy GAed version.

## Upgrade execution

For upgrade we have few pytest marks defined [here](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/framework/pytest_customization/marks.py):

* **pre_upgrade** - mark tests which are suppose to run before upgrade
* **upgrade** - this is mark for upgrade itself
* **post_upgrade** - mark tests which are suppose to run after upgrade

Those marks has to be imported from mentioned
[module](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/framework/pytest_customization/marks.py) and your test have to
be decorated by those marks as we combine those mark decorators with order
decorator used by `pytest-ordering` plugin to properly order the tests.

To execute the upgrade you can use this command:

```bash
run-ci tests/
    --cluster-name kerberos_ID-ocs-deployment \
    --cluster-path /home/my_user/my-ocs-dir \
    -m upgrade
```

If you would like to trigger also pre/post upgrade tests run with following
markers:

```bash
run-ci tests/
    --cluster-name kerberos_ID-ocs-deployment \
    --cluster-path /home/my_user/my-ocs-dir \
    -m 'pre_upgrade or upgrade or post_upgrade'
```

In the case you would like to upgrade just to the one following DS build and not
to the latest one, please pass `conf/ocsci/upgrade_to_next_build.yaml`
configuration file to the upgrade execution.

For upgrade between versions like 4.2 to 4.3 you need to:

* specify version of OCS you have installed with `--ocs-version` param. This will
    load proper config for this version (default is loaded latest one version
    specified in OCS-CI default config). Example
* specify version of OCS you would like to upgrade to with `--upgrade-ocs-version`.
    This will automatically load new config for specific version.

Example:

```bash
run-ci tests/
    --cluster-name kerberos_ID-ocs-deployment \
    --cluster-path /home/my_user/my-ocs-dir \
    -m 'pre_upgrade or upgrade or post_upgrade' --ocs-version 4.2 --upgrade-ocs-version 4.3
```

Upgrade to specific build via config file:

```yaml
UPGRADE:
  upgrade_ocs_registry_image: "quay.io/rhceph-dev/ocs-olm-operator:latest-4.3"
```

Upgrade to specific build via cli option:

```bash
run-ci tests/
    --cluster-name kerberos_ID-ocs-deployment \
    --cluster-path /home/my_user/my-ocs-dir \
    -m 'pre_upgrade or upgrade or post_upgrade' --ocs-version 4.2 --upgrade-ocs-version 4.3 \
    --upgrade-ocs-registry-image 'quay.io/rhceph-dev/ocs-olm-operator:latest-4.3'

```

## Live upgrade

In the case you would like to upgrade from GAed version to GAed version you need
to use `--live-deploy` parameter for deployment as mentioned above, but also
when you starting upgrade execution to have proper must gather image from live content.

Also the `conf/upgrade/upgrade_in_current_source.yaml` should be passed via `--ocsci-conf`, to let
OCS-CI know, that it should not update catalog source to the internal build, but
keep in GAed content.

> In the case of Z-stream upgrade, only manual approval strategy job can be used.
> Or manually passing: `conf/ocsci/manual_subscription_plan_approval.yaml`
> The cluster needs to be installed before content is released.
> There is no way to install previous Z-stream version once content is released!

## Stage upgrade

In the case of upgrade from stage. This config file: `conf/ocsci/stage-rh-osbs.yaml`
should be passed via `--ocsci-conf` parameter.

Also the `conf/upgrade/upgrade_in_current_source.yaml` should be passed via `--ocsci-conf`, to let
OCS-CI know, that it should not update catalog source to the internal build, but
keep in stage content.

> In the case of Z-stream upgrade, only manual approval strategy job can be used.
> Or manually passing: `conf/ocsci/manual_subscription_plan_approval.yaml`
> The cluster needs to be installed before push to the stage.
> There is no way to install previous Z-stream version once content is pushed to the stage!
