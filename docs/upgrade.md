# Upgrade

Upgrade can be performed to the:

* latest one DS build (by default)
* next one DS build from current one - `upgrade_to_latest: false` has to be
  set, see here: [upgrade_to_next_build.yaml](/conf/ocsci/upgrade_to_next_build.yaml)
  for more details.

## Deployment

To be able to upgrade your cluster you cannot deploy the latest downstream
build, because then you won't have any next build available for upgrade.

For this purpose we have this config file: [upgrade.yaml](/conf/ocsci/upgrade.yaml)
which you can pass via `--ocsci-conf` when you are running the deployment. This will
automatically take not latest build but one before.

## Upgrade execution

For upgrade we have few pytest marks defined [here](/ocs_ci/framework/pytest_customization/marks.py):

* **pre_upgrade** - mark tests which are suppose to run before upgrade
* **upgrade** - this is mark for upgrade itself
* **post_upgrade** - mark tests which are suppose to run after upgrade

Those marks has to be imported from mentioned
[module](/ocs_ci/framework/pytest_customization/marks.py) and your test have to
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

## TODO

Once there will be 4.3 build we will need to implement the functionality to
specify which version is going to be used for the upgrade. Like 4.2 -> 4.3.
