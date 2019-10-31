# Upgrade

Currently the upgrade can be performed just between follow up downstream builds
e.g. 171 -> 172.
There is opened [BZ](https://bugzilla.redhat.com/show_bug.cgi?id=1767400) which
tracking the issue which prevents us to upgrade to the latest available build.

There is prepared config option `upgrade_to_latest` in the DEPLOYMENT section
which can be set or will be the default value when the BZ gets resolved.

## Deployment

To be able upgrade your cluster you cannot deploy the latest downstream
build, because then you won't have any next build available for upgrade.

For this purpose we have this config file: [upgrade.yaml](/conf/ocsci/upgrade.yaml)
which you can pass via `--ocsci-conf` when you are running the deployment. This will
automatically take not latest build but one before.

## Upgrade execution

For upgrade we have few pytest marks defined [here](/ocs_ci/framework/pytest_customization/marks.py):

* **before_upgrade** - mark tests which are suppose to run before upgrade
* **upgrade** - this is mark for upgrade itself
* **after_upgrade** - mark tests which are suppose to run after upgrade

Those marks has to be imported from mentioned
[modul](/ocs_ci/framework/pytest_customization/marks.py) and your test have to
be decorated by those marks as we combine those mark decorators with order
decorator used by `pytest-ordering` plugin to properly order the tests.

To execute the upgrade you can use this command:

```bash
run-ci tests/
    --cluster-name kerberos_ID-ocs-deployment \
    --cluster-path /home/my_user/my-ocs-dir \
    -m 'before_upgrade or upgrade or after_upgrade'
 ```

Of course if you would like to run just the upgrade and not any other tests
before and after upgrade you can use just `-m upgrade`

## TODO

Once there will be 4.3 build and we will have different branch for OCS-CI we
will need to run upgrade in two steps. First run just with `-m before_upgrade`
from 4.2 branch if we will have such tests and then with
`-m 'upgrade or after_upgrade'` from 4.3 (master branch) to run everything from
proper branches designed for version of OCS.
