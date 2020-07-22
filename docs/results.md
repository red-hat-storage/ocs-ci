# Results

## Report Portal

Sending test results to report portal is supported via the [pytest agent](https://github.com/reportportal/agent-python-pytest).

#### Configuration

We have a configuration file template that you can fill out with our team's report portal
instance and user information in order to enable posting results.

1. Copy the config template located at `templates/reporting/report_portal.cfg` to another location
2. Edit the placeholder values here with the report portal instance/user details.

You will now be able to post your test results to report portal by providing the appropriate CLI options.

Ex.
```bash
run-ci tests/test_pytest.py --reportportal --rp-launch CfgTest --rp-launch-desc "An example launch description" -c /home/$USER/report_portal.cfg
```


#### Tags

Launches are tagged with various information so we can create filters
in report portal.

##### Basic Tags
Several tags have different values based on the type of deployment or
cluster environment.

platform
```
aws / vsphere / baremetal
```
deployment_type
```
ipi / upi
```
us_ds
```
upstream / downstream
```

##### Key/Value Tags
Some tags are key:value pairs. These are usually used for versions.

```
worker_instance_type:m4.xlarge
```
```
ocs_version:4.5
```
```
ocp_version:4.5
```
```
ocs_registry_image:quay.io/rhceph-dev/ocs-olm-operator:latest-4.5
```
```
ocs_registry_tag:latest-4.5
```

##### Boolean Tags
Several tags will not exist if the tag does not apply to the deployment
or the cluster environment.

```
ui_deployment
live_deployment
stage_deployment
production
fips
```
