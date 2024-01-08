.. _results:

# Results
=========

## Report Portal

Sending test results to report portal is now done via
[rp_preproc](https://gitlab.cee.redhat.com/ccit/reportportal/rp_preproc/-/blob/rpv5/docs/Users.md).
We have automated a lot of the process around rp_preproc such that you should only need
to provide a JUnit XML report and the corresponding ocs-ci logs directory. For more
information on that process works check out the
[README](https://gitlab.cee.redhat.com/ocs/ocs4-jenkins/-/blob/master/scripts/python/report_portal/README.md).

### Launch Attributes

Launches are tagged with various attributes, so we can create filters in report portal.
Launch attributes are either single string values or key:value pairs delimited by a
colon (:). More information on how these are defined and consumed
[here](https://gitlab.cee.redhat.com/ocs/ocs4-jenkins/-/blob/master/scripts/python/report_portal/README.md).

#### Examples
Some attributes will be single strings like `upstream`, `stage`, or `fips`. This will
generally be the case with "boolean" type attributes.

Other attributes will be represented by key:value pairs like `platform:aws`,
`ocp_version:4.8`, or `worker_instance_type:m4.xlarge`. For these attributes there are
several values that could be paired with the key, so we choose this format.

Several attributes have different values based on the type of deployment or
cluster environment.
