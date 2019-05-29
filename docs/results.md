# Results

## Report Portal

Sending test results to report portal is supported via the [pytest agent](https://github.com/reportportal/agent-python-pytest).
For in-depth details on how to utilize this plugin, check their documentation.

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
Please check out the [doc](https://github.com/reportportal/agent-python-pytest) for more info on how to use this plugin properly.
