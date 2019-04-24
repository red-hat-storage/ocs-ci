# Results

**WIP**

In order to post results properly or receive results emails you must first configure your `~/.ocs-ci.yaml` file.
Please see the [Initial Setup](#initial-setup) section of the readme if you haven't done that.

## Polarion

**WIP**

Results are posted to polarion if the `--post-results` argument is passed to
`run.py`. When this argument is used, any tests that have a `polarion-id` 
configured in the suite will have it's result posted to polarion.

## Report Portal

**WIP**

Results are posted to report portal if the `--report-portal` argument is passed to `run.py`.

## Email

**WIP**

A result email is automatically sent to the address configured in your
`~/.ocs-ci.yaml` file.

In addition to personally configured emails, if the `--post-results` or
`--report-portal` arguments are passed to `run.py` an email will also be sent
to QE (email pending)