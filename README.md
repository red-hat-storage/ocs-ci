# OCS-CI

OCS CI is a framework to test Red Hat OCS features and functionality using AWS
and other supported platforms. The framework is based on CephCI
  ( https://github.com/red-hat-storage/cephci )

## Getting Started
#### Prerequisites
1. Python 3.7
2. AWS Account credentials when testing with AWS platforms

#### Installing
It is recommended that you use a python virtual environment to install the necessary dependencies

1. Setup a python 3.7 virtual environment. This is actually quite easy to do now.
    * `python3.7 -m venv <path/to/venv>`
    * `source <path/to/venv>/bin/activate`
2. Install requirements with `pip install -r requirements.txt`

#### Initial Setup

## Tests

#### AWS and CentralCI Authentication files
AWS and CentralCI Authentication files will reside in users home dir and will be used by
CLI option

#### Cluster Configuration
Cluster configuration that defines Openshift/Kubernetes Cluster along with Ceph Configuration
will reside in conf/ folder, This is still a work in progress.

#### Test Suites
All test suite configurations are found inside the `suites` directory.

```
tests:
- test:
    name: install OCS
    desc: Minimal installation of OCS
    module: test_ocs_basic_install.py
    config:
      installer-version: v0.16.1
      destroy-cluster: False
```

## Usage
**Work in progress**
`run.py` is the main script for ocs-ci. You can view the full usage details by passing in the `--help` argument.
```
python run.py --help
```
#### Required Arguments
There are a few arguments that are required ocs test execution:

* `--cred <cred_file>`
* `--suite <suite_file>`

#### Useful Arguments
Some non-required arguments that we end up using a lot:
* `--log level <level>` - set the log level that is output to stdout.

#### Examples
Run OCS install suite:
```
python run.py --cred ~/aws.yml --suite suites/ocs_basic_install.yml
--log-level info
```

## Results
**WIP**
In order to post results properly or receive results emails you must first configure your `~/.ocsci.yaml` file.
Please see the [Initial Setup](#initial-setup) section of the readme if you haven't done that.

#### Polarion
**WIP**
Results are posted to polarion if the `--post-results` argument is passed to `run.py`.
When this argument is used, any tests that have a `polarion-id` configured in the suite
will have it's result posted to polarion.

#### Report Portal
**WIP**
Results are posted to report portal if the `--report-portal` argument is passed to `run.py`.

#### Email
**WIP**
A result email is automatically sent to the address configured in your `~/.ocsci.yaml` file.
In addition to personally configured emails, if the `--post-results` or `--report-portal` arguments are
passed to `run.py` an email will also be sent to `ocsci@redhat.com`
