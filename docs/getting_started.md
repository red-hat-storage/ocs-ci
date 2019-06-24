
# Getting Started

## Prerequisites

1. Python >= 3.6
2. Configure AWS Account credentials when testing with AWS platforms,
   check default section in `~/.aws/credentials` for access/secret key
   [check aws-configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).
3. oc client binary is installed on your localhost and binary is listed in $PATH
   (running oc version on terminal should display version > 3.11).
   Latest client can be downloaded from [oc-client](https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/).

## Installing

It is recommended that you use a python virtual environment to install the
necessary dependencies

1. Clone ocs-ci repository from
    [https://github.com/red-hat-storage/ocs-ci](https://github.com/red-hat-storage/ocs-ci)
    via cmd `git clone git@github.com:red-hat-storage/ocs-ci.git`.
2. Go to ocs-ci folder `cd ocs-ci`.
3. Setup a python 3.7 virtual environment. This is actually quite easy to do
    now. Use hidden `.venv` or normal `venv` folder for virtual env as we are
    ignoring this in flake8 configuration in tox.

    * `python3.7 -m venv <path/to/venv>`
    * `source <path/to/.venv>/bin/activate`

4. Upgrade pip with `pip install --upgrade pip`
5. Install requirements with `pip install -r requirements.txt`

## Initial Setup

### OCS-CI config

Configure your ocs-ci.yaml and pass it with --ocsci-conf parameter

This file is used to allow configuration around a number of things within ocs-ci.
The default file is in conf/ocsci/default_config.yaml.
TODO: Once moved to pytest update properly this file: `ocs-ci.yaml.template`.

The required keys are in the template. Values are placeholders and should be replaced by legitimate values.
Values for report portal or polarion are only required if you plan on posting to that particular service.

Move a copy of the template to your conf directory and edit it from there with
the proper values and pass it with --ocsci-conf parameter to pytest.


### Pull Secret

In order to deploy a cluster to AWS with the Openshift Installer,
you will need to download the pull secret for your account.
Download this file from [openshift.com](https://cloud.openshift.com/clusters/install)
and place in the `data` directory at the root level of the project.
If there is no `data` directory, create one.
The name of the file should be `pull-secret`.

## Tests

### AWS and CentralCI Authentication files

AWS and CentralCI Authentication files will reside in users home dir and will be used by
CLI option

### Cluster Configuration

Cluster configuration that defines Openshift/Kubernetes Cluster along with Ceph Configuration
will reside in conf/ folder, This is still a work in progress.
