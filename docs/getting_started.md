
# Getting Started

## Prerequisites

1. Python >= 3.6
2. Configure AWS Account credentials when testing with AWS platforms,
   check default section in `~/.aws/credentials` for access/secret key
   [check aws-configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).
3. oc client binary is installed on your localhost and binary is listed in $PATH
   (running oc version on terminal should display version > 3.11).
   Latest client can be downloaded from [oc-client](https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/).
4. For vSphere based installations, [terraform](https://learn.hashicorp.com/terraform/getting-started/install.html)
   and [jq]( https://stedolan.github.io/jq/download/) should be installed ( terraform version should be 0.11.13  )

#### AWS UPI
There are additional prerequisites if you plan to execute AWS UPI deployments

1. Install the `jq` and `awscli` system packages

##### Mac OSX Users
The system `sed` package is not compatible with the script used to install AWS
UPI. To resolve this issue, you must install `gnu-sed`. You can do this with brew.

    brew install gnu-sed

In addition to this, you will need to ensure that `gnu-sed` is used instead
of the system `sed`. To do this you will need to update your PATH accordingly.
In your shell rc file (`~/.bashrc`, `~/.zshrc`, etc.) add the following
line to the end of the file.

    export PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"

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

4. Upgrade pip and setuptools with `pip install --upgrade pip setuptools`
5. Install requirements with `pip install -r requirements.txt`

## Initial Setup

### OCS-CI config

Configure your ocs-ci.yaml and pass it with --ocsci-conf parameter

This file is used to allow configuration around a number of things within ocs-ci.
The default file is in `ocs_ci/framework/conf/default_config.yaml`.

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

In addition you will need to add a registry auth to your pull-secret to
support deploying CI / Nightly builds. Please follow the instructions
[here](https://mojo.redhat.com/docs/DOC-1204026) to do so.

### SSH key

We would like to use a shared ssh key with engineering which allows us to connect
to the nodes via known ssh key for QE and engineering.
To setup the shared public ssh key for your deployment you have to follow
these steps:

Download private openshift-dev ssh key from secret location to
`~/.ssh/openshift-dev.pem`.

```console
chmod 600 ~/.ssh/openshift-dev.pem
ssh-keygen -y -f ~/.ssh/openshift-dev.pem > ~/.ssh/openshift-dev.pub
```

Ask people on ocs-qe mailing list or chat room if you don't know where to find the
secret URL for openshift-dev key. Or look for this mail thread:
`Libra ssh key replaced by openshift-dev key` where the URL was mentioned.

If you would like to use a different path, you can overwrite it in the custom
config file under the DEPLOYMENT section with this key and value:
`ssh_key: "~/your/custom/path/ssh-key.pub"`.

If you don't want to use the shared key, you can change this value to
`~/.ssh/id_rsa.pub` to use your own public key.

> If the public key does not exist, the deployment of this public key is skipped.

How to connect to the node via SSH you can find [here](./debugging.md).

## Tests

### AWS and CentralCI Authentication files

AWS and CentralCI Authentication files will reside in users home dir and will be used by
CLI option

### Cluster Configuration

Cluster configuration that defines Openshift/Kubernetes Cluster along with Ceph Configuration
will reside in conf/ folder, This is still a work in progress.

### Email

To send test run reports to email ID's, postfix should be installed on fedora

    * `sudo dnf install postfix`
    * `systemctl enable postfix.service`
    * `systemctl start postfix.service`
