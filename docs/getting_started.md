
# Getting Started

## Prerequisites

* Python >= 3.6
* oc client binary available in the $PATH.
  (running the oc version on terminal should display version > 3.11).
  The latest client can be downloaded from [the oc-client public mirror](https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/).
* GNU sed [1]

### Additional Prerequisites

#### AWS (IPI and UPI)

Configure AWS account credentials when testing with AWS platforms, using the
`aws` command line program.
Ensure that the [AWS configuration file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
is properly set up: either the default section in `~/.aws/credentials` must
have the correct values for the access key and secret key or you must ensure
that the profile you are using is set with the `AWS_PROFILE` environment
variable.

#### AWS UPI

The following must be installed:

* `awscli`
* [jq]( https://stedolan.github.io/jq/download/)

##### AWS UPI with RHEL workers
Along with the AWS UPI prerequisites:

* openshift-dev.pem needs to be available
* provide ops-mirror.pem in data/ directory [ops-mirror](https://github.com/openshift/shared-secrets/blob/master/mirror/ops-mirror.pem).

#### VMWare vSphere

The following must be installed:

* [terraform](https://learn.hashicorp.com/terraform/getting-started/install.html)
  version >= 0.11.13
* [jq]( https://stedolan.github.io/jq/download/)

#### Testing the OpenShift UI

For UI testing follow the instructions in [openshift console](https://github.com/openshift/console#dependencies) repository.
After cloning the repository and setting all dependencies create a configuration file with a RUN section with the `openshift_console_path`
   parameter pointing to your local clone of openshift console repo. See example in `conf/ocsci/ui-testing.yaml`.


## Installing

When developing or trying out the ocs-ci, it is recommended that you use a
python virtual environment to install the necessary dependencies and manage
the library modules.

1. Clone the [ocs-ci repository](https://github.com/red-hat-storage/ocs-ci)
    via the `git clone git@github.com:red-hat-storage/ocs-ci.git` command.
2. Go to the ocs-ci directory: `cd ocs-ci`.
3. Set up a [python virtual environment](https://docs.python.org/3.6/tutorial/venv.html).
   Typically, one will use a directory named `.venv` or `venv`. Both names are
   safe to use and ignored by the flake8 configuration in tox.
   Example:

    * `python -m venv <path/to/venv>`
    * `source <path/to/.venv>/bin/activate`

4. Upgrade pip and setuptools with `pip install --upgrade pip setuptools`
5. Install requirements with `pip install -r requirements.txt`
6. If you plan on working on the sources, you may also install the
   development decencies, such as
   `pre-config` tool to enforce commits sign-offs, flake8 compliance and more:

   * `pip install -r requirements-dev.txt`
   * `pre-commit install --hook-type pre-commit --hook-type commit-msg`

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

##### Nightly Builds

In addition you will need to add a registry auth to your pull-secret to
support deploying CI / Nightly builds. Please follow the instructions
[here](https://mojo.redhat.com/docs/DOC-1204026) to do so.

##### Quay Private Repos

To support pulling images from the new private repositories in quay, you will
need to add yet another registry auth to the auths section of your pull-secret.
Ask people on ocs-qe mailing list or chat room if you don't know where to find
the TOKEN.

```json
{"quay.io/rhceph-dev": { "auth": "TOKEN"}}
```

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

### Authentication Config

For some services we will require additional information in order to
successfully authenticate. This is a simple yaml file that you will need to
create manually.

Create a file under `ocs-ci/data/` named `auth.yaml`.

#### Quay

To authenticate with quay you will need to have an access token. You can
generate one yourself by following [the API doc](https://docs.quay.io/api/) or
you may use the one QE has generated already. Ask people on ocs-qe mailing list
or chat room if you don't know where to find the access token.

To enable ocs-ci to use this token, add the following to your `auth.yaml`:

```yaml
quay:
  access_token: 'YOUR_TOKEN'
```

## Deploying Clusters and Running Tests

Once you have set up the configuration files you are ready to start
executing the code included in the ocs-ci. First you must prepare
your environment for running the scripts:

* Run: `python setup.py develop`
* Verify that the `run-ci` script is now available in your path.
  Run `run-ci --help` and check that it prints help text.

### Deploying a Cluster

<!-- TODO(jjm) - I haven't got a working cluster using this yet. -->

When using a OpenShift with automatically provisioned hosts, you can request
that the ocs ci system set up the cluster for you. Example:

```
run-ci -m deployment --deploy --ocsci-conf <my_config.yaml>  --cluster-path=<cluster_dir>  --cluster-name=<prefix>-<name>
```

Depending on your infrastructure you may have additional options to pass. For
shared infrastructure on Red Hat AWS the prefix value in the cluster name should
be your kerberos id.

### Running Tests

<!-- TODO(jjm) - I haven't gotten a cluster yet so this is speculation. -->

To execute tests against a cluster that was deployed as in the previous section:

```
run-ci --ocsci-conf=ocs-ci-ocs.yaml --cluster-path=<cluster_dir>  --cluster-name=<prefix>-<name>
```

The `run-ci` tool wraps `py.test` any valid options for that tool can be passed
to `run-ci` to limit test selection, configure logging, etc.


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


## Notes

[1]: For Mac OSX Users

    The system `sed` package is not compatible with the script used to install AWS
    UPI. To resolve this issue, you must install `gnu-sed`. You can do this with brew.

        `brew install gnu-sed`

   In addition to this, you will need to ensure that `gnu-sed` is used instead
   of the system `sed`. To do this you will need to update your PATH accordingly.
   In your shell rc file (`~/.bashrc`, `~/.zshrc`, etc.) add the following
   line to the end of the file.

        `export PATH="/usr/local/opt/gnu-sed/libexec/gnubin:$PATH"`
