

# Getting Started

## Prerequisites

1. Python version >= 3.8
2. Following dependency packages for fedora/centos for successfully installing modules in virtualenv
   - gcc, git, openssl-devel, python3-devel or python specific version packages 
   depends on Python version installed e.g. python38-devel (or similar packages for ubuntu).
3. Configure AWS Account credentials when testing with AWS platforms,
   check default section in `~/.aws/credentials` for access/secret key
   [check aws-configuration](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).
4. oc client binary is installed on your localhost and binary is listed in $PATH
   (running oc version on terminal should display version > 3.11).
   Latest client can be downloaded from [oc-client](https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/).
5. For vSphere based installations, [terraform](https://learn.hashicorp.com/terraform/getting-started/install.html)
   and [jq]( https://stedolan.github.io/jq/download/) should be installed ( terraform version should be 0.11.13  )
6. Installation of ovirt-engine-sdk-python requires `curl-config` and
   `libxml/xmlreader.h` (they are on Fedora, RHEL or CentOS provided by
   packages `libcurl-devel` and `libxml2-devel` respectively).

#### AWS UPI
There are additional prerequisites if you plan to execute AWS UPI deployments

1. Install the `jq`, `yq` (v4.12.1+), and `awscli` system packages.

##### AWS UPI with RHEL workers
Along with AWS UPI prerequisites we need following

1. openshift-dev.pem needs to be available to ocs-ci
2. provide authentication credentials for mirror.openshift.com in `auth.yaml`:

```yaml
mirror_openshift:
  user: "USERNAME"
  password: "PASSWORD"
```

#### vSphere UPI
For vSphere UPI deployments, RHCOS templates must be present on Cluster in Datacenter

Till OCP version 4.9, download RHCOS templates from [here](https://raw.githubusercontent.com/openshift/installer/release-4.9/data/data/rhcos.json)
e.g: From above url, form the exact download path using baseURI + vmware (in images section )

[link](https://rhcos-redirector.apps.art.xq1c.p1.openshiftapps.com/art/storage/releases/rhcos-4.9/49.84.202110081407-0/x86_64/rhcos-49.84.202110081407-0-vmware.x86_64.ova) to download OCP 4.9 RHCOS template

From OCP 4.10 version, download RHCOS templates from [here](https://github.com/openshift/installer/blob/release-4.10/data/data/coreos/rhcos.json)

e.g: [link](https://rhcos-redirector.apps.art.xq1c.p1.openshiftapps.com/art/storage/releases/rhcos-4.10/410.84.202205191234-0/x86_64/rhcos-410.84.202205191234-0-vmware.x86_64.ova) to download OCP 4.10 RHCOS template

For GA'ed version, download RHCOS templates from [here](https://mirror.openshift.com/pub/openshift-v4/dependencies/rhcos/4.10/4.10.3/)

Follow this [procedure](https://docs.vmware.com/en/VMware-vSphere/7.0/com.vmware.vsphere.vm_admin.doc/GUID-17BEDA21-43F6-41F4-8FB2-E01D275FE9B4.html) to deploy ova/ovf template in vCenter.

#### vSphere IPI
Since vSphere IPI deployment require access to vCenter, we must add vCenter’s trusted root CA certificates to the
system trust before installing an OCP cluster

Follow this [procedure](https://docs.openshift.com/container-platform/4.7/installing/installing_vsphere/installing-vsphere-installer-provisioned.html#installation-adding-vcenter-root-certificates_installing-vsphere-installer-provisioned)
to add vCenter’s trusted root CA certificates.

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
3. Setup a python 3.8 virtual environment. This is actually quite easy to do
    now. Use hidden `.venv` or normal `venv` folder for virtual env as we are
    ignoring this in flake8 configuration in tox.

    * `python3.8 -m venv <path/to/venv>`
    * `source <path/to/.venv>/bin/activate`

4. Upgrade pip and setuptools with `pip install --upgrade pip setuptools`

** On Python3.8, there is a bug on numpy with setuptools 65.6.0 https://github.com/numpy/numpy/issues/22623
WA : `pip install setuptools==65.5.0`

Expected Error:
```
  from . import ccompiler
  File "ocs-ci/venv/lib/python3.8/site-packages/numpy/distutils/ccompiler.py", line 20, in <module>
    from numpy.distutils import log
  File "ocs-ci/venv/lib/python3.8/site-packages/numpy/distutils/log.py", line 4, in <module>
    from distutils.log import Log as old_Log
  ImportError: cannot import name 'Log' from 'distutils.log' /
    (ocs-ci/venv/lib/python3.8/site-packages/setuptools/_distutils/log.py)
```
5. Install requirements with `pip install -r requirements.txt`
6. Install pre-config to enforce commits sign-offs, flake8 compliance and more

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

### Performance Tests
The OCS performance tests need to have an elastic-search server for running benchmarks and for storing the results.
If an elastic-search server is not available, the tests can deploy ES in the system under test for the benchmark, and it will
dump all results in JSON file.

The support for automated deployment of the Elastic-search server is available only for x86_64 architecture
for other architecture (e.g. PPC / s390), since the benchmark can not deploy ES server on the OCP cluster,
an ES server need to be deployed in the LAB, and the IP/Port need to be configured in the configuration file.

All elasticsearch configuration done in the `ocs_ci/framework/conf/default_config.yaml` at the `PERF:` section.

For creating a new performance test, pleas use the template from : `template_test/Performance_template.py`
### Pull Secret

In order to deploy a cluster to AWS with the Openshift Installer,
you will need to download the pull secret for your account.
Download this file from [openshift.com](https://console.redhat.com/openshift/install/pull-secret)
and place in the `data` directory at the root level of the project.
If there is no `data` directory, create one.
The name of the file should be `pull-secret`.

#### Brew registry access for non-GA LSO testing

If you intend to test non-GA versions of the local storage operator you will need access
to provided authentication for `brew.registry.redhat.io` in your pull secret. In order
to obtain credentials for this brew registry you will need to do the following:

1. Send a request to `operator-pipeline-wg@redhat.com` for `brew.registry.redhat.io`
   credentials for you or your team. You will want to provide an email address to
   associate the account with. You may be required to provide a gpg public key in order
   to receive the credentials.
2. Once you have received credentials for the brew registry you will need to log in to
   the registry in order to obtain the data you will add to your pull secret. You can
   log in to the registry using `docker` and the provided credentials:
   * `docker login brew.registry.redhat.io`
3. Once you have successfully logged in, you can retrieve the auth data from
   `~/.docker/config.json`. Grab the auth section for `brew.registry.redhat.com`, it
   will look something like this


    "brew.registry.redhat.io" : {
      "auth" : "TOKEN"
    },

4. Add that auth section to your existing pull secret.

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

#### GitHub

For disconnected cluster installation, we need to access github api (during
downloading opm tool) which have very strict rate limit for unauthenticated
requests ([60 requests per hour](https://docs.github.com/en/rest/overview/resources-in-the-rest-api#rate-limiting)).
To avoid API rate limit exceeded errors, you can provide github authentication
credentials (username and token) obtained on [Personal access tokens](https://github.com/settings/tokens)
page (Settings -> Developer settings -> Personal access tokens).

```yaml
github:
  username: "GITHUB_USERNAME"
  token: "GITHUB_TOKEN"
```

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
