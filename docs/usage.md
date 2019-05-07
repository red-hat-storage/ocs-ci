# Usage

**Work in progress**

`run.py` is the main script for ocs-ci. You can view the full usage details by passing in the `--help` argument.

```bash
python3 run.py --help
```

## Required Arguments

There are a few arguments that are required ocs test execution:

* `--cred <cred_file>` - if you have aws configured by `aws configure`, see:
    [AWS doc](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)
    this parameter is not needed
* `--suite <suite_file>`

## Useful Arguments

Some non-required arguments that we end up using a lot:

* `--log-level <level>` - set the log level that is output to stdout.
* `--cluster-name <name>` - name of cluster.
* `--cluster-path <path>` - path where to create the directory which will
    contains all the installation/authentication information about cluster.
    `Use this parameter when running on already deployed cluster!` You can
    pass the cluster path from previous execution if was created automatically.

## Examples

* Run OCS install suite:

```bash
python3 run.py --cred ~/aws.yml --suite suites/ocs_basic_install.yml \
     --log-level info
```

* Run with specific name of cluster and cluster directory without sendig email:

```bash
python3 run.py --cluster-name=my-testing-cluster \
    --suite=suites/custom-test.yml --cluster-path=/home/your_login/my-testing-cluster \
    --no-email
```
