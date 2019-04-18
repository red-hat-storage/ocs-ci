# Usage

**Work in progress**

`run.py` is the main script for ocs-ci. You can view the full usage details by passing in the `--help` argument.

```bash
python run.py --help
```

## Required Arguments

There are a few arguments that are required ocs test execution:

* `--cred <cred_file>`
* `--suite <suite_file>`

## Useful Arguments

Some non-required arguments that we end up using a lot:

* `--log level <level>` - set the log level that is output to stdout.

## Examples

Run OCS install suite:

```bash
python run.py --cred ~/aws.yml --suite suites/ocs_basic_install.yml --log-level info
```