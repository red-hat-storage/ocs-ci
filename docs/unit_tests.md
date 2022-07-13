# Unit tests

We're in the process of writing unit tests for the ocs-ci python modules,
including our pytest plugins.

We are using [pytest](https://docs.pytest.org/en/latest/) framework
and test execution is handled via [tox](https://tox.readthedocs.io/), as
configured in [tox.ini](https://github.com/red-hat-storage/ocs-ci/tree/master/tox.ini) config file.

The tests are executed in [Travis CI](https://travis-ci.org/red-hat-storage/ocs-ci)
for each pull request created for this repository.

## How to run the unit tests myself?

Make sure you have python 3.8 and tox installed from binary packages of your
distribution.

Then in root directory of the repository just run:

```
$ tox
```

This will execute the tests in the same way as in
[Travis CI](https://travis-ci.org/red-hat-storage/ocs-ci), including flake8
checks.

## What tests are available?

You can list available tests by doing:
```
$ tox -e py37 -- --collect-only
```

## How to run particular test only?

It's possible to pass additional pytest arguments to tox like this:

```
$ tox -e py37 -- ocsci/pytest_customization/tests/test_pytest.py::test_config_parametrize
```

Or like this:
```
$ tox -e py37 -- -k test_config_parametrize
```

This can be also helpful during debugging:

```
$ tox -e py37 -- -v --pdb
```

## Pytest integration tests

In `test_pytest.py` file, we have unit tests covering our pytest plugins. The
testing is done via
[pytester](https://docs.pytest.org/en/latest/_modules/_pytest/pytester.html),
which is official pytest module for testing pytest plugins via pytest.
