# Writing tests

In this documentation we will inform you about basic structure where implement
code and some structure and objects you can use.

Please feel free to update this documentation because it will help others ppl
a lot!

## Pytest marks

We have predefined some of pytest marks you can use to decorate your tests.
You can find them defined in [pytest.ini](https://github.com/red-hat-storage/ocs-ci/tree/master/pytest.ini) where we inform
pytest about those marks.

We have markers defined in pytest_customization package under
[marks.py](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/framework/pytest_customization/marks.py) plugin. From your tests you
can import directly from `ocsci.testlib` module with this statement:
`from ocsci.testlib import tier1` for example.

## Bugzilla decorator (DEPRECATED - see Jira decorator section)

As ODF product moves from Bugzilla to [Jira](https://issues.redhat.com/projects/DFBUGS/) for bug reporting, this bugzilla plugin will get removed later on.
Please do not use it anymore, but rather use pytest-jira plugin described below in next section!

You can mark test with specific bugzilla decorator as you can see in following
example:

```python
import pytest

from ocs_ci.framework.testlib import bugzilla, ManageTest

@bugzilla('1726266')
class TestPvCreation(ManageTest):
    pass


# or you can directly use pytest.mark like:
@pytest.mark.bugzilla('bugzilla_id')  # where bugzilla_id can be e.g. 1726266
    pass
```

For more details what else is possible with `pytest_marker_bugzilla` plugin
look at the
[project documentation](https://github.com/eanxgeek/pytest_marker_bugzilla).

## Jira decorator

As ODF product moves from Bugzilla to [Jira](https://issues.redhat.com/projects/DFBUGS/) for bug
reporting, we should be using pytest-jira plugin for skipping tests with open issue.

To enable pytest-jira plugin, run run-ci command with `--jira` parameter.

You can mark test with specific jira decorator as you can see in following
example:

```python
import pytest

from ocs_ci.framework.testlib import jira, ManageTest

@jira('DFBUGS-31', run=False)
class TestPvCreation(ManageTest):
    pass
```

The `run` argument is set to `False` in QE jenkins in `jira.cfg` file:
`run_test_case = False`

But as this value doesn't need to be set in other users, so we recommend to explicitly set run
argument explicitly!

```python
# or you can directly use pytest.mark like:
@pytest.mark.jira('DFBUGS-31', run=False)
    pass
```

For more details what else is possible with `pytest-jira` plugin
look at the
[project documentation](https://github.com/rhevm-qe-automation/pytest_jira).

## Skipping tests based on ocs version

You can skip a test which is not applicable for specific ocs version using

```@skipif_ocs_version([expression1, expression2, ...])``` decorator.

example:
```python
from ocs_ci.framework.testlib import skipif_ocs_version

@skipif_ocs_version(['>4.1','<4.3'])
def test_fio_workload():
    pass
```

You can also specify a single expression in a string like:
```python
@skipif_ocs_version('<4.3')
def test_fio_workload():
    pass
```

## Base test classes for teams

Those are located in [testlib.py](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/framework/testlib.py) which you can also
import from `ocsci.testlib` module with statement:
`from ocsci.testlib import manage` which is base test class for manage team.


## Constants and Defaults

Many of our tests utilize defaults and constants. These are both defined in
`ocs/constants.py` and `ocs/defaults.py` respectively. Constants and defaults
are fairly similar but functionally different which is why we have chosen
to separate them into their own modules.

If your test requires one of these you can easily import it.
If you intend to implement a new one (generally if more than one test will
utilize it), please consider whether or not that value might change between
different test executions. If it's something like a filepath (unchanging),
it's probably a constant. If tests may overwrite the value, it's most likely a
default.

Note these modules are not intended to be a dumping ground for any variable
your test might need. These are designed to be homes for widely used variables
that need to be consistent across a test execution. You can learn more from
viewing the existing constants and defaults in their respective modules.

## Fixture usage

It's documented [here](./fixture_usage.md).

## Other notes

Of course you can import in one line both team base class and marker with
statement: `from ocsci.testlib import manage, tier1`
