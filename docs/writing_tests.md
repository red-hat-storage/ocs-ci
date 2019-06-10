# Writing tests

In this documentation we will inform you about basic structure where implement
code and some structure and objects you can use.

Please feel free to update this documentation because it will help others ppl
a lot!

## Pytest marks

We have predefined some of pytest marks you can use to decorate your tests.
You can find them defined in [pytest.ini](../pytest.ini) where we inform
pytest about those marks.

We have markers defined in pytest_customization package under
[marks.py](../ocsci/pytest_customization/marks.py) plugin. From your tests you
can import directly from `ocsci.testlib` module with this statement:
`from ocsci.testlib import tier1` for example.


## Base test calsses for teams

Those are located in [testlib.py](../ocsci/testlib.py) which you can also
import from `ocsci.testlib` module with statement:
`from ocsci.testlib import manage` which is base test class for manage team.


## Other notes

Of course you can import in one line both team base class and marker with
statement: `from ocsci.testlib import manage, tier1`
