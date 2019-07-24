# Guidelines for fixtures usage in OCS-CI

## Basic information

What is a pytest Fixture? You can find definition and examples documentation in
[pytest web](https://docs.pytest.org/en/latest/fixture.html). Please read this
documentation first.

We should keep our fixtures organized and have all the dependencies well
defined in fixtures.

All the fixtures should be documented - documenting the return value is
mandatory.

Fixture is compose of its setup part, the code which is done for prepare
resource and teardown part which is after yield of added via
`request.addfinalizer` method. It's strictly forced to use `addfinalizer`
method cause it can happen that what is after yield won't be proceed when
exception will be thrown in the middle of the setup.

The finalizer should be added as soon as possible to make sure the finalizer
will be called at the end. (Otherwise we will hit the same issue as we have
with yield)

## Use cases

TODO: Mention function factories and use cases and when to use which approach.
Something like:

* Setup resource, test (using resources), teardown - regular usage of fixture
* Test creating resources as part of test, teardown of resources - fixture
  factory solve the issue here, see the
  [documentation](https://docs.pytest.org/en/latest/fixture.html#factories-as-fixtures)
* Setup resources,  test (using resources , deleting resources), teardown
  shouldn’t fail on those deleted resources

## Avoid

Here is the list of things we should avoid in our fixtures:

* Using references to `request.node.cls` for set, altering or reading class
  attributes.
* Using globals for sharing data.
* Using yield.
* Using `@pytest.mark.usefixtures`.
* Using more than one assert in the teardown when another teardown action is
  between them. (This will lead that the rest of resources won't be cleaned
  once assertion exception raised).

## Examples of fixtures and tests

Here are few simple examples of how to use fixtures.

* [conftest.py](./python-examples/fixtures/conftest.py) - Example of fixture
  definitions in conftest.py file for fixtures meant to be shared between tests.
* [ocs.py](./python-examples/fixtures/conftest.py) - Example of some OCS
  resources and helper methods for examples.
* [test_fixtures.py](./python-examples/fixtures/conftest.py) - Examples of tests.

Please go over example files above to understand the logic of fixtures.

## Output of tests examples

Below you can see output of test examples linked above. From this output you
can see order of creating and teardowning objects.

```bash
=========================== test session starts ===============================
platform darwin -- Python 3.7.3, pytest-5.0.1, py-1.8.0, pluggy-0.12.0 --
.venv/bin/python3
cachedir: .pytest_cache
rootdir: ./fixtures
collected 7 items

test_fixtures.py::TestCreatingPVCsFromTest::test_create_pvcs[2]
INFO:conftest:Creating storage class: storage_class_6543
INFO:conftest:Setup of pvcs
INFO:test_fixtures:['pvc_2800', 'pvc_2580']
PASSED
test_fixtures.py::TestCreatingPVCsFromTest::test_create_pvcs[4]
INFO:test_fixtures:['pvc_7185', 'pvc_5700', 'pvc_3564', 'pvc_5209']
PASSED
test_fixtures.py::TestCreatingPVCsFromTest::test_share_pvcs
INFO:test_fixtures:Shared pvcs usage: ['pvc_5280', 'pvc_6276']
PASSED
test_fixtures.py::TestCreatingPVCsFromTest::test_use_shared_pvcs
INFO:test_fixtures:self.shared_pvcs are: ['pvc_5280', 'pvc_6276']
INFO:test_fixtures:Deleting shared pvc: pvc_5280 from test
INFO:ocs:Deleting pvc: pvc_5280
INFO:test_fixtures:Deleted pvc pvc_5280 from test, shouldn't be deleted in finalizer!
INFO:test_fixtures:Doing something with pvc: <ocs.PVC object at 0x10d95fa90>
PASSEDINFO:conftest:In finalizer
INFO:ocs:Deleting pvc: pvc_2800
INFO:ocs:Deleting pvc: pvc_2580
INFO:ocs:Deleting pvc: pvc_7185
INFO:ocs:Deleting pvc: pvc_5700
INFO:ocs:Deleting pvc: pvc_3564
INFO:ocs:Deleting pvc: pvc_5209
INFO:ocs:Deleting pvc: pvc_6276
INFO:ocs:Deleting storage class: storage_class_6543

test_fixtures.py::TestPVCsCreatedInSetup::test_need_3_pvc
INFO:conftest:Creating storage class: storage_class_2964
INFO:conftest:Setup of pvcs
INFO:test_fixtures:Here you can use those 3 pvcs: ['pvc_2435', 'pvc_2764', 'pvc_4108']
INFO:test_fixtures:Will delete PVC pvc_2764 as part of test
INFO:ocs:Deleting pvc: pvc_2764
INFO:test_fixtures:Test finished
PASSEDINFO:conftest:In finalizer
INFO:ocs:Deleting pvc: pvc_2435
INFO:ocs:Deleting pvc: pvc_4108
INFO:ocs:Deleting storage class: storage_class_2964

test_fixtures.py::TestPVC::test_one_pvc
INFO:conftest:Creating storage class: storage_class_1006
INFO:conftest:Creating pvc: pvc_4810
INFO:test_fixtures:This test is using one pvc: pvc_4810 created in fixturesetup
PASSED
test_fixtures.py::TestPVC::test_use_same_one_pvc_plus_storage_class
INFO:test_fixtures:This test is using same one pvc:  pvc_4810
INFO:test_fixtures:Storage class used is storage_class_1006
PASSEDINFO:ocs:Deleting pvc: pvc_4810
INFO:ocs:Deleting storage class: storage_class_1006
=========================== 7 passed in 0.02 seconds ==========================
```
