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
============================= test session starts ==============================
platform darwin -- Python 3.7.3, pytest-5.0.1, py-1.8.0, pluggy-0.12.0
rootdir: ocs-ci, inifile: pytest.ini
collected 7 items

test_fixtures.py::TestCreatingPVCsFromTest::test_create_pvcs[2]
-------------------------------- live log setup --------------------------------
11:30:55 - MainThread - conftest - INFO - Creating storage class: storage_class_9884
11:30:55 - MainThread - conftest - INFO - Setup of pvcs
-------------------------------- live log call ---------------------------------
11:30:55 - MainThread - test_fixtures - INFO - Here you can do something with storage class: storage_class_9884, it should be the same class level scope SC which will be used in pvc_factory function as well
11:30:55 - MainThread - test_fixtures - INFO - Created pvcs: ['pvc_2563', 'pvc_6782']
PASSED
test_fixtures.py::TestCreatingPVCsFromTest::test_create_pvcs[4]
-------------------------------- live log call ---------------------------------
11:30:55 - MainThread - test_fixtures - INFO - Here you can do something with storage class: storage_class_9884, it should be the same class level scope SC which will be used in pvc_factory function as well
11:30:55 - MainThread - test_fixtures - INFO - Created pvcs: ['pvc_7217', 'pvc_5011', 'pvc_2953', 'pvc_6950']
PASSED
test_fixtures.py::TestCreatingPVCsFromTest::test_alter_shared_pvcs
-------------------------------- live log call ---------------------------------
11:30:55 - MainThread - test_fixtures - INFO - Not shared pvc has name pvc_2552
11:30:55 - MainThread - test_fixtures - INFO - Mark PVC: pvc_2563 for delete in next test.
11:30:55 - MainThread - test_fixtures - INFO - Mark PVC: pvc_7217 for delete in next test.
11:30:55 - MainThread - test_fixtures - INFO - Mark PVC: pvc_2953 for delete in next test.
11:30:55 - MainThread - test_fixtures - INFO - Shared pvcs: ['pvc_2563', 'pvc_6782', 'pvc_7217', 'pvc_5011', 'pvc_2953', 'pvc_6950']
PASSED
test_fixtures.py::TestCreatingPVCsFromTest::test_delete_some_shared_pvcs
-------------------------------- live log call ---------------------------------
11:30:55 - MainThread - test_fixtures - INFO - self.shared_pvcs are: ['pvc_2563', 'pvc_6782', 'pvc_7217', 'pvc_5011', 'pvc_2953', 'pvc_6950']
11:30:55 - MainThread - test_fixtures - INFO - Deleting shared pvc with name: pvc_2563 from test
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_2563
11:30:55 - MainThread - test_fixtures - INFO - Deleting shared pvc with name: pvc_7217 from test
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_7217
11:30:55 - MainThread - test_fixtures - INFO - Deleting shared pvc with name: pvc_2953 from test
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_2953
11:30:55 - MainThread - test_fixtures - INFO - Deleted pvc ['pvc_2563', 'pvc_7217', 'pvc_2953'], shouldn't be deleted in finalizer!
PASSED
------------------------------ live log teardown -------------------------------
11:30:55 - MainThread - conftest - INFO - In finalizer
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_6782
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_5011
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_6950
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_2552
11:30:55 - MainThread - ocs - INFO - Deleting storage class: storage_class_9884

test_fixtures.py::TestPVCsCreatedInSetup::test_need_3_pvc
-------------------------------- live log setup --------------------------------
11:30:55 - MainThread - conftest - INFO - Creating storage class: storage_class_5475
11:30:55 - MainThread - conftest - INFO - Setup of pvcs
-------------------------------- live log call ---------------------------------
11:30:55 - MainThread - test_fixtures - INFO - Here you can use those 3 pvcs: ['pvc_9453', 'pvc_2721', 'pvc_5792']
11:30:55 - MainThread - test_fixtures - INFO - Will delete PVC pvc_2721 as part of test
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_2721
11:30:55 - MainThread - test_fixtures - INFO - Test finished, the rest of PVCs will be deleted in finalizer
PASSED
------------------------------ live log teardown -------------------------------
11:30:55 - MainThread - conftest - INFO - In finalizer
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_9453
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_5792
11:30:55 - MainThread - ocs - INFO - Deleting storage class: storage_class_5475

test_fixtures.py::TestPVC::test_one_pvc
-------------------------------- live log setup --------------------------------
11:30:55 - MainThread - conftest - INFO - Creating storage class: storage_class_4084
11:30:55 - MainThread - conftest - INFO - Creating pvc: pvc_5444
-------------------------------- live log call ---------------------------------
11:30:55 - MainThread - test_fixtures - INFO - This test is using one pvc: pvc_5444 created in fixture setup
PASSED
test_fixtures.py::TestPVC::test_use_same_one_pvc_plus_storage_class
-------------------------------- live log call ---------------------------------
11:30:55 - MainThread - test_fixtures - INFO - This test is using same one pvc:  pvc_5444
11:30:55 - MainThread - test_fixtures - INFO - Storage class in class level scope is: storage_class_4084
11:30:55 - MainThread - test_fixtures - INFO - Storage class in cls_pvc should be the same: storage_class_4084
PASSED
------------------------------ live log teardown -------------------------------
11:30:55 - MainThread - ocs - INFO - Deleting pvc: pvc_5444
11:30:55 - MainThread - ocs - INFO - Deleting storage class: storage_class_4084


=========================== 7 passed in 0.05 seconds ===========================
```
