.. _fixture_usage:

Guidelines for fixtures usage in OCS-CI
=========================================

## Basic information

What is a pytest Fixture? You can find definition and examples documentation in
[pytest web](https://docs.pytest.org/en/latest/fixture.html). Please read this
documentation first.

We should keep our fixtures organized and have all the dependencies well
defined in fixtures.

All the fixtures should be documented - documenting the return value is
mandatory.

Fixture is composed of its setup part, the code which is done for prepare
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

* Using references to `request.node.cls` for setting, altering or reading class
  attributes.
* Using globals for sharing data.
* Using yield.
* Using `@pytest.mark.usefixtures`.
* Using more than one assert in the teardown when another teardown action is
  between them. (This will lead to a situation where the rest of resources
  aren't cleaned.

## Examples of fixtures and tests

Here are few simple examples of how to use fixtures.

* [conftest.py](https://github.com/red-hat-storage/ocs-ci/tree/master/docs/python-examples/fixtures/conftest.py) - Example of fixture
  definitions in conftest.py file for fixtures meant to be shared between tests.
* [test_fixtures.py](https://github.com/red-hat-storage/ocs-ci/tree/master/docs/python-examples/fixtures/test_fixtures.py) - Examples of tests.
