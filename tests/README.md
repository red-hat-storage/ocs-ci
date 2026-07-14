# Tests Directory

This directory is where all product test suites, test cases, and test fixtures are defined.

## Organization
Tests will be organized into one of three top level categories: Functional, Cross Functional, or Libtest.

### Functional
Tests that are focused on one particular component or functionality of ODF will belong in the Functional module.

An example of this kind of testing is our [mcg](https://github.com/red-hat-storage/ocs-ci/tree/master/tests/manage/mcg) testing. This module is aimed at testing one ODF component (MCG) and all of the tests in this module are aimed at testing one particular feature of that component (bucket_creation, multi_region, object_versioning, etc.).

### Cross Functional
Tests that target multiple components or perform actions that span different areas of the product will belong in the Cross Functional module.

An example of this kind of testing is our [longevity](https://github.com/red-hat-storage/ocs-ci/tree/master/tests/e2e/longevity) testing. These test cases are not targeting one specific feature of the product. Rather, they are creating scenarios that represent real life user behavior which spans across several different product features/components. Each test case under this module performs very different actions from the others with regards to which components are involved. See [test_stage1.py](https://github.com/red-hat-storage/ocs-ci/blob/master/tests/e2e/longevity/test_stage1.py) for a specific example.

### Libtest
These test cases are designed to test functionality of the framework. These differ from unit tests due to fact they need an operational ODF cluster to fully test the framework component.

## Where does my test case/suite belong?
If you are unsure of where your test case/suite belongs in this organization you can start by asking a couple simple questions.

1. Does my test qualify the product (ODF) or does it verify some part of the test framework?

If your test is designed to qualify ODF in some way, it will go in either Functional or Cross Functional. Move on to question 2.

If your test is designed to qualify the test framework, yet requires an ODF cluster to exist in order to execute, it is a Libtest. If it doesn't require an ODF cluster it's most likely a unit test and isn't the target of this README. Please check out the [unit tests doc](https://github.com/red-hat-storage/ocs-ci/tree/master/docs/unit_tests.md)

2. Does my test target one specific feature or does it target more than one / leverage multiple features to create a user scenario under test?

If your test is qualifying one feature of the product, it most likely belongs in Functional.

If your test is qualifying multiple features or performing actions across multiple components it is probably Cross Functional.

The majority of our testing is most likely going to end up in the Functional module as most of our test cases/suites are aimed at one feature of the product. Some of our tesing however is aimed at a larger picture, especially when it comes to things like performance, scale, or longevity testing. These tests will often leverage multiple components to simulate a real user scenario as opposed to verifying specific functionality. These tests are being considered cross functional.
