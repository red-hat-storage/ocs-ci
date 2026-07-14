# Cross Functional Testing
Tests that target multiple components or perform actions that span different areas of the product will belong in the Cross Functional module.

An example of this kind of testing is our [longevity](https://github.com/red-hat-storage/ocs-ci/tree/master/tests/e2e/longevity) testing. These test cases are not targeting one specific feature of the product. Rather, they are creating scenarios that represent real life user behavior which spans across several different product features/components. Each test case under this module performs very different actions from the others with regards to which components are involved. See [test_stage1.py](https://github.com/red-hat-storage/ocs-ci/blob/master/tests/e2e/longevity/test_stage1.py) for a specific example.
