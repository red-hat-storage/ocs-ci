# bin directory

This directory is placed into `PATH` environment variable so that tools located
here are available to all tests (this is done in
[`run_ocsci.py`](https://github.com/red-hat-storage/ocs-ci/tree/master/ocs_ci/run_ocsci.py) wrapper).

The directory is intentionally empty (there should be no other files committed
there). Command line tools like `oc` and `openshift-install` are installed
there during deployment (see
[test_deployment.py](https://github.com/red-hat-storage/ocs-ci/tree/master/tests/ecosystem/deployment/test_deployment.py)).
