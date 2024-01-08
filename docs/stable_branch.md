.. _stable-branch:

Stable Branch
===============

## Overview
The OCS-CI team has adopted the use of a stable branch for ocs-ci. The intention
here is to perform additional testing on code changes that are merged to our master branch
before they are deemed stable. Once testing has passed, the new changes will be propagated
to our stable branch and can be used for production testing or any other testing that
requires the use of our stable branch.

The initial proposal, discussion, and implementation details can be found in a
[shared google doc](https://docs.google.com/document/d/1qY8t5cSIeLauGFkc5JrBTj0fcryxxCxPeYJsltT7W_8/edit).
Note this document is only available to Red Hat internal accounts.

## Definition of Stable
Before we can start accepting code to our stable branch, we need to determine what "stable"
means to us. This definition is certainly subject to change as we identify gaps or
redundancies in our testing. That said, we have determined that the following level of
testing will qualify our code as stable:

* Acceptance tests on AWS
* Acceptance tests on VSphere

Acceptance tests are all tests defined in ocs-ci that are decorated with the `@acceptance`
marker.

## Impact on the Development Process
There should really be no difference in how changes to our codebase are introduced.
Developers will still create their own forks and merge requests to our master branch.
The only real differences will be the following:

* Developers will have to troubleshoot and fix issues that cause acceptance testing
  to fail.
* New contributions will need to pass our stable testing before those changes will reflect
  in our production testing.

## Automation
Testing new changes to master branch and qualifying them for promotion to the stable
branch is done through scheduled automation. The current schedule is to run tests twice
weekly and automatically promote changes that pass testing. Automation is done via a
[jenkins pipeline](https://ocs4-jenkins-csb-ocsqe.apps.ocp4.prod.psi.redhat.com/job/qe-ocs-ci-stable-branch-pipeline/).

If all tests pass, a fast-forward merge is performed to update the stable branch with
the latest changes to master and a new [tag](https://github.com/red-hat-storage/ocs-ci/tags)
is created which includes the OCS version and a timestamp (e.g. `stable-ocs-4.7-202103021622`).
An email is also sent out stating that the promotion was successful and listing the commits
that have been merged to the stable branch. If any tests fail, an email is sent out describing
the failed tests and notifying the team that action is required to resolve the failures.

## Resolving failed tests
When our automation does not pass, manual intervention is required to troubleshoot the
failure(s), determine which changes to the master branch caused them, and coordinate
with the original developer of the change to implement a fix. If a fix is not able to
be merged to master in a timely manner then the original commit(s) may be reverted to
get stable branch testing back to a passing state.
