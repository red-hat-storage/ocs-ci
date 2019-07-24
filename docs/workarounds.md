# Tracking of workarounds

By *a workaround* we mean any change in test or other qe code which overcomes a
bug in the product we are testing. Sometimes it's necessary to overcome a
problem in a non standard way (eg. by hardcoding particular version of some
component when the latest version is crashing during installation) to prevent
temporal blocking of our testing or test development.

But the important part is that any workaround is *minimal* and *temporary*. We
need to make sure that:

- a workaround is removed when the bug is fixed
- we know which workarounds we have in the code and why, so that we can reason
  about validity of test results at any point in time

Without this our testing results could be spolied by incorrect results.

## How to track a workaround?

For the reasons noted above, we are tracking any workaround in the following
way:

- When you create a pull request with a workaround, state clearly which
  bug/issue is the root cause and why.
- Make minimal changes so that the workaround is placed in dedicated pull
  request with a single commit and could be later reverted without affecting
  anything else.
- The person who creates a pull request with a workaround is also responsible
  for creating issue for workaround removal, listing again link to the issue
  and the pull request which introduces the workaround, to be reverted later.
- The workaround issue has a title starting with "remove workaround" and
  "workaround" label.
- Person who does a review of a pull request with a workaround needs to check
  these rules and the validity of "workaround removal" issue as well.

## Example

A pull request with a workaround: [ocs-ci #433](https://github.com/red-hat-storage/ocs-ci/pull/433)

Workaround removal issue: [ocs-ci #438](https://github.com/red-hat-storage/ocs-ci/issues/438)

Note that this example has one problem: the root cause issue is not linked, and
so we don't know when we can safely remove the workaround.
