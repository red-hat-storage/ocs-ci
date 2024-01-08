.. _code-review:

Code review/contribution best practices in OCS-CI
=================================================

**Let's use the following guidelines for Code Reviews and contribution:**

* Create own fork of the repository and submit PR from your fork.
* If a github issue describing the changes you are making does not exist be sure to
    create one before creating a pull request. This is a *requirement* for all
    bugs, feature requests, and enhancements to the framework or our tests that
    involve more than a few lines of code changes. If the PR is extremely small,
    still be sure to include a purpose for the change in the description.
* Please follow rules we have set in [coding guidelines](./coding_guidelines.md)
    and read it properly before submitting your first PR.
* Run couple of real tests before submitting for library changes.
    For test changes, they should be run on at least one environment before
    pull request is submitted.
* Run the `tox` command locally before submitting the patch to github to see if
    you are passing the tests we are running in Travis CI.
* It is better to submit small changes in core libraries to avoid regression.
* Make sure you sign-off your commits with `git commit -s`
  [Developer Certificate of Origin (DCO)](https://github.com/probot/dco#how-it-works)
  is enabled which means all PRs will be checked for the submitter GitHub account.
* If the change is still Work In Progress (WIP), please add WIP to the name of
    PR which indicates that patch is not ready for the  merge yet. We have
    installed this [application WIP](https://github.com/marketplace/wip). This
    magic word will prevent us to merge this change which contains WIP in the
    name. Once you finish with patch you can remove it from the PR name.
* Include `Fixes: #issue_id` in the description of your pull request in order
    to have github automatically close the issue when the pull request is
    merged. This also helps maintain a reference to the issue for merge commits
    in our git log.
* Request review from a few of project maintainers or related folks.
    Currently we are using the CODEOWNERS file which should auto assign
    [top level reviewers](https://github.com/orgs/red-hat-storage/teams/top-level-reviewers/members).
    If you feel ownership of some package/module, please add yourself to this
    file [CODEOWNERS](https://github.com/red-hat-storage/ocs-ci/tree/master/.github/CODEOWNERS) following conventions from
    comments or [documentation](https://help.github.com/en/articles/about-code-owners).
* Thumbs up, LGTM indicates that change looks OK, but change needs to be
    approved  from github by at least 2 project maintainers before merge.
* All comments should be addressed or responded and once comment is considered
    as closed we should Resolve conversation to close the comment.
* If there are new review comments for a PR that is older than 4 days, the
    author can take it up as follow on PR after the issue is filed in github
    to address new review comments.
* If some fixes required from the comment, please do not do
    `git commit --amend` and `git push --force` as we lose the history of
    changes and it's hard for reviewer to see what was really changed from the
    last comment. Instead of force pushing, please do another commit with
    commit message `Fixing comments from PR` and at the end we will do
    **Squash and merge** in github. If the PR is big and contains a lot of
    changes, and once all comments are addressed, you can squash related
    commits together and force push to the branch. Then we will just merge
    whole PR.
* When squashing and merging a PR, do your best to clean up the commit message. This
    message should be a clear description of the changes that were introduced. Please
    remove any duplicated sign off messages as well as any messages that contain little
    or no information such as "fixup" or "addressed comments." These messages don't do
    us any good once things are merged to master and just clutter up the git log. If
    there is any doubt around what the message should be, consult with the PR's author.
* If the pull request introduces a workaround, the reviewer should check that
  rules noted in [Tracking of workarounds](./workarounds.md) page are
  followed. The person merging a workaround is responsible for checking that all
  information related to the workaround is correct.
* MCG test cases should work on any cloud platform (AWS, Azure, GCP, ...).
