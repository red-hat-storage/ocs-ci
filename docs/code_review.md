# Code review/contribution best practices in OCS-CI

**Lets use the following guidelines for Code Reviews and contribution:**

* Create own fork of the repository and submit PR from your fork.
* Please follow rules we have set in [coding guidelines](./coding_guidelines.md)
    and read it properly before submitting your first PR.
* Run couple of real tests before submitting for library changes.
    For test changes, they should be run on at least one environment before
    pull request is submitted.
* Run the `tox` command locally before submitting the patch to github to see if
    you are passing the tests we are running in Travis CI.
* It is better to submit small changes in core libraries to avoid regression.
* If the change is still Work In Progress (WIP), please add WIP to the name of
    PR which indicates that patch is not ready for the  merge yet. We have
    installed this [application WIP](https://github.com/marketplace/wip). This
    magic word will prevent us to merge this change which contains WIP in the
    name. Once you finish with patch you can remove it from the PR name.
* Request review from a few of project maintainers or related folks.
    Currently we are using the CODEOWNERS file which should auto assign
    [top level reviewers](https://github.com/orgs/red-hat-storage/teams/top-level-reviewers/members).
     If you feel ownership of some package/module, please add yourself to this
    file [CODEOWNERS](../.github/CODEOWNERS) following conventions from
    comments or [documentation](https://help.github.com/en/articles/about-code-owners).
* Thumbs up, LGTM indicates that change looks OK, but change needs to be
    approved  from github by at least 2 project maintainers before merge.
* All comments should be addressed or responded and once comment is considered
    as closed we should Resolve conversation to close the comment.
* If some fixes required from the comment, please do not do
    `git commit --amend` and `git push --force` as we lose the history of
    changes and it's hard for reviewer to see what was really changed from the
    last comment. Instead of force pushing, please do another commit with
    commit message `Fixing comments from PR` and at the end we will do
    **Squash and merge** in github. If the PR is big and contains a lot of
    changes, and once all comments are addressed, you can squash related
    commits together and force push to the branch. Then we will just merge
    whole PR.
* If the pull request introduces a workaround, reviewer should check that
  rules noted in [Tracking of workarounds](./workarounds.md) page are
  followed. Person merging a workaround is responsible of checking that all
  information related to the workaround is correct.
