# Code review best practices in OCS-CI

**Lets use the following guidelines for Code Reviews:**

* Run couple of real tests before submitting for library changes.
    For test changes, they should be run on at least one environment before
    pull request is submitted.
* It is better to submit small changes in core libraries to avoid regression.
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
