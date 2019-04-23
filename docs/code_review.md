# Code review best practices in OCS-CI

**Lets use the following guidelines for Code Reviews:**

* Run couple of real tests before submitting for library changes.
    For test changes, they should be run on at least one environment before
    pull request is submitted.
* It is better to submit small changes in core libraries to avoid regression.
* Request review from few of project maintainers. At the start of the effort,
    please request review from: `@petr-balogh, @RazTamir, @shylesh,
    @vasukulkarni, @clacroix12, @ebenahar, @dahorak`
* Thumbs up, LGTM indicates that change looks OK, but change needs to be
    approved  from github by at least 2 project maintainers before merge.
* All comments should be addressed or responded and once comment is considered
    as closed we should Resolve conversation to close the comment.
* If some fixes required from the comment, please do not do
    `git commit --amend` and `git push --force` as we lose the history of
    changes and it's hard for reviewer to see what was really changed from the
    last comment. Instead of force pushing, please do another commit with
    commit message `Fixing comments from PR` and at the end we will do
    **Squash and merge** in github.
