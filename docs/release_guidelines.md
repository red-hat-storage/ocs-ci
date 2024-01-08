.. _release-guidelines:

OCS-CI Release Branches
=========================

## Goals

Release branches are now available for users to deploy or test specific versions of ODF with. These branches are based off of our stable branch and are created after ODF has hit GA. There are a few goals behind implementing these release branches.

The first goal is stability of testing existing releases. Since the code that will be used to target existing releases isn't constantly being updated like our master/stable branches, the code will be much more stable and allow for greater consistency when testing released versions of ODF. The only changes to these existing release branches will be major bug fixes that are backported (or cherry-picked) from stable or important test cases that absolutely need to be retroactively added for older versions.

The second goal is greater consistency in the framework. Now that we have release branches that exist for testing older versions of the product, we don't need to carry logic forward to handle every version forever. We have many areas of the code that are performing checks on the product version and then performing actions if we are deploying or testing a version greater than a specific version. This will no longer be necessary once it can be assumed that the latest code will always include a feature and so the code that is performing this version check can be removed. This will enable us to reduce the amount of code in the framework that simply exists to make sure our framework is doing something that the product supports based on version, reducing a lot of technical debt.

Overall, implementing release branches should allow us greater stability in testing existing releases as well as reducing the complexity of some of our framework code.

## Supported versions and use-cases

Release branches will support deployments and testing of ODF/OCP versions N and N-1, N being the version specified in the name of the branch.

N: Standard use-case of deployment and testing the version specified in the release branch name.
N-1: For upgrade purposes. We need to be able to deploy N-1, run pre-upgrade tests, upgrade and reload configs for version N, then run post-upgrade tests. This process requires that release branches support the deployment and testing of previous versions.

For example, the branch release-4.8 will be able to deploy OCP/ODF 4.8 as well as 4.7 to support upgrade testing. However, it will not officially support deployment or testing of versions <= 4.6.


## Usage

### Locally

As these are simply git branches we are creating, you just need to perform a git checkout on the specific release branch you wish to run your deployment or tests with in order to use it.

```
git fetch upstream
git checkout release-4.12
```

> Note that `upstream` is the name of the remote for the base repository, not your personal fork.

### Jenkins

From Jenkins we will automatically be using the release branch if no other ocs-ci branch has been specified. Which specific release branch we use will be dynamically based on the `OCS_VERSION` or `UPGRADE_OCS_VERSION` parameters. For example, if `OCS_VERSION` is 4.13 we will use `release-4.13`. If this release branch doesn't exist yet, we will default back to the `stable` branch. This logic can be overridden by specifying a branch to use.

## Creating Release Branches

**This process is handled by the project maintainers and done alongside the corresponding ODF GA release**

Release branches will branch off of the `stable` branch. More specifically, we will target one of the tags we use when updating our stable branch. This ensures that the code we are basing the release branch off of has passed our stable branch verification testing. You can view these in the [github tags view](https://github.com/red-hat-storage/ocs-ci/tags), they will have the format `stable-ocs-x.y-timestamp`. Once we know the tag we are basing the branch off of, we can then go about creating the branch from either the git CLI or the Github UI.

### Github UI

From the Github UI, navigate to the list of [branches](https://github.com/red-hat-storage/ocs-ci/branches). From here you can click on new branch, selecting the `stable` branch as the base.

> Note that the branch creation process only works this way if the latest stable commit is the point we wish to branch off from. Otherwise we will need to create the branch using the CLI.

### CLI

First, ensure your local repository is up to date with the remote.

> Note that the following command is destructive so make sure you have any local changes stashed or committed before continuing.

```
git checkout master && git fetch --all && git reset --hard upstream/master
```

Checkout the stable branch
```
git checkout stable
```

Create a local branch using the stable tag

```
git checkout -b release-4.12 stable-ocs-4.12-202301310444
```

Finally, push the branch to the remote.
```
git push upstream release-4.12
```

> Branches with the `release-*` naming convention will automatically be considered protected branches due to our repository configuration.

### Post-Creation Steps

Given the nature of our release cadence and testing, we will generally want to start preparing for testing the next release after we have prepared a release branch. For example once we create a release branch for 4.12 we will update the code to default to deploying and testing 4.13. This update will include bumping the version in several places in the [default configuration](https://github.com/red-hat-storage/ocs-ci/blob/master/ocs_ci/framework/conf/default_config.yaml) as well as updating the project version in our [setup.py](https://github.com/red-hat-storage/ocs-ci/blob/master/setup.py). You can see an example of one of these updates [here](https://github.com/red-hat-storage/ocs-ci/pull/7028/files).


## Backporting Changes

Most changes to the repository will end up being merged to master, propagated to the stable branch and eventually end up on the next release branch. Critical bug fixes or important test cases may be selected to be backported to existing release branches. In order to backport changes to an existing release branch, we will need to take the following steps.

### Creating a cherry-pick PR

The first step in the process will be to checkout the master branch and ensure it is up to date, as well as fetch remote branches.

> Note that the following command is destructive so make sure you have any local changes stashed or committed before continuing.

```
git checkout master && git fetch --all && git reset --hard upstream/master
```

Before we can cherry-pick our change we will need to know the commit hash. You can find this using:
```
git log
```

Once we have the hash, we will create a local branch based off of the release branch we wish to backport our changes to.
```
git checkout -b release-4.8-cherry-pick-pr-4765 upstream/release-4.8
```

Then, we will cherry pick the commit to our branch using the hash.
```
git cherry-pick 46568b9b4b8c2ceabad665b839655e0b19ab3634
```

And push our branch to our fork of the repository.
```
git push origin release-4.8-cherry-pick-pr-4765
```

Once we have our branch pushed to our fork, we can then open a pull request from Github. **Be sure to change the base branch to the target release branch (in our example this would be `release-4.8`).**

### Run PR Validation

Run the PR validation Job using the target release branch as the `OCS_CI_PR_BASE_BRANCH`. The standard level of verification for testing changes backported to release branches is a 100% pass rate of the `acceptance` test suite.

### Merge to the release branch

Once the changes have been verified, merge the PR. You can then perform the same process for any other release branches that your changes may need to be backported to.

## Fixes related to specific release versions

There may be a time where a particular bug fix needs to be applied to a specific release branch while not being something we want to merge to master. In this scenario, we can follow a similar process to how we merge changes to master with two exceptions.

1. The development branch will be based off of the **target release branch**, not master.

2. The target branch of the pull request will be the **release branch**, not master.

For example, a bug is discovered that only affects `4.11` releases that we need to implement a change for. Since this change isn't necessary for the later releases, we don't need to merge this to master and backport the change to previous releases. We can simply create a development branch off of our `release-4.11` branch, implement the fix, raise the pull request with `release-4.11` as the base branch.

An important thing to remember in a scenario like this is the fact that our release branches support N and N-1 versions. This is significant if the fixes intended for a particular version are required for deployment, upgrades, or pre-upgrade testing it may be necessary to cherry-pick the change to the N+1 release branch. For example, if a bugfix is necessary for 4.10 deployments, the change will also need to be cherry-picked to the 4.11 release branch since `release-4.11` supports deployments of 4.10 for upgrade testing purposes.

> These will likely be rare occurances as most changes will be aimed at our master branch and backported to previous releases when necessary.
