# OCS-CI Release Branches

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

Note that `upstream` is the name of the remote for the base repository, not your personal fork.

### Jenkins

From Jenkins we will automatically be using the release branch if no other ocs-ci branch has been specified. Which specific release branch we use will be dynamically based on the `OCS_VERSION` or `UPGRADE_OCS_VERSION` parameters. For example, if `OCS_VERSION` is 4.13 we will use `release-4.13`. If this release branch doesn't exist yet, we will default back to the `stable` branch. This logic can be overridden by specifying a branch to use.

## Creating Release Branches

Release branches will branch off of the `stable` branch. Before we can create a release branch, we will need to know which commit to branch off from. Once we know the commit we are basing the branch off of, we can then go about creating the branch from either the git CLI or the Github UI.

### Github UI

From the Github UI, navigate to the list of [branches](https://github.com/red-hat-storage/ocs-ci/branches). From here you can click on new branch, selecting the `stable` branch as the base. Note that the branch creation process only works this way if the latest stable commit is the point we wish to branch off from. Otherwise we will need to create the branch using the CLI.

### CLI

First, ensure your local repository is up to date with the remote. Note that the following command is destructive so make sure you have any local changes stashed or committed before continuing.
```
git checkout master && git fetch --all && git reset --hard upstream/master
```

Checkout the stable branch
```
git checkout stable
```

Determine the starting point of the new release branch, for this you will need the git hash. You can find this using:
```
git log
```

Once you know the hash, we will create the new release branch using the hash from earlier as the base. Note that x.y is the major.minor version.
```
git checkout -b release-x.y 46568b9b4b8c2ceabad665b839655e0b19ab3634
```

Finally, push the branch to the remote.
```
git push upstream release-x.y
```


## Backporting Changes

Most changes to the repository will end up being merged to master, propagage to the stable branch and evnetually end up on the next release branch. Critical bug fixes or important test cases may be selected to be backported to existing release branches. In order to backport changes to an existing release branch, we will need to take the following steps.

### Creating a cherry-pick PR

The first step in the process will be to checkout the master branch and ensure it is up to date, as well as fetch remote branches. Note that the following command is destructive so make sure you have any local changes stashed or committed before continuing.
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

Once we have our branch pushed to our fork, we can then open a pull request from Github. Be sure to change the base branch to the target release branch (in our example this would be `release-4.8`).

### Run PR Validation

Run the PR validation Job using the target release branch as the `OCS_CI_PR_BASE_BRANCH`. The standard level of verification for testing changes backported to release branches is a 100% pass rate of the `acceptance` test suite.

### Merge to the release branch

Once the changes have been verified, merge the PR. You can then perform the same process for any other release branches that your changes may need to be backported to.
