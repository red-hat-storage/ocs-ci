#!/usr/bin/env bash
set -e
branch_name=$BRANCH
if [ -z "$branch_name" ]; then
    branch_name=$( git rev-parse --abbrev-ref HEAD )
fi
commit=$COMMIT
if [ -z "$commit" ]; then
    commit=$( git rev-parse HEAD )
fi
github_remote=$( git remote -v |grep -m1 $FORK |sed 's/.*:\(.*\)\/.*/\1/' )
echo Branch name: $branch_name

trap func exit

function func() {
    git checkout $branch_name
}

releases=${1}

echo "If there is any unsaved change it will be stashed via git stash now, to recover run: git stash pop"
git stash

for release in ${releases//,/ }; do
    release_branch="release-$release"
    cherry_pick_branch_name="${branch_name}-release-${release}"
    echo "============================================================="
    echo release $release cherry-pick branch: $cherry_pick_branch_name
    echo "============================================================="
    git checkout -b $cherry_pick_branch_name
    git reset --hard ${UPSTREAM}/$release_branch
    git cherry-pick $commit
    git push $FORK $cherry_pick_branch_name
    echo "============================================================="
    echo "|!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!|"
    echo "============================================================="
    echo "USE THIS LINK TO OPEN THE PR AGAINST SPECIFIC $release_branch!"
    echo "https://github.com/red-hat-storage/ocs-ci/compare/${release_branch}...${github_remote}:ocs-ci:${cherry_pick_branch_name}?expand=1"
    echo "============================================================="
done
