"""
GitHub client for fetching upstream fix PR diffs and creating ocs-ci PRs.
"""

import logging
import re
import subprocess
from typing import Optional

import requests

from ocs_ci.utility.zstream_test_gen.config import GeneratorConfig

log = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
GITHUB_PR_PATTERN = re.compile(r"https?://github\.com/([^/]+/[^/]+)/pull/(\d+)")


class GitHubClient:
    """
    GitHub API client for fetching PR diffs and managing ocs-ci PRs.

    Args:
        cfg: Generator configuration with GitHub credentials.

    """

    def __init__(self, cfg: GeneratorConfig):
        self.cfg = cfg
        self.session = requests.Session()
        if cfg.github.token:
            self.session.headers["Authorization"] = f"token {cfg.github.token}"
        self.session.headers["Accept"] = "application/vnd.github.v3+json"
        self.session.headers["User-Agent"] = "ocs-ci-zstream-test-gen"

    def get_pr_diff(self, pr_url: str) -> Optional[str]:
        """
        Fetch the diff for a GitHub pull request.

        Args:
            pr_url: Full GitHub PR URL, e.g.
                    "https://github.com/ceph/ceph-csi/pull/6472".

        Returns:
            Optional[str]: The PR diff as a string, or None on failure.

        """
        match = GITHUB_PR_PATTERN.match(pr_url)
        if not match:
            log.warning("Could not parse PR URL: %s", pr_url)
            return None

        repo = match.group(1)
        pr_number = match.group(2)

        api_url = f"{GITHUB_API}/repos/{repo}/pulls/{pr_number}"
        log.info("Fetching PR diff: %s #%s", repo, pr_number)

        try:
            response = self.session.get(
                api_url,
                headers={"Accept": "application/vnd.github.v3.diff"},
                timeout=30,
            )
            response.raise_for_status()
            diff = response.text

            # Truncate very large diffs to avoid overloading the LLM
            max_diff_chars = 15000
            if len(diff) > max_diff_chars:
                log.warning(
                    "PR diff is %d chars, truncating to %d",
                    len(diff),
                    max_diff_chars,
                )
                diff = diff[:max_diff_chars] + "\n\n... [diff truncated] ..."

            return diff
        except requests.RequestException:
            log.warning("Failed to fetch PR diff for %s", pr_url, exc_info=True)
            return None

    def get_pr_info(self, pr_url: str) -> Optional[dict]:
        """
        Fetch metadata for a GitHub pull request.

        Args:
            pr_url: Full GitHub PR URL.

        Returns:
            Optional[dict]: PR info including title, body, files changed.

        """
        match = GITHUB_PR_PATTERN.match(pr_url)
        if not match:
            return None

        repo = match.group(1)
        pr_number = match.group(2)
        api_url = f"{GITHUB_API}/repos/{repo}/pulls/{pr_number}"

        try:
            response = self.session.get(api_url, timeout=30)
            response.raise_for_status()
            data = response.json()

            return {
                "title": data.get("title", ""),
                "body": data.get("body", ""),
                "state": data.get("state", ""),
                "merged": data.get("merged", False),
                "files_changed": data.get("changed_files", 0),
                "additions": data.get("additions", 0),
                "deletions": data.get("deletions", 0),
                "base_branch": data.get("base", {}).get("ref", ""),
            }
        except requests.RequestException:
            log.warning("Failed to fetch PR info for %s", pr_url, exc_info=True)
            return None

    def create_branch_and_pr(
        self,
        branch_name: str,
        target_branch: str,
        file_path: str = "",
        file_content: str = "",
        commit_message: str = "",
        pr_title: str = "",
        pr_body: str = "",
        draft: bool = True,
        labels: Optional[list] = None,
        files: Optional[list] = None,
    ) -> Optional[dict]:
        """
        Create a branch, commit file(s), and open a PR via the GitHub API.

        This uses the Git Data API to create the branch and commit without
        needing a local clone. Supports committing multiple files in a
        single commit (e.g., test file + helper additions).

        Args:
            branch_name: Name for the new branch.
            target_branch: Branch to open the PR against (e.g., "master").
            file_path: Path of a single file (legacy, used if files is None).
            file_content: Content of a single file (legacy, used if files is None).
            commit_message: Commit message.
            pr_title: Pull request title.
            pr_body: Pull request body (markdown).
            draft: Whether to create as a draft PR.
            labels: Optional list of label names to apply.
            files: Optional list of dicts with 'path' and 'content' keys.
                   If provided, file_path/file_content are ignored.

        Returns:
            Optional[dict]: PR info with 'url', 'number', 'html_url', or None.

        """
        upstream_repo = self.cfg.github.ocsci_repo
        push_repo = self.cfg.github.fork_repo or upstream_repo
        log.info(
            "Creating PR: push to %s, PR against %s (%s -> %s)",
            push_repo,
            upstream_repo,
            branch_name,
            target_branch,
        )

        # Build file list from either new multi-file param or legacy single-file
        if files is None:
            files = [{"path": file_path, "content": file_content}]

        try:
            # 1. Get the SHA of the target branch from upstream
            ref_url = (
                f"{GITHUB_API}/repos/{upstream_repo}/git/refs/heads/{target_branch}"
            )
            ref_resp = self.session.get(ref_url, timeout=30)
            ref_resp.raise_for_status()
            base_sha = ref_resp.json()["object"]["sha"]

            # 2. Create the new branch on the push repo (fork or upstream)
            #    If the branch already exists (422), delete it first and retry.
            create_ref_url = f"{GITHUB_API}/repos/{push_repo}/git/refs"
            ref_data = {
                "ref": f"refs/heads/{branch_name}",
                "sha": base_sha,
            }
            create_resp = self.session.post(create_ref_url, json=ref_data, timeout=30)
            if create_resp.status_code == 422:
                log.info(
                    "Branch %s already exists on %s, replacing it",
                    branch_name,
                    push_repo,
                )
                del_url = f"{GITHUB_API}/repos/{push_repo}/git/refs/heads/{branch_name}"
                self.session.delete(del_url, timeout=30)
                create_resp = self.session.post(
                    create_ref_url, json=ref_data, timeout=30
                )
            create_resp.raise_for_status()

            # 3. Create blobs on the push repo
            blob_url = f"{GITHUB_API}/repos/{push_repo}/git/blobs"
            tree_entries = []
            for file_entry in files:
                blob_data = {
                    "content": file_entry["content"],
                    "encoding": "utf-8",
                }
                blob_resp = self.session.post(blob_url, json=blob_data, timeout=30)
                blob_resp.raise_for_status()
                tree_entries.append(
                    {
                        "path": file_entry["path"],
                        "mode": "100644",
                        "type": "blob",
                        "sha": blob_resp.json()["sha"],
                    }
                )

            # 4. Get the base tree from the push repo
            commit_url = f"{GITHUB_API}/repos/{push_repo}/git/commits/{base_sha}"
            commit_resp = self.session.get(commit_url, timeout=30)
            commit_resp.raise_for_status()
            base_tree_sha = commit_resp.json()["tree"]["sha"]

            # 5. Create a new tree with all files on the push repo
            tree_url = f"{GITHUB_API}/repos/{push_repo}/git/trees"
            tree_data = {
                "base_tree": base_tree_sha,
                "tree": tree_entries,
            }
            tree_resp = self.session.post(tree_url, json=tree_data, timeout=30)
            tree_resp.raise_for_status()
            new_tree_sha = tree_resp.json()["sha"]

            # 6. Create a commit on the push repo
            new_commit_url = f"{GITHUB_API}/repos/{push_repo}/git/commits"
            new_commit_data = {
                "message": commit_message,
                "tree": new_tree_sha,
                "parents": [base_sha],
            }
            new_commit_resp = self.session.post(
                new_commit_url, json=new_commit_data, timeout=30
            )
            new_commit_resp.raise_for_status()
            new_commit_sha = new_commit_resp.json()["sha"]

            # 7. Update the branch ref on the push repo
            update_ref_url = (
                f"{GITHUB_API}/repos/{push_repo}/git/refs/heads/{branch_name}"
            )
            update_data = {"sha": new_commit_sha}
            self.session.patch(update_ref_url, json=update_data, timeout=30)

            # 8. Create the pull request via gh CLI (handles cross-fork auth)
            if push_repo != upstream_repo:
                fork_owner = push_repo.split("/")[0]
                head_ref = f"{fork_owner}:{branch_name}"
            else:
                head_ref = branch_name

            gh_cmd = [
                "gh",
                "pr",
                "create",
                "--repo",
                upstream_repo,
                "--head",
                head_ref,
                "--base",
                target_branch,
                "--title",
                pr_title,
                "--body",
                pr_body,
            ]
            if draft:
                gh_cmd.append("--draft")
            if labels:
                for label in labels:
                    gh_cmd.extend(["--label", label])

            result = subprocess.run(
                gh_cmd,
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                log.error("gh pr create failed: %s", result.stderr.strip())
                return None

            pr_url_str = result.stdout.strip()
            pr_number = int(pr_url_str.rstrip("/").split("/")[-1])

            log.info("Created PR #%d: %s", pr_number, pr_url_str)
            return {
                "url": pr_url_str,
                "number": pr_number,
                "html_url": pr_url_str,
            }

        except (requests.RequestException, subprocess.SubprocessError):
            log.error("Failed to create PR on %s", upstream_repo, exc_info=True)
            return None

    def ensure_fork_synced(self, branch: str = "master"):
        """
        Sync the fork's branch with upstream so that upstream commits exist on the fork.

        GitHub forks may fall behind upstream; the Git Data API can't create
        a ref pointing to a SHA that doesn't exist on the fork. This merges
        upstream into the fork via the merge-upstream endpoint.

        Args:
            branch: Branch to sync (default "master").

        """
        fork_repo = self.cfg.github.fork_repo
        if not fork_repo or fork_repo == self.cfg.github.ocsci_repo:
            return

        url = f"{GITHUB_API}/repos/{fork_repo}/merge-upstream"
        try:
            resp = self.session.post(url, json={"branch": branch}, timeout=30)
            if resp.status_code == 200:
                log.info("Fork %s synced with upstream (%s)", fork_repo, branch)
            elif resp.status_code == 409:
                log.debug("Fork %s already up to date (%s)", fork_repo, branch)
            else:
                resp.raise_for_status()
        except requests.RequestException:
            log.warning("Failed to sync fork %s", fork_repo, exc_info=True)

    def create_backport_pr(
        self,
        source_branch: str,
        release_branch: str,
        commit_message: str,
        pr_title: str,
        pr_body: str,
        draft: bool = True,
        files: Optional[list] = None,
    ) -> Optional[dict]:
        """
        Create a backport PR by committing file(s) to a release branch.

        Rather than cherry-picking (which requires a local clone), this creates
        a new branch from the release branch and commits the files directly.

        Args:
            source_branch: The original feature branch name (for naming).
            release_branch: The release branch to target, e.g. "release-4.22".
            commit_message: Commit message.
            pr_title: PR title.
            pr_body: PR body.
            draft: Whether to create as draft.
            files: List of dicts with 'path' and 'content' keys.

        Returns:
            Optional[dict]: PR info or None.

        """
        backport_branch = f"{source_branch}-backport-{release_branch}"
        return self.create_branch_and_pr(
            branch_name=backport_branch,
            target_branch=release_branch,
            commit_message=commit_message,
            pr_title=f"[{release_branch}] {pr_title}",
            pr_body=pr_body,
            draft=draft,
            files=files,
        )
