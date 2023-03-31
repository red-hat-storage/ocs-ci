"""
This module is used for generating simple one commit cherry-picks.
"""

import argparse
import os

from ocs_ci.ocs.constants import SCRIPT_DIR

from ocs_ci.utility.utils import (
    exec_cmd,
)

CHERRY_PICK_SCRIPT = os.path.join(SCRIPT_DIR, "cherry_pick", "cherry-picks.sh")


def init_arg_parser():
    """
    Init argument parser.

    Returns:
        object: Parsed arguments

    """

    parser = argparse.ArgumentParser(
        description="""
        OCS-CI do cherry-pick util which creates cherry-picks of last single commit in current branch.

        Example of usage:
        cherry-picks --upstream origin -f pbalogh --releases 4.11,4.10,4.9,4.8 \
            --commit 57ef4fdac1d1ee63ed06e289e62481b09d2fe5e5 -b fix-for-apply-icsp-for-upgrade-scenario
        """,
    )
    parser.add_argument(
        "--upstream",
        "-u",
        action="store",
        required=True,
        help="Upstream remote name (e.g. upstream)",
    )
    parser.add_argument(
        "--fork",
        "-f",
        action="store",
        required=True,
        help="Name of remote of own fork to which it will push the cherry-pick branch.",
    )
    parser.add_argument(
        "--releases",
        "-r",
        action="store",
        required=True,
        help="Comma separated list of releases you would like to cherry-pick (e.g 4.12,4.11,4.10).",
    )
    parser.add_argument(
        "--commit",
        "-c",
        action="store",
        help="Commit to cherry-pick, if not defined it will use current commit",
    )
    parser.add_argument(
        "--branch",
        "-b",
        action="store",
        help="Base branch name, if not defined it will use current branch",
    )
    args = parser.parse_args()

    return args


def main():
    """
    Main function for cherry-picks entrypoint
    """
    args = init_arg_parser()

    os.environ["UPSTREAM"] = args.upstream
    os.environ["FORK"] = args.fork
    os.environ["COMMIT"] = args.commit
    os.environ["BRANCH"] = args.branch
    script_process = exec_cmd(f"{CHERRY_PICK_SCRIPT} {args.releases}")
    print(script_process.stdout.decode())


if __name__ == "__main__":
    main()
