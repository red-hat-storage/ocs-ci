#!/usr/bin/env python3
"""
OCS-CI code vector database CLI.

Pipeline:
  OCS-CI repo dirs → Code Parser → Metadata → Qdrant Vector DB → Retrieval API

Indexed directories (config.INDEX_DIR_NAMES):
  conf/, Docker_files/, docs/, examples/, external/, ocs_ci/ (ocs-ci),
  scripts/, src/, template_test/, terraform/, tests/

Run from the ocs-ci repository root (not from this directory):

  cd /path/to/ocs-ci
  python .claude/vectorDB/vector_db_cli.py create
  python .claude/vectorDB/vector_db_cli.py update
  python .claude/vectorDB/vector_db_cli.py status
  python .claude/vectorDB/vector_db_cli.py search "noobaa bucket replication"
  python .claude/vectorDB/vector_db_cli.py cleanup --all

If cwd is already .claude/vectorDB/, use: python vector_db_cli.py <command>
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from pathlib import Path

# Ensure module imports resolve when run as a script
_MODULE_DIR = Path(__file__).resolve().parent
_REQUIREMENTS = _MODULE_DIR / "requirements.txt"
if str(_MODULE_DIR) not in sys.path:
    sys.path.insert(0, str(_MODULE_DIR))


def _missing_dependencies() -> list[str]:
    missing: list[str] = []
    try:
        import qdrant_client  # noqa: F401
    except ImportError:
        missing.append("qdrant-client")
    try:
        import sentence_transformers  # noqa: F401
    except ImportError:
        missing.append("sentence-transformers")
    return missing


def _ensure_dependencies() -> None:
    missing = _missing_dependencies()
    if not missing:
        return
    print(
        "Missing Python packages for the vector DB:",
        ", ".join(missing),
        file=sys.stderr,
    )
    print(file=sys.stderr)
    print("Install into the same interpreter you use to run this CLI:", file=sys.stderr)
    print(f"  {sys.executable} -m pip install -r {_REQUIREMENTS}", file=sys.stderr)
    print(file=sys.stderr)
    print("Or run:", file=sys.stderr)
    print(
        f"  {sys.executable} {_MODULE_DIR / 'vector_db_cli.py'} install-deps",
        file=sys.stderr,
    )
    raise SystemExit(1)


def _pip_available() -> bool:
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "--version"],
            check=True,
            capture_output=True,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def _bootstrap_pip() -> None:
    print(f"Bootstrapping pip for {sys.executable} ...", file=sys.stderr)
    subprocess.check_call([sys.executable, "-m", "ensurepip", "--upgrade"])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])


def cmd_install_deps(_args: argparse.Namespace) -> int:
    if not _pip_available():
        _bootstrap_pip()
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "-r", str(_REQUIREMENTS)],
    )
    print(f"Installed vector DB dependencies for {sys.executable}")
    return 0


from config import (  # noqa: E402
    DEFAULT_BATCH_SIZE,
    DEFAULT_COLLECTION,
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_TOP_K,
    INDEX_DIR_NAMES,
    REPO_ROOT,
)

log = logging.getLogger("vector_db")


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--collection",
        default=DEFAULT_COLLECTION,
        help=f"Qdrant collection name (default: {DEFAULT_COLLECTION})",
    )
    parser.add_argument(
        "--qdrant-url",
        default=None,
        help="Remote Qdrant URL (default: embedded local storage)",
    )
    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help=f"HuggingFace embedding model (default: {DEFAULT_EMBEDDING_MODEL})",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")


def cmd_create(args: argparse.Namespace) -> int:
    from index_manager import create_db

    index_dir = Path(args.index_dir).resolve() if args.index_dir else None
    result = create_db(
        index_dir=index_dir,
        collection=args.collection,
        embedding_model=args.embedding_model,
        batch_size=args.batch_size,
        qdrant_url=args.qdrant_url,
        max_files=args.max_files,
    )
    print(json.dumps(result, indent=2))
    return 0


def cmd_update(args: argparse.Namespace) -> int:
    from index_manager import update_db

    index_dir = Path(args.index_dir).resolve() if args.index_dir else None
    result = update_db(
        index_dir=index_dir,
        collection=args.collection,
        embedding_model=args.embedding_model,
        batch_size=args.batch_size,
        qdrant_url=args.qdrant_url,
        max_files=args.max_files,
    )
    print(json.dumps(result, indent=2))
    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    from index_manager import cleanup_db

    result = cleanup_db(
        collection=args.collection,
        qdrant_url=args.qdrant_url,
        remove_data=args.all,
    )
    print(json.dumps(result, indent=2))
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    from index_manager import db_status

    result = db_status(collection=args.collection, qdrant_url=args.qdrant_url)
    print(json.dumps(result, indent=2))
    return 0


def cmd_search(args: argparse.Namespace) -> int:
    from retrieval import find_similar_tests

    chunk_type = None if args.all_content else "test"
    matches = find_similar_tests(
        args.query,
        top_k=args.top_k,
        collection=args.collection,
        embedding_model=args.embedding_model,
        qdrant_url=args.qdrant_url,
        score_threshold=args.score_threshold,
        chunk_type=chunk_type,
    )
    if args.json:
        print(json.dumps(matches, indent=2))
        return 0

    if not matches:
        print("No matches found. Run 'create' first to build the index.")
        return 1

    for i, match in enumerate(matches, 1):
        label = match.get("node_id") or match.get("file_path")
        print(f"\n{i}. {label}  (score={match['score']})")
        if match.get("chunk_type"):
            print(f"   type: {match['chunk_type']}  dir: {match.get('source_dir', '')}")
        if match.get("coverage_areas"):
            print(f"   areas: {', '.join(match['coverage_areas'])}")
        if match.get("jira_ids"):
            print(f"   jira: {', '.join(match['jira_ids'])}")
        if match.get("docstring"):
            excerpt = match["docstring"].replace("\n", " ")[:120]
            print(f"   doc: {excerpt}")
        if match.get("pytest_command"):
            print(f"   cmd: {match['pytest_command']}")
    return 0


def cmd_search_issue(args: argparse.Namespace) -> int:
    from retrieval import find_similar_tests_for_issue

    issues_path = (
        REPO_ROOT
        / ".claude/workflow/issue_verification_workflow/run_record"
        / f"{args.run_id}_odf-{args.odf_version}"
        / f"{args.run_id}_issues.json"
    )
    if not issues_path.is_file():
        print(f"Issues file not found: {issues_path}", file=sys.stderr)
        return 1

    issues = json.loads(issues_path.read_text(encoding="utf-8"))
    issue = next((i for i in issues if i.get("key") == args.issue_key), None)
    if not issue:
        print(f"Issue {args.issue_key} not found in {issues_path}", file=sys.stderr)
        return 1

    matches = find_similar_tests_for_issue(
        issue,
        top_k=args.top_k,
        collection=args.collection,
        embedding_model=args.embedding_model,
        qdrant_url=args.qdrant_url,
    )

    if args.json:
        print(
            json.dumps(
                {
                    "issue_key": args.issue_key,
                    "matching_test_count": len(matches),
                    "matching_tests": matches,
                },
                indent=2,
            )
        )
        return 0

    print(f"Similar tests for {args.issue_key}: {issue.get('summary', '')[:80]}")
    for i, match in enumerate(matches, 1):
        print(f"\n{i}. {match['node_id']}  (score={match['score']})")
        print(f"   cmd: {match.get('pytest_command')}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="OCS-CI code vector database — index and search test metadata",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_install = sub.add_parser(
        "install-deps",
        help="Install qdrant-client and sentence-transformers for this Python",
    )
    p_install.set_defaults(func=cmd_install_deps)

    p_create = sub.add_parser(
        "create", help="Build a fresh vector DB from configured dirs"
    )
    _add_common_args(p_create)
    p_create.add_argument(
        "--index-dir",
        help=f"Limit to one index dir ({', '.join(INDEX_DIR_NAMES)})",
    )
    p_create.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_create.add_argument(
        "--max-files", type=int, default=None, help="Limit files (debug)"
    )
    p_create.set_defaults(func=cmd_create)

    p_update = sub.add_parser("update", help="Incrementally update changed files")
    _add_common_args(p_update)
    p_update.add_argument(
        "--index-dir",
        help=f"Limit to one index dir ({', '.join(INDEX_DIR_NAMES)})",
    )
    p_update.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    p_update.add_argument("--max-files", type=int, default=None)
    p_update.set_defaults(func=cmd_update)

    p_cleanup = sub.add_parser(
        "cleanup", help="Delete collection and optional local data"
    )
    _add_common_args(p_cleanup)
    p_cleanup.add_argument(
        "--all",
        action="store_true",
        help="Also remove data/qdrant/ and manifest.json",
    )
    p_cleanup.set_defaults(func=cmd_cleanup)

    p_status = sub.add_parser("status", help="Show collection and manifest status")
    _add_common_args(p_status)
    p_status.set_defaults(func=cmd_status)

    p_search = sub.add_parser(
        "search", help="Find similar tests by natural-language query"
    )
    _add_common_args(p_search)
    p_search.add_argument("query", help="Search query text")
    p_search.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    p_search.add_argument("--score-threshold", type=float, default=None)
    p_search.add_argument(
        "--all-content",
        action="store_true",
        help="Search all indexed content, not just tests",
    )
    p_search.add_argument("--json", action="store_true", help="JSON output")
    p_search.set_defaults(func=cmd_search)

    p_issue = sub.add_parser(
        "search-issue",
        help="Find similar tests for a z-stream run-record issue",
    )
    _add_common_args(p_issue)
    p_issue.add_argument("--run-id", required=True, help="Z-stream run id")
    p_issue.add_argument("--odf-version", required=True, help="ODF version (e.g. 4.22)")
    p_issue.add_argument(
        "--issue-key", required=True, help="JIRA issue key (e.g. DFBUGS-784)"
    )
    p_issue.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    p_issue.add_argument("--json", action="store_true")
    p_issue.set_defaults(func=cmd_search_issue)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command != "install-deps":
        _ensure_dependencies()
    if args.command != "install-deps" and hasattr(args, "verbose"):
        _configure_logging(args.verbose)
    elif args.command != "install-deps":
        _configure_logging(False)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
