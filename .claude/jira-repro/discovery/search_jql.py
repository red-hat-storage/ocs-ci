#!/usr/bin/env python3
"""Discover DFBUGS keys in ON_QA for a target ODF release (CLI argument).

Filters issues by JIRA **Target Release** matching the --odf-version / ODF_VERSION
passed on the command line (exact version or same z-stream, e.g. 4.22 ~ 4.22.1).

Environment:
  JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN (or JIRA_MCP_* via mcp-env.sh)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

try:
    import requests
    import yaml
except ImportError as exc:
    print(f"search_jql: missing dependency: {exc}", file=sys.stderr)
    sys.exit(1)

from release_match import (
    cli_to_target_release_value,
    field_to_text,
    target_release_matches,
)

ROOT = Path(__file__).resolve().parents[3]
DISC = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT / ".claude" / "configs" / "jira-discovery.yaml"

# Fallback when jira-discovery.yaml has no jql_templates.primary
PRIMARY_JQL_DEFAULT = (
    'project = {project} AND "Target Release" = {target_release} '
    'AND status = "{status}" ORDER BY created DESC'
)

_FIELD_ID_CACHE: dict[str, str | None] = {}


def primary_jql_template(cfg: dict | None = None) -> str:
    """Primary JQL template from config, else built-in default."""
    templates = (cfg or {}).get("jql_templates") or {}
    return templates.get("primary") or PRIMARY_JQL_DEFAULT


def load_config() -> dict:
    path = Path(os.environ.get("JIRA_DISCOVERY_CONFIG", DEFAULT_CONFIG))
    if not path.is_file():
        return {}
    with path.open() as f:
        return yaml.safe_load(f) or {}


def target_release_field_name(cfg: dict) -> str:
    return (cfg.get("target_release") or {}).get("jql_field", "Target Release")


def build_jql(
    template: str,
    project: str,
    status: str,
    odf_version: str,
    *,
    target_release: str | None = None,
) -> str:
    tr = target_release or cli_to_target_release_value(odf_version)
    return (
        template.format(
            project=project,
            status=status,
            odf_version=odf_version,
            target_release=tr,
        )
        .replace("\n", " ")
        .strip()
    )


def auth_headers() -> tuple[dict[str, str], requests.auth.AuthBase | None]:
    mode = os.environ.get("JIRA_AUTH", "basic").lower()
    email = os.environ.get("JIRA_EMAIL", "")
    token = os.environ.get("JIRA_API_TOKEN", "")
    if mode == "bearer":
        return {"Authorization": f"Bearer {token}", "Accept": "application/json"}, None
    return {"Accept": "application/json"}, (email, token)


def make_session() -> requests.Session | None:
    base = os.environ.get("JIRA_URL", "").rstrip("/")
    email = os.environ.get("JIRA_EMAIL", "")
    token = os.environ.get("JIRA_API_TOKEN", "")
    if not all([base, token]) or (
        os.environ.get("JIRA_AUTH", "basic") == "basic" and not email
    ):
        return None
    headers, auth = auth_headers()
    session = requests.Session()
    session.headers.update(headers)
    if auth:
        session.auth = auth
    return session


def resolve_field_id(
    session: requests.Session, base: str, field_name: str
) -> str | None:
    cache_key = f"{base}:{field_name}"
    if cache_key in _FIELD_ID_CACHE:
        return _FIELD_ID_CACHE[cache_key]

    field_id: str | None = None
    for api in ("rest/api/3/field", "rest/api/2/field"):
        try:
            resp = session.get(urljoin(base + "/", api), timeout=60)
            resp.raise_for_status()
            for item in resp.json():
                if item.get("name") == field_name:
                    field_id = item.get("id")
                    break
            if field_id:
                break
        except requests.RequestException:
            continue

    _FIELD_ID_CACHE[cache_key] = field_id
    return field_id


def parse_issues_from_search(
    data: dict, release_field_id: str | None
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for item in data.get("issues", []):
        if not isinstance(item, dict) or "key" not in item:
            continue
        fields = item.get("fields") or {}
        release_val = None
        if release_field_id and release_field_id in fields:
            release_val = fields[release_field_id]
        elif "Target Release" in fields:
            release_val = fields["Target Release"]
        issues.append({"key": item["key"], "target_release": release_val})
    return issues


def search_v2(
    base: str,
    jql_str: str,
    max_results: int,
    session: requests.Session,
    release_field_id: str | None,
) -> list[dict[str, Any]]:
    url = urljoin(base + "/", "rest/api/2/search")
    fields = ["key"]
    if release_field_id:
        fields.append(release_field_id)
    collected: list[dict[str, Any]] = []
    start_at = 0
    while len(collected) < max_results:
        body = {
            "jql": jql_str,
            "startAt": start_at,
            "maxResults": min(100, max_results - len(collected)),
            "fields": fields,
        }
        resp = session.post(url, json=body, timeout=90)
        resp.raise_for_status()
        data = resp.json()
        batch = parse_issues_from_search(data, release_field_id)
        if not batch:
            break
        collected.extend(batch)
        if len(batch) == 0 or len(collected) >= data.get("total", len(collected)):
            break
        start_at = len(collected)
    return collected[:max_results]


def search_v3_jql(
    base: str,
    jql_str: str,
    max_results: int,
    session: requests.Session,
    release_field_id: str | None,
) -> list[dict[str, Any]]:
    url = urljoin(base + "/", "rest/api/3/search/jql")
    fields = ["key"]
    if release_field_id:
        fields.append(release_field_id)
    collected: list[dict[str, Any]] = []
    token: str | None = None
    while len(collected) < max_results:
        body: dict[str, Any] = {
            "jql": jql_str,
            "maxResults": min(100, max_results - len(collected)),
            "fields": fields,
        }
        if token:
            body["nextPageToken"] = token
        resp = session.post(url, json=body, timeout=90)
        resp.raise_for_status()
        data = resp.json()
        batch = parse_issues_from_search(data, release_field_id)
        collected.extend(batch)
        token = data.get("nextPageToken")
        if not token or not batch:
            break
    return collected[:max_results]


def search_v3_legacy(
    base: str,
    jql_str: str,
    max_results: int,
    session: requests.Session,
    release_field_id: str | None,
) -> list[dict[str, Any]]:
    url = urljoin(base + "/", "rest/api/3/search")
    fields = ["key"]
    if release_field_id:
        fields.append(release_field_id)
    body = {"jql": jql_str, "maxResults": max_results, "fields": fields}
    resp = session.post(url, json=body, timeout=90)
    resp.raise_for_status()
    return parse_issues_from_search(resp.json(), release_field_id)[:max_results]


def run_search(
    jql_str: str,
    release_field_id: str | None,
    *,
    verbose: bool = False,
) -> tuple[list[dict[str, Any]], str | None]:
    base = os.environ.get("JIRA_URL", "").rstrip("/")
    session = make_session()
    if not session:
        return [], "JIRA_URL, JIRA_EMAIL, and JIRA_API_TOKEN must be set"

    cfg = load_config()
    pref = os.environ.get("JIRA_API_VERSION", "")
    versions = [int(pref)] if pref else list(cfg.get("api_preference", [3, 2]))
    max_results = int(cfg.get("defaults", {}).get("max_results", 100))
    errors: list[str] = []

    for ver in versions:
        try:
            if ver == 2:
                issues = search_v2(
                    base, jql_str, max_results, session, release_field_id
                )
                api = "rest/api/2/search"
            elif ver == 3:
                try:
                    issues = search_v3_jql(
                        base, jql_str, max_results, session, release_field_id
                    )
                    api = "rest/api/3/search/jql"
                except requests.HTTPError as exc:
                    if exc.response is not None and exc.response.status_code in (
                        404,
                        410,
                    ):
                        issues = search_v3_legacy(
                            base, jql_str, max_results, session, release_field_id
                        )
                        api = "rest/api/3/search"
                    else:
                        raise
            else:
                continue
            if verbose:
                print(
                    f"search_jql: API {api} returned {len(issues)} issue(s) before Target Release filter",
                    file=sys.stderr,
                )
            return issues, None
        except requests.HTTPError as exc:
            detail = ""
            if exc.response is not None:
                try:
                    detail = exc.response.json().get(
                        "errorMessages", [exc.response.text]
                    )[0]
                except Exception:
                    detail = exc.response.text[:500]
            errors.append(
                f"API v{ver}: HTTP {exc.response.status_code if exc.response else '?'} — {detail}"
            )
        except requests.RequestException as exc:
            errors.append(f"API v{ver}: {exc}")

    return [], "; ".join(errors) if errors else "all API versions failed"


def filter_by_target_release(
    issues: list[dict[str, Any]],
    target_release: str,
    *,
    verbose: bool = False,
) -> tuple[list[str], list[dict[str, str]]]:
    """Keep issues whose Target Release matches CLI target_release argument."""
    matched: list[str] = []
    excluded: list[dict[str, str]] = []
    seen: set[str] = set()

    for item in issues:
        key = item.get("key")
        if not key or key in seen:
            continue
        seen.add(key)
        release_val = item.get("target_release")
        release_text = field_to_text(release_val)
        if target_release_matches(release_val, target_release):
            matched.append(key)
        else:
            excluded.append(
                {
                    "key": key,
                    "target_release": release_text or "(empty)",
                    "reason": f"does not match CLI target release {target_release!r}",
                }
            )

    if verbose and excluded:
        print(
            f"search_jql: excluded {len(excluded)} issue(s) — Target Release mismatch",
            file=sys.stderr,
        )
        for row in excluded[:5]:
            print(
                f"  {row['key']}: {row['target_release']!r}",
                file=sys.stderr,
            )
        if len(excluded) > 5:
            print(f"  ... and {len(excluded) - 5} more", file=sys.stderr)

    return matched, excluded


def discover(
    odf_version: str,
    status: str,
    project: str,
    *,
    verbose: bool = False,
    try_alternates: bool = True,
) -> dict[str, Any]:
    cfg = load_config()
    templates: dict[str, str] = cfg.get("jql_templates", {})
    release_field = target_release_field_name(cfg)
    allow_unfiltered = bool(cfg.get("allow_unfiltered_fallback", False))

    target_release_jira = cli_to_target_release_value(odf_version)

    session = make_session()
    base = os.environ.get("JIRA_URL", "").rstrip("/")
    release_field_id = None
    if session and base:
        release_field_id = resolve_field_id(session, base, release_field)

    tried: list[dict[str, Any]] = []
    last_error: str | None = None

    order = ["primary"]
    if try_alternates:
        order.extend(k for k in templates if k != "primary")

    for name in order:
        if name == "project_only" and not allow_unfiltered:
            continue
        if name == "primary":
            tpl = primary_jql_template(cfg)
        else:
            tpl = templates.get(name)
        if not tpl:
            continue
        jql_str = build_jql(
            tpl,
            project,
            status,
            odf_version,
            target_release=target_release_jira,
        )
        if verbose:
            print(
                f"search_jql: trying template={name!r} "
                f"(Target Release={target_release_jira!r})\n  {jql_str}",
                file=sys.stderr,
            )

        raw_issues, err = run_search(jql_str, release_field_id, verbose=verbose)
        matched, excluded = filter_by_target_release(
            raw_issues, odf_version, verbose=verbose
        )

        tried.append(
            {
                "template": name,
                "jql": jql_str,
                "count_before_filter": len(raw_issues),
                "count_after_filter": len(matched),
                "error": err,
            }
        )
        if err:
            last_error = err
            continue

        if matched:
            return {
                "odf_version": odf_version,
                "target_release_jira": target_release_jira,
                "target_release_filter": target_release_jira,
                "status": status,
                "project": project,
                "issue_keys": matched,
                "excluded_mismatch": excluded[:20],
                "excluded_mismatch_count": len(excluded),
                "jql_used": jql_str,
                "template_used": name,
                "release_field": release_field,
                "tried": tried,
            }

        if raw_issues and not matched:
            # JQL returned candidates but none matched Target Release — do not accept
            if verbose:
                print(
                    f"search_jql: template {name!r} had {len(raw_issues)} hits "
                    f"but 0 matched Target Release={odf_version!r}",
                    file=sys.stderr,
                )

    return {
        "odf_version": odf_version,
        "target_release_jira": target_release_jira,
        "target_release_filter": target_release_jira,
        "status": status,
        "project": project,
        "issue_keys": [],
        "jql_used": tried[-1]["jql"] if tried else None,
        "template_used": None,
        "tried": tried,
        "error": last_error
        or f"No issues with Target Release = {target_release_jira!r} (from CLI {odf_version!r})",
        "hint": (
            f"Expected Target Release value {target_release_jira!r} for CLI version {odf_version!r}. "
            f"Edit .claude/configs/jira-discovery.yaml"
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DFBUGS JIRA discovery filtered by Target Release vs CLI ODF version"
    )
    parser.add_argument(
        "--odf-version",
        default=os.environ.get("ODF_VERSION", ""),
        help="Target release from run.sh (matched against JIRA Target Release field)",
    )
    parser.add_argument("--status", default=os.environ.get("JIRA_STATUS", "ON_QA"))
    parser.add_argument("--project", default=os.environ.get("JIRA_PROJECT", "DFBUGS"))
    parser.add_argument("--out", type=str, default="")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--no-alternates", action="store_true")
    parser.add_argument("--print-jql", action="store_true")
    args = parser.parse_args()

    if not args.odf_version:
        print("search_jql: pass --odf-version or export ODF_VERSION", file=sys.stderr)
        sys.exit(2)

    cfg = load_config()
    if args.print_jql:
        tr = cli_to_target_release_value(args.odf_version)
        print(
            f"# CLI version: {args.odf_version} -> Target Release: {tr}",
            file=sys.stderr,
        )
        print(
            build_jql(
                primary_jql_template(cfg),
                args.project,
                args.status,
                args.odf_version,
                target_release=tr,
            )
        )
        return

    result = discover(
        args.odf_version,
        args.status,
        args.project,
        verbose=args.verbose,
        try_alternates=not args.no_alternates,
    )
    text = json.dumps(result, indent=2) + "\n"

    if args.out:
        Path(args.out).write_text(text)

    print(text)

    if result.get("error") and not result.get("issue_keys"):
        print(f"search_jql: ERROR — {result['error']}", file=sys.stderr)
        if result.get("hint"):
            print(f"search_jql: hint — {result['hint']}", file=sys.stderr)
        sys.exit(1)

    if not make_session():
        print("search_jql: ERROR — JIRA credentials not set", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
