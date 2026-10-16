"""Build and trigger parameterized Jenkins test runs."""

import logging
import time
from typing import Any

from config import TEST_RUN_DEFAULT_OVERRIDES, TEST_RUN_OVERRIDE_KEYS
from jenkins.rest_client import RestJenkinsClient, extract_build_parameters
from jenkins.url_parser import parse_jenkins_url
from models import JobRef, TriggerResult

log = logging.getLogger(__name__)


def node_id_to_file_path(node_id: str) -> str:
    """Convert pytest node id to file path (strip ::TestClass::test_name)."""
    if "::" in node_id:
        return node_id.split("::", 1)[0]
    return node_id


def normalize_test_paths(test_paths: list[str]) -> list[str]:
    """Deduplicate and normalize test paths or node ids to file paths."""
    seen: set[str] = set()
    ordered: list[str] = []
    for item in test_paths:
        path = node_id_to_file_path(item.strip())
        if path and path not in seen:
            seen.add(path)
            ordered.append(path)
    return ordered


def build_test_run_parameters(
    source_parameters: dict[str, Any],
    test_paths: list[str],
    *,
    test_name_expression: str = "",
    run_teardown: bool = False,
    additional_pytest_params: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Copy source build parameters and apply test-run overrides."""
    params = dict(source_parameters)
    paths = normalize_test_paths(test_paths)
    if not paths:
        raise ValueError("At least one test path is required")

    overrides: dict[str, Any] = {
        **TEST_RUN_DEFAULT_OVERRIDES,
        "TEST_PATH": " ".join(paths),
        "RUN_TEARDOWN": run_teardown,
    }
    if test_name_expression:
        overrides["TEST_NAME_EXPRESSION"] = test_name_expression
    if additional_pytest_params:
        overrides["ADDITIONAL_PYTEST_PARAMS"] = additional_pytest_params

    for key, value in overrides.items():
        if key in TEST_RUN_OVERRIDE_KEYS or key in params:
            params[key] = value

    return params, overrides


def load_source_build_parameters(
    client: RestJenkinsClient,
    source_job_url: str,
) -> tuple[JobRef, dict[str, Any]]:
    """Load parameters from a source build URL."""
    job_ref = parse_jenkins_url(source_job_url)
    if job_ref.build_number is None:
        raise ValueError(f"Source job URL must include build number: {source_job_url}")
    build_data = client.get_build(job_ref)
    return job_ref, extract_build_parameters(build_data)


def _queue_to_build_url(
    client: RestJenkinsClient, queue_url: str, job_name: str
) -> tuple[str | None, int | None]:
    """Poll queue item until executable build is available."""
    for _ in range(60):
        item = client.get_queue_item(queue_url)
        executable = item.get("executable") or {}
        number = executable.get("number")
        if number is not None:
            base = queue_url.split("/queue/")[0]
            build_url = f"{base}/job/{job_name}/{number}/"
            return build_url, int(number)
        if item.get("cancelled"):
            break
        time.sleep(2)
    return None, None


def trigger_test_run(
    source_job_url: str,
    test_paths: list[str],
    *,
    test_name_expression: str = "",
    run_teardown: bool = False,
    additional_pytest_params: str = "",
    dry_run: bool = True,
    client: RestJenkinsClient | None = None,
) -> TriggerResult:
    """
    Trigger qe-deploy-ocs-cluster with test paths copied from a source build.

    Uses REST buildWithParameters (Jenkins MCP triggerBuild cannot pass parameters).
    """
    rest = client or RestJenkinsClient()
    source_ref, source_params = load_source_build_parameters(rest, source_job_url)
    params, overrides = build_test_run_parameters(
        source_params,
        test_paths,
        test_name_expression=test_name_expression,
        run_teardown=run_teardown,
        additional_pytest_params=additional_pytest_params,
    )

    if dry_run:
        return TriggerResult(
            dry_run=True,
            source_job_url=source_job_url,
            new_job_url=None,
            build_number=None,
            parameters=params,
            parameter_overrides=overrides,
            message="Dry run: parameters prepared but Jenkins was not triggered",
        )

    job_only = JobRef(
        base_url=source_ref.base_url,
        job_name=source_ref.job_name,
        build_number=None,
        url=f"{source_ref.base_url}/job/{source_ref.job_name}/",
    )
    queue_location = rest.trigger_build_with_parameters(job_only, params)
    log.info("Triggered Jenkins build; queue location: %s", queue_location)

    new_url, build_number = _queue_to_build_url(
        rest, queue_location, source_ref.job_name
    )
    if not new_url:
        new_url = queue_location

    return TriggerResult(
        dry_run=False,
        source_job_url=source_job_url,
        new_job_url=new_url,
        build_number=build_number,
        parameters=params,
        parameter_overrides=overrides,
        message="Jenkins build triggered",
    )
