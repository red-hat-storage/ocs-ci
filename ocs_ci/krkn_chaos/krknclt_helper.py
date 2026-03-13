"""
Helper functions for krknctl chaos testing.
"""

import json
import logging
import os
import random
import string

from jinja2 import Template

from ocs_ci.ocs.constants import (
    KRKN_OUTPUT_DIR,
    KRKNCTL_PLAN_TEMPLATE,
    # Component label constants used by krkn tests (rook-ceph + noobaa only, no 419 CSI)
    OSD_APP_LABEL,
    MON_APP_LABEL,
    MGR_APP_LABEL,
    MDS_APP_LABEL,
    RGW_APP_LABEL,
    OPERATOR_LABEL,
    NOOBAA_APP_LABEL,
)

log = logging.getLogger(__name__)

# App names derived from krkn component labels (app= value); excludes 419 CSI app labels
KRKN_APP_LABEL_CONSTANTS = (
    OSD_APP_LABEL,
    MON_APP_LABEL,
    MGR_APP_LABEL,
    MDS_APP_LABEL,
    RGW_APP_LABEL,
    OPERATOR_LABEL,
    NOOBAA_APP_LABEL,
)
CEPH_APP_SELECTORS = [label.split("=", 1)[1] for label in KRKN_APP_LABEL_CONSTANTS]


def generate_random_plan_file(
    namespace="openshift-storage",
    exclude_scenarios=None,
    **kwargs,
):
    """
    Generate a new scenario plan file for krknctl by rendering the jinja template
    and saving it under the krkn output directory with a random name.

    Reads the template at ocs_ci/krkn_chaos/template/scenarios/keknctl/plan.json.j2,
    fills it with random selectors and the given context, optionally excludes
    scenarios, appends a random suffix to each scenario key, and writes the
    result to {KRKN_OUTPUT_DIR}/plan_<random>.json (same location as other krkn output).

    Args:
        namespace (str): Target namespace for chaos scenarios. Defaults to
            "openshift-storage".
        exclude_scenarios (list): Scenario keys to exclude from the plan
            (e.g. ["dummy-scenario", "chaos-recommender"]). Excluded entries
            are removed from the generated plan.
        **kwargs: Optional template variables to override (e.g. duration,
            node_selector). If not provided, pod_selector, label_selector, and
            workers are set randomly as below.

    Template variables set by this function (unless overridden by kwargs):
        - namespace: "openshift-storage"
        - pod_selector: "{app: <random from CEPH_APP_SELECTORS (all krkn component app names)>}"
        - label_selector: random from same list
        - workers: random int between 1 and 6 (for node-memory-hog NUMBER_OF_WORKERS)

    Returns:
        str: Absolute path to the generated plan JSON file.
    """
    if not os.path.isfile(KRKNCTL_PLAN_TEMPLATE):
        raise FileNotFoundError(
            f"krknctl plan template not found at {KRKNCTL_PLAN_TEMPLATE}"
        )

    exclude_scenarios = exclude_scenarios or []

    # Random values for selectors and workers.
    # Use key=value form for label selectors (e.g. app=rook-ceph-mon).
    # POD_SELECTOR format for application-outages: "{app: rook-ceph-mgr}" (space after colon).
    pod_app = random.choice(CEPH_APP_SELECTORS)
    label_app = random.choice(CEPH_APP_SELECTORS)
    pod_selector = f"{{app: {pod_app}}}"
    label_selector = f"app={label_app}"
    number_of_workers = str(random.randint(1, 6))

    context = {
        "namespace": namespace,
        "pod_selector": pod_selector,
        "label_selector": label_selector,
        "pod_label": label_selector,
        "workers": number_of_workers,
        **kwargs,
    }

    with open(KRKNCTL_PLAN_TEMPLATE, "r") as f:
        template_content = f.read()

    template = Template(template_content)
    rendered = template.render(**context)

    rendered_stripped = rendered.strip() if rendered else ""
    if not rendered_stripped:
        raise ValueError(
            f"Krknctl plan template rendered to empty content. "
            f"Template path: {KRKNCTL_PLAN_TEMPLATE}. "
            "Ensure the template file exists and contains valid Jinja2 that outputs JSON."
        )
    plan_data = json.loads(rendered)

    # Remove excluded scenarios (top-level keys that are scenario names)
    for key in list(plan_data.keys()):
        if key.startswith("_"):
            continue
        if key in exclude_scenarios:
            del plan_data[key]
            log.debug("Excluded scenario from plan: %s", key)

    # Append random suffix to scenario keys (e.g. node-memory-hog -> node-memory-hog_xyzsj).
    # The "name" field inside each scenario stays unchanged (krknctl uses it as scenario type).
    name_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
    suffixed_plan = {}
    key_mapping = {}  # old_key -> new_key for updating depends_on
    for scenario_key, scenario_obj in plan_data.items():
        if scenario_key.startswith("_"):
            suffixed_plan[scenario_key] = scenario_obj
        else:
            new_key = f"{scenario_key}_{name_suffix}"
            key_mapping[scenario_key] = new_key
            suffixed_plan[new_key] = scenario_obj
    # Update depends_on to reference suffixed keys (e.g. "root" -> "root_xyzsj")
    for scenario_obj in suffixed_plan.values():
        if isinstance(scenario_obj, dict) and "depends_on" in scenario_obj:
            dep = scenario_obj["depends_on"]
            if dep in key_mapping:
                scenario_obj["depends_on"] = key_mapping[dep]
    plan_data = suffixed_plan

    os.makedirs(KRKN_OUTPUT_DIR, exist_ok=True)
    file_suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    plan_filename = f"plan_{file_suffix}.json"
    plan_path = os.path.join(KRKN_OUTPUT_DIR, plan_filename)

    with open(plan_path, "w") as f:
        json.dump(plan_data, f, indent=2)

    log.info(
        "Generated krknctl plan file: %s (namespace=%s, pod_selector=%s, "
        "label_selector=%s, workers=%s, excluded=%s)",
        plan_path,
        namespace,
        pod_selector,
        label_selector,
        number_of_workers,
        exclude_scenarios,
    )
    return os.path.abspath(plan_path)
