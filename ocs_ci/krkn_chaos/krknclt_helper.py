"""
Krkctl plan generation for chaos testing.

The plan template (plan.json.j2) matches the workable krknctl plan format: each
scenario key is "scenario_name_{{ suffix }}" and depends_on is "root_{{ suffix }}".
PlanGenerator parses the Jinja template, fills parameters, and writes the plan file;
the instance holds the plan file path after generation.
"""

import json
import logging
import os
import random
import string
import copy

from jinja2 import Template

from ocs_ci.ocs.constants import (
    KRKN_OUTPUT_DIR,
    KRKNCTL_PLAN_TEMPLATE,
    OSD_APP_LABEL,
    MON_APP_LABEL,
    MGR_APP_LABEL,
    MDS_APP_LABEL,
    RGW_APP_LABEL,
    OPERATOR_LABEL,
    NOOBAA_APP_LABEL,
)

log = logging.getLogger(__name__)

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

# Base scenario names in the template (keys are "name_{{ suffix }}" in output).
KRKNCTL_PLAN_SCENARIO_KEYS = (
    "root",
    "application-outages",
    "container-scenarios",
    "network-chaos",
    "node-cpu-hog",
    "node-io-hog",
    "node-memory-hog",
    "node-network-filter",
    "pod-network-chaos",
    "pod-network-filter",
    "pod-scenarios",
    "service-disruption-scenarios",
    "syn-flood",
)

ROOT_SCENARIO_KEY = "root"

# Plan with only root + service-disruption-scenarios (for test_random_service_disruption).
SERVICE_DISRUPTION_INCLUDE_SCENARIOS = (
    ROOT_SCENARIO_KEY,
    "service-disruption-scenarios",
)

# Plan with only root + application-outages (expanded per label).
APPLICATION_OUTAGES_INCLUDE_SCENARIOS = (
    ROOT_SCENARIO_KEY,
    "application-outages",
)

# App labels used when expanding application-outages (one node per label).
# Use KRKN_APP_LABEL_CONSTANTS so application-outage covers OSD, MON, MGR, MDS, RGW, operator, Noobaa.
APPLICATION_OUTAGES_APP_LABELS = KRKN_APP_LABEL_CONSTANTS


def _full_key(base_name, suffix):
    """Plan key for a scenario: base_name_suffix (e.g. application-outages_5j6t5)."""
    return f"{base_name}_{suffix}"


def _label_to_slug(label_selector):
    """Convert label_selector like 'app=rook-ceph-osd' to a slug 'rook-ceph-osd'."""
    if "=" in label_selector:
        return label_selector.split("=", 1)[1].replace(".", "-")
    return label_selector.replace(".", "-")


def _label_to_short_slug(label_selector):
    """Convert label_selector to short slug for plan keys: 'app=rook-ceph-osd' -> 'osd'."""
    app_value = _label_to_slug(label_selector)
    if app_value.startswith("rook-ceph-"):
        return app_value.split("rook-ceph-", 1)[1]
    return app_value


def _label_to_pod_selector(label_selector):
    """Convert label_selector 'app=rook-ceph-osd' to POD_SELECTOR value '{app: rook-ceph-osd}'."""
    app_value = _label_to_slug(label_selector)
    return f"{{app: {app_value}}}"


class PlanGenerator:
    """
    Generates krknctl plan JSON files from the Jinja template.

    Holds scenario names and exposes one method that parses the template,
    fills parameters, applies exclusions/overrides, and writes the plan file.
    After generate() is called, plan_path is set to the written file location.
    """

    # Scenario names defined in the plan template (same as KRKNCTL_PLAN_SCENARIO_KEYS).
    SCENARIO_NAMES = KRKNCTL_PLAN_SCENARIO_KEYS

    def __init__(
        self,
        namespace="openshift-storage",
        include_scenarios=None,
        exclude_scenarios=None,
        scenario_overrides=None,
        use_random_selectors=True,
        label_selectors=None,
        **template_vars,
    ):
        self.namespace = namespace
        self.include_scenarios = (
            include_scenarios  # None or list; takes precedence over exclude
        )
        self.exclude_scenarios = exclude_scenarios or []
        self.scenario_overrides = scenario_overrides or {}
        self.use_random_selectors = use_random_selectors
        self.label_selectors = label_selectors  # list of label strings; expands service-disruption per label
        self.template_vars = template_vars
        self.plan_path = None
        self._suffix = None

    def generate(self):
        """
        Parse the Jinja template, fill parameters, apply exclusions and overrides,
        write the plan file, and set self.plan_path to the written path.

        Returns:
            str: Absolute path to the generated plan JSON file.
        """
        if not os.path.isfile(KRKNCTL_PLAN_TEMPLATE):
            raise FileNotFoundError(
                f"krknctl plan template not found at {KRKNCTL_PLAN_TEMPLATE}"
            )

        self._suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=5)
        )

        if self.use_random_selectors:
            pod_app = random.choice(CEPH_APP_SELECTORS)
            label_app = random.choice(CEPH_APP_SELECTORS)
            pod_selector = f"{{app: {pod_app}}}"
            label_selector = f"app={label_app}"
            workers = str(random.randint(1, 6))
        else:
            pod_selector = self.template_vars.get("pod_selector", "")
            label_selector = self.template_vars.get("label_selector", "")
            workers = self.template_vars.get("workers", "1")

        context = {
            "suffix": self._suffix,
            "namespace": self.namespace,
            "pod_selector": pod_selector,
            "label_selector": label_selector,
            "pod_label": label_selector,
            "workers": workers,
        }
        context.update(self.template_vars)

        with open(KRKNCTL_PLAN_TEMPLATE, "r") as f:
            template = Template(f.read())
        rendered = template.render(**context)
        rendered_stripped = rendered.strip() if rendered else ""
        if not rendered_stripped:
            raise ValueError(
                f"Plan template rendered to empty. Path: {KRKNCTL_PLAN_TEMPLATE}"
            )
        plan_data = json.loads(rendered)

        if self.include_scenarios:
            self._keep_only_included(plan_data)
            if self.label_selectors:
                if "application-outages" in self.include_scenarios:
                    self._expand_application_outages_by_labels(plan_data)
                if "service-disruption-scenarios" in self.include_scenarios:
                    self._expand_service_disruption_by_labels(plan_data)
        else:
            self._remove_excluded(plan_data)
            self._warn_if_root_excluded(plan_data)
        self._apply_overrides(plan_data)

        os.makedirs(KRKN_OUTPUT_DIR, exist_ok=True)
        dir_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        plan_dir = os.path.join(KRKN_OUTPUT_DIR, dir_suffix)
        os.makedirs(plan_dir, exist_ok=True)
        file_suffix = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=8)
        )
        self.plan_path = os.path.join(plan_dir, f"plan_{file_suffix}.json")
        with open(self.plan_path, "w") as f:
            json.dump(plan_data, f, indent=2)

        log.info(
            "Generated krknctl plan: %s (namespace=%s, suffix=%s, include=%s, exclude=%s)",
            self.plan_path,
            self.namespace,
            self._suffix,
            self.include_scenarios,
            self.exclude_scenarios,
        )
        return os.path.abspath(self.plan_path)

    def _keep_only_included(self, plan_data):
        """Keep only scenarios in include_scenarios (and _comment keys). Root must be in include_scenarios if needed."""
        include_set = set(self.include_scenarios)
        for k in list(plan_data.keys()):
            if k.startswith("_"):
                continue
            base = k.rsplit("_", 1)[0] if "_" in k else k
            if base not in include_set:
                del plan_data[k]
                log.debug("Removed scenario (not in include list): %s", base)

    def _expand_application_outages_by_labels(self, plan_data):
        """
        Replace the single application-outages node with one node per label in
        label_selectors, each with its own POD_SELECTOR. Keys become
        application-outages_<short_slug>_<suffix> (e.g. application-outages_osd_xxyyzz).
        """
        base_key = _full_key("application-outages", self._suffix)
        if base_key not in plan_data:
            log.warning(
                "application-outages key %s not in plan; skip expand by labels",
                base_key,
            )
            return
        template_node = plan_data.pop(base_key)
        root_key = _full_key(ROOT_SCENARIO_KEY, self._suffix)
        for label in self.label_selectors:
            node = copy.deepcopy(template_node)
            node.setdefault("env", {})["POD_SELECTOR"] = _label_to_pod_selector(label)
            if "depends_on" in node:
                node["depends_on"] = root_key
            short_slug = _label_to_short_slug(label)
            new_key = f"application-outages_{short_slug}_{self._suffix}"
            plan_data[new_key] = node
            log.debug("Added application-outages node for label: %s", label)

    def _expand_service_disruption_by_labels(self, plan_data):
        """
        Replace the single service-disruption-scenarios node with one node per label
        in label_selectors, each with its own LABEL_SELECTOR. Keys become
        service-disruption-scenarios_<slug>_<suffix> for readability.
        """
        base_key = _full_key("service-disruption-scenarios", self._suffix)
        if base_key not in plan_data:
            log.warning(
                "service-disruption-scenarios key %s not in plan; skip expand by labels",
                base_key,
            )
            return
        template_node = plan_data.pop(base_key)
        for label in self.label_selectors:
            node = copy.deepcopy(template_node)
            node.setdefault("env", {})["LABEL_SELECTOR"] = label
            slug = _label_to_slug(label)
            new_key = f"service-disruption-scenarios_{slug}_{self._suffix}"
            plan_data[new_key] = node
            log.debug("Added service-disruption node for label: %s", label)

    def _remove_excluded(self, plan_data):
        excluded_set = set(self.exclude_scenarios)
        for base in excluded_set:
            key = _full_key(base, self._suffix)
            if key in plan_data:
                del plan_data[key]
                log.debug("Excluded scenario from plan: %s", base)

    def _warn_if_root_excluded(self, plan_data):
        if ROOT_SCENARIO_KEY not in self.exclude_scenarios:
            return
        root_key = _full_key(ROOT_SCENARIO_KEY, self._suffix)
        remaining = [k for k in plan_data if not k.startswith("_") and k != root_key]
        if remaining:
            log.warning(
                "Root scenario is excluded but %s remain; DAG may be invalid (depends_on root).",
                remaining,
            )

    def _apply_overrides(self, plan_data):
        if not self.scenario_overrides:
            return
        for base_name, overrides in self.scenario_overrides.items():
            key = _full_key(base_name, self._suffix)
            if key not in plan_data or not isinstance(plan_data[key], dict):
                log.warning(
                    "scenario_overrides key %s not in plan, skipping", base_name
                )
                continue
            scenario = plan_data[key]
            if "env" in overrides and isinstance(overrides["env"], dict):
                env = scenario.setdefault("env", {})
                for k, v in overrides["env"].items():
                    env[k] = str(v)
                    log.debug("Override %s env %s = %s", base_name, k, v)


def generate_plan_file(
    namespace="openshift-storage",
    include_scenarios=None,
    exclude_scenarios=None,
    scenario_overrides=None,
    use_random_selectors=True,
    **template_vars,
):
    """
    Generate a krknctl plan JSON file from the Jinja template.

    Uses PlanGenerator: parses template, fills parameters, writes file.
    When include_scenarios is set, only those scenarios (plus root if listed) are kept;
    otherwise exclude_scenarios is used to remove scenarios.
    Returns the plan file path.
    """
    generator = PlanGenerator(
        namespace=namespace,
        include_scenarios=include_scenarios,
        exclude_scenarios=exclude_scenarios,
        scenario_overrides=scenario_overrides,
        use_random_selectors=use_random_selectors,
        **template_vars,
    )
    return generator.generate()


def generate_random_plan_file(
    namespace="openshift-storage",
    include_scenarios=None,
    exclude_scenarios=None,
    scenario_overrides=None,
    **kwargs,
):
    """
    Generate a plan file with random pod/label selectors.

    Convenience wrapper: creates PlanGenerator with use_random_selectors=True
    and returns the plan path.
    """
    return generate_plan_file(
        namespace=namespace,
        include_scenarios=include_scenarios,
        exclude_scenarios=exclude_scenarios,
        scenario_overrides=scenario_overrides,
        use_random_selectors=True,
        **kwargs,
    )
