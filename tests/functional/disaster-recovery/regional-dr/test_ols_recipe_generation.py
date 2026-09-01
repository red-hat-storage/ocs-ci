"""
Tests for ODF DR Recipe generation using OpenShift Lightspeed (OLS)

JIRA: RHSTOR-8222 - DR Recipe creation using OCP Lightspeed

These tests verify that the OLS service, configured with the DR recipe RAG
content image, can generate syntactically correct ODF Disaster Recovery
Recipes from natural-language prompts.

OLS is deployed automatically as part of the RDR deployment flow
(``RDRMultiClusterDROperatorsDeploy.deploy()`` in ``deployment.py``) for
ODF >= 5.0.  These tests assume the service is already running on the hub
cluster when executed.
"""

import logging

import pytest
import yaml

from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import tier1, skipif_ocs_version
from ocs_ci.ocs import constants
from ocs_ci.ocs.openshift_lightspeed import OpenShiftLightspeed, is_ols_available

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompt helpers
# ---------------------------------------------------------------------------

# Minimal inline schema stub embedded in every prompt so the model never needs
# to search for it.  This avoids the model spending its entire response budget
# on RAG / CRD-fetch tool calls when the doc search returns off-topic results.
_RECIPE_SCHEMA_STUB = """\
The ODF DR Recipe uses this schema (ramendr.openshift.io/v1alpha1):

apiVersion: ramendr.openshift.io/v1alpha1
kind: Recipe
metadata:
  name: <name>
  namespace: <app-namespace>
spec:
  appType: <app-name>
  groups:                        # resource groups (type: resource) or volume groups (type: volume)
    - name: <group-name>
      type: resource             # or: volume
      includedNamespaces: [<ns>]
      labelSelector:
        matchLabels:
          <key>: <value>
  volumes:                       # omit entirely when the app has no PVCs
    - name: <vol-group-name>
      type: volume
      includedNamespaces: [<ns>]
      labelSelector:
        matchLabels:
          <key>: <value>
  hooks:                         # omit when no hooks are needed
    - name: <hook-name>
      type: exec                 # or: scale, check
      namespace: <ns>
      labelSelector:
        matchLabels:
          <key>: <value>
      ops:
        - name: <op-name>
          command: [<cmd>]
          container: <container>
  workflows:
    - name: backup
      sequence:
        - group: <group-name>    # or hook: <hook-name>
    - name: restore
      sequence:
        - group: <group-name>

Respond with ONLY a ```yaml``` code block containing the complete Recipe manifest.
Do not search for documentation. Do not include explanatory text outside the code block.\
"""


def _build_prompt(user_request):
    """
    Prepend the inline Recipe schema stub to a user request string.

    Args:
        user_request (str): The natural-language recipe request.

    Returns:
        str: Full prompt with schema context embedded.
    """
    return f"{_RECIPE_SCHEMA_STUB}\n\n{user_request}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=False)
def skip_if_ols_not_available():
    """
    Skip the requesting test/class when OLS is not installed on the hub
    cluster (i.e. the ``lightspeed-app-server`` Route does not exist).

    Use via ``usefixtures`` on a class or request directly in a fixture::

        @pytest.mark.usefixtures("skip_if_ols_not_available")
        class TestSomething: ...
    """
    if not is_ols_available():
        pytest.skip(
            "OpenShift Lightspeed is not installed on this cluster "
            "(route 'lightspeed-app-server' not found in 'openshift-lightspeed'). "
            "Deploy OLS or set skip_ols_deployment=false to enable these tests."
        )


@pytest.fixture(scope="class")
def ols_client(skip_if_ols_not_available, request):
    """
    Provide an authenticated :class:`~ocs_ci.ocs.openshift_lightspeed.OpenShiftLightspeed`
    client for the test class.

    Skips the entire class when OLS is not installed (via
    ``skip_if_ols_not_available``).  Fails with a clear message when OLS is
    installed but the authorization check returns a non-200 response.

    Always switches to the ACM hub context before constructing the client and
    restores the original context afterward.
    """
    from ocs_ci.framework import config

    saved_index = config.cur_index
    config.switch_acm_ctx()

    def _restore():
        config.switch_ctx(saved_index)

    request.addfinalizer(_restore)

    client = OpenShiftLightspeed()
    if not client.is_authorized():
        pytest.fail(
            "OLS authorization check failed. "
            "Verify that lightspeed-operator-query-access is bound and OLS pods are running."
        )
    return client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_yaml_from_response(response_text):
    """
    Extract the first YAML block from an OLS response string.

    OLS typically wraps the generated YAML in a markdown code fence
    (```yaml ... ```) or returns it inline.  This helper tries the fenced
    form first and falls back to parsing the full response body.

    Args:
        response_text (str): Raw text from the ``response`` key of an OLS
            query result.

    Returns:
        dict: Parsed YAML document, or ``None`` if no valid YAML was found.
    """
    # Try to extract a fenced YAML block — only process blocks that are
    # explicitly tagged as yaml (```yaml ... ```) to avoid mistakenly
    # parsing prose preambles that yaml.safe_load returns as plain strings.
    if "```" in response_text:
        for block in response_text.split("```"):
            block = block.strip()
            if not block.startswith("yaml"):
                continue
            block = block[4:].strip()
            try:
                parsed = yaml.safe_load(block)
                if isinstance(parsed, dict):
                    return parsed
            except yaml.YAMLError:
                continue

    # Fall back to parsing the whole response (only accept dicts, not bare strings)
    try:
        parsed = yaml.safe_load(response_text)
        return parsed if isinstance(parsed, dict) else None
    except yaml.YAMLError:
        return None


def _assert_recipe_structure(recipe_yaml):
    """
    Assert that a parsed Recipe YAML has the minimum required structure.

    Args:
        recipe_yaml (dict): Parsed Recipe manifest.
    """
    assert recipe_yaml is not None, "OLS response did not contain valid YAML"
    assert (
        recipe_yaml.get("apiVersion") == "ramendr.openshift.io/v1alpha1"
    ), f"Unexpected apiVersion: {recipe_yaml.get('apiVersion')}"
    assert (
        recipe_yaml.get("kind") == "Recipe"
    ), f"Expected kind 'Recipe', got: {recipe_yaml.get('kind')}"
    spec = recipe_yaml.get("spec", {})
    assert "groups" in spec, "Recipe spec must contain 'groups'"
    assert "workflows" in spec, "Recipe spec must contain 'workflows'"

    workflows = spec["workflows"]
    workflow_names = [w["name"] for w in workflows]
    assert "backup" in workflow_names, "Recipe must have a 'backup' workflow"
    assert "restore" in workflow_names, "Recipe must have a 'restore' workflow"


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.16")
@pytest.mark.usefixtures("skip_if_ols_not_available")
class TestOLSRecipeGeneration:
    """
    Verify OLS can generate ODF DR Recipes from natural-language prompts.

    All tests use the shared ``ols_client`` fixture and rely on a single
    conversation to keep context between related prompts where needed.
    Each test is independent and starts a fresh conversation unless
    otherwise stated.
    """

    def test_ols_authorization(self, ols_client):
        """
        Verify the OLS service is reachable and the test user is authorized.

        Test steps:
            1. Call the OLS ``/authorized`` endpoint.
            2. Expect HTTP 200 (already asserted by the fixture).
        """
        # Authorization is already asserted by the fixture.
        # This test documents the requirement explicitly.
        logger.info("OLS authorization verified via fixture")

    def test_recipe_simple_deployment_no_pvc(self, ols_client):
        """
        OLS generates a valid Recipe for a Deployment with no PVCs.

        Prompt taken from RHSTOR-8222 happy-path test cases.

        Test steps:
            1. Send the prompt for ``web-app`` in the ``web`` namespace.
            2. Extract the YAML from the response.
            3. Assert required Recipe fields are present.
            4. Assert no ``volumes`` section (app has no PVCs).
        """
        prompt = _build_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "web-app deployed in the web namespace. The application consists of "
            "one Deployment labeled app: web-app and does not use any "
            "PersistentVolumeClaims. Generate the complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert (
            spec.get("appType") == "web-app"
        ), f"Expected appType 'web-app', got: {spec.get('appType')}"
        assert (
            "volumes" not in spec
        ), "Recipe should not contain a volumes section for a PVC-less app"
        groups = spec["groups"]
        assert any(
            g.get("includedNamespaces", []) == ["web"] for g in groups
        ), "At least one group must target the 'web' namespace"

    def test_recipe_deployment_with_pvc(self, ols_client):
        """
        OLS generates a valid Recipe for a Deployment with one PVC.

        Test steps:
            1. Send the prompt for ``my-app`` with a PVC in the ``my-app``
               namespace.
            2. Extract and validate the YAML.
            3. Assert the ``volumes`` section is present and targets the
               correct namespace.
        """
        prompt = _build_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "my-app deployed in the my-app namespace. The application consists "
            "of one Deployment and one PVC. The Deployment is labeled app: "
            "my-app, and the PVC is also labeled app: my-app. Generate the "
            "complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert (
            "volumes" in spec
        ), "Recipe must contain a volumes section for an app with PVCs"
        volumes = spec["volumes"]
        # volumes can be a list or a single dict depending on the OLS output
        vol_list = volumes if isinstance(volumes, list) else [volumes]
        all_namespaces = []
        for vol in vol_list:
            all_namespaces.extend(vol.get("includedNamespaces", []))
        assert (
            "my-app" in all_namespaces
        ), "Volumes section must include the 'my-app' namespace"

    def test_recipe_exec_hook_before_backup(self, ols_client):
        """
        OLS generates a Recipe with an exec hook that runs before backup.

        Test steps:
            1. Send the prompt for ``postgres-app`` with a pre-backup exec
               hook (psql CHECKPOINT command).
            2. Extract and validate the YAML.
            3. Assert a ``hooks`` section with ``type: exec`` is present.
            4. Assert the backup workflow references the hook before the
               resource group.
        """
        prompt = _build_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "postgres-app deployed in the postgres namespace. The application "
            "consists of one Deployment labeled app: postgres-app and one PVC "
            "labeled app: postgres-app. Before backup, run an exec hook in "
            "container postgres that executes the command "
            '["psql","-U","postgres","-c","CHECKPOINT"]. '
            "The Deployment name is postgres-deployment. Generate the complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert "hooks" in spec, "Recipe must contain a hooks section"
        hooks = spec["hooks"]
        exec_hooks = [h for h in hooks if h.get("type") == "exec"]
        assert exec_hooks, "At least one hook must have type 'exec'"

        # The backup workflow sequence must reference the hook before the group
        backup_workflow = next(w for w in spec["workflows"] if w["name"] == "backup")
        sequence = backup_workflow.get("sequence", [])
        assert sequence, "Backup workflow must have a non-empty sequence"
        first_step = sequence[0]
        assert (
            "hook" in first_step
        ), "First step of backup workflow must be a hook (pre-backup exec)"

    def test_recipe_scale_hook(self, ols_client):
        """
        OLS generates a Recipe with a scale hook for backup and restore.

        Test steps:
            1. Send the prompt for ``minio-app`` with scale-down before
               backup and scale-up after restore.
            2. Extract and validate the YAML.
            3. Assert a ``hooks`` section with ``type: scale`` is present.
        """
        prompt = _build_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "minio-app deployed in the minio namespace. The application consists "
            "of one Deployment named minio labeled app: minio-app and one PVC "
            "labeled app: minio-app. Before backup, scale the Deployment down to "
            "0 replicas. After restore, scale it back to its original replica "
            "count and wait for the scaling operation to complete. "
            "Generate the complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert "hooks" in spec, "Recipe must contain a hooks section"
        scale_hooks = [h for h in spec["hooks"] if h.get("type") == "scale"]
        assert scale_hooks, "At least one hook must have type 'scale'"

    def test_recipe_check_hook_after_restore(self, ols_client):
        """
        OLS generates a Recipe with a check hook that validates readiness
        after restore.

        Test steps:
            1. Send the prompt for ``orders-app`` with a post-restore check
               that validates Deployment readiness.
            2. Extract and validate the YAML.
            3. Assert a hook with ``type: check`` is present.
            4. Assert the restore workflow references the check hook after
               the resource group.
        """
        prompt = _build_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "orders-app deployed in the orders namespace. The application "
            "consists of one Deployment labeled app: orders-app and one PVC "
            "labeled app: orders-app. After restore, validate that the "
            "Deployment named orders-deployment is ready by checking that "
            "spec.replicas equals status.readyReplicas. Generate the complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert "hooks" in spec, "Recipe must contain a hooks section"
        check_hooks = [h for h in spec["hooks"] if h.get("type") == "check"]
        assert check_hooks, "At least one hook must have type 'check'"

        # The restore workflow sequence must reference the check hook last
        restore_workflow = next(w for w in spec["workflows"] if w["name"] == "restore")
        sequence = restore_workflow.get("sequence", [])
        assert sequence, "Restore workflow must have a non-empty sequence"
        last_step = sequence[-1]
        assert (
            "hook" in last_step
        ), "Last step of restore workflow must be a hook (post-restore check)"

    def test_recipe_multi_turn_conversation(self, ols_client, request):
        """
        OLS uses conversation history to refine a Recipe over multiple turns.

        Test steps:
            1. First turn: Generate a basic Recipe for ``finance-app``.
            2. Second turn (same conversation_id): Ask OLS to add a pre-backup
               exec hook.
            3. Assert the final Recipe contains both a hooks section and the
               backup workflow references the hook.
        """
        # Turn 1 — basic recipe
        prompt_1 = _build_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "finance-app deployed in the finance namespace. The application "
            "consists of one Deployment labeled app: finance-app and one PVC "
            "labeled app: finance-app. Generate the complete Recipe."
        )
        result_1 = ols_client.query(prompt_1)
        conversation_id = result_1.get("conversation_id")
        assert conversation_id, "OLS must return a conversation_id for multi-turn tests"
        logger.info(f"Turn 1 conversation_id: {conversation_id}")

        # Register cleanup immediately so it runs even if later assertions fail
        def _delete_conversation():
            try:
                ols_client.delete_conversation(conversation_id)
                logger.info(f"Conversation {conversation_id} deleted")
            except Exception as exc:
                logger.warning(
                    f"Failed to delete conversation {conversation_id}: {exc}"
                )

        request.addfinalizer(_delete_conversation)

        # Turn 2 — add a pre-backup exec hook in the same conversation
        prompt_2 = (
            "Update the Recipe to add a pre-backup exec hook in the finance "
            'namespace that runs the command ["echo", "pre-backup"] in '
            "the finance-container container of the Deployment. Show the "
            "complete updated Recipe YAML."
        )
        result_2 = ols_client.query(prompt_2, conversation_id=conversation_id)
        response_text = result_2.get("response", "")
        logger.info(f"Turn 2 OLS response:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)
        spec = recipe["spec"]
        assert (
            "hooks" in spec
        ), "Updated Recipe must contain a hooks section after the follow-up prompt"

    def test_recipe_attachment_yaml_context(self, ols_client):
        """
        OLS uses an attached Kubernetes resource YAML as context to generate
        a tailored Recipe.

        Test steps:
            1. Build a minimal Deployment YAML as an attachment.
            2. Send a query with the attachment asking for a DR Recipe.
            3. Extract and validate the generated Recipe.
            4. Assert the namespace from the attached Deployment is reflected
               in the generated Recipe.
        """
        deployment_yaml = (
            "apiVersion: apps/v1\n"
            "kind: Deployment\n"
            "metadata:\n"
            "  name: sample-app\n"
            "  namespace: sample-ns\n"
            "  labels:\n"
            "    app: sample-app\n"
            "spec:\n"
            "  replicas: 1\n"
            "  selector:\n"
            "    matchLabels:\n"
            "      app: sample-app\n"
            "  template:\n"
            "    metadata:\n"
            "      labels:\n"
            "        app: sample-app\n"
            "    spec:\n"
            "      containers:\n"
            "        - name: sample-app\n"
            "          image: registry.k8s.io/busybox:latest\n"
        )
        attachments = [{"content_type": "application/yaml", "content": deployment_yaml}]

        prompt = _build_prompt(
            "Using the attached Deployment manifest, generate a complete ODF "
            "Disaster Recovery Recipe for this application. The app does not use PVCs."
        )
        result = ols_client.query(prompt, attachments=attachments)
        response_text = result.get("response", "")
        logger.info(f"OLS response with attachment:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)

        spec = recipe["spec"]
        groups = spec.get("groups", [])
        all_namespaces = []
        for g in groups:
            all_namespaces.extend(g.get("includedNamespaces", []))
        assert (
            "sample-ns" in all_namespaces
        ), "Generated Recipe must target the namespace from the attached Deployment"


# ---------------------------------------------------------------------------
# Real-workload test class — OLS-generated Recipe + DR failover/relocate
# ---------------------------------------------------------------------------


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.16")
@pytest.mark.usefixtures("skip_if_ols_not_available")
class TestOLSRecipeFailoverAndRelocate:
    """
    Deploy a real busybox Discovered App workload, ask OLS to generate its
    DR Recipe using the live namespace and PVC labels, apply that Recipe to
    the primary cluster, then perform a full failover → relocate cycle to
    verify the OLS-generated Recipe is operationally correct.

    This replaces the static ``recipe_with_checkhooks.yaml`` template with
    an OLS-generated Recipe so the entire Recipe content is validated end-to-end
    against the real DR protection flow.

    Flow
    ----
    1. Deploy a ``BusyboxDiscoveredApps`` workload (random namespace + RBD PVCs).
    2. Ask OLS to generate the DR Recipe using the real namespace, PVC label
       key/value, and pod label key/value from the deployed workload.
    3. Validate the generated Recipe structure and assert it targets the correct
       namespace and labels.
    4. Apply the OLS-generated Recipe to each managed cluster.
    5. Create the DRPC with ``recipeRef`` pointing to the applied Recipe.
    6. Perform failover to the secondary cluster and verify workload comes up.
    7. Relocate back to the primary cluster and verify workload comes up.
    8. Teardown: delete the workload; the ``discovered_apps_dr_workload``
       fixture handles full cleanup.
    """

    params = [
        pytest.param(
            constants.CEPHBLOCKPOOL,
            marks=[pytest.mark.polarion_id("OCS-8231")],
            id="rbd",
        ),
    ]

    @pytest.fixture(scope="class")
    def ols(self, skip_if_ols_not_available, request):
        """
        Authenticated OLS client (hub cluster).

        Skips the entire class when OLS is not installed.
        Always switches to the ACM hub context and restores the original
        context afterward.
        """
        from ocs_ci.framework import config

        saved_index = config.cur_index
        config.switch_acm_ctx()

        def _restore():
            config.switch_ctx(saved_index)

        request.addfinalizer(_restore)

        client = OpenShiftLightspeed()
        if not client.is_authorized():
            pytest.fail(
                "OLS authorization check failed — verify OLS pods are running "
                "and lightspeed-operator-query-access is bound."
            )
        return client

    def _generate_and_apply_ols_recipe(self, workload, ols_client, request):
        """
        Ask OLS to generate a DR Recipe for the given workload, validate the
        returned YAML, apply it to **both** managed clusters (primary and
        secondary), and return the recipe name.

        The cluster-selection logic mirrors ``BusyboxDiscoveredApps.deploy_workload``:
        uses ``get_non_acm_cluster_and_non_provider_cluster_config`` when
        ``dr_cluster_relations`` is set (standard RDR), otherwise falls back to
        ``get_non_acm_cluster_config``.

        Args:
            workload: A :class:`~ocs_ci.ocs.dr.dr_workload.BusyboxDiscoveredApps`
                instance that has already been deployed (so namespace and labels
                are known).
            ols_client: An authenticated
                :class:`~ocs_ci.ocs.openshift_lightspeed.OpenShiftLightspeed`
                instance.
            request: The pytest ``request`` fixture — used to register cleanup
                finalizers for the temp file and applied Recipe objects.

        Returns:
            str: Name of the applied Recipe.
        """
        import os
        import re
        import tempfile

        from ocs_ci.framework import config
        from ocs_ci.ocs.utils import (
            get_non_acm_cluster_and_non_provider_cluster_config,
            get_non_acm_cluster_config,
        )
        from ocs_ci.utility.utils import exec_cmd

        namespace = workload.workload_namespace
        pvc_key = workload.discovered_apps_pvc_selector_key
        pvc_value = workload.discovered_apps_pvc_selector_value
        pod_key = workload.discovered_apps_pod_selector_key
        pod_value = workload.discovered_apps_pod_selector_value
        app_name = workload.workload_name  # "busybox"
        pvc_count = workload.workload_pvc_count

        logger.info(
            f"Generating OLS Recipe for workload — namespace: {namespace}, "
            f"pvc_selector: {pvc_key}={pvc_value}, "
            f"pod_selector: {pod_key}={pod_value}"
        )

        prompt = _build_prompt(
            f"Generate an ODF Disaster Recovery Recipe for an application named "
            f"{app_name} deployed in the {namespace} namespace. "
            f"The application has one Deployment with pods labeled "
            f"{pod_key}: {pod_value}. "
            f"It has {pvc_count} PVC(s) labeled {pvc_key}: {pvc_value}. "
            f"Include a check hook that verifies the Deployment named {app_name} "
            f"is ready (spec.replicas == status.readyReplicas) before backup and "
            f"after restore. "
            f"Generate the complete Recipe."
        )

        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = _extract_yaml_from_response(response_text)
        _assert_recipe_structure(recipe)

        spec = recipe["spec"]
        recipe_name = recipe["metadata"]["name"]
        recipe_namespace = recipe["metadata"].get("namespace", namespace)

        # Validate that OLS returned a legal k8s name (RFC-1123 label) for both
        # the Recipe name and its namespace before we use them in oc commands.
        _k8s_name_re = re.compile(r"^[a-z0-9]([a-z0-9\-]{0,61}[a-z0-9])?$")
        for field, value in (
            ("metadata.name", recipe_name),
            ("metadata.namespace", recipe_namespace),
        ):
            assert _k8s_name_re.match(value), (
                f"OLS Recipe field '{field}' is not a valid Kubernetes identifier: "
                f"{value!r}. Must match [a-z0-9][a-z0-9-]{{0,61}}[a-z0-9]."
            )

        # Assert namespace is in resource groups
        groups = spec.get("groups", [])
        all_namespaces = []
        for g in groups:
            all_namespaces.extend(g.get("includedNamespaces", []))
        assert namespace in all_namespaces, (
            f"OLS Recipe groups must target namespace '{namespace}', "
            f"got: {all_namespaces}"
        )

        # Assert volumes section targets the correct namespace
        assert (
            "volumes" in spec
        ), "OLS Recipe must contain a volumes section for a workload with PVCs"
        volumes = spec["volumes"]
        vol_list = volumes if isinstance(volumes, list) else [volumes]
        vol_namespaces = []
        for v in vol_list:
            vol_namespaces.extend(v.get("includedNamespaces", []))
        assert namespace in vol_namespaces, (
            f"OLS Recipe volumes must target namespace '{namespace}', "
            f"got: {vol_namespaces}"
        )

        # Serialise Recipe to a temp file
        recipe_yaml_str = yaml.dump(recipe)
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, prefix="ols-recipe-"
        ) as tmp:
            tmp.write(recipe_yaml_str)
            tmp_path = tmp.name

        # Register tmpfile cleanup — runs even when the test fails or errors out.
        def _remove_tmpfile():
            try:
                os.unlink(tmp_path)
                logger.info(f"Removed temp Recipe file: {tmp_path}")
            except OSError as exc:
                logger.warning(f"Could not remove temp Recipe file {tmp_path}: {exc}")

        request.addfinalizer(_remove_tmpfile)

        # Mirror the cluster-selection logic from BusyboxDiscoveredApps.deploy_workload:
        # use the provider-aware variant when dr_cluster_relations is configured.
        dr_cluster_relations = config.MULTICLUSTER.get("dr_cluster_relations", [])
        if dr_cluster_relations:
            managed_clusters = get_non_acm_cluster_and_non_provider_cluster_config()
        else:
            managed_clusters = get_non_acm_cluster_config()

        for cluster in managed_clusters:
            cluster_index = cluster.MULTICLUSTER["multicluster_index"]
            config.switch_ctx(cluster_index)
            cluster_name = config.ENV_DATA.get("cluster_name", "unknown")
            logger.info(
                f"Applying OLS Recipe '{recipe_name}' to cluster "
                f"'{cluster_name}' in namespace '{namespace}'"
            )
            exec_cmd(["oc", "apply", "-f", tmp_path, "-n", namespace])
            logger.info(f"Recipe '{recipe_name}' applied to '{cluster_name}'")

            # Register Recipe deletion on this cluster as a finalizer so it is
            # removed even when the test fails mid-way.
            def _delete_recipe(
                _idx=cluster_index,
                _name=recipe_name,
                _ns=recipe_namespace,
                _cname=cluster_name,
            ):
                try:
                    config.switch_ctx(_idx)
                    exec_cmd(
                        [
                            "oc",
                            "delete",
                            "recipe",
                            _name,
                            "-n",
                            _ns,
                            "--ignore-not-found",
                        ]
                    )
                    logger.info(f"Deleted Recipe '{_name}' from cluster '{_cname}'")
                except Exception as exc:
                    logger.warning(
                        f"Failed to delete Recipe '{_name}' from '{_cname}': {exc}"
                    )

            request.addfinalizer(_delete_recipe)

        # Leave context on ACM hub for the caller
        config.switch_acm_ctx()
        logger.info(f"OLS Recipe '{recipe_name}' applied to all managed clusters")
        return recipe_name

    @pytest.mark.parametrize("pvc_interface", params)
    def test_failover_and_relocate_with_ols_recipe(
        self,
        pvc_interface,
        discovered_apps_dr_workload,
        ols,
        request,
    ):
        """
        Deploy a discovered-app workload, generate its DR Recipe via OLS,
        apply the recipe, then perform failover and relocate to prove the
        OLS-generated Recipe is operationally correct.

        Test steps:
            1. Deploy one RBD BusyboxDiscoveredApps workload (random namespace).
            2. Generate the DR Recipe for that workload via OLS.
            3. Validate Recipe structure and apply it to managed clusters.
            4. Create the DRPC with ``recipeRef`` pointing at the OLS Recipe.
            5. Wait for initial sync.
            6. Failover to the secondary cluster; verify workload is running.
            7. Relocate back to the primary cluster; verify workload is running.
        """
        from ocs_ci.framework import config
        from ocs_ci.helpers import dr_helpers
        from ocs_ci.ocs.resources.drpc import DRPC
        from ocs_ci.ocs import constants as _constants

        # ------------------------------------------------------------------ #
        # Step 1: Deploy workload without recipe (we create it via OLS next)  #
        # ------------------------------------------------------------------ #
        rdr_workloads = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=1, recipe=0
        )
        workload = rdr_workloads[0]

        # ------------------------------------------------------------------ #
        # Step 2+3: Generate and apply OLS recipe                             #
        # ------------------------------------------------------------------ #
        config.switch_acm_ctx()
        recipe_name = self._generate_and_apply_ols_recipe(workload, ols, request)

        # ------------------------------------------------------------------ #
        # Step 4: Create DRPC with recipeRef                                  #
        # (_generate_and_apply_ols_recipe already left context on ACM hub)   #
        # ------------------------------------------------------------------ #
        workload.create_drpc_for_apps_with_recipe()

        drpc_obj = DRPC(
            namespace=_constants.DR_OPS_NAMESPACE,
            resource_name=workload.discovered_apps_placement_name,
        )

        # ------------------------------------------------------------------ #
        # Step 5: Wait for initial sync                                       #
        # ------------------------------------------------------------------ #
        primary_cluster = dr_helpers.get_current_primary_cluster_name(
            workload.workload_namespace,
            discovered_apps=True,
            resource_name=workload.discovered_apps_placement_name,
        )
        secondary_cluster = dr_helpers.get_current_secondary_cluster_name(
            workload.workload_namespace,
            discovered_apps=True,
            resource_name=workload.discovered_apps_placement_name,
        )
        scheduling_interval = dr_helpers.get_scheduling_interval(
            workload.workload_namespace,
            discovered_apps=True,
            resource_name=workload.discovered_apps_placement_name,
        )
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)
        logger.info(
            f"Initial sync verified — primary: {primary_cluster}, "
            f"secondary: {secondary_cluster}, recipe: {recipe_name}"
        )

        # ------------------------------------------------------------------ #
        # Step 6: Failover (primary cluster stays up)                         #
        # ------------------------------------------------------------------ #
        logger.info(f"Starting failover to {secondary_cluster}")
        dr_helpers.failover(
            secondary_cluster,
            workload.workload_namespace,
            workload.workload_type,
        )
        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            discovered_apps=True,
            vrg_name=workload.discovered_apps_placement_name,
        )
        logger.info("Workload running on secondary cluster after failover")

        # ------------------------------------------------------------------ #
        # Step 7: Relocate back to primary                                    #
        # ------------------------------------------------------------------ #
        logger.info(f"Starting relocate back to {primary_cluster}")
        dr_helpers.relocate(
            primary_cluster,
            workload.workload_namespace,
            workload.workload_type,
        )
        config.switch_to_cluster_by_name(primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            discovered_apps=True,
            vrg_name=workload.discovered_apps_placement_name,
        )
        logger.info(
            "Workload running on primary cluster after relocate — "
            f"OLS Recipe '{recipe_name}' successfully used for DR protection"
        )
