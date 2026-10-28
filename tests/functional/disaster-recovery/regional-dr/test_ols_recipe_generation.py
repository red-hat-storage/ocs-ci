"""
Tests for ODF DR Recipe generation using OpenShift Lightspeed (OLS)

JIRA: RHSTOR-8222 - DR Recipe creation using OCP Lightspeed

These tests verify that the OLS service, configured with the DR recipe RAG
content image, can generate syntactically correct ODF Disaster Recovery
Recipes from natural-language prompts.

OLS is deployed automatically as part of the RDR deployment flow
(``RDRMultiClusterDROperatorsDeploy.deploy()`` in ``deployment.py``) for
ODF >= 4.23.  These tests assume the service is already running on the hub
cluster when executed.
"""

import logging

import pytest
from semantic_version import Version

from ocs_ci.framework.pytest_customization.marks import rdr, turquoise_squad
from ocs_ci.framework.testlib import acceptance, tier1, skipif_ocs_version

from ocs_ci.helpers.dr_helpers import (
    assert_recipe_structure,
    generate_and_apply_ols_recipe,
)
from ocs_ci.helpers.dr_helpers_ui import (
    verify_pending_cleanup_alert_firing,
    verify_pending_cleanup_alert_resolved,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.acm.acm import AcmAddClusters
from ocs_ci.ocs.openshift_lightspeed import (
    OpenShiftLightspeed,
    build_ols_recipe_prompt,
    extract_yaml_from_ols_response,
    is_ols_available,
)
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility.version import get_semantic_ocs_version_from_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session", autouse=False)
def skip_if_ols_not_available():
    """
    Skip the requesting test/class when OLS is not installed on the hub
    cluster (i.e. the ``lightspeed-app-server`` Route does not exist).

    The check must run against the ACM hub context because OLS is only
    deployed there, not on the managed clusters.

    Use via ``usefixtures`` on a class or request directly in a fixture::

        @pytest.mark.usefixtures("skip_if_ols_not_available")
        class TestSomething: ...
    """
    from ocs_ci.framework import config

    saved_index = config.cur_index
    config.switch_acm_ctx()
    try:
        if not is_ols_available():
            pytest.skip(
                "OpenShift Lightspeed is not installed on this cluster "
                "(route 'lightspeed-app-server' not found in 'openshift-lightspeed'). "
                "Deploy OLS or set skip_ols_deployment=false to enable these tests."
            )
    finally:
        config.switch_ctx(saved_index)


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
    client.ensure_working_model()
    if not client.is_authorized():
        pytest.fail(
            "OLS authorization check failed. "
            "Verify that lightspeed-operator-query-access is bound and OLS pods are running."
        )
    return client


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


@rdr
@tier1
@turquoise_squad
@skipif_ocs_version("<4.23")
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
        prompt = build_ols_recipe_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "web-app deployed in the web namespace. The application consists of "
            "one Deployment labeled app: web-app and does not use any "
            "PersistentVolumeClaims. Generate the complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = extract_yaml_from_ols_response(response_text)
        assert_recipe_structure(recipe)

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
        prompt = build_ols_recipe_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "my-app deployed in the my-app namespace. The application consists "
            "of one Deployment and one PVC. The Deployment is labeled app: "
            "my-app, and the PVC is also labeled app: my-app. Generate the "
            "complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = extract_yaml_from_ols_response(response_text)
        assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert (
            "volumes" in spec
        ), "Recipe must contain a volumes section for an app with PVCs"
        volumes = spec["volumes"]
        if not volumes.get("includedNamespaces"):
            logger.error(
                "OLS omitted volumes.includedNamespaces. Full volumes section: %r",
                volumes,
            )
        assert volumes.get("includedNamespaces"), (
            "Recipe spec.volumes must include 'includedNamespaces'. "
            "OLS omitted the field."
        )
        if "my-app" not in volumes["includedNamespaces"]:
            logger.error(
                "OLS volumes.includedNamespaces does not contain 'my-app': %r",
                volumes["includedNamespaces"],
            )
        assert (
            "my-app" in volumes["includedNamespaces"]
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
        prompt = build_ols_recipe_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "postgres-app deployed in the postgres namespace. The application "
            "consists of one Deployment labeled app: postgres-app and one PVC "
            "labeled app: postgres-app. Before backup, run an exec hook in "
            "container postgres that executes the command "
            "'psql -U postgres -c CHECKPOINT' as a plain string. "
            "The Deployment name is postgres-deployment. Generate the complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = extract_yaml_from_ols_response(response_text)
        assert_recipe_structure(recipe)

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
        prompt = build_ols_recipe_prompt(
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

        recipe = extract_yaml_from_ols_response(response_text)
        assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert "hooks" in spec, "Recipe must contain a hooks section"
        scale_hooks = [h for h in spec["hooks"] if h.get("type") == "scale"]
        assert scale_hooks, "At least one hook must have type 'scale'"

    def test_recipe_check_hook_after_restore(self, ols_client):
        """
        OLS generates a Recipe with a check hook that validates readiness
        after restore, using the correct 'chks' field and slash-notation
        workflow reference.

        Test steps:
            1. Send the prompt for ``orders-app`` with a post-restore check
               that validates Deployment readiness using chks/condition syntax.
            2. Extract and validate the YAML.
            3. Assert a hook with ``type: check`` and a ``chks`` list is present.
            4. Assert the restore workflow references the check hook (with slash
               notation) after the resource group.
        """
        prompt = build_ols_recipe_prompt(
            "Generate an ODF Disaster Recovery Recipe for an application named "
            "orders-app deployed in the orders namespace. The application "
            "consists of one Deployment labeled app: orders-app and one PVC "
            "labeled app: orders-app. After restore, add a check hook named "
            "check-orders-ready of type check. "
            "The check hook must use a 'chks' list (NOT 'ops') with one entry "
            "named check-deployment-ready whose condition is "
            "'{$.spec.replicas} == {$.status.readyReplicas}'. "
            "In the restore workflow sequence, reference the check hook using "
            "slash notation: hook: check-orders-ready/check-deployment-ready. "
            "Generate the complete Recipe."
        )
        result = ols_client.query(prompt)
        response_text = result.get("response", "")
        logger.info(f"OLS response:\n{response_text}")

        recipe = extract_yaml_from_ols_response(response_text)
        assert_recipe_structure(recipe)

        spec = recipe["spec"]
        assert "hooks" in spec, "Recipe must contain a hooks section"
        check_hooks = [h for h in spec["hooks"] if h.get("type") == "check"]
        assert check_hooks, "At least one hook must have type 'check'"

        # check hooks must use 'chks', not 'ops'
        for ch in check_hooks:
            assert "chks" in ch, (
                f"Check hook '{ch.get('name')}' must use 'chks' key, "
                f"got keys: {list(ch.keys())}"
            )

        # The restore workflow sequence must reference the check hook last
        restore_workflow = next(w for w in spec["workflows"] if w["name"] == "restore")
        sequence = restore_workflow.get("sequence", [])
        assert sequence, "Restore workflow must have a non-empty sequence"
        last_step = sequence[-1]
        assert (
            "hook" in last_step
        ), "Last step of restore workflow must be a hook (post-restore check)"

        # The hook reference must use slash notation (hook-name/chk-name)
        hook_ref = last_step["hook"]
        assert "/" in str(hook_ref), (
            f"Restore workflow hook reference must use slash notation "
            f"'<hook-name>/<chk-name>', got: {hook_ref!r}"
        )

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
        prompt_1 = build_ols_recipe_prompt(
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
        logger.info(f"OLS query (turn 2):\n{prompt_2}")
        result_2 = ols_client.query(prompt_2, conversation_id=conversation_id)
        response_text = result_2.get("response", "")
        logger.info(f"Turn 2 OLS response:\n{response_text}")

        recipe = extract_yaml_from_ols_response(response_text)
        assert_recipe_structure(recipe)
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
        attachments = [
            {
                "attachment_type": "configuration",
                "content_type": "application/yaml",
                "content": deployment_yaml,
            }
        ]

        prompt = build_ols_recipe_prompt(
            "Using the attached Deployment manifest, generate a complete ODF "
            "Disaster Recovery Recipe for this application. The app does not use PVCs."
        )
        result = ols_client.query(prompt, attachments=attachments)
        response_text = result.get("response", "")
        logger.info(f"OLS response with attachment:\n{response_text}")

        recipe = extract_yaml_from_ols_response(response_text)
        assert_recipe_structure(recipe)

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
@skipif_ocs_version("<4.23")
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
            False,
            constants.CEPHBLOCKPOOL,
            marks=[pytest.mark.polarion_id("OCS-8231"), acceptance],
            id="primary_up-rbd",
        ),
        pytest.param(
            True,
            constants.CEPHBLOCKPOOL,
            marks=[pytest.mark.polarion_id("OCS-8232"), acceptance],
            id="primary_down-rbd",
        ),
        pytest.param(
            False,
            constants.CEPHFILESYSTEM,
            marks=[pytest.mark.polarion_id("OCS-8233"), acceptance],
            id="primary_up-cephfs",
        ),
        pytest.param(
            True,
            constants.CEPHFILESYSTEM,
            marks=[pytest.mark.polarion_id("OCS-8234"), acceptance],
            id="primary_down-cephfs",
        ),
    ]

    @pytest.fixture(scope="class")
    def ols(self, ols_client):
        """
        Authenticated OLS client (hub cluster) — delegates to the module-level
        ``ols_client`` fixture which handles context switching, authorization
        checks, and teardown.
        """
        return ols_client

    def _generate_and_apply_ols_recipe(self, workload, ols_client):
        """
        Delegate to :func:`~ocs_ci.helpers.dr_helpers.generate_and_apply_ols_recipe`.

        Ask OLS to generate a DR Recipe for the given workload, validate the
        returned YAML, apply it to both managed clusters, and return the recipe name.

        Args:
            workload: A deployed workload instance (namespace and labels known).
            ols_client: An authenticated
                :class:`~ocs_ci.ocs.openshift_lightspeed.OpenShiftLightspeed` instance.

        Returns:
            str: Name of the applied Recipe.
        """
        return generate_and_apply_ols_recipe(workload, ols_client)

    @pytest.mark.parametrize("primary_cluster_down, pvc_interface", params)
    def test_failover_and_relocate_with_ols_recipe(
        self,
        primary_cluster_down,
        pvc_interface,
        discovered_apps_dr_workload,
        nodes_multicluster,
        node_restart_teardown,
        ols,
        setup_acm_ui,
        request,
    ):
        """
        Deploy a discovered-app workload, generate its DR Recipe via OLS,
        apply the recipe, then perform failover and relocate to prove the
        OLS-generated Recipe is operationally correct.

        Parametrized over PVC interface (RBD / CephFS) and whether the
        primary cluster is powered off before failover (power-off / power-on).

        Test steps:
            1. Deploy workload (random namespace).
            2. Generate the DR Recipe for that workload via OLS.
            3. Validate Recipe structure and apply it to managed clusters.
            4. Create the DRPC with ``recipeRef`` pointing at the OLS Recipe.
            5. Wait for initial sync.
            6. Optionally power off the primary cluster nodes.
            7. Failover to the secondary cluster; run discovered-apps cleanup
               on old primary; verify workload is running.
            8. Optionally power the primary cluster back on.
            9. Relocate back to the primary cluster; verify workload is running.
        """
        from time import sleep

        from ocs_ci.framework import config
        from ocs_ci.helpers import dr_helpers
        from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
        from ocs_ci.ocs.resources.drpc import DRPC
        from ocs_ci.ocs import constants as _constants

        # ------------------------------------------------------------------ #
        # Step 1: Deploy workload for OLS recipe protection (no DRPC yet)    #
        # ------------------------------------------------------------------ #
        rdr_workloads = discovered_apps_dr_workload(
            pvc_interface=pvc_interface, kubeobject=0, ols_recipe=1
        )
        workload = rdr_workloads[0]

        # ------------------------------------------------------------------ #
        # Step 2+3: Generate and apply OLS recipe                             #
        # Skip if OLS LLM backend is unavailable (transient 502) rather than #
        # failing — the workload cleanup finalizer still runs normally.       #
        # ------------------------------------------------------------------ #
        import requests as _requests

        config.switch_acm_ctx()
        try:
            recipe_name = self._generate_and_apply_ols_recipe(workload, ols)
        except _requests.HTTPError as exc:
            pytest.skip(f"OLS LLM backend unavailable, skipping test: {exc}")

        # ------------------------------------------------------------------ #
        # Step 4: Create recipe-based DRPC (first and only DRPC for this     #
        # workload — no label-selector DRPC was created in step 1)           #
        # ------------------------------------------------------------------ #
        workload.create_drpc_for_apps_with_recipe(recipe_name=recipe_name)

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
        # Step 6: Optionally power off primary cluster before failover        #
        # ------------------------------------------------------------------ #
        config.switch_to_cluster_by_name(primary_cluster)
        primary_cluster_index = config.cur_index
        primary_cluster_nodes = get_node_objs()

        wait_time = 2 * scheduling_interval  # minutes
        logger.info(f"Waiting {wait_time}m before failover")
        sleep(wait_time * 60)

        if primary_cluster_down:
            config.switch_to_cluster_by_name(primary_cluster)
            logger.info(f"Stopping nodes of primary cluster: {primary_cluster}")
            nodes_multicluster[primary_cluster_index].stop_nodes(primary_cluster_nodes)

        # ------------------------------------------------------------------ #
        # Step 7: Failover to secondary cluster                               #
        # ------------------------------------------------------------------ #
        logger.info(f"Starting failover to {secondary_cluster}")
        dr_helpers.failover(
            failover_cluster=secondary_cluster,
            namespace=workload.workload_namespace,
            workload_type=workload.workload_type,
            workload_placement_name=workload.discovered_apps_placement_name,
            old_primary=primary_cluster,
            # Skip odf-cli validation inside failover() — workload pods are not
            # yet up at that point; we validate below after resources are ready.
            skip_odf_cli_validation=True,
            discovered_apps=True,
        )
        # Verify ApplicationCleanupPending alert fire/resolve around cleanup (OCS 4.22+)
        if get_semantic_ocs_version_from_config() >= Version("4.22", partial=True):
            wait_time_for_alert = (
                constants.ALERT_APPLICATION_CLEANUP_PENDING_THRESHOLD + 180
            )
            logger.info(
                f"Waiting {wait_time_for_alert}s for ApplicationCleanupPending "
                "alert to fire after failover"
            )
            sleep(wait_time_for_alert)
            acm_obj = AcmAddClusters()
            verify_pending_cleanup_alert_firing(
                acm_obj,
                "Failover",
                drpc_name=workload.discovered_apps_placement_name,
            )
            logger.info("Doing cleanup operations after failover")
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=workload.discovered_apps_placement_name,
                old_primary=primary_cluster,
                workload_namespace=workload.workload_namespace,
                workload_dir=workload.workload_dir,
                vrg_name=workload.discovered_apps_placement_name,
            )
            verify_pending_cleanup_alert_resolved(
                acm_obj,
                "Failover",
                drpc_name=workload.discovered_apps_placement_name,
            )
        else:
            logger.info("Doing cleanup operations after failover")
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=workload.discovered_apps_placement_name,
                old_primary=primary_cluster,
                workload_namespace=workload.workload_namespace,
                workload_dir=workload.workload_dir,
                vrg_name=workload.discovered_apps_placement_name,
            )

        config.switch_to_cluster_by_name(secondary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=workload.discovered_apps_placement_name,
        )
        logger.info("Workload running on secondary cluster after failover")

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify deletion of ReplicationDestination resources on the old
            # secondary cluster (now acting as secondary after failover)
            config.switch_to_cluster_by_name(secondary_cluster)
            dr_helpers.wait_for_replication_destinations_deletion(
                workload.workload_namespace
            )
            # Verify creation of ReplicationDestination resources on the new
            # secondary cluster (old primary)
            config.switch_to_cluster_by_name(primary_cluster)
            dr_helpers.wait_for_replication_destinations_creation(
                workload.workload_pvc_count, workload.workload_namespace
            )

        # ------------------------------------------------------------------ #
        # Step 8: Power primary cluster back on if it was stopped             #
        # ------------------------------------------------------------------ #
        if primary_cluster_down:
            logger.info(
                f"Waiting {wait_time}m before starting nodes of primary cluster"
            )
            sleep(wait_time * 60)
            nodes_multicluster[primary_cluster_index].start_nodes(primary_cluster_nodes)
            wait_for_nodes_status([node.name for node in primary_cluster_nodes])
            logger.info("Wait for all pods in openshift-storage to be in running state")
            assert wait_for_pods_to_be_running(
                timeout=720
            ), "Not all the pods reached running state"
            logger.info("Checking for Ceph Health OK")
            ceph_health_check()

        logger.info(f"Waiting {wait_time}m before relocate")
        sleep(wait_time * 60)

        logger.info("Checking for lastKubeObjectProtectionTime after failover")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, workload.kubeobject_capture_interval_int
        )

        logger.info("Checking for lastGroupSyncTime after failover")
        dr_helpers.verify_last_group_sync_time(drpc_obj, scheduling_interval)

        # ------------------------------------------------------------------ #
        # Step 9: Relocate back to primary                                    #
        # ------------------------------------------------------------------ #
        logger.info(f"Starting relocate back to {primary_cluster}")
        dr_helpers.relocate(
            preferred_cluster=primary_cluster,
            namespace=workload.workload_namespace,
            workload_type=workload.workload_type,
            workload_placement_name=workload.discovered_apps_placement_name,
            discovered_apps=True,
            old_primary=secondary_cluster,
            workload_instance=workload,
            # Skip odf-cli validation inside relocate() — workload pods are not
            # yet up at that point; we validate below after resources are ready.
            skip_odf_cli_validation=True,
        )

        # Verify ApplicationCleanupPending alert fire/resolve around cleanup (OCS 4.22+)
        if get_semantic_ocs_version_from_config() >= Version("4.22", partial=True):
            wait_time_for_alert = (
                constants.ALERT_APPLICATION_CLEANUP_PENDING_THRESHOLD + 180
            )
            logger.info(
                f"Waiting {wait_time_for_alert}s for ApplicationCleanupPending "
                "alert to fire after relocate"
            )
            sleep(wait_time_for_alert)
            # Create a fresh ACM UI session as the previous may have expired
            acm_obj = AcmAddClusters()
            verify_pending_cleanup_alert_firing(
                acm_obj,
                "Relocate",
                drpc_name=workload.discovered_apps_placement_name,
            )
            logger.info("Doing cleanup operations after relocate")
            dr_helpers.do_discovered_apps_cleanup(
                drpc_name=workload.discovered_apps_placement_name,
                old_primary=secondary_cluster,
                workload_namespace=workload.workload_namespace,
                workload_dir=workload.workload_dir,
                vrg_name=workload.discovered_apps_placement_name,
            )
            verify_pending_cleanup_alert_resolved(
                acm_obj,
                "Relocate",
                drpc_name=workload.discovered_apps_placement_name,
            )

        config.switch_to_cluster_by_name(primary_cluster)
        dr_helpers.wait_for_all_resources_creation(
            workload.workload_pvc_count,
            workload.workload_pod_count,
            workload.workload_namespace,
            timeout=1200,
            discovered_apps=True,
            vrg_name=workload.discovered_apps_placement_name,
        )

        if pvc_interface == constants.CEPHFILESYSTEM:
            # Verify deletion of ReplicationDestination resources on the old
            # secondary cluster (primary before failover, now acting as secondary
            # after relocate back)
            config.switch_to_cluster_by_name(primary_cluster)
            dr_helpers.wait_for_replication_destinations_deletion(
                workload.workload_namespace
            )
            # Verify creation of ReplicationDestination resources on the current
            # secondary cluster
            config.switch_to_cluster_by_name(secondary_cluster)
            dr_helpers.wait_for_replication_destinations_creation(
                workload.workload_pvc_count, workload.workload_namespace
            )

        logger.info("Checking for lastKubeObjectProtectionTime after relocate")
        dr_helpers.verify_last_kubeobject_protection_time(
            drpc_obj, workload.kubeobject_capture_interval_int
        )

        logger.info(
            "Workload running on primary cluster after relocate — "
            f"OLS Recipe '{recipe_name}' successfully used for DR protection"
        )
