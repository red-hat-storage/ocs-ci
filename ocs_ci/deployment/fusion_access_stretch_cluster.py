"""
Deployment helper for RHSTOR-8877 — Fusion Access HA (Stretch Cluster).

Orchestrates the following sequence for a 2 AZ + arbiter stretched OCP cluster.
The base OCP cluster is already installed and zone-labelled by the provisioner;
this module picks up from there:

    1. Apply topology zone labels to master and worker nodes (idempotent).
    2. Identify the arbiter master node.
    3. Create LUNs/disks on *all* nodes, masters included, via the iSCSI server.
    4. Deploy IBM Fusion components.
    5. Install the standalone FDF service stack only — storage is skipped.
    6. Install the KMM operator.
    7. Create the ``ibm-entitlement-key`` secret in ``ibm-spectrum-scale``.
    8. Configure the image registry and connect the external registry to SAN.

Step 8 exercises the Connect External Storage → Storage for SAN wizard to
confirm:
    - Default node-selection state (workers only, disk-node role).
    - Include Control Plane Nodes toggle behaviour.
    - Stretch Cluster toggle behaviour (auto-check, role assignment, arbiter lock).
    - Non-arbiter role changes.
    - LUN discovery (at least one LUN visible).
    - LUN group creation and filesystem health in ibm-spectrum-scale namespace.
"""

import base64
import logging

from ocs_ci.deployment.fdf_standalone import StandaloneFDFCatalogSource
from ocs_ci.deployment.fusion import FusionDeployment
from ocs_ci.deployment.image_registry import ImageRegistryConfigurator
from ocs_ci.deployment.kmm import KMMInstaller
from ocs_ci.deployment.zones import are_zone_labels_present, create_dummy_zone_labels
from ocs_ci.framework import config
from ocs_ci.helpers.helpers import (
    create_custom_secret_for_cnsa_rm,
    create_ibm_entitlement_secret,
    create_unique_resource_name,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_master_nodes, get_worker_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.ocs.ui.base_ui import close_browser, login_ui
from ocs_ci.ocs.ui.page_objects.stretch_cluster_san_ui import (
    ROLE_ARBITER_NODE,
    ROLE_CLUSTER_NODE,
    ROLE_DISK_NODE,
    StretchClusterSANUI,
)
from ocs_ci.utility.iscsi_config import iscsi_setup

logger = logging.getLogger(__name__)

# Zone identifiers matching the deployment config
ARBITER_ZONE = "arbiter"
DATA_ZONE_1 = "data-1"
DATA_ZONE_2 = "data-2"

# Prefix for LUN group names created during deployment validation
_LUN_GROUP_NAME_PREFIX = "lun-group-ha"


def apply_stretch_cluster_zone_labels():
    """
    Label all cluster nodes with the correct topology zone.

    Masters receive zone labels in the order defined by
    ``ENV_DATA.master_availability_zones`` (arbiter, data-1, data-2).
    Workers receive zone labels in the order defined by
    ``ENV_DATA.worker_availability_zones`` (data-1, data-2 interleaved).

    When ``DEPLOYMENT.dummy_zone_node_labels`` is True the generic
    :func:`~ocs_ci.deployment.zones.create_dummy_zone_labels` helper handles
    the labelling.  Otherwise nodes are expected to carry cloud-provider zone
    labels already and this step is a no-op.

    Raises:
        AssertionError: If the zone labels are still missing after labelling.
    """
    if not config.DEPLOYMENT.get("dummy_zone_node_labels", False):
        logger.info(
            "dummy_zone_node_labels is not enabled; "
            "assuming cloud-provider zone labels are already present"
        )
        return

    if are_zone_labels_present():
        logger.info("Zone labels already present on all nodes — skipping labelling")
        return

    logger.info(
        "Applying topology zone labels to all cluster nodes "
        "(masters: arbiter/data-1/data-2, workers: data-1/data-2)"
    )
    create_dummy_zone_labels()

    assert (
        are_zone_labels_present()
    ), "Zone labels are still missing on at least one node after labelling"
    logger.info("Zone labels successfully applied to all cluster nodes")


def get_arbiter_node_name():
    """
    Verify and return the arbiter master node name.

    Confirms that exactly one master carries
    ``topology.kubernetes.io/zone=arbiter``.

    Returns:
        str: Name of the arbiter master node.

    Raises:
        AssertionError: If no master node carries the arbiter zone label.
    """
    node_ocp = OCP(kind="node")
    selector = f"{constants.ZONE_LABEL}={ARBITER_ZONE}"
    arbiter_nodes = node_ocp.get(selector=selector).get("items", [])
    arbiter_master_names = [
        n["metadata"]["name"]
        for n in arbiter_nodes
        if "node-role.kubernetes.io/master" in n["metadata"].get("labels", {})
    ]
    assert arbiter_master_names, (
        f"No master node carries the zone label "
        f"'{constants.ZONE_LABEL}={ARBITER_ZONE}'"
    )
    arbiter_name = arbiter_master_names[0]
    logger.info(f"Arbiter master node: {arbiter_name}")
    return arbiter_name


def setup_iscsi_luns_on_all_nodes():
    """
    Create and attach the SAN LUNs on every cluster node, masters included.

    Delegates to :func:`~ocs_ci.utility.iscsi_config.iscsi_setup`, which
    configures the iSCSI target ACLs, logs the initiators in and validates the
    resulting sessions and block devices.  ``iscsi_include_masters`` is forced
    on so that ``get_worker_node_names()`` returns the control-plane nodes too
    — on a stretch cluster the arbiter master must also see the LUNs.

    The ``san_iscsi_*`` keys used by the Fusion Access SAN tests are accepted as
    aliases for the ``iscsi_target_*`` keys ``iscsi_setup`` reads.
    """
    for san_key, iscsi_key in (
        ("san_iscsi_ip", "iscsi_target_ip"),
        ("san_iscsi_iqn", "iscsi_target_iqn"),
    ):
        if iscsi_key not in config.ENV_DATA and san_key in config.ENV_DATA:
            config.ENV_DATA[iscsi_key] = config.ENV_DATA[san_key]

    config.ENV_DATA["iscsi_include_masters"] = True
    logger.info("Creating iSCSI LUNs on all cluster nodes (masters + workers)")
    iscsi_setup()
    logger.info("iSCSI LUNs are available on all cluster nodes")


def deploy_fusion():
    """
    Install the IBM Fusion operator and create the SpectrumFusion CR.

    Delegates to :class:`~ocs_ci.deployment.fusion.FusionDeployment` which
    handles catalog source creation, namespace, subscription, and CR creation.
    """
    logger.info("Deploying IBM Fusion operator")
    FusionDeployment().deploy()
    logger.info("IBM Fusion operator deployed successfully")


def deploy_fdf_service_stack():
    """
    Install the standalone FDF (Fusion Data Foundation) service stack only.

    Creates the IBM-owned ``ibm-operators`` CatalogSource so the FDF operator
    is installable.  **No StorageCluster is created** — on a Fusion Access HA
    cluster the storage comes from the SAN LUNs connected later through the
    Fusion Access wizard, so the ODF storage deployment is deliberately
    skipped.

    Raises:
        AssertionError: If FDF standalone deployment is not enabled.
    """
    assert config.DEPLOYMENT.get(
        "fdf_standalone_deployment", False
    ), "fdf_standalone_deployment must be True for Fusion Access HA deployment"
    logger.info("Creating standalone FDF CatalogSource (ibm-operators)")
    StandaloneFDFCatalogSource().create_catalog_source()
    logger.info(
        "Standalone FDF CatalogSource created — skipping StorageCluster creation; "
        "storage is provided by the SAN LUNs"
    )


def configure_kmm():
    """
    Install the KMM (Kernel Module Management) operator.

    Delegates to :meth:`~ocs_ci.deployment.kmm.KMMInstaller.deploy_kmm_operator`,
    which creates the namespace and subscription and then runs the post-install
    verification (pods Running, deployments Available).  Passing both check
    flags keeps the step idempotent across re-runs.

    Secure Boot validation is bypassed when ``DEPLOYMENT.kmm_secure_boot``
    is False (the default for the initial TP testing phase).
    """
    logger.info("Installing KMM operator")
    KMMInstaller().deploy_kmm_operator(check_kmm_deployed=True, check_kmm_ready=True)
    logger.info("KMM operator installed successfully")

    if not config.DEPLOYMENT.get("kmm_secure_boot", False):
        logger.info(
            "kmm_secure_boot is False — Secure Boot validation bypassed "
            "(expected for initial TP phase)"
        )


def configure_image_registry():
    """
    Configure the image registry for the Fusion Access HA environment.

    Defaults to external registry.  When ``DEPLOYMENT.use_internal_registry``
    is True, the internal OCP registry is configured with a PVC storage backend.
    """
    if config.DEPLOYMENT.get("use_internal_registry", False):
        logger.info("Configuring internal image registry with PVC storage")
        ImageRegistryConfigurator().configure_image_registry_with_pvc()
        logger.info("Internal image registry configured with PVC storage successfully")
    else:
        logger.info(
            "use_internal_registry is False — "
            "external (Quay) image registry will be used; "
            "no internal registry setup required at this stage"
        )


def deploy_fusion_access_operator():
    """
    Install the Fusion Access Operator and create the FusionAccess CR.

    Delegates to :class:`~ocs_ci.deployment.fusion_access.FusionAccessOperator`
    which handles the certified-operators Subscription, CSV wait, and CR creation.
    """
    # Imported lazily: ocs_ci.utility.operators, which fusion_access pulls in,
    # cannot be imported before ocs_ci.helpers.helpers without tripping a
    # pre-existing circular import. Keeping it local lets this module (and the
    # fusion_access_ha conftest that imports it) load in any order.
    from ocs_ci.deployment.fusion_access import FusionAccessOperator

    logger.info("Deploying Fusion Access Operator for SAN")
    FusionAccessOperator().deploy()
    logger.info("Fusion Access Operator deployed and FusionAccess CR is Ready")


def _read_node_roles(san_ui):
    """
    Read the node-selection table and return the role of every visible node.

    Args:
        san_ui (StretchClusterSANUI): UI page-object positioned at the
            node-selection page.

    Returns:
        dict: Mapping of node name to the role shown in the UI.
    """
    rows = san_ui.get_visible_node_rows()
    return {
        san_ui.get_node_name_in_row(i): san_ui.get_node_role_in_row(i)
        for i in range(1, len(rows) + 1)
    }


def _assert_node_roles(role_by_name, nodes, expected_role):
    """
    Assert that every node in *nodes* is present in the table with the role
    *expected_role*.

    A node that is missing from the table is a failure in its own right — the
    role check must not be skipped for it, or the validation would pass
    vacuously.

    Args:
        role_by_name (dict): Mapping of node name to the role shown in the UI.
        nodes (list[str]): Node names that must be present.
        expected_role (str): Role each of those nodes must carry.

    Raises:
        AssertionError: If a node is missing or carries a different role.
    """
    missing = [n for n in nodes if n not in role_by_name]
    assert not missing, (
        f"Deployment validation FAIL: node(s) {missing} are absent from the "
        f"node table; visible nodes: {sorted(role_by_name)}"
    )
    wrong = {n: role_by_name[n] for n in nodes if role_by_name[n] != expected_role}
    assert not wrong, (
        f"Deployment validation FAIL: expected role '{expected_role}' for "
        f"{list(wrong)}, got {wrong}"
    )


def _validate_node_selection_default_state(san_ui):
    """
    Verify the default state of the node-selection wizard page.

    Confirms both toggles are unchecked and only worker nodes appear in the
    table with role ``disk-node``.

    Args:
        san_ui (StretchClusterSANUI): UI page-object instance already
            positioned at the node-selection page.

    Raises:
        AssertionError: If the page is not in the expected default state.
    """
    assert not san_ui.is_include_control_plane_enabled(), (
        "Deployment validation FAIL: "
        "'Include control plane nodes' toggle should be OFF by default"
    )
    assert not san_ui.is_stretch_cluster_enabled(), (
        "Deployment validation FAIL: "
        "'Stretch cluster' toggle should be OFF by default"
    )

    visible_roles = san_ui.get_all_visible_roles()
    assert visible_roles, (
        "Deployment validation FAIL: "
        "Node table must not be empty when neither toggle is active"
    )
    non_disk = [r for r in visible_roles if r != ROLE_DISK_NODE]
    assert not non_disk, (
        f"Deployment validation FAIL: "
        f"Expected all nodes to be '{ROLE_DISK_NODE}' by default, "
        f"but found: {non_disk}"
    )
    logger.info(
        f"Default state verified — {len(visible_roles)} worker node(s) "
        f"visible, all with role '{ROLE_DISK_NODE}'"
    )


def _validate_include_control_plane_toggle(
    san_ui, all_master_nodes, all_worker_nodes, arbiter_node
):
    """
    Enable the "Include control plane nodes" toggle and verify the resulting
    node table state.

    Confirms:
    - All master nodes appear with role ``cluster-node``.
    - All worker nodes remain with role ``disk-node``.
    - A non-arbiter master's role dropdown offers Cluster node and Disk node.

    Args:
        san_ui (StretchClusterSANUI): UI page-object instance.
        all_master_nodes (list[str]): All master node names.
        all_worker_nodes (list[str]): All worker node names.
        arbiter_node (str): Name of the arbiter master node.

    Raises:
        AssertionError: On any validation failure.
    """
    logger.info("Enabling 'Include control plane nodes' toggle")
    san_ui.enable_include_control_plane()

    rows = san_ui.get_visible_node_rows()
    expected_min = len(all_master_nodes) + len(all_worker_nodes)
    assert len(rows) >= expected_min, (
        f"Deployment validation FAIL: "
        f"Expected at least {expected_min} nodes after enabling "
        f"control-plane toggle; found {len(rows)}"
    )

    role_by_name = _read_node_roles(san_ui)

    _assert_node_roles(role_by_name, all_master_nodes, ROLE_CLUSTER_NODE)
    _assert_node_roles(role_by_name, all_worker_nodes, ROLE_DISK_NODE)

    non_arbiter_masters = [m for m in all_master_nodes if m != arbiter_node]
    if non_arbiter_masters:
        options = san_ui.get_role_dropdown_options_for_node(non_arbiter_masters[0])
        option_texts_lower = [o.lower() for o in options]
        assert any("cluster" in t for t in option_texts_lower), (
            f"Deployment validation FAIL: "
            f"'Cluster node' missing from role dropdown; got {options}"
        )
        assert any("disk" in t for t in option_texts_lower), (
            f"Deployment validation FAIL: "
            f"'Disk node' missing from role dropdown; got {options}"
        )
    logger.info("Include control plane nodes toggle verified")


def _validate_stretch_cluster_toggle(
    san_ui, all_master_nodes, all_worker_nodes, arbiter_node
):
    """
    Enable the "Stretch cluster" toggle and verify the resulting page state.

    Confirms:
    - "Include control plane nodes" is auto-checked and disabled.
    - Arbiter node has role ``arbiter-node`` with its dropdown disabled.
    - Other masters have role ``cluster-node``; workers have ``disk-node``.

    Args:
        san_ui (StretchClusterSANUI): UI page-object instance.
        all_master_nodes (list[str]): All master node names.
        all_worker_nodes (list[str]): All worker node names.
        arbiter_node (str): Name of the arbiter master node.

    Raises:
        AssertionError: On any validation failure.
    """
    logger.info("Enabling 'Stretch cluster' toggle")
    san_ui.enable_stretch_cluster()

    assert san_ui.is_include_control_plane_enabled(), (
        "Deployment validation FAIL: "
        "'Include control plane nodes' must be auto-checked when Stretch is ON"
    )
    assert san_ui.is_include_control_plane_disabled(), (
        "Deployment validation FAIL: "
        "'Include control plane nodes' must be disabled when Stretch is ON"
    )

    role_by_name = _read_node_roles(san_ui)

    _assert_node_roles(role_by_name, [arbiter_node], ROLE_ARBITER_NODE)
    assert san_ui.is_role_dropdown_disabled_for_node(arbiter_node), (
        f"Deployment validation FAIL: "
        f"Role dropdown must be disabled for arbiter node '{arbiter_node}'"
    )
    _assert_node_roles(
        role_by_name,
        [m for m in all_master_nodes if m != arbiter_node],
        ROLE_CLUSTER_NODE,
    )
    _assert_node_roles(role_by_name, all_worker_nodes, ROLE_DISK_NODE)
    logger.info("Stretch cluster toggle verified")


def _validate_non_arbiter_role_changes(
    san_ui, all_master_nodes, arbiter_node, data_zone_1_workers
):
    """
    Verify that role changes are allowed for non-arbiter nodes and locked for
    the arbiter node (Stretch cluster toggle already enabled).

    Confirms:
    - A worker can be changed to ``cluster-node``.
    - A non-arbiter master can be changed to ``disk-node``.
    - The arbiter dropdown remains disabled throughout.

    Args:
        san_ui (StretchClusterSANUI): UI page-object instance (Stretch ON).
        all_master_nodes (list[str]): All master node names.
        arbiter_node (str): Name of the arbiter master node.
        data_zone_1_workers (list[str]): Worker nodes in data zone 1.

    Raises:
        AssertionError: On any validation failure.
    """
    logger.info("Verifying non-arbiter node role changes")

    if data_zone_1_workers:
        test_worker = data_zone_1_workers[0]
        san_ui.change_node_role(test_worker, ROLE_CLUSTER_NODE)
        _assert_node_roles(_read_node_roles(san_ui), [test_worker], ROLE_CLUSTER_NODE)

    non_arbiter_masters = [m for m in all_master_nodes if m != arbiter_node]
    if non_arbiter_masters:
        test_master = non_arbiter_masters[0]
        san_ui.change_node_role(test_master, ROLE_DISK_NODE)
        _assert_node_roles(_read_node_roles(san_ui), [test_master], ROLE_DISK_NODE)

    assert san_ui.is_role_dropdown_disabled_for_node(arbiter_node), (
        f"Deployment validation FAIL: "
        f"Arbiter '{arbiter_node}' role dropdown must remain disabled"
    )
    logger.info("Non-arbiter role changes verified")


def _validate_lun_discovery(san_ui):
    """
    Proceed to the LUN discovery page and confirm at least one LUN is visible.

    LUNs connected to all disk nodes must appear; LUNs not connected to all
    disk nodes are excluded by the Fusion Access UI.

    Args:
        san_ui (StretchClusterSANUI): UI page-object instance with Stretch
            cluster toggle already enabled, positioned at the node-selection page.

    Raises:
        AssertionError: If the LUN discovery table is empty.
    """
    logger.info("Proceeding to LUN discovery page")
    san_ui.click_next_button()

    wwids = san_ui.get_lun_wwids()
    assert wwids, (
        "Deployment validation FAIL: "
        "LUN discovery table is empty; expected LUNs connected to all disk nodes "
        "to be displayed after enabling Stretch cluster"
    )
    logger.info(f"LUN discovery verified — {len(wwids)} LUN(s) found: {wwids}")
    return wwids


def _create_quay_registry_secret():
    """
    Create the Quay docker-registry secret in the ``ibm-spectrum-scale``
    namespace that the Fusion Access UI wizard uses to pull images from the
    external (Quay) registry.

    Required keys in ``config.ENV_DATA``:
        - ``san_quay_server``   — e.g. ``"quay.io/org"``
        - ``san_quay_username`` — Quay username
        - ``san_quay_password`` — Quay password / robot-account token
        - ``san_quay_email``    — email address

    Raises:
        ValueError: If any required config key is missing.
    """
    quay_server = config.ENV_DATA.get("san_quay_server")
    quay_username = config.ENV_DATA.get("san_quay_username")
    quay_password = config.ENV_DATA.get("san_quay_password")
    quay_email = config.ENV_DATA.get("san_quay_email")

    missing = [
        k
        for k, v in {
            "san_quay_server": quay_server,
            "san_quay_username": quay_username,
            "san_quay_password": quay_password,
            "san_quay_email": quay_email,
        }.items()
        if not v
    ]
    if missing:
        raise ValueError(
            f"Missing required config keys for Quay registry secret: {missing}. "
            "Set them under ENV_DATA in your cluster config."
        )

    secret_name = constants.IBM_QUAYIO_SECRET_NAME
    namespace = constants.IBM_STORAGE_SCALE_NAMESPACE
    logger.info(
        f"Creating Quay docker-registry secret '{secret_name}' "
        f"in namespace '{namespace}' for external registry access"
    )
    auth_b64 = base64.b64encode(f"{quay_username}:{quay_password}".encode()).decode()
    docker_config = {
        "auths": {
            quay_server: {
                "username": quay_username,
                "password": quay_password,  # pragma: allowlist secret
                "email": quay_email,
                "auth": auth_b64,
            }
        }
    }
    create_custom_secret_for_cnsa_rm(
        name=secret_name,
        namespace=namespace,
        data_dict={".dockerconfigjson": docker_config},
        secret_type="kubernetes.io/dockerconfigjson",
    )
    logger.info(f"Quay registry secret '{secret_name}' created successfully")


def _validate_lun_group_and_filesystem_health():
    """
    Create a LUN group via the UI wizard, then verify:
    - The LUN group reaches the Healthy/Connected state.
    - All pods in the ``ibm-spectrum-scale`` namespace are running.

    When ``DEPLOYMENT.use_internal_registry`` is True the wizard uses the
    internal registry; no Quay secret is created and no secret is selected in
    the UI.  When False (the default) a Quay docker-registry secret is created
    pre-step and selected in the wizard.

    Raises:
        AssertionError: If the LUN group is not healthy or pods are not running.
        ValueError: If required external registry config keys are missing.
    """
    use_internal = config.DEPLOYMENT.get("use_internal_registry", False)

    if not use_internal:
        # Pre-step: create the Quay docker-registry secret
        _create_quay_registry_secret()

    lun_group_name = create_unique_resource_name(_LUN_GROUP_NAME_PREFIX, "test")
    registry_path = "internal" if use_internal else "external"
    logger.info(
        f"Creating LUN group '{lun_group_name}' via Connect External Storage wizard "
        f"({registry_path} registry path)"
    )

    san_ui = StretchClusterSANUI()

    # Navigate to Storage > External systems → Connect → SAN → Next
    san_ui.navigate_to_connect_external_storage_san()

    if not use_internal:
        # External registry: enter URL + repo + select secret
        image_registry_url = config.ENV_DATA.get("san_image_registry_url", "quay.io")
        logger.info(f"Entering image registry URL: {image_registry_url}")
        san_ui.enter_image_registry_url(image_registry_url)

        image_repository_name = config.ENV_DATA.get("san_image_repository_name")
        logger.info(f"Entering image repository name: {image_repository_name}")
        san_ui.enter_image_repository_name(image_repository_name)

        # Select the Quay secret from the dropdown
        logger.info("Selecting registry secret key from dropdown")
        san_ui.select_secret_key()
    else:
        # Internal registry: UI pre-populates from the cluster's internal registry;
        # enter only the image repository name — no secret selection required.
        image_repository_name = config.ENV_DATA.get("san_image_repository_name")
        logger.info(
            f"Internal registry path: entering image repository name only: "
            f"{image_repository_name}"
        )
        san_ui.enter_image_repository_name(image_repository_name)

    # Stretch cluster — auto-checks Include CP nodes, assigns arbiter-node role
    logger.info("Enabling Stretch cluster toggle")
    san_ui.enable_stretch_cluster()

    # Advance to LUN discovery page
    san_ui.click_next_button()

    # Select one LUN, name the group, connect
    san_ui.select_luns_from_table(num_luns=1)
    san_ui.enter_lun_group_name(lun_group_name)
    san_ui.click_connect_and_create()

    logger.info(f"Waiting for LUN group '{lun_group_name}' to become Healthy/Connected")
    san_ui.navigate_to_san_storage_tab()
    san_ui.wait_for_filesystem_and_verify_connection(lun_group_name)
    logger.info(f"LUN group '{lun_group_name}' is Healthy/Connected")

    logger.info(
        f"Verifying all pods are Running in namespace '{constants.IBM_STORAGE_SCALE_NAMESPACE}'"
    )
    assert wait_for_pods_to_be_running(
        namespace=constants.IBM_STORAGE_SCALE_NAMESPACE,
        timeout=600,
    ), (
        "Deployment validation FAIL: not all pods reached the Running state in "
        f"namespace '{constants.IBM_STORAGE_SCALE_NAMESPACE}'"
    )
    logger.info(
        f"All pods in '{constants.IBM_STORAGE_SCALE_NAMESPACE}' are Running — "
        "deployment validation complete"
    )


def validate_deployment_ui(
    arbiter_node, all_master_nodes, all_worker_nodes, data_zone_1_workers
):
    """
    Execute the full UI deployment validation suite.

    Called as the final step of :func:`deploy_fusion_access_stretch_cluster`
    after all stack components are running.  Validates the Connect External
    Storage → Storage for SAN wizard end-to-end and confirms the LUN group
    and filesystem are healthy.

    Validation steps:
        1. Default node-selection page state (workers only, disk-node role).
        2. Include Control Plane Nodes toggle — masters appear as cluster-node.
        3. Stretch Cluster toggle — auto-check, role assignment, arbiter locked.
        4. Non-arbiter role changes — workers/masters can change; arbiter locked.
        5. LUN discovery — at least one LUN visible for all disk nodes.
        6. LUN group creation — group is Healthy/Connected; ibm-spectrum-scale
           pods are Running.

    Args:
        arbiter_node (str): Name of the arbiter master node.
        all_master_nodes (list[str]): All master node names.
        all_worker_nodes (list[str]): All worker node names.
        data_zone_1_workers (list[str]): Worker nodes in data zone 1.
    """
    logger.info("Starting UI deployment validation")
    login_ui()
    try:
        logger.info("Step 1 — Verify default node-selection page state")
        san_ui = StretchClusterSANUI()
        san_ui.navigate_to_connect_external_storage_san()
        _validate_node_selection_default_state(san_ui)

        logger.info("Step 2 — Verify Include Control Plane Nodes toggle")
        # Navigate fresh to reset toggle state before each check.
        san_ui = StretchClusterSANUI().navigate_to_connect_external_storage_san()
        _validate_include_control_plane_toggle(
            san_ui, all_master_nodes, all_worker_nodes, arbiter_node
        )

        logger.info("Step 3 — Verify Stretch Cluster toggle")
        san_ui = StretchClusterSANUI().navigate_to_connect_external_storage_san()
        _validate_stretch_cluster_toggle(
            san_ui, all_master_nodes, all_worker_nodes, arbiter_node
        )

        logger.info("Step 4 — Verify non-arbiter role changes (Stretch ON)")
        # Reuse the same session; Stretch is already ON from step 3.
        _validate_non_arbiter_role_changes(
            san_ui, all_master_nodes, arbiter_node, data_zone_1_workers
        )

        logger.info("Step 5 — Verify LUN discovery")
        # Navigate fresh with Stretch ON to reach the LUN discovery page.
        san_ui = StretchClusterSANUI().navigate_to_connect_external_storage_san()
        san_ui.enable_stretch_cluster()
        _validate_lun_discovery(san_ui)

        logger.info("Step 6 — Create LUN group and verify filesystem health")
        _validate_lun_group_and_filesystem_health()
    finally:
        close_browser()

    logger.info("UI deployment validation completed successfully")


def deploy_fusion_access_stretch_cluster():
    """
    Execute the complete Fusion Access HA (stretch cluster) deployment sequence
    followed by UI deployment validation.

    Deployment steps:
        1. Apply topology zone labels (arbiter + data zones).
        2. Identify the arbiter master node.
        3. Create the SAN LUNs on all nodes, masters included, over iSCSI.
        4. Deploy IBM Fusion components.
        5. Install the standalone FDF service stack only (storage skipped).
        6. Install the KMM operator.
        7. Create the IBM entitlement secret in ``ibm-spectrum-scale``.
        8. Configure the image registry, deploy the Fusion Access Operator and
           connect the external registry to the SAN through the UI wizard.

    Must be called after the base OCP cluster has been provisioned from::

        conf/deployment/vsphere/upi_2az_rhcos_vsan_lso_vmdk_3m_4w_arbiter_fusion_access.yaml

    Raises:
        AssertionError: If any deployment step or validation check fails.
    """
    logger.info("Starting Fusion Access HA stretch cluster deployment")

    logger.info("Step 1 — Apply topology zone labels")
    apply_stretch_cluster_zone_labels()

    logger.info("Step 2 — Identify arbiter master node")
    arbiter_node = get_arbiter_node_name()
    logger.info(f"Arbiter node confirmed: {arbiter_node}")

    logger.info("Step 3 — Create iSCSI LUNs on all nodes (masters + workers)")
    setup_iscsi_luns_on_all_nodes()

    logger.info("Step 4 — Deploy IBM Fusion components")
    deploy_fusion()

    logger.info("Step 5 — Install FDF service stack (storage skipped)")
    deploy_fdf_service_stack()

    logger.info("Step 6 — Install KMM operator")
    configure_kmm()

    logger.info("Step 7 — Create IBM entitlement secret in ibm-spectrum-scale")
    create_ibm_entitlement_secret(namespace=constants.IBM_STORAGE_SCALE_NAMESPACE)

    logger.info("Step 8 — Configure image registry and connect it to the SAN")
    configure_image_registry()
    deploy_fusion_access_operator()
    validate_deployment_ui(
        arbiter_node=arbiter_node,
        all_master_nodes=get_master_nodes(),
        all_worker_nodes=get_worker_nodes(),
        data_zone_1_workers=get_data_zone_worker_nodes(DATA_ZONE_1),
    )

    logger.info("Fusion Access HA stretch cluster deployment completed successfully")


def get_nodes_by_zone(zone):
    """
    Return all node names (masters and workers) that carry a specific zone label.

    Args:
        zone (str): Zone value, e.g. ``'arbiter'``, ``'data-1'``, ``'data-2'``.

    Returns:
        list[str]: Sorted list of node names in the requested zone.
    """
    node_ocp = OCP(kind="node")
    selector = f"{constants.ZONE_LABEL}={zone}"
    items = node_ocp.get(selector=selector).get("items", [])
    names = sorted(n["metadata"]["name"] for n in items)
    logger.debug(f"Nodes in zone '{zone}': {names}")
    return names


def get_data_zone_worker_nodes(zone):
    """
    Return worker node names that carry a specific zone label.

    Args:
        zone (str): Zone value, e.g. ``'data-1'`` or ``'data-2'``.

    Returns:
        list[str]: Sorted list of worker node names in that zone.
    """
    all_workers = set(get_worker_nodes())
    zone_nodes = get_nodes_by_zone(zone)
    return [n for n in zone_nodes if n in all_workers]
