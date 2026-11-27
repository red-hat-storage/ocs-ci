"""
This module provisions SAN storage for IBM Fusion Access, after the Fusion Access
operator itself has been installed by
:class:`~ocs_ci.deployment.fusion_access.FusionAccessOperator`.

Storage is provisioned through the OpenShift console wizard rather than by applying
CRs directly. Creating a LUN group in the console is what makes the Fusion Access
operator generate the whole backend chain on its own:

    LUN group  ->  Cluster  ->  Daemon  ->  LocalDisk  ->  Filesystem
                                                       ->  FilesystemRecoveryData

Driving the UI therefore keeps the deployment on the same path a customer takes, and
avoids this module having to know how those CRs are shaped.

The wizard lives under "Storage -> Data Foundation -> External systems", which is
served by the ``odf-console`` plugin. That plugin comes from ``odf-operator``, so
Fusion Data Foundation has to be on the cluster before this module runs, even
though FA-MDR takes none of its storage from it. A cluster without it is caught
up front by :meth:`FusionAccessStorage.verify_console_plugin_enabled` rather than
in a Selenium timeout.

The UI interaction itself is not reimplemented here — it reuses
:class:`~ocs_ci.ocs.ui.page_objects.fusion_access_ui.FusionAccessUI`, the same page
object used by ``tests/cross_functional/ui/fusion_access/test_create_filesystem.py``.
What this module adds on top is the deployment-time concerns that a test does not
have: prerequisite secrets, iSCSI discovery, and a CLI verification that the backend
chain really did get created and is usable for DR.

Deployment flow:
  1. Create the IBM entitlement secret in ``ibm-spectrum-scale``.
  2. Configure multipath on the workers, via MachineConfig.
  3. Run iSCSI discovery and login on all worker nodes so LUNs are visible to the UI.
  4. Verify every LUN came up multipathed, identically on all workers.
  5. Create the Quay pull secret used for the GPFS kmod image build.
  6. Drive the console wizard to connect the SAN and create a LUN group.
  7. Verify the backend chain from the CLI: Cluster, Daemon, Filesystem, and
     FilesystemRecoveryData.
"""

import base64
import json
import logging
import re

from time import sleep

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_custom_secret_for_cnsa_rm
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    TimeoutExpiredError,
)
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    TimeoutSampler,
    load_auth_config,
    wait_for_machineconfigpool_status,
)

logger = logging.getLogger(__name__)

# Dominated by the GPFS kmod build/sign/load, minutes on a cold cluster.
SCALE_CLUSTER_TIMEOUT = 3600
FILESYSTEM_TIMEOUT = 2400
FSRD_PRESENT_TIMEOUT = 600
FSRD_EXPORT_TIMEOUT = 900
# One reboot of every worker, serialised by maxUnavailable.
MULTIPATH_MC_TIMEOUT = 2400


class FusionAccessStorage(object):
    """
    Provision Fusion Access SAN storage by creating a LUN group through the
    OpenShift console, then verify the resources the operator creates in response.

    All configuration is read from the OCS-CI config at runtime.

    ENV_DATA keys:

    * ``san_iscsi_portals`` (list): iSCSI target portal IPs. Use this for an
      array pair presenting several portals each.
    * ``san_iscsi_ip`` (str): single iSCSI target portal IP, the one portal
      spelling of ``san_iscsi_portals``.
    * ``san_iscsi_iqn`` (str): iSCSI target IQN. Optional, every discovered
      target is logged in to when it is not set.
    * ``san_expected_paths_per_lun`` (int): paths every LUN must have after
      login. Defaults to the number of configured portals.
    * ``san_image_registry_url`` (str): Registry for the GPFS kmod image.
      Defaults to ``"quay.io"``.
    * ``san_image_repository_name`` (str): Repository for the GPFS kmod image.
    * ``san_quay_server`` (str): Quay server for the pull secret.
    * ``san_quay_username`` (str): Quay username.
    * ``san_quay_password`` (str): Quay password.
    * ``san_quay_email`` (str): Quay email.
    * ``san_lun_group_name`` (str): LUN group to create. Defaults to
      ``"lungroup1"``.
    * ``san_num_luns`` (int): Number of LUNs to put in the group. Defaults to 1.
    * ``san_create_lun_group`` (bool): Whether this cluster creates the LUN
      group. Defaults to True. Both sites of a Metro-DR pair see the same
      volumes, so only one of them may claim them into a LUN group; set this
      False on the peer, which then gets the SAN plumbing and CNSA but no
      filesystem of its own.

    The Quay credentials are secrets, so they belong in an untracked conf passed
    with ``--ocsci-conf``, not in a conf under version control.

    AUTH keys, read from ``data/auth.yaml`` or from an ``AUTH`` section of a
    conf passed with ``--ocsci-conf``:

    * ``ibm_entitlement_key`` (str): IBM entitlement key for ``cp.icr.io``.

    """

    def __init__(self):
        self.namespace = constants.IBM_STORAGE_SCALE_NAMESPACE
        self.lun_group_name = config.ENV_DATA.get("san_lun_group_name", "lungroup1")
        self.num_luns = config.ENV_DATA.get("san_num_luns", 1)
        self.create_lun_group = config.ENV_DATA.get("san_create_lun_group", True)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def deploy(self):
        """
        Run the full Fusion Access storage provisioning flow.
        """
        logger.info("Starting Fusion Access SAN storage provisioning")
        self.create_entitlement_secret()
        # Before discovery, so that multipathd is already running when the LUNs
        # arrive. Coming late means the first paths are claimed as bare sd
        # devices and only get folded into a map on the next multipathd scan.
        self.configure_multipath()
        self.discover_iscsi_luns()
        self.verify_multipath()
        self.create_quay_secret()
        if self.create_lun_group:
            self.create_lun_group_via_ui()
        else:
            logger.info(
                "san_create_lun_group is False, leaving the LUN group to the "
                "peer cluster"
            )
        self.verify_storage_backend()
        logger.info("Fusion Access SAN storage provisioning completed")

    # ------------------------------------------------------------------
    # Prerequisites
    # ------------------------------------------------------------------

    @staticmethod
    def get_entitlement_key():
        """
        Read the IBM entitlement key.

        ``data/auth.yaml`` is where a credential like this belongs, but that file
        is only read on demand by :func:`load_auth_config` - nothing merges it
        into ``config.AUTH``, which is populated from ``--ocsci-conf`` files
        only. So both are consulted, the conf first so that a run can override
        what is on disk. In auth.yaml the key is accepted at the top level or
        under ``AUTH``.

        Returns:
            str: the entitlement key, empty when it is configured nowhere

        """
        ent_key = config.AUTH.get("ibm_entitlement_key")
        if ent_key:
            return ent_key
        auth_config = load_auth_config() or {}
        return auth_config.get("ibm_entitlement_key") or auth_config.get(
            "AUTH", {}
        ).get("ibm_entitlement_key", "")

    def apply_dockerconfig_secret(self, secret_name, auths):
        """
        Create a pull secret, or replace it when its contents have drifted.

        Skipping an existing secret outright is wrong for a credential. These
        tokens get rotated, and a secret left over from a previous run then
        keeps a stale one indefinitely - the deployment logs "already exists,
        skipping creation" and the failure lands somewhere far away, as
        ``unauthorized`` from a registry, long after the config was corrected.
        Comparing against the desired contents keeps the step idempotent while
        still letting a rotation take effect.

        The secret is deleted and recreated rather than patched, because an
        ``oc patch`` would merge into the old ``auths`` map and leave entries
        for registries that are no longer configured.

        Args:
            secret_name (str): Name of the secret
            auths (dict): The ``auths`` map for ``.dockerconfigjson``

        """
        ocp_obj = ocp.OCP(kind="Secret", namespace=self.namespace)
        desired = {"auths": auths}
        if ocp_obj.is_exist(resource_name=secret_name):
            encoded = (
                ocp_obj.get(resource_name=secret_name)
                .get("data", {})
                .get(".dockerconfigjson", "")
            )
            try:
                current = json.loads(base64.b64decode(encoded))
            except (ValueError, TypeError):
                current = None
            if current == desired:
                logger.info(
                    f"Secret '{secret_name}' already exists with the configured "
                    "credentials, leaving it alone"
                )
                return
            # Deliberately no diff in the log - the values are credentials.
            logger.warning(
                f"Secret '{secret_name}' exists but does not match the "
                "configured credentials, replacing it"
            )
            ocp_obj.delete(resource_name=secret_name)

        logger.info(f"Creating secret '{secret_name}' in namespace '{self.namespace}'")
        create_custom_secret_for_cnsa_rm(
            name=secret_name,
            namespace=self.namespace,
            data_dict={".dockerconfigjson": desired},
            secret_type="kubernetes.io/dockerconfigjson",
        )
        ocp_obj.wait_for_resource(
            condition="kubernetes.io/dockerconfigjson",
            resource_name=secret_name,
            column="TYPE",
            timeout=60,
        )
        logger.info(f"Secret '{secret_name}' verified in namespace '{self.namespace}'")

    def create_entitlement_secret(self):
        """
        Create the IBM entitlement secret used to pull the IBM Storage Scale images.

        Raises:
            ValueError: If ``ibm_entitlement_key`` is configured nowhere.
        """
        ent_key = self.get_entitlement_key()
        if not ent_key:
            raise ValueError(
                "ibm_entitlement_key is not set in data/auth.yaml nor in "
                "config.AUTH. Cannot create IBM entitlement secret."
            )

        auth_b64 = base64.b64encode(f"cp:{ent_key}".encode()).decode()
        self.apply_dockerconfig_secret(
            constants.IBM_ENTITLEMENT_SECRET_NAME,
            {
                "cp.icr.io": {
                    "username": "cp",
                    "password": ent_key,  # pragma: allowlist secret
                    "auth": auth_b64,
                }
            },
        )

    def create_quay_secret(self):
        """
        Create the docker-registry secret used by KMM to push the built GPFS kmod
        image.

        Replaced rather than skipped when it already exists but holds different
        credentials, so that a rotated robot token takes effect on a re-run.
        """
        quay_server = config.ENV_DATA.get("san_quay_server")
        quay_username = config.ENV_DATA.get("san_quay_username")
        quay_password = config.ENV_DATA.get("san_quay_password")
        quay_email = config.ENV_DATA.get("san_quay_email")
        missing = [
            key
            for key, value in {
                "san_quay_server": quay_server,
                "san_quay_username": quay_username,
                "san_quay_password": quay_password,
                "san_quay_email": quay_email,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(
                f"Missing required ENV_DATA keys for the Quay pull secret: {missing}"
            )

        # Built as a manifest rather than 'oc create secret docker-registry',
        # which would put the Quay password in the argument list for anything
        # that can read /proc. Also removes the quoting hazard for passwords
        # containing shell metacharacters.
        auth_b64 = base64.b64encode(
            f"{quay_username}:{quay_password}".encode()
        ).decode()
        self.apply_dockerconfig_secret(
            constants.IBM_QUAYIO_SECRET_NAME,
            {
                quay_server: {
                    "username": quay_username,
                    "password": quay_password,  # pragma: allowlist secret
                    "email": quay_email,
                    "auth": auth_b64,
                }
            },
        )

    def iscsi_portals(self):
        """
        Portal IPs to run discovery against.

        ``san_iscsi_portals`` is the list form, for an array pair presenting
        several portals each, which is what a Metro-DR site looks like.
        ``san_iscsi_ip`` stays accepted as the single portal spelling.

        Returns:
            list: portal IPv4 addresses

        Raises:
            ValueError: If no portal is configured, or one is not an IPv4
                address.

        """
        portals = config.ENV_DATA.get("san_iscsi_portals")
        if not portals:
            single = config.ENV_DATA.get("san_iscsi_ip")
            portals = [single] if single else []
        if not portals:
            raise ValueError(
                "No iSCSI portal configured, set ENV_DATA['san_iscsi_portals'] "
                "or ENV_DATA['san_iscsi_ip']"
            )
        for portal in portals:
            if not re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", portal or ""):
                raise ValueError(f"iSCSI portal '{portal}' is not a valid IPv4 address")
        return list(portals)

    def configure_multipath(self):
        """
        Put /etc/multipath.conf on every worker and enable multipathd and
        iscsid, through a MachineConfig.

        A MachineConfig and not an 'oc debug' loop, for two reasons. A worker
        added after the deployment has to come up configured the same way,
        otherwise it joins the Scale cluster with single path LUNs under
        node local mpath names. And RHCOS writes
        /etc/iscsi/initiatorname.iscsi the first time iscsid starts, so until
        this has run the node has no IQN at all, and there is nothing for the
        storage admin to map volumes to.

        Costs one rolling reboot of the worker pool. Skipped when the
        MachineConfig is already there, so a re-run does not reboot anything.

        """
        from ocs_ci.ocs.resources.machineconfig import machineconfig_exists
        from ocs_ci.ocs.resources.ocs import OCS

        if machineconfig_exists(constants.FUSION_ACCESS_MULTIPATH_MC_NAME):
            logger.info(
                f"MachineConfig '{constants.FUSION_ACCESS_MULTIPATH_MC_NAME}' "
                "already exists, leaving the worker pool alone"
            )
            return

        with open(constants.FUSION_ACCESS_MULTIPATH_CONF) as conf_file:
            multipath_conf = conf_file.read()
        encoded = base64.b64encode(multipath_conf.encode()).decode()

        mc_data = templating.load_yaml(constants.FUSION_ACCESS_MULTIPATH_MC)
        mc_data["spec"]["config"]["storage"]["files"][0]["contents"][
            "source"
        ] = f"data:text/plain;charset=utf-8;base64,{encoded}"

        logger.info(
            f"Creating MachineConfig '{constants.FUSION_ACCESS_MULTIPATH_MC_NAME}', "
            "the worker pool will reboot"
        )
        OCS(**mc_data).create()
        wait_for_machineconfigpool_status(
            constants.WORKER_MACHINE, timeout=MULTIPATH_MC_TIMEOUT
        )
        logger.info("Workers rebooted with multipathd and iscsid enabled")

    def discover_iscsi_luns(self):
        """
        Run iSCSI discovery and login on every worker node so the SAN LUNs are
        visible in the console when the wizard runs.

        Every configured portal is discovered before anything logs in, then the
        node logs in to all of them at once. Logging in per portal as it is
        discovered would bring the first LUNs up single pathed and leave
        multipathd to merge the rest in later, which it does, but it makes the
        intermediate state hard to tell apart from a genuinely missing path.

        The node records are then set to log in on boot. The discovery itself
        does not survive a reboot otherwise, and a worker that comes back with
        no sessions takes its NSDs down with it.

        Raises:
            ValueError: If a configured portal or the target IQN is malformed.

        """
        portals = self.iscsi_portals()
        # Optional. Without it every discovered target is logged in to, which
        # is what a two array site wants anyway.
        iscsi_iqn = config.ENV_DATA.get("san_iscsi_iqn")
        if iscsi_iqn and not re.fullmatch(
            r"iqn\.\d{4}-\d{2}\.[a-zA-Z0-9.\-:]+", iscsi_iqn
        ):
            raise ValueError(f"san_iscsi_iqn '{iscsi_iqn}' is not a valid IQN")

        logger.info(
            f"Starting iSCSI discovery on all worker nodes against {len(portals)} "
            f"portal(s)"
        )
        # '&&' throughout, so a failed discovery is not masked by a later step
        # reporting success.
        discovery = " && ".join(
            f'chroot /host iscsiadm -m discovery -t st -p "{portal}"'
            for portal in portals
        )
        if iscsi_iqn:
            login = " && ".join(
                f'chroot /host iscsiadm --mode node --target "{iscsi_iqn}" '
                f'--portal "{portal}" -l'
                for portal in portals
            )
        else:
            login = "chroot /host iscsiadm --mode node --loginall=all"
        persist = (
            "chroot /host iscsiadm --mode node -o update "
            "-n node.startup -v automatic"
        )

        ocp_obj = ocp.OCP()
        ocp_obj.exec_oc_cmd(
            f"get nodes -l node-role.kubernetes.io/worker --no-headers -o name "
            f"| xargs -I {{}} -- oc debug {{}} -- bash -c "
            f"'{discovery} && {login} && {persist}'",
            shell=True,
            out_yaml_format=False,
            secrets=portals + ([iscsi_iqn] if iscsi_iqn else []),
        )
        logger.info("LUN discovery completed, waiting for LUNs to surface in the UI")
        sleep(120)

    def verify_multipath(self):
        """
        Check that the LUNs came up as multipath maps with more than one path,
        on every worker.

        Worth checking rather than assuming, because a half configured fabric
        is not visibly broken at this point. Single pathed LUNs build a
        filesystem perfectly well and only show themselves when a path drops,
        which on a DR cluster is the one moment that matters. The names are
        also compared across nodes: WWID naming is the whole reason
        user_friendly_names is off, so nodes disagreeing on a name means the
        config did not land.

        Config keys (ENV_DATA):

        * ``san_expected_paths_per_lun`` (int): paths every map must have.
          Defaults to the number of configured portals.

        Raises:
            ResourceWrongStatusException: If a worker has no multipath maps,
              a map has fewer paths than expected, or the workers do not agree
              on the set of maps.

        """
        from ocs_ci.ocs.node import get_worker_nodes

        expected_paths = config.ENV_DATA.get(
            "san_expected_paths_per_lun", len(self.iscsi_portals())
        )
        ocp_obj = ocp.OCP()
        per_node = {}
        for node_name in get_worker_nodes():
            output = ocp_obj.exec_oc_cmd(
                f"debug node/{node_name} -- chroot /host multipath -ll",
                out_yaml_format=False,
            )
            per_node[node_name] = self._parse_multipath_ll(output)
            logger.info(f"{node_name}: multipath maps {per_node[node_name]}")

        problems = []
        for node_name, maps in per_node.items():
            if not maps:
                problems.append(f"{node_name} has no multipath maps")
                continue
            for wwid, paths in maps.items():
                if paths < expected_paths:
                    problems.append(
                        f"{node_name} map {wwid} has {paths} path(s), "
                        f"expected {expected_paths}"
                    )

        # Same volumes presented at both sites is what makes the LocalDisk CRs
        # portable, so a node seeing a different set is a mapping error.
        name_sets = {node: set(maps) for node, maps in per_node.items()}
        if len(set(map(frozenset, name_sets.values()))) > 1:
            problems.append(f"workers disagree on the multipath maps: {name_sets}")

        if problems:
            raise ResourceWrongStatusException(
                "Multipath verification failed: " + "; ".join(problems)
            )
        logger.info(
            f"All workers see the same multipath maps, each with at least "
            f"{expected_paths} paths"
        )

    @staticmethod
    def _parse_multipath_ll(output):
        """
        Count the usable paths per map in 'multipath -ll' output.

        A map header starts in column 0 and names the dm device, every path
        below it is indented and ends in its state triplet. Only paths that are
        both active and ready are counted, a faulty one is a path that exists
        and cannot be used.

        Args:
            output (str): raw 'multipath -ll' output

        Returns:
            dict: map name -> number of active ready paths

        """
        maps = {}
        current = None
        for line in output.splitlines():
            if line and not line[0].isspace() and "dm-" in line:
                current = line.split()[0]
                maps[current] = 0
            elif current and "active ready" in line:
                maps[current] += 1
        return maps

    # ------------------------------------------------------------------
    # UI driven LUN group creation
    # ------------------------------------------------------------------

    @staticmethod
    def verify_console_plugin_enabled():
        """
        Check that the console plugin owning the wizard is enabled.

        "Storage -> Data Foundation -> External systems" is served by
        ``odf-console``, which is registered by ``odf-operator``. On a cluster
        without it the navigation simply has no such link, and the wizard step
        below spends its whole timeout looking for an element that can never
        appear - with the AI locator fallback billing for the privilege. Failing
        here instead names the missing prerequisite directly.

        Raises:
            ResourceWrongStatusException: If the plugin is not enabled.

        """
        console = ocp.OCP(kind="Console.operator.openshift.io", resource_name="cluster")
        plugins = console.get().get("spec", {}).get("plugins") or []
        if constants.ODF_CONSOLE_PLUGIN not in plugins:
            raise ResourceWrongStatusException(
                f"Console plugin '{constants.ODF_CONSOLE_PLUGIN}' is not enabled "
                f"(enabled plugins: {plugins}). It is registered by odf-operator, "
                "so the LUN group wizard is unreachable until Fusion Data "
                "Foundation is deployed on this cluster."
            )
        logger.info(f"Console plugin '{constants.ODF_CONSOLE_PLUGIN}' is enabled")

    def create_lun_group_via_ui(self):
        """
        Drive the OpenShift console wizard to connect the SAN and create a LUN group.

        Creating the LUN group is what triggers the Fusion Access operator to create
        the Cluster, Daemon, LocalDisk and Filesystem resources in the backend, so
        nothing below this point applies those CRs directly.
        """
        from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
        from ocs_ci.ocs.ui.page_objects.fusion_access_ui import FusionAccessUI

        self.verify_console_plugin_enabled()

        image_registry_url = config.ENV_DATA.get("san_image_registry_url", "quay.io")
        image_repository_name = config.ENV_DATA.get("san_image_repository_name")
        if not image_repository_name:
            raise ValueError("ENV_DATA['san_image_repository_name'] must be set")

        login_ui()
        try:
            fusion_access = FusionAccessUI()

            logger.info("Navigating to Storage > External systems")
            fusion_access.nav_external_systems_page()

            logger.info("Connecting external systems, selecting SAN")
            fusion_access.click_connect_external_systems()
            fusion_access.select_storage_area_network()
            fusion_access.click_next_button()

            logger.info(
                f"Configuring kmod image: {image_registry_url}/{image_repository_name}"
            )
            fusion_access.enter_image_registry_url(image_registry_url)
            fusion_access.enter_image_repository_name(image_repository_name)
            fusion_access.select_secret_key()

            fusion_access.select_all_nodes_option()

            logger.info(f"Creating LUN group '{self.lun_group_name}'")
            fusion_access.enter_lun_group_name(self.lun_group_name)
            selected_luns = fusion_access.select_luns_from_table(num_luns=self.num_luns)
            logger.info(f"Selected LUNs: {selected_luns}")

            fusion_access.click_connect_and_create()
            fusion_access.take_screenshot("lun_group_creation_initiated")

            # Navigating away too early drops the request mid-flight.
            sleep(60)
            fusion_access.refresh_page()
            fusion_access.page_has_loaded()
            sleep(10)

            fusion_access.nav_external_systems_page()
            fusion_access.navigate_to_san_storage_tab()
            fusion_access.wait_for_filesystem_and_verify_connection(
                lun_group_name=self.lun_group_name,
            )
            logger.info(f"LUN group '{self.lun_group_name}' is connected and healthy")
        finally:
            close_browser()

    # ------------------------------------------------------------------
    # Backend verification
    # ------------------------------------------------------------------

    def verify_storage_backend(self):
        """
        Verify the resources the operator created in response to the LUN group.

        The console only reports on the LUN group itself, so this checks the chain
        it produced underneath, ending at the DR-relevant state.
        """
        logger.info("Verifying IBM Storage Scale backend resources")
        self.wait_for_scale_cluster_ready()
        cluster_id = self.get_gpfs_cluster_id()
        if not self.create_lun_group:
            logger.info(
                "san_create_lun_group is False, stopping after the Scale Cluster "
                "and GPFS cluster id checks - this cluster owns no filesystem"
            )
            return
        filesystems = self.get_filesystem_names()
        if not filesystems:
            raise ResourceWrongStatusException(
                "No Filesystem resources were created by the LUN group "
                f"'{self.lun_group_name}'"
            )
        for filesystem in filesystems:
            self.wait_for_filesystem_ready(filesystem)
        self.verify_recovery_data_exported(filesystems, cluster_id)

    def wait_for_scale_cluster_ready(self):
        """
        Wait for the Scale ``Cluster`` CR to report the GPFS kernel modules loaded.

        Raises:
            ResourceWrongStatusException: If the Cluster does not become ready.
        """
        cluster_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_CLUSTER_KIND,
            namespace=self.namespace,
            resource_name=constants.IBM_STORAGE_SCALE_CLUSTER_NAME,
        )
        # Success alone does not mean the kmod loaded, KMMModulesReady reports that.
        logger.info("Waiting for the Scale Cluster CR to be ready")
        conditions = {}
        try:
            for conditions in TimeoutSampler(
                timeout=SCALE_CLUSTER_TIMEOUT,
                sleep=30,
                func=self._get_conditions,
                resource=cluster_ocp,
            ):
                if all(
                    conditions.get(condition) == "True"
                    for condition in ("Success", "KMMModulesReady")
                ):
                    logger.info(f"Scale Cluster is ready: {conditions}")
                    break
                logger.info(f"Scale Cluster not ready yet: {conditions}")
        except TimeoutExpiredError as ex:
            raise ResourceWrongStatusException(
                f"Scale Cluster did not become ready within "
                f"{SCALE_CLUSTER_TIMEOUT}s. Last conditions: {conditions}"
            ) from ex
        logger.info("GPFS kernel modules are loaded")

    def get_gpfs_cluster_id(self):
        """
        Return the GPFS cluster id from the ``Daemon`` CR.

        Returns:
            str: The GPFS cluster id.

        Raises:
            ResourceWrongStatusException: If the Daemon CR never reports a cluster id.
        """
        daemon_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_DAEMON_KIND,
            namespace=self.namespace,
            resource_name=constants.IBM_STORAGE_SCALE_DAEMON_NAME,
        )
        logger.info("Waiting for the Daemon CR to report a GPFS cluster id")
        # The sampler raises TimeoutExpiredError rather than falling out of the
        # loop, so the translation has to happen here or it never happens.
        try:
            for cluster_id in TimeoutSampler(
                timeout=SCALE_CLUSTER_TIMEOUT,
                sleep=30,
                func=self._get_cluster_id,
                resource=daemon_ocp,
            ):
                if cluster_id:
                    logger.info(f"GPFS cluster id is {cluster_id}")
                    return cluster_id
        except TimeoutExpiredError as ex:
            raise ResourceWrongStatusException(
                "Daemon CR never reported a GPFS cluster id within "
                f"{SCALE_CLUSTER_TIMEOUT}s"
            ) from ex

    def get_filesystem_names(self):
        """
        Return the names of the Filesystem CRs the operator created.

        Returns:
            list: Filesystem names.
        """
        filesystem_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND, namespace=self.namespace
        )
        # dont_raise=True returns None, not {}, when the lookup fails, and the
        # caller needs the empty list so it can raise its own descriptive error.
        items = (filesystem_ocp.get(dont_raise=True) or {}).get("items", [])
        names = [item["metadata"]["name"] for item in items]
        logger.info(f"Filesystems created by the LUN group: {names}")
        return names

    def wait_for_filesystem_ready(self, filesystem_name):
        """
        Wait for a Filesystem to report Success, Healthy and Mounted.

        Args:
            filesystem_name (str): Name of the Filesystem CR.

        Raises:
            ResourceWrongStatusException: If the filesystem does not become ready.
        """
        filesystem_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND,
            namespace=self.namespace,
            resource_name=filesystem_name,
        )
        logger.info(f"Waiting for filesystem '{filesystem_name}' to be mounted")
        conditions = {}
        try:
            for conditions in TimeoutSampler(
                timeout=FILESYSTEM_TIMEOUT,
                sleep=30,
                func=self._get_conditions,
                resource=filesystem_ocp,
            ):
                if all(
                    conditions.get(condition) == "True"
                    for condition in ("Success", "Healthy", "Mounted")
                ):
                    logger.info(
                        f"Filesystem '{filesystem_name}' is ready: {conditions}"
                    )
                    return
                logger.info(
                    f"Filesystem '{filesystem_name}' not ready yet: {conditions}"
                )
        except TimeoutExpiredError as ex:
            raise ResourceWrongStatusException(
                f"Filesystem '{filesystem_name}' was not ready within "
                f"{FILESYSTEM_TIMEOUT}s. Last conditions: {conditions}"
            ) from ex

    def verify_recovery_data_exported(self, filesystem_names, cluster_id):
        """
        Verify every FilesystemRecoveryData has exported its configuration.

        The FSRD holds the only copy of the filesystem configuration needed to
        recover it on the DR peer, and an unexported one is silent — the filesystem
        still reports healthy and mounted. A deployment that finishes with an
        unexported FSRD looks fine and fails at failover, so this is checked
        explicitly rather than inferred from filesystem health.

        Args:
            filesystem_names (list): Filesystems whose FSRDs should be exported.
            cluster_id (str): GPFS cluster id, used to repair an empty dr-primary
                label.

        Raises:
            ResourceWrongStatusException: If an FSRD is still unexported after
                repair.
        """
        unexported = self._get_unexported_fsrds(filesystem_names)
        if not unexported:
            logger.info("All FilesystemRecoveryData resources are exported")
            return

        # FSRDs lag Filesystem readiness, labelling one that does not exist yet fails.
        self._wait_for_fsrds_present(filesystem_names)
        unexported = self._get_unexported_fsrds(filesystem_names)
        if not unexported:
            logger.info("All FilesystemRecoveryData resources are exported")
            return

        # An empty dr-primary label is skipped forever, and only re-read on restart.
        logger.warning(
            f"FilesystemRecoveryData not exported for {unexported}. "
            f"Setting the dr-primary label to {cluster_id} and restarting the operator."
        )
        fsrd_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_FSRD_KIND, namespace=self.namespace
        )
        label = f"{constants.IBM_STORAGE_SCALE_DR_PRIMARY_LABEL}={cluster_id}"
        for name in unexported:
            # --overwrite is required, the label already exists with an empty value
            fsrd_ocp.exec_oc_cmd(
                f"label {constants.IBM_STORAGE_SCALE_FSRD_KIND} {name} "
                f"--overwrite {label}",
                out_yaml_format=False,
            )
        self._restart_scale_operator()

        logger.info("Waiting for the FilesystemRecoveryData resources to export")
        try:
            for unexported in TimeoutSampler(
                timeout=FSRD_EXPORT_TIMEOUT,
                sleep=30,
                func=self._get_unexported_fsrds,
                filesystem_names=filesystem_names,
            ):
                if not unexported:
                    break
                logger.info(f"Still waiting on FilesystemRecoveryData: {unexported}")
        except TimeoutExpiredError as ex:
            raise ResourceWrongStatusException(
                f"FilesystemRecoveryData still not exported after repair: {unexported}. "
                "These filesystems cannot be recovered on the DR peer."
            ) from ex
        logger.info("All FilesystemRecoveryData resources are exported")

    def _wait_for_fsrds_present(self, filesystem_names):
        """
        Wait until a FilesystemRecoveryData exists for every filesystem.

        Args:
            filesystem_names (list): Filesystems whose FSRDs should exist.

        Raises:
            ResourceWrongStatusException: If an FSRD is never created.
        """
        fsrd_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_FSRD_KIND, namespace=self.namespace
        )
        missing = []
        try:
            for missing in TimeoutSampler(
                timeout=FSRD_PRESENT_TIMEOUT,
                sleep=15,
                func=lambda: [
                    name
                    for name in filesystem_names
                    if not fsrd_ocp.get(resource_name=name, dont_raise=True)
                ],
            ):
                if not missing:
                    break
                logger.info(
                    f"Waiting for FilesystemRecoveryData to be created: {missing}"
                )
        except TimeoutExpiredError as ex:
            raise ResourceWrongStatusException(
                f"FilesystemRecoveryData was never created for {missing}"
            ) from ex

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @retry((AssertionError, CommandFailed, TimeoutExpiredError), tries=20, delay=30)
    def _get_unexported_fsrds(self, filesystem_names):
        """
        Return the FSRDs that have no ``lastExportTime``.

        Args:
            filesystem_names (list): Filesystems to check.

        Returns:
            list: Names of FSRDs that have not exported.
        """
        fsrd_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_FSRD_KIND, namespace=self.namespace
        )
        unexported = []
        for name in filesystem_names:
            fsrd = fsrd_ocp.get(resource_name=name, dont_raise=True)
            if not fsrd:
                unexported.append(name)
                continue
            if not fsrd.get("status", {}).get("lastExportTime"):
                unexported.append(name)
        return unexported

    def _restart_scale_operator(self):
        """
        Restart the IBM Storage Scale operator and wait for it to come back.
        """
        pod_ocp = ocp.OCP(kind=constants.POD, namespace=self.namespace)
        logger.info("Restarting the IBM Storage Scale operator")
        pod_ocp.exec_oc_cmd(
            "delete pod -l app.kubernetes.io/name=operator", out_yaml_format=False
        )
        ocp.OCP(kind=constants.DEPLOYMENT, namespace=self.namespace).exec_oc_cmd(
            "rollout status deploy/ibm-spectrum-scale-controller-manager "
            "--timeout=300s",
            out_yaml_format=False,
        )
        # Room for the reconcile loop to work through every filesystem.
        sleep(60)

    @staticmethod
    def _get_conditions(resource):
        """
        Return a resource's status conditions as a ``{type: status}`` mapping.

        Args:
            resource (OCP): The resource to read.

        Returns:
            dict: Condition type to status.
        """
        data = resource.get(dont_raise=True) or {}
        return {
            condition["type"]: condition["status"]
            for condition in data.get("status", {}).get("conditions", [])
        }

    @staticmethod
    def _get_cluster_id(resource):
        """
        Return ``status.clusterID`` from a Daemon CR, or None.

        Args:
            resource (OCP): The Daemon resource.

        Returns:
            str: The GPFS cluster id, or None if not reported yet.
        """
        data = resource.get(dont_raise=True) or {}
        return data.get("status", {}).get("clusterID")
