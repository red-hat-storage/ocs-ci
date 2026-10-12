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

The UI interaction itself is not reimplemented here — it reuses
:class:`~ocs_ci.ocs.ui.page_objects.fusion_access_ui.FusionAccessUI`, the same page
object used by ``tests/cross_functional/ui/fusion_access/test_create_filesystem.py``.
What this module adds on top is the deployment-time concerns that a test does not
have: prerequisite secrets, iSCSI discovery, and a CLI verification that the backend
chain really did get created and is usable for DR.

Deployment flow:
  1. Create the IBM entitlement secret in ``ibm-spectrum-scale``.
  2. Run iSCSI discovery and login on all worker nodes so LUNs are visible to the UI.
  3. Create the Quay pull secret used for the GPFS kmod image build.
  4. Drive the console wizard to connect the SAN and create a LUN group.
  5. Verify the backend chain from the CLI: Cluster, Daemon, Filesystem, and
     FilesystemRecoveryData.
"""

import base64
import json
import logging
import os
import re
import tempfile

from time import sleep

from ocs_ci.framework import config
from ocs_ci.helpers.helpers import create_custom_secret_for_cnsa_rm
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    ResourceWrongStatusException,
    TimeoutExpiredError,
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    TimeoutSampler,
    exec_cmd,
    wait_for_machineconfigpool_status,
)

logger = logging.getLogger(__name__)

# How long to let the operator build the GPFS kmod and bring the daemon up. The
# kmod build plus sign plus load is the long pole here, several minutes on a cold
# cluster.
SCALE_CLUSTER_TIMEOUT = 3600
FILESYSTEM_TIMEOUT = 2400


class FusionAccessStorage(object):
    """
    Provision Fusion Access SAN storage by creating a LUN group through the
    OpenShift console, then verify the resources the operator creates in response.

    All configuration is read from the OCS-CI config at runtime.

    ENV_DATA keys:

    * ``san_iscsi_ip`` (str): iSCSI target portal IP.
    * ``san_iscsi_iqn`` (str): iSCSI target IQN.
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

    AUTH keys:

    * ``ibm_entitlement_key`` (str): IBM entitlement key for ``cp.icr.io``.

    """

    def __init__(self):
        self.namespace = constants.IBM_STORAGE_SCALE_NAMESPACE
        self.lun_group_name = config.ENV_DATA.get("san_lun_group_name", "lungroup1")
        self.num_luns = config.ENV_DATA.get("san_num_luns", 1)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def deploy(self):
        """
        Run the full Fusion Access storage provisioning flow.
        """
        logger.info("Starting Fusion Access SAN storage provisioning")
        self.create_entitlement_secret()
        self.discover_iscsi_luns()
        self.create_quay_secret()
        self.create_lun_group_via_ui()
        self.verify_storage_backend()
        logger.info("Fusion Access SAN storage provisioning completed")

    # ------------------------------------------------------------------
    # Prerequisites
    # ------------------------------------------------------------------

    def create_entitlement_secret(self):
        """
        Create the IBM entitlement secret used to pull the IBM Storage Scale images.

        Raises:
            ValueError: If ``ibm_entitlement_key`` is not set in config.AUTH.
        """
        ent_key = config.AUTH.get("ibm_entitlement_key")
        if not ent_key:
            raise ValueError(
                "ibm_entitlement_key is not set in config.AUTH. "
                "Cannot create IBM entitlement secret."
            )

        secret_name = constants.IBM_ENTITLEMENT_SECRET_NAME
        ocp_obj = ocp.OCP(kind="Secret", namespace=self.namespace)
        if ocp_obj.is_exist(resource_name=secret_name):
            logger.info(f"Secret '{secret_name}' already exists, skipping creation")
            return

        auth_b64 = base64.b64encode(f"cp:{ent_key}".encode()).decode()
        logger.info(f"Creating secret '{secret_name}' in namespace '{self.namespace}'")
        create_custom_secret_for_cnsa_rm(
            name=secret_name,
            namespace=self.namespace,
            data_dict={
                ".dockerconfigjson": {
                    "auths": {
                        "cp.icr.io": {
                            "username": "cp",
                            "password": ent_key,  # pragma: allowlist secret
                            "auth": auth_b64,
                        }
                    }
                }
            },
            secret_type="kubernetes.io/dockerconfigjson",
        )
        ocp_obj.wait_for_resource(
            condition="kubernetes.io/dockerconfigjson",
            resource_name=secret_name,
            column="TYPE",
            timeout=60,
        )
        logger.info(f"Secret '{secret_name}' verified in namespace '{self.namespace}'")

    def create_quay_secret(self):
        """
        Create the docker-registry secret used by KMM to push the built GPFS kmod
        image.

        Skipped when the secret already exists, so that a re-run of the deployment
        does not fail on an existing secret.
        """
        secret_name = constants.IBM_QUAYIO_SECRET_NAME
        ocp_obj = ocp.OCP(kind="Secret", namespace=self.namespace)
        if ocp_obj.is_exist(resource_name=secret_name):
            logger.info(f"Secret '{secret_name}' already exists, skipping creation")
            return

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

        logger.info(
            f"Creating docker-registry secret '{secret_name}' in '{self.namespace}'"
        )
        ocp_obj.exec_oc_cmd(
            f"create secret docker-registry {secret_name} "
            f"--docker-server='{quay_server}' "
            f"--docker-username='{quay_username}' "
            f"--docker-password='{quay_password}' "
            f"--docker-email='{quay_email}'",
            secrets=[quay_password, quay_username],
        )
        ocp_obj.wait_for_resource(
            condition="kubernetes.io/dockerconfigjson",
            resource_name=secret_name,
            column="TYPE",
            timeout=60,
        )
        logger.info(f"Secret '{secret_name}' created successfully")

    def discover_iscsi_luns(self):
        """
        Run iSCSI discovery and login on every worker node so the SAN LUNs are
        visible in the console when the wizard runs.

        Raises:
            ValueError: If the configured portal IP or IQN is malformed.
        """
        iscsi_ip = config.ENV_DATA.get("san_iscsi_ip")
        iscsi_iqn = config.ENV_DATA.get("san_iscsi_iqn")

        # Validate before interpolating into a shell command.
        if not re.fullmatch(r"[\d]{1,3}(\.[\d]{1,3}){3}", iscsi_ip or ""):
            raise ValueError(f"san_iscsi_ip '{iscsi_ip}' is not a valid IPv4 address")
        if not re.fullmatch(r"iqn\.\d{4}-\d{2}\.[a-zA-Z0-9.\-:]+", iscsi_iqn or ""):
            raise ValueError(f"san_iscsi_iqn '{iscsi_iqn}' is not a valid IQN")

        logger.info("Starting iSCSI LUN discovery on all worker nodes")
        ocp_obj = ocp.OCP()
        ocp_obj.exec_oc_cmd(
            f"get nodes -l node-role.kubernetes.io/worker --no-headers -o name "
            f"| xargs -I {{}} -- oc debug {{}} -- bash -c "
            f'\'chroot /host iscsiadm -m discovery -t st -p "{iscsi_ip}"; '
            f'chroot /host iscsiadm --mode node --target "{iscsi_iqn}" '
            f'--portal "{iscsi_ip}" -l\' 2> /dev/null',
            shell=True,
            out_yaml_format=False,
            secrets=[iscsi_ip, iscsi_iqn],
        )
        logger.info("LUN discovery completed, waiting for LUNs to surface in the UI")
        sleep(120)

    # ------------------------------------------------------------------
    # UI driven LUN group creation
    # ------------------------------------------------------------------

    def create_lun_group_via_ui(self):
        """
        Drive the OpenShift console wizard to connect the SAN and create a LUN group.

        Creating the LUN group is what triggers the Fusion Access operator to create
        the Cluster, Daemon, LocalDisk and Filesystem resources in the backend, so
        nothing below this point applies those CRs directly.
        """
        from ocs_ci.ocs.ui.base_ui import login_ui, close_browser
        from ocs_ci.ocs.ui.page_objects.fusion_access_ui import FusionAccessUI

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

            # Give the backend a moment before navigating away, otherwise the
            # request can be dropped mid-flight.
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
        logger.info("Waiting for the Scale Cluster CR to reach Success=True")
        for conditions in TimeoutSampler(
            timeout=SCALE_CLUSTER_TIMEOUT,
            sleep=30,
            func=self._get_conditions,
            resource=cluster_ocp,
        ):
            if conditions.get("Success") == "True":
                logger.info(f"Scale Cluster is ready: {conditions}")
                break
            logger.info(f"Scale Cluster not ready yet: {conditions}")

        # KMMModulesReady is the condition that actually tells us the GPFS kmod
        # built, signed and loaded. Without it the daemon never starts.
        conditions = self._get_conditions(cluster_ocp)
        if conditions.get("KMMModulesReady") != "True":
            raise ResourceWrongStatusException(
                f"GPFS kernel modules are not loaded. Cluster conditions: {conditions}"
            )
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
        for cluster_id in TimeoutSampler(
            timeout=SCALE_CLUSTER_TIMEOUT,
            sleep=30,
            func=self._get_cluster_id,
            resource=daemon_ocp,
        ):
            if cluster_id:
                logger.info(f"GPFS cluster id is {cluster_id}")
                return cluster_id
        raise ResourceWrongStatusException("Daemon CR never reported a GPFS cluster id")

    def get_filesystem_names(self):
        """
        Return the names of the Filesystem CRs the operator created.

        Returns:
            list: Filesystem names.
        """
        filesystem_ocp = ocp.OCP(
            kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND, namespace=self.namespace
        )
        items = filesystem_ocp.get(dont_raise=True).get("items", [])
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
                logger.info(f"Filesystem '{filesystem_name}' is ready: {conditions}")
                return
            logger.info(f"Filesystem '{filesystem_name}' not ready yet: {conditions}")

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

        # An FSRD reconciled before the Daemon CR exists gets an empty dr-primary
        # label and is then skipped forever, with no event and no error. Setting the
        # label is not enough on its own, the controller only re-reads it when its
        # cache starts, so the operator has to be restarted too.
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

        unexported = self._get_unexported_fsrds(filesystem_names)
        if unexported:
            raise ResourceWrongStatusException(
                f"FilesystemRecoveryData still not exported after repair: {unexported}. "
                "These filesystems cannot be recovered on the DR peer."
            )
        logger.info("All FilesystemRecoveryData resources are exported")

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
        # The export happens a couple of seconds after the controller starts, but
        # give the reconcile loop room to work through every filesystem.
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


def add_entitlement_to_pull_secret():
    """
    Add the IBM registry credentials to the cluster wide pull secret.

    IBM Storage Scale pulls its pre release images from ``preprod.icr.io``,
    which is not part of the stock pull secret. Without the entry the Scale
    daemon pods sit in ImagePullBackOff.

    The credentials are the IBM entitlement key, the same one the
    ``ibm-entitlement-key`` secret carries, under the fixed ``cp`` username.
    No-op when ``config.AUTH["ibm_entitlement_key"]`` is not set.

    Config keys:

    * ``AUTH.ibm_entitlement_key`` (str): the IBM entitlement key.
    * ``ENV_DATA.ibm_registries`` (list): registries to add. Defaults to
      ``["preprod.icr.io"]``.

    """
    ent_key = config.AUTH.get("ibm_entitlement_key")
    if not ent_key:
        logger.info(
            "ibm_entitlement_key is not set, leaving the cluster pull secret alone"
        )
        return

    registries = config.ENV_DATA.get("ibm_registries", [constants.IBM_PREPROD_REGISTRY])
    auth = base64.b64encode(f"cp:{ent_key}".encode()).decode()

    ocp_obj = ocp.OCP(
        kind=constants.SECRET, namespace=constants.OPENSHIFT_CONFIG_NAMESPACE
    )
    pull_secret = ocp_obj.get(resource_name="pull-secret")
    secret_data = json.loads(
        base64.b64decode(pull_secret["data"][".dockerconfigjson"]).decode()
    )
    auths = secret_data.setdefault("auths", {})

    missing = [r for r in registries if auths.get(r, {}).get("auth") != auth]
    if not missing:
        logger.info(
            f"Pull secret already carries credentials for {registries}, nothing to do"
        )
        return

    for registry in missing:
        logger.info(f"Adding '{registry}' credentials to the cluster pull secret")
        auths[registry] = {"auth": auth}  # pragma: allowlist secret

    # NamedTemporaryFile rather than a plain path, the file holds every
    # registry credential the cluster has.
    with tempfile.NamedTemporaryFile(
        mode="w+", prefix="pull-secret", suffix=".json"
    ) as secret_file:
        json.dump(secret_data, secret_file)
        secret_file.flush()
        os.chmod(secret_file.name, 0o600)
        exec_cmd(
            f"oc set data secret/pull-secret "
            f"-n {constants.OPENSHIFT_CONFIG_NAMESPACE} "
            f"--from-file=.dockerconfigjson={secret_file.name}"
        )

    # Updating the pull secret restarts the kubelet on every node, so the
    # rollout has to finish before anything tries to pull from the new registry.
    logger.info("Waiting for the node rollout triggered by the pull secret update")
    wait_for_machineconfigpool_status(node_type="all")
