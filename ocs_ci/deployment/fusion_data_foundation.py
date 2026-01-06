"""
This module contains functions needed to install IBM Fusion Data Foundation.
"""

import json
import logging
import os
import tempfile

import yaml

from ocs_ci.deployment.helpers import storage_class
from ocs_ci.deployment.helpers.lso_helpers import add_disks_lso
from ocs_ci.deployment.helpers.storage_class import get_storageclass
from ocs_ci.framework import config

from ocs_ci.helpers.helpers import create_lvs_resource
from ocs_ci.ocs import constants, defaults, node
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating, version
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import run_cmd

from ocs_ci.ocs.resources.storage_cluster import StorageCluster
from ocs_ci.utility.storage_cluster_setup import StorageClusterSetup
from ocs_ci.utility.operators import LocalStorageOperator

import time
from ocs_ci.utility.utils import (
    wait_for_machineconfigpool_status,
    get_running_ocp_version,
)

logger = logging.getLogger(__name__)


class FusionDataFoundationDeployment:
    def __init__(self):
        self.pre_release = config.DEPLOYMENT.get("fdf_pre_release", False)
        self.kubeconfig = config.RUN["kubeconfig"]
        self.lso_enabled = config.DEPLOYMENT.get("local_storage", False)
        self.fdf_skip_storage_setup = config.DEPLOYMENT.get(
            "fdf_skip_storage_setup", False
        )
        storage_class.set_custom_storage_class_path()

    @property
    def storage_class(self):
        if not config.ENV_DATA.get("storage_class"):
            sc = storage_class.get_storageclass() or constants.DEFAULT_STORAGECLASS_LSO
            self.storage_class = sc
            return sc
        return config.ENV_DATA["storage_class"]

    @storage_class.setter
    def storage_class(self, value):
        config.ENV_DATA["storage_class"] = value

    @property
    def custom_storage_class_path(self):
        return config.ENV_DATA["custom_storage_class_path"]

    def deploy(self):
        """
        Installs IBM Fusion Data Foundation.
        """

        logger.info("Installing IBM Fusion Data Foundation")
        if self.pre_release:
            self.create_image_tag_mirror_set()
            self.create_image_digest_mirror_set()
            self.setup_fdf_pre_release_deployment()

        self.create_fdf_service_cr()
        self.verify_fdf_installation()
        if not self.fdf_skip_storage_setup:
            self.setup_storage()

    def ensure_lso_installed(self):
        """
        In the case of LSO is not available - bring catalog for unreleased version and install it
        """

        logger.info("Ensuring Local Storage Operator (LSO) is installed")
        lso_operator = LocalStorageOperator()
        if not lso_operator.is_available():
            lso_operator.create_catalog()
            lso_operator.deploy()

    def create_image_tag_mirror_set(self):
        """
        Create or update ImageTagMirrorSet.
        """
        logger.info("Creating or Updating FDF ImageTagMirrorSet")

        imagetag_file = constants.FDF_IMAGE_TAG_MIRROR_SET

        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f {imagetag_file}", silent=True
        )

    def create_image_digest_mirror_set(self):
        """
        Create or update ImageTagMirrorSet.
        """
        logger.info("Creating FDF ImageDigestMirrorSet")
        image_digest_mirror_set = extract_image_digest_mirror_set()

        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f {image_digest_mirror_set}",
            silent=True,
        )
        os.remove(image_digest_mirror_set)

    def create_fdf_service_cr(self):
        """
        Create Fusion Data Foundation Service CR.
        """
        logger.info("Creating FDF service CR")
        run_cmd(
            f"oc --kubeconfig {self.kubeconfig} apply -f {constants.FDF_SERVICE_CR}",
            silent=True,
        )

    def setup_fdf_pre_release_deployment(self):
        """
        Perform steps to prepare for a Pre-release deployment of FDF.
        """
        time.sleep(60)
        wait_for_machineconfigpool_status(node_type="all")

        fdf_image_tag = config.DEPLOYMENT.get("fdf_image_tag")
        fdf_catalog_name = defaults.FUSION_CATALOG_NAME
        fdf_registry = config.DEPLOYMENT.get("fdf_pre_release_registry")
        fdf_image_digest = config.DEPLOYMENT.get("fdf_pre_release_image_digest")
        pull_secret = os.path.join(constants.DATA_DIR, "pull-secret")

        if not fdf_image_digest:
            logger.info("Retrieving imageDigest")
            cmd = f"skopeo inspect docker://{fdf_registry}/{fdf_catalog_name}:{fdf_image_tag} --authfile {pull_secret}"
            catalog_data = run_cmd(cmd)
            fdf_image_digest = json.loads(catalog_data).get("Digest")
            logger.info(f"Retrieved image digest: {fdf_image_digest}")
            config.DEPLOYMENT["fdf_pre_release_image_digest"] = fdf_image_digest

        ocp_version = f"ocp{get_running_ocp_version().replace('.', '')}-t"
        logger.info(f"OCP version: {ocp_version}")
        logger.info("Updating FusionServiceDefinition")
        params_dict = {
            "spec": {
                "onboarding": {
                    "serviceOperatorSubscription": {
                        "multiVersionCatSrcDetails": {
                            ocp_version: {
                                "imageDigest": fdf_image_digest,
                                "registryPath": fdf_registry,
                            }
                        }
                    }
                }
            }
        }
        params = json.dumps(params_dict)
        cmd = (
            f"oc --kubeconfig {self.kubeconfig} -n {constants.FDF_NAMESPACE} patch FusionServiceDefinition "
            f"data-foundation-service -p '{params}' --type merge"
        )
        run_patch_cmd(cmd)

    def verify_fdf_installation(self):
        """
        Verify the FDF installation was successful.
        """
        logger.info("Verifying FDF installation")
        fusion_service_instance_health_check()
        wait_for_storageclusters_crd()
        self.get_installed_version()
        logger.info("FDF successfully installed")

    def get_installed_version(self):
        """
        Retrieve the installed FDF version.

        Returns:
            str: Installed FDF version.

        """
        logger.info("Retrieving installed FDF version")
        results = run_cmd(
            f"oc get FusionServiceInstance {constants.FDF_SERVICE_NAME} "
            f"-n {constants.FDF_NAMESPACE} --kubeconfig {self.kubeconfig} -o yaml"
        )
        version = yaml.safe_load(results)["status"]["currentVersion"]
        config.ENV_DATA["fdf_version"] = version
        logger.info(f"Installed FDF version: {version}")
        return version

    def setup_storage(self):
        """
        Setup storage
        """
        logger.info("Configuring storage.")
        if self.lso_enabled:
            self.ensure_lso_installed()
        self.patch_catalogsource()

        fusion_version = config.ENV_DATA["fusion_version"].replace("v", "")
        fusion_version = version.get_semantic_version(fusion_version, True)

        # Storage configuration method changed in Fusion 2.11
        if fusion_version < version.VERSION_2_11:
            self.create_odfcluster()
            odfcluster_status_check()
        else:
            logger.info("Storage configuration for Fusion 2.11 or greater")
            if self.lso_enabled:
                add_disks_lso()
            clustersetup = StorageClusterSetup()
            create_lvs_resource(self.storage_class, self.storage_class)
            if config.ENV_DATA.get("mark_masters_schedulable", False):
                node.mark_masters_schedulable()
            add_storage_label()
            clustersetup.setup_storage_cluster()
            storagecluster_health_check()

    def patch_catalogsource(self):
        """
        Patch the isf-data-foundation-catalog in order to ensure it is prioritized over redhat-operators.
        """
        logger.info(f"Patching catalogsource {defaults.FUSION_CATALOG_NAME}")
        # TODO: change label for GA versions to not cause issues with future upgrades
        params_dict = {"metadata": {"labels": {"ocs-operator-internal": "true"}}}
        params = json.dumps(params_dict)
        cmd = (
            f"oc --kubeconfig {self.kubeconfig} -n {constants.MARKETPLACE_NAMESPACE} patch CatalogSource "
            f"{defaults.FUSION_CATALOG_NAME} -p '{params}' --type merge"
        )
        run_patch_cmd(cmd)

    @staticmethod
    def create_odfcluster():
        """
        Create OdfCluster CR
        """

        logger.info("Creating OdfCluster CR")
        storageclass = get_storageclass()
        worker_nodes = node.get_worker_nodes()
        with open(constants.FDF_ODFCLUSTER_CR, "r") as f:
            odfcluster_data = yaml.safe_load(f.read())

        odfcluster_data["spec"]["deviceSets"][0]["storageClass"] = storageclass
        odfcluster_data["spec"]["storageNodes"] = worker_nodes

        odfcluster_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="odfcluster", delete=False
        )
        templating.dump_data_to_temp_yaml(odfcluster_data, odfcluster_data_yaml.name)

        run_cmd(f"oc create -f {odfcluster_data_yaml.name}")


@retry((AssertionError, KeyError), 20, 60, backoff=1)
def fusion_service_instance_health_check():
    """
    Ensure the FusionServiceInstance is in the Healthy state.

    Raises:
        AssertionError: If the FusionServiceInstance is not in a completed state.
        KeyError: If the health status isn't present in the FusionServiceInstance data.

    """
    instance = FusionServiceInstance(
        resource_name=constants.FDF_SERVICE_NAME,
        namespace=constants.FDF_NAMESPACE,
    )
    instance_status = instance.data["status"]
    service_health = instance_status["health"]
    install_percent = instance_status["installStatus"]["progressPercentage"]
    assert service_health == "Healthy"
    assert install_percent == 100


@retry((AssertionError, KeyError), 20, 60, backoff=1)
def odfcluster_status_check():
    """
    Ensure the OdfCluster is in a Ready state.

    Raises:
        AssertionError: If the OdfCluster is not in a completed state.
        KeyError: If the status phase isn't present in the OdfCluster data.

    """
    odfcluster = OdfCluster(
        resource_name="odfcluster", namespace="ibm-spectrum-fusion-ns"
    )
    odfcluster_status = odfcluster.data["status"]
    odfcluster_phase = odfcluster_status["phase"]
    assert odfcluster_phase == "Ready"
    ceph_cluster_health = odfcluster_status["cephClusterHealth"]
    assert ceph_cluster_health == "HEALTH_OK"
    logger.info("OdfCluster created successfully")


def extract_image_digest_mirror_set():
    """
    Extract the ImageDigestMirrorSet from the FDF build.

    Returns:
        str: Name of the extracted ImageDigestMirrorSet

    """
    pull_secret = os.path.join(constants.DATA_DIR, "pull-secret")
    fdf_registry = config.DEPLOYMENT.get("fdf_pre_release_registry")
    fdf_catalog_name = defaults.FUSION_CATALOG_NAME
    fdf_image_tag = config.DEPLOYMENT.get("fdf_image_tag")

    filename = constants.FDF_IMAGE_DIGEST_MIRROR_SET_FILENAME
    cmd = (
        f"oc image extract --filter-by-os linux/amd64 --registry-config "
        f"{pull_secret} {fdf_registry}/{fdf_catalog_name}:{fdf_image_tag} --confirm --path /{filename}:./"
    )
    run_cmd(cmd)
    return filename


def is_not_arbiter_node(node_obj):
    """
    Determines if a node contains the arbiter zone label.
    Used to filter arbiter node from node list.

    Args:
        node_obj (ocs_ci.ocs.ocp.OCP): OCP Node object

    Returns:
        bool: True if node doesn't contain the labelj, False if it does

    """
    arbiter_zone = config.DEPLOYMENT.get(
        "arbiter_zone", constants.ARBITER_ZONE_LABEL[0]
    )
    zone_key = "topology.kubernetes.io/zone"
    data = node_obj.data
    metadata = data.get("metadata")
    labels = metadata.get("labels")
    return not labels.get(zone_key) == arbiter_zone


def add_storage_label():
    """
    Add storage label on nodes.
    """
    if config.ENV_DATA.get("mark_masters_schedulable", False):
        all_nodes = node.get_all_nodes()
        nodes = node.get_node_objs(all_nodes)
        # Filter arbiter node if configured
        if config.DEPLOYMENT.get("arbiter_deployment"):
            nodes = list(filter(is_not_arbiter_node, nodes))
    else:
        nodes = node.get_nodes(node_type="worker")
    node.label_nodes(nodes)


@retry(CommandFailed, 12, 5, backoff=1)
def run_patch_cmd(cmd):
    """
    Wrapper for run_cmd so we can retry if an CommandFailed is encountered
    """
    out = run_cmd(cmd)
    assert "patched" in out


@retry((AssertionError, KeyError), 20, 60, backoff=1)
def storagecluster_health_check():
    """
    Ensure the StorageCluster (Ceph backend) is healthy and resilient.

    Raises:
        AssertionError: If the StorageCluster is not in a Ready state
                        or Ceph health is not HEALTH_OK.
        KeyError: If expected status keys are missing.
    """
    storagecluster = StorageCluster(
        resource_name="ocs-storagecluster",
        namespace="openshift-storage",
    )

    status = storagecluster.data.get("status", {})
    phase = status.get("phase")

    logger.info(f"StorageCluster phase: {phase}")

    assert phase == "Ready", f"StorageCluster phase is not Ready (found: {phase})"

    logger.info("StorageCluster is healthy and in Ready state.")


def wait_for_storageclusters_crd():
    """
    Wait for the storageclusters CRD to exist.
    """
    logger.info("Waiting for the StorageClusters CRD to exist")

    @retry((CommandFailed, AssertionError, KeyError), 30, 30, backoff=1)
    def _wait_for_storageclusters_crd():
        storageclusters_crd = CustomResourceDefinition(
            resource_name="storageclusters.ocs.openshift.io",
        )
        status = storageclusters_crd.data.get("status", {})
        conditions = status.get("conditions")
        established_status_exists = False

        for condition in conditions:
            if condition.get("type") == "Established":
                established_status_exists = True
                assert condition.get("status") == "True"

        assert established_status_exists

    _wait_for_storageclusters_crd()


class FusionServiceInstance(OCP):
    def __init__(self, resource_name="", *args, **kwargs):
        super(FusionServiceInstance, self).__init__(
            resource_name=resource_name, kind="FusionServiceInstance", *args, **kwargs
        )


class OdfCluster(OCP):
    def __init__(self, resource_name="", *args, **kwargs):
        super(OdfCluster, self).__init__(
            resource_name=resource_name, kind="OdfCluster", *args, **kwargs
        )


class CustomResourceDefinition(OCP):
    def __init__(self, resource_name="", *args, **kwargs):
        super(CustomResourceDefinition, self).__init__(
            resource_name=resource_name,
            kind="CustomResourceDefinition",
            *args,
            **kwargs,
        )
