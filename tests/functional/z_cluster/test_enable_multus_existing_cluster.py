"""
Test module for enabling Multus networking on an existing OCS cluster.
"""

import logging
import json
import tempfile

from ocs_ci.framework.testlib import polarion_id

from ocs_ci.utility.reporting import get_polarion_id
from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_managed_service,
    skipif_external_mode,
    brown_squad,
    vsphere_platform_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_cluster,
    verify_multus_network,
    verify_storage_cluster,
)
from ocs_ci.ocs.resources.pod import get_osd_pods, wait_for_storage_pods
from ocs_ci.utility import templating
from ocs_ci.utility.utils import run_cmd, TimeoutSampler
from ocs_ci.utility.vsphere import VSPHERE


logger = logging.getLogger(__name__)


@brown_squad
@skipif_managed_service
@skipif_external_mode
@tier2
@vsphere_platform_required
@polarion_id(get_polarion_id())
class TestEnableMultusExistingCluster(ManageTest):
    """
    Test class for enabling Multus on an existing cluster.
    """

    @tier2
    def test_enable_multus_on_existing_cluster(self):
        """
        Test enabling Multus networking on an existing OCS cluster.

        Steps:
        1. Verify StorageCluster is ready
        2. Verify OSD pods are up and running
        3. Add interface to compute VMs for vSphere platform
        4. Install NMState for vSphere platform
        5. Create NetworkAttachmentDefinition using existing templates
        6. Update StorageCluster to use Multus networking
        7. Wait for OSD pods to restart with Multus configuration
        8. Verify Multus networking is configured on pods
        """
        logger.info("Starting test to enable Multus on existing cluster")

        logger.info("Step 1: Verifying StorageCluster is ready")
        verify_storage_cluster()
        logger.info("StorageCluster is Ready")

        logger.info("Step 2: Verifying OSD pods are running")
        wait_for_storage_pods()
        osd_pods = get_osd_pods()
        logger.info(f"All {len(osd_pods)} OSD pods are running")

        sc = get_storage_cluster()

        logger.info("Step 3: Adding interface to compute VMs for vSphere platform")
        # Add interface to compute VMs for multus networking
        vsphere_obj = VSPHERE()
        vsphere_obj.add_interface_to_compute_vms()

        logger.info("Interface added to compute VMs successfully")

        logger.info("Step 5: Creating Multus NetworkAttachmentDefinitions")

        # Static configuration for vSphere multus networking
        create_public_net = False  # Disabled for vSphere as per user request
        create_cluster_net = True

        # Static multus configuration values
        multus_public_net_name = "public-net"
        multus_public_net_namespace = "openshift-storage"
        multus_public_net_interface = constants.VSPHERE_MULTUS_INTERFACE  # "ens224"
        multus_public_net_range = "192.168.20.0/24"
        multus_public_net_type = "macvlan"
        multus_public_net_mode = "bridge"

        multus_cluster_net_name = "private-net"
        multus_cluster_net_namespace = "openshift-storage"
        multus_cluster_net_interface = constants.VSPHERE_MULTUS_INTERFACE  # "ens224"
        multus_cluster_net_range = "192.168.30.0/24"
        multus_cluster_net_mode = "bridge"

        # Create Public Network
        if create_public_net:
            logger.info("Creating Multus public network")
            nad_to_load = constants.MULTUS_PUBLIC_NET_YAML

            public_net_data = templating.load_yaml(nad_to_load)
            public_net_data["metadata"]["name"] = multus_public_net_name
            public_net_data["metadata"]["namespace"] = multus_public_net_namespace

            public_net_config_str = public_net_data["spec"]["config"]
            public_net_config_dict = json.loads(public_net_config_str)
            public_net_config_dict["master"] = multus_public_net_interface
            public_net_config_dict["ipam"]["range"] = multus_public_net_range

            public_net_config_dict["type"] = multus_public_net_type
            public_net_config_dict["mode"] = multus_public_net_mode
            public_net_data["spec"]["config"] = json.dumps(public_net_config_dict)

            public_net_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="multus_public", delete=False
            )
            templating.dump_data_to_temp_yaml(public_net_data, public_net_yaml.name)
            run_cmd(f"oc create -f {public_net_yaml.name}")
            logger.info(
                f"Created public NetworkAttachmentDefinition: {multus_public_net_name}"
            )

        # Create Cluster Network
        if create_cluster_net:
            logger.info("Creating Multus cluster network")
            nad_to_load = constants.MULTUS_CLUSTER_NET_YAML

            cluster_net_data = templating.load_yaml(nad_to_load)
            cluster_net_data["metadata"]["name"] = multus_cluster_net_name
            cluster_net_data["metadata"]["namespace"] = multus_cluster_net_namespace

            cluster_net_config_str = cluster_net_data["spec"]["config"]
            cluster_net_config_dict = json.loads(cluster_net_config_str)
            cluster_net_config_dict["master"] = multus_cluster_net_interface

            cluster_net_config_dict["ipam"]["range"] = multus_cluster_net_range

            cluster_net_config_dict["mode"] = multus_cluster_net_mode
            cluster_net_data["spec"]["config"] = json.dumps(cluster_net_config_dict)

            cluster_net_yaml = tempfile.NamedTemporaryFile(
                mode="w+", prefix="multus_cluster", delete=False
            )
            templating.dump_data_to_temp_yaml(cluster_net_data, cluster_net_yaml.name)
            run_cmd(f"oc create -f {cluster_net_yaml.name}")
            logger.info(
                f"Created cluster NetworkAttachmentDefinition: {multus_cluster_net_name}"
            )

        logger.info("Step 6: Updating StorageCluster to enable Multus networking")

        selectors = {}
        if create_public_net:
            selectors["public"] = (
                f"{multus_public_net_namespace}/{multus_public_net_name}"
            )

        if create_cluster_net:
            selectors["cluster"] = (
                f"{multus_cluster_net_namespace}/{multus_cluster_net_name}"
            )

        patch = {
            "spec": {
                "network": {
                    "provider": "multus",
                    "selectors": selectors,
                }
            }
        }

        sc.patch(params=json.dumps(patch), format_type="merge")
        logger.info(f"StorageCluster patched with Multus configuration: {selectors}")

        logger.info("Step 7: Waiting for OSD pods to restart with Multus configuration")

        def check_osd_pods_have_multus():
            """Check if OSD pods have Multus annotations."""
            osd_pods = get_osd_pods()
            if not osd_pods:
                return False

            for pod in osd_pods:
                annotations = pod.data["metadata"].get("annotations", {})
                if "k8s.v1.cni.cncf.io/networks" not in annotations:
                    logger.info(f"Pod {pod.name} does not have Multus annotation yet")
                    return False

                networks = annotations["k8s.v1.cni.cncf.io/networks"]

                # Check for configured networks
                if create_public_net:
                    if multus_public_net_name not in networks:
                        logger.info(f"Pod {pod.name} public network not configured yet")
                        return False

                if create_cluster_net:
                    if multus_cluster_net_name not in networks:
                        logger.info(
                            f"Pod {pod.name} cluster network not configured yet"
                        )
                        return False

            logger.info(f"All {len(osd_pods)} OSD pods have Multus configuration")
            return True

        # Wait up to 10 minutes for pods to reconfigure
        sample = TimeoutSampler(
            timeout=600,
            sleep=30,
            func=check_osd_pods_have_multus,
        )
        if not sample.wait_for_func_status(result=True):
            raise TimeoutError(
                "OSD pods did not get Multus configuration within timeout"
            )

        logger.info("Step 8: Verifying Multus configuration using existing validation")
        verify_multus_network()

        logger.info("Verifying StorageCluster is Ready after Multus enablement")
        verify_storage_cluster()
        logger.info("StorageCluster is Ready")

        logger.info("Successfully enabled Multus on existing cluster")
