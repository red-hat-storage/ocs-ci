"""
Test module for enabling Multus networking on an existing OCS cluster.
"""

import logging
import json
import tempfile
import pytest
import ipaddress

from ocs_ci.framework.testlib import (
    ManageTest,
    tier2,
    skipif_managed_service,
    skipif_external_mode,
    brown_squad,
)
from ocs_ci.framework.pytest_customization.marks import ignore_leftovers
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources.storage_cluster import (
    get_storage_cluster,
    verify_storage_cluster,
    verify_multus_network,
)
from ocs_ci.ocs.resources.pod import (
    get_osd_pods,
    wait_for_storage_pods,
    get_ceph_tools_pod,
)
from ocs_ci.utility import templating
from ocs_ci.utility.operators import NMStateOperator
from ocs_ci.utility.utils import run_cmd, TimeoutSampler, ceph_health_check


logger = logging.getLogger(__name__)


@brown_squad
@skipif_managed_service
@skipif_external_mode
@tier2
@ignore_leftovers
@pytest.mark.skipif(
    config.ENV_DATA["platform"].lower() != constants.BAREMETAL_PLATFORM,
    reason="Test runs ONLY on bare metal platform",
)
# TODO: Add polarion ID
class TestEnableMultusExistingCluster(ManageTest):
    """
    Test class for enabling Multus on an existing cluster.
    """

    @tier2
    def test_enable_multus_on_existing_cluster(self, pvc_factory, pod_factory):
        """
        Test enabling Multus networking on an existing OCS cluster.

        Steps:
        1. Verify StorageCluster is ready
        2. Verify OSD pods are up and running
        3. Install NMState operator and create an instance, then configure node network
           configuration policy on all worker nodes
        4. Create NetworkAttachmentDefinition using existing templates
        5. Update StorageCluster to use Multus networking
        6. Wait for OSD pods to restart with Multus configuration
        7. Verify Multus networking is configured on pods (excluding MDS pod checks)
        8. Create RBD and CephFS PVCs and attach to pods
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

        logger.info("Step 3: Installing NMState operator and creating an instance")
        nmstate_obj = NMStateOperator(create_catalog=True)
        nmstate_obj.deploy()
        from ocs_ci.helpers.helpers import (
            configure_node_network_configuration_policy_on_all_worker_nodes,
        )

        configure_node_network_configuration_policy_on_all_worker_nodes()

        logger.info("Step 4: Creating Multus NetworkAttachmentDefinitions")

        create_public_net = config.ENV_DATA.get("multus_create_public_net", True)
        create_cluster_net = config.ENV_DATA.get("multus_create_cluster_net", True)

        multus_public_net_name = config.ENV_DATA.get(
            "multus_public_net_name", "public-net"
        )
        multus_public_net_namespace = config.ENV_DATA.get(
            "multus_public_net_namespace", "openshift-storage"
        )
        multus_public_net_interface = config.ENV_DATA.get(
            "multus_public_net_interface_bm", "enp1s0f1"
        )
        multus_public_net_range = config.ENV_DATA.get(
            "multus_public_net_range", "192.168.20.0/24"
        )
        multus_public_net_type = config.ENV_DATA.get(
            "multus_public_net_type", "macvlan"
        )
        multus_public_net_mode = config.ENV_DATA.get("multus_public_net_mode", "bridge")

        multus_cluster_net_name = config.ENV_DATA.get(
            "multus_cluster_net_name", "private-net"
        )
        multus_cluster_net_namespace = config.ENV_DATA.get(
            "multus_cluster_net_namespace", "openshift-storage"
        )
        multus_cluster_net_interface = config.ENV_DATA.get(
            "multus_cluster_net_interface_bm", "enp1s0f1"
        )
        multus_cluster_net_range = config.ENV_DATA.get(
            "multus_cluster_net_range", "192.168.30.0/24"
        )
        multus_cluster_net_mode = config.ENV_DATA.get(
            "multus_cluster_net_mode", "bridge"
        )

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
            public_net_config_dict["ipam"]["routes"] = [
                {"dst": config.ENV_DATA["multus_destination_route"]}
            ]

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

        logger.info("Step 5: Updating StorageCluster to enable Multus networking")

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

        # Get the storage cluster resource name
        resource_name = sc.get()["items"][0]["metadata"]["name"]
        sc.patch(
            resource_name=resource_name, params=json.dumps(patch), format_type="merge"
        )
        logger.info(f"StorageCluster patched with Multus configuration: {selectors}")

        logger.info("Step 6: Waiting for OSD pods to restart with Multus configuration")

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

        logger.info("Step 7: Verifying Multus configuration (excluding MDS pod checks)")
        verify_multus_network(skip_mds=True)

        logger.info("Verifying StorageCluster is Ready after Multus enablement")
        verify_storage_cluster()
        logger.info("StorageCluster is Ready")

        config.ENV_DATA["is_multus_enabled"] = True

        logger.info("Step 8: Creating RBD and CephFS PVCs with pods")

        rbd_pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        logger.info(f"Created RBD PVC: {rbd_pvc_obj.name}")

        rbd_pod_obj = pod_factory(
            interface=constants.CEPHBLOCKPOOL,
            pvc=rbd_pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        logger.info(f"Created pod with RBD PVC: {rbd_pod_obj.name}")

        cephfs_pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            size=5,
            access_mode=constants.ACCESS_MODE_RWO,
            status=constants.STATUS_BOUND,
        )
        logger.info(f"Created CephFS PVC: {cephfs_pvc_obj.name}")

        cephfs_pod_obj = pod_factory(
            interface=constants.CEPHFILESYSTEM,
            pvc=cephfs_pvc_obj,
            status=constants.STATUS_RUNNING,
        )
        logger.info(f"Created pod with CephFS PVC: {cephfs_pod_obj.name}")

        logger.info("Successfully enabled Multus on existing cluster")

    @pytest.fixture(scope="function", autouse=True)
    def disable_multus_teardown(self, request):
        """
        Teardown fixture to disable Multus and restore cluster to original state.
        This ensures the cluster is brought back to its original state even if
        the test fails prematurely.
        """
        yield

        logger.info("Teardown: Disabling Multus on the cluster")
        try:
            sc = get_storage_cluster()
            resource_name = sc.get()["items"][0]["metadata"]["name"]
            sc_data = sc.get(resource_name=resource_name)

            # Check if Multus is enabled
            if "network" in sc_data.get("spec", {}):
                network_spec = sc_data["spec"]["network"]
                if network_spec.get("provider") == "multus":
                    # Remove ceph network configurations before removing network spec
                    tool_pod = get_ceph_tools_pod()
                    tool_pod.exec_ceph_cmd(
                        ceph_cmd="ceph config rm global public_network", format=None
                    )
                    tool_pod.exec_ceph_cmd(
                        ceph_cmd="ceph config rm global cluster_network", format=None
                    )

                    # Remove Multus configuration by removing the network spec using JSON patch
                    patch = '[{"op": "remove", "path": "/spec/network"}]'
                    sc.patch(
                        resource_name=resource_name,
                        params=patch,
                        format_type="json",
                    )
                    logger.info("Multus disabled on StorageCluster")

                    # Wait for OSD pods to restart without Multus
                    logger.info(
                        "Waiting for OSD pods to restart without Multus configuration"
                    )
                    wait_for_storage_pods()
                    config.ENV_DATA["is_multus_enabled"] = False

                    # Verify OSD pods don't have public or cluster network IPs in Ceph dump
                    logger.info("Verifying ceph OSD dump")
                    public_network = ipaddress.ip_network(
                        config.ENV_DATA.get(
                            "multus_public_net_range", "192.168.20.0/24"
                        )
                    )
                    cluster_network = ipaddress.ip_network(
                        config.ENV_DATA.get(
                            "multus_cluster_net_range", "192.168.30.0/24"
                        )
                    )

                    osd_dump_dict = get_ceph_tools_pod().exec_ceph_cmd(
                        "ceph osd dump --format json"
                    )
                    osds_data = osd_dump_dict["osds"]

                    for osd_data in osds_data:
                        osd_id = osd_data["osd"]
                        # Check public address
                        if osd_data.get("public_addr"):
                            public_ip = ipaddress.ip_address(
                                osd_data["public_addr"].split("/")[0].split(":")[0]
                            )
                            assert public_ip not in public_network, (
                                f"\nOSD {osd_id} has public network IP {public_ip} "
                                f"in range {public_network}"
                                f"\nActual public address: {osd_data['public_addr']}"
                            )
                        # Check cluster address
                        if osd_data.get("cluster_addr"):
                            cluster_ip = ipaddress.ip_address(
                                osd_data["cluster_addr"].split("/")[0].split(":")[0]
                            )
                            assert cluster_ip not in cluster_network, (
                                f"\nOSD {osd_id} has cluster network IP {cluster_ip} "
                                f"in range {cluster_network}"
                                f"\nActual cluster address: {osd_data['cluster_addr']}"
                            )

                    verify_storage_cluster()
                    logger.info("StorageCluster is Ready after Multus disablement")
                else:
                    logger.info("Multus is not enabled, skipping teardown")
            else:
                logger.info("No network configuration found, skipping teardown")
        except Exception as e:
            logger.warning(f"Error during Multus teardown: {e}")

        try:
            logger.info("Silence the ceph warnings by archiving the crash")
            tool_pod = get_ceph_tools_pod()
            tool_pod.exec_ceph_cmd(ceph_cmd="ceph crash archive-all", format=None)
            logger.info(
                "Perform Ceph health check with increased timeout after silencing warnings"
            )
            # Increase timeout: tries=40 (default 20), delay=60 (default 30)
            ceph_health_check(tries=40, delay=60, fix_ceph_health=True)
        except Exception as e:
            logger.warning(f"Error during ceph crash archive or health check: {e}")
