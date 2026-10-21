"""
Test topology-aware provisioning in ODF external mode (GA).

Validates that PVs are created in the correct Ceph pool based on the node's
failure domain (hostname or zone) when using the non-resilient StorageClass
with topologyConstrainedPools.

Jira: RHSTOR-5525
"""

import json
import logging
import time

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import external_mode_required
from ocs_ci.framework.testlib import (
    ManageTest,
    brown_squad,
    ignore_leftovers,
    polarion_id,
    tier2,
)
from ocs_ci.helpers.helpers import (
    create_pvc,
    create_pod,
    create_resource,
    is_volume_present_in_backend,
    verify_volume_deleted_in_backend,
    wait_for_resource_state,
)
from ocs_ci.deployment.helpers.external_cluster_helpers import (
    get_external_cluster_instance,
    patch_external_cluster_secret,
    restore_external_cluster_secret,
    save_external_cluster_secret,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import get_worker_nodes, get_node_objs
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    delete_pods,
    get_ocs_operator_pod,
    get_operator_pods,
    get_pod_node,
)
from ocs_ci.utility.utils import TimeoutSampler

log = logging.getLogger(__name__)


def _get_worker_hostnames():
    """
    Get hostnames for all worker nodes.

    Returns:
        list[str]: Worker node hostnames (kubernetes.io/hostname label values).

    """
    worker_names = get_worker_nodes()
    workers = get_node_objs(worker_names)
    return [w.data["metadata"]["labels"][constants.HOSTNAME_LABEL] for w in workers]


def _build_topology_config():
    """
    Build topology configuration from config overrides or auto-detection.

    Returns:
        dict: Keys: pool_names, failure_domain_label, failure_domain_values,
              pool_size, pg_num.

    """
    topo_cfg = config.EXTERNAL_MODE.get("topology", {})
    hostnames = _get_worker_hostnames()

    if len(hostnames) < 3:
        pytest.skip(
            f"Need at least 3 worker nodes for topology test, found {len(hostnames)}"
        )

    pool_names = topo_cfg.get(
        "pool_names",
        [f"topology-pool-{i + 1}" for i in range(3)],
    )
    fd_label = topo_cfg.get("failure_domain_label", "host")
    fd_values = topo_cfg.get("failure_domain_values", hostnames[:3])
    pool_size = topo_cfg.get("pool_size", 3)
    pg_num = topo_cfg.get("pg_num", 32)

    assert len(pool_names) == len(fd_values), (
        f"Topology config mismatch: pool_names={len(pool_names)} "
        f"!= failure_domain_values={len(fd_values)}"
    )

    return {
        "pool_names": pool_names,
        "failure_domain_label": fd_label,
        "failure_domain_values": fd_values,
        "pool_size": pool_size,
        "pg_num": pg_num,
    }


def _build_pool_to_node_map(pool_names, fd_values):
    """
    Build a mapping from pool name to failure domain value (hostname/zone).

    Args:
        pool_names (list[str]): Pool names.
        fd_values (list[str]): Failure domain values (same order as pools).

    Returns:
        dict[str, str]: {pool_name: failure_domain_value}

    """
    return dict(zip(pool_names, fd_values))


def _get_pv_pool(pvc_obj):
    """
    Get the Ceph pool name from a bound PVC's backing PV.

    Args:
        pvc_obj: PVC object (must be Bound).

    Returns:
        str: Pool name from PV's CSI volumeAttributes.

    """
    pv_data = pvc_obj.backed_pv_obj.get()
    return pv_data["spec"]["csi"]["volumeAttributes"]["pool"]


def _restart_operators_and_wait():
    """
    Restart OCS and Rook-Ceph operators, then wait for StorageCluster Ready.

    """
    ns = config.ENV_DATA["cluster_namespace"]

    log.info("Restarting OCS operator")
    ocs_pod = get_ocs_operator_pod(namespace=ns)
    delete_pods([ocs_pod])

    log.info("Restarting Rook-Ceph operator")
    rook_pods = get_operator_pods(namespace=ns)
    delete_pods(rook_pods)

    log.info("Waiting for StorageCluster to reach Ready state")
    sc_ocp = OCP(
        kind="StorageCluster",
        namespace=ns,
    )
    for sample in TimeoutSampler(
        timeout=300,
        sleep=15,
        func=sc_ocp.get,
    ):
        items = sample.get("items", [])
        if items and items[0].get("status", {}).get("phase") == constants.STATUS_READY:
            log.info("StorageCluster is Ready")
            break


def _has_rgw_endpoint():
    """
    Check if the external cluster has RGW deployed.

    Returns:
        bool: True if any node has the 'rgw' role.

    """
    node_roles = config.EXTERNAL_MODE.get("external_cluster_node_roles", {})
    return any("rgw" in node.get("role", []) for node in node_roles.values())


def _build_exporter_topology_params(topo_config, ext_cluster):
    """
    Build the exporter script parameters string for topology-aware setup.

    Starts with the full base params (CephFS, auth, and optionally RGW) to
    ensure the exporter output is complete, then appends topology-specific flags.

    Args:
        topo_config (dict): Topology configuration from _build_topology_config().
        ext_cluster (ExternalCluster): ExternalCluster instance.

    Returns:
        str: Parameter string for ExternalCluster.run_exporter_script().

    """
    include_rgw = _has_rgw_endpoint()
    params = ext_cluster.build_exporter_base_params(include_rgw=include_rgw)

    pools_csv = ",".join(topo_config["pool_names"])
    fd_values_csv = ",".join(topo_config["failure_domain_values"])
    fd_label = topo_config["failure_domain_label"]

    params += (
        f" --topology-pools {pools_csv}"
        f" --topology-failure-domain-label {fd_label}"
        f" --topology-failure-domain-values {fd_values_csv}"
    )
    return params


@brown_squad
@tier2
@ignore_leftovers
@external_mode_required
class TestTopologyAwarenessExternal(ManageTest):
    """
    Test topology-aware provisioning in external mode (GA).

    Validates that PVs are created in the correct Ceph pool based on
    the node's failure domain when using the non-resilient StorageClass.
    """

    @pytest.fixture(autouse=True, scope="class")
    def topology_setup(self, request):
        """
        Set up topology-aware provisioning on the external cluster.

        Steps:
        1. Save original external cluster secret
        2. Auto-detect or load topology config
        3. Create topology pools on external Ceph
        4. Run exporter with topology flags
        5. Patch secret + restart operators
        6. Wait for topology SC to be auto-created

        Cleanup (addfinalizer):
        1. Restore original secret
        2. Restart operators
        3. Delete topology pools
        """
        topo_config = _build_topology_config()
        pool_names = topo_config["pool_names"]
        fd_values = topo_config["failure_domain_values"]

        log.info(
            f"Topology config: pools={pool_names}, "
            f"fd_label={topo_config['failure_domain_label']}, "
            f"fd_values={fd_values}"
        )

        # Save original secret for cleanup
        original_secret = save_external_cluster_secret()

        # Register finalizer BEFORE any mutating operations so cleanup
        # runs even if setup fails partway through (e.g., SSH timeout
        # during pool creation leaves orphaned pools on external Ceph).
        def finalizer():
            log.info("Topology test cleanup: restoring original configuration")

            restore_external_cluster_secret(original_secret)
            _restart_operators_and_wait()

            sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_NON_RESILIENT_RBD
            sc_ocp = OCP(kind=constants.STORAGECLASS)
            try:
                sc_ocp.delete(resource_name=sc_name)
                log.info(f"Deleted topology StorageClass {sc_name}")
            except CommandFailed:
                log.info(f"StorageClass {sc_name} not found, skipping deletion")

            ext_cluster_cleanup = get_external_cluster_instance()
            ext_cluster_cleanup.cleanup_replica_one_pools(pool_names)
            log.info("Topology cleanup completed")

        request.addfinalizer(finalizer)

        # Store config on the class for test methods
        request.cls.topo_config = topo_config
        request.cls.pool_names = pool_names
        request.cls.pool_to_node = _build_pool_to_node_map(pool_names, fd_values)
        request.cls.node_to_pool = {v: k for k, v in request.cls.pool_to_node.items()}

        # Create topology pools on external Ceph
        ext_cluster = get_external_cluster_instance()
        created_pools = ext_cluster.create_topology_pools(
            pool_names=pool_names,
            pool_size=topo_config["pool_size"],
            pg_num=topo_config["pg_num"],
        )
        log.info(f"Created topology pools: {created_pools}")

        # Run exporter with topology flags (includes full base params)
        topology_params = _build_exporter_topology_params(topo_config, ext_cluster)
        log.info(f"Running exporter with topology params: {topology_params}")
        exporter_output = ext_cluster.run_exporter_script(params=topology_params)
        log.info("Exporter script completed successfully")

        # Patch secret and restart operators
        patch_external_cluster_secret(exporter_output)
        _restart_operators_and_wait()

        # Wait for topology SC to be auto-created by the operator.
        # StorageCluster reaches Ready before the SC is reconciled.
        sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_NON_RESILIENT_RBD
        sc_ocp = OCP(kind=constants.STORAGECLASS)
        log.info(f"Waiting for StorageClass {sc_name} to be created")
        sc_created = False
        for sample in TimeoutSampler(
            timeout=120,
            sleep=10,
            func=sc_ocp.get,
            resource_name=sc_name,
            dont_raise=True,
        ):
            if sample:
                log.info(f"StorageClass {sc_name} created by operator")
                sc_created = True
                break
        assert sc_created, f"StorageClass {sc_name} was not created within timeout"

    @polarion_id("OCS-7930")
    def test_topology_sc_auto_created(self):
        """
        Verify that ODF auto-creates the non-resilient StorageClass with
        correct topology parameters after the external cluster secret is
        updated with topology configuration.
        """
        sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_NON_RESILIENT_RBD
        sc_ocp = OCP(kind=constants.STORAGECLASS)
        sc_data = sc_ocp.get(resource_name=sc_name)

        # volumeBindingMode must be WaitForFirstConsumer
        assert (
            sc_data["volumeBindingMode"] == "WaitForFirstConsumer"
        ), f"Expected WaitForFirstConsumer, got {sc_data['volumeBindingMode']}"

        # topologyConstrainedPools must list all pools
        params = sc_data.get("parameters", {})
        topo_pools_raw = params.get("topologyConstrainedPools")
        assert (
            topo_pools_raw
        ), f"topologyConstrainedPools not found in SC {sc_name} parameters"
        topo_pools = json.loads(topo_pools_raw)

        pool_names_in_sc = [p["poolName"] for p in topo_pools]
        for pool in self.pool_names:
            assert (
                pool in pool_names_in_sc
            ), f"Pool {pool} not found in topologyConstrainedPools: {pool_names_in_sc}"

        # topologyFailureDomainLabel should match config
        expected_fd_label = self.topo_config["failure_domain_label"]
        assert params.get("topologyFailureDomainLabel") == expected_fd_label, (
            f"Expected topologyFailureDomainLabel={expected_fd_label}, "
            f"got {params.get('topologyFailureDomainLabel')}"
        )
        log.info(f"StorageClass {sc_name} validated successfully")

    @polarion_id("OCS-7931")
    def test_pvc_pending_without_pod(self, project_factory):
        """
        Verify that PVC with topology SC stays Pending when no pod
        consumes it (WaitForFirstConsumer behavior).
        """
        ns = project_factory().namespace
        pvc = create_pvc(
            namespace=ns,
            sc_name=constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_NON_RESILIENT_RBD,
            size="1Gi",
            access_mode=constants.ACCESS_MODE_RWO,
        )

        # PVC should stay Pending (WaitForFirstConsumer)
        time.sleep(10)
        pvc.reload()
        assert (
            pvc.status == constants.STATUS_PENDING
        ), f"Expected PVC to be Pending, got {pvc.status}"
        log.info("PVC correctly stays Pending without a consuming pod")

    @polarion_id("OCS-7932")
    def test_single_pod_topology_placement(self, project_factory):
        """
        Verify that a single pod's PV is created in the correct topology
        pool based on the scheduled node's failure domain.
        """
        ns = project_factory().namespace
        pvc = create_pvc(
            namespace=ns,
            sc_name=constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_NON_RESILIENT_RBD,
            size="1Gi",
            access_mode=constants.ACCESS_MODE_RWO,
        )

        pod_obj = create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc.name,
            namespace=ns,
        )
        wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=300)
        pod_obj.reload()

        # Get the node where the pod is running
        node_obj = get_pod_node(pod_obj)
        node_hostname = node_obj.data["metadata"]["labels"][constants.HOSTNAME_LABEL]
        log.info(f"Pod scheduled on node: {node_hostname}")

        # Get pool from PV
        pvc.reload()
        wait_for_resource_state(pvc, constants.STATUS_BOUND, timeout=60)
        pv_pool = _get_pv_pool(pvc)
        log.info(f"PV created in pool: {pv_pool}")

        # Verify pool matches the node's expected mapping
        expected_pool = self.node_to_pool.get(node_hostname)
        assert (
            expected_pool is not None
        ), f"Node {node_hostname} not found in topology mapping: {self.node_to_pool}"
        assert pv_pool == expected_pool, (
            f"PV pool {pv_pool} does not match expected pool {expected_pool} "
            f"for node {node_hostname}"
        )
        log.info(f"Topology placement verified: node={node_hostname} -> pool={pv_pool}")

        # Write data and verify RBD image exists in the correct pool
        pod_obj.exec_cmd_on_pod(
            command="dd if=/dev/urandom of=/var/lib/www/html/testfile bs=1M count=50"
        )
        image_uuid = pvc.image_uuid
        assert is_volume_present_in_backend(
            interface=constants.CEPHBLOCKPOOL,
            image_uuid=image_uuid,
            pool_name=pv_pool,
        ), f"RBD image {image_uuid} not found in pool {pv_pool}"
        log.info(f"Data verified in pool {pv_pool}, image uuid {image_uuid}")

    @polarion_id("OCS-7933")
    def test_statefulset_spreads_across_pools(self, project_factory):
        """
        Verify that a 3-replica StatefulSet with topologySpreadConstraints
        places each PV in the correct pool based on each pod's node.
        """
        proj = project_factory()
        ns = proj.namespace
        sc_name = constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_NON_RESILIENT_RBD
        replica_count = len(self.pool_names)

        sts_name = "topo-sts"
        sts_yaml = {
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": sts_name, "namespace": ns},
            "spec": {
                "serviceName": sts_name,
                "replicas": replica_count,
                "selector": {"matchLabels": {"app": sts_name}},
                "template": {
                    "metadata": {"labels": {"app": sts_name}},
                    "spec": {
                        "topologySpreadConstraints": [
                            {
                                "maxSkew": 1,
                                "topologyKey": constants.HOSTNAME_LABEL,
                                "whenUnsatisfiable": "DoNotSchedule",
                                "labelSelector": {"matchLabels": {"app": sts_name}},
                            }
                        ],
                        "containers": [
                            {
                                "name": "test",
                                "image": "busybox",
                                "command": ["sh", "-c", "sleep 3600"],
                                "volumeMounts": [
                                    {"name": "data", "mountPath": "/data"}
                                ],
                            }
                        ],
                    },
                },
                "volumeClaimTemplates": [
                    {
                        "metadata": {"name": "data"},
                        "spec": {
                            "accessModes": [constants.ACCESS_MODE_RWO],
                            "storageClassName": sc_name,
                            "resources": {"requests": {"storage": "1Gi"}},
                        },
                    }
                ],
            },
        }

        create_resource(**sts_yaml)
        log.info(f"Created StatefulSet {sts_name} with {replica_count} replicas")

        # Wait for all pods to be Running
        pod_ocp = OCP(kind=constants.POD, namespace=ns)
        for i in range(replica_count):
            pod_name = f"{sts_name}-{i}"
            log.info(f"Waiting for pod {pod_name}")
            pod_ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=pod_name,
                timeout=300,
            )

        # Verify each pod's PV is in the correct pool
        pools_used = set()
        pvc_ocp = OCP(kind=constants.PVC, namespace=ns)
        for i in range(replica_count):
            pod_name = f"{sts_name}-{i}"
            pvc_name = f"data-{sts_name}-{i}"

            # Get pod's node
            pod_data = pod_ocp.get(resource_name=pod_name)
            node_name = pod_data["spec"]["nodeName"]
            node_objs = get_node_objs([node_name])
            node_hostname = node_objs[0].data["metadata"]["labels"][
                constants.HOSTNAME_LABEL
            ]

            # Get PVC's pool
            pvc_data = pvc_ocp.get(resource_name=pvc_name)
            pv_name = pvc_data["spec"]["volumeName"]
            pv_ocp = OCP(kind=constants.PV)
            pv_data = pv_ocp.get(resource_name=pv_name)
            pv_pool = pv_data["spec"]["csi"]["volumeAttributes"]["pool"]

            expected_pool = self.node_to_pool.get(node_hostname)
            assert expected_pool is not None, (
                f"Node {node_hostname} not found in topology mapping: "
                f"{self.node_to_pool}"
            )
            assert pv_pool == expected_pool, (
                f"Pod {pod_name} on node {node_hostname}: "
                f"PV pool {pv_pool} != expected {expected_pool}"
            )
            pools_used.add(pv_pool)
            log.info(f"Pod {pod_name}: node={node_hostname}, pool={pv_pool} - CORRECT")

        assert len(pools_used) == replica_count, (
            f"Expected {replica_count} different pools, "
            f"got {len(pools_used)}: {pools_used}"
        )
        log.info(f"All {replica_count} topology pools used: {pools_used}")

    @polarion_id("OCS-7934")
    def test_pvc_deletion_cleans_rbd_image(self, project_factory):
        """
        Verify that deleting a PVC removes the RBD image from the Ceph pool
        (reclaimPolicy: Delete).
        """
        ns = project_factory().namespace
        pvc = create_pvc(
            namespace=ns,
            sc_name=constants.DEFAULT_EXTERNAL_MODE_STORAGECLASS_NON_RESILIENT_RBD,
            size="1Gi",
            access_mode=constants.ACCESS_MODE_RWO,
        )

        pod_obj = create_pod(
            interface_type=constants.CEPHBLOCKPOOL,
            pvc_name=pvc.name,
            namespace=ns,
        )
        wait_for_resource_state(pod_obj, constants.STATUS_RUNNING, timeout=300)
        pod_obj.reload()

        # Write some data
        pod_obj.exec_cmd_on_pod(
            command="dd if=/dev/urandom of=/var/lib/www/html/testfile bs=1M count=10"
        )

        # Record pool and image uuid
        pvc.reload()
        pv_pool = _get_pv_pool(pvc)
        image_uuid = pvc.image_uuid
        log.info(f"RBD image uuid {image_uuid} in pool {pv_pool}")

        # Verify image exists before deletion
        assert is_volume_present_in_backend(
            interface=constants.CEPHBLOCKPOOL,
            image_uuid=image_uuid,
            pool_name=pv_pool,
        )

        # Delete pod and PVC
        pod_obj.delete()
        pod_obj.ocp.wait_for_delete(resource_name=pod_obj.name, timeout=120)
        pvc.delete()
        pvc.ocp.wait_for_delete(resource_name=pvc.name, timeout=120)

        # Verify RBD image is removed (reclaimPolicy: Delete)
        assert verify_volume_deleted_in_backend(
            interface=constants.CEPHBLOCKPOOL,
            image_uuid=image_uuid,
            pool_name=pv_pool,
            timeout=120,
        ), f"RBD image {image_uuid} was not deleted from pool {pv_pool}"
        log.info(
            f"RBD image {image_uuid} removed from pool {pv_pool} after PVC deletion"
        )
