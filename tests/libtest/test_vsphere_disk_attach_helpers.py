import logging

import pytest

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ManageTest,
    libtest,
    brown_squad,
    ignore_leftovers,
    skipif_no_lso,
    vsphere_platform_required,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.constants import NODE_READY, VM_POWERED_ON
from ocs_ci.ocs.node import get_nodes, wait_for_nodes_status
from ocs_ci.utility.vsphere import VSPHERE

log = logging.getLogger(__name__)


@brown_squad
@libtest
@ignore_leftovers
@skipif_no_lso
@vsphere_platform_required
class TestVsphereDiskAttachHelpers(ManageTest):
    """
    Exercise the vSphere disk-attachment helper methods that were fixed
    to prevent etcd quorum loss (control-plane stabilization) and to
    retry VM power-on when a VM fails to obtain an IP address.

    Prerequisites:
      - vSphere UPI cluster with LSO installed (not necessarily provisioned)
      - At least one compute and one control-plane VM in the resource pool
    """

    @pytest.fixture(autouse=True)
    def setup_vsphere(self):
        """Set up the VSPHERE connection and discover VMs."""
        self.vsphere = VSPHERE(
            config.ENV_DATA["vsphere_server"],
            config.ENV_DATA["vsphere_user"],
            config.ENV_DATA["vsphere_password"],
        )
        self.cluster_name = config.ENV_DATA["cluster_name"]
        self.datacenter = config.ENV_DATA["vsphere_datacenter"]
        self.cluster = config.ENV_DATA["vsphere_cluster"]

        vms = self.vsphere.get_all_vms_in_pool(
            self.cluster_name,
            self.datacenter,
            self.cluster,
        )
        self.worker_vms = [vm for vm in vms if "compute" in vm.name]
        self.master_vms = [vm for vm in vms if "control-plane" in vm.name]
        log.info(
            "Discovered %d worker VM(s) and %d control-plane VM(s)",
            len(self.worker_vms),
            len(self.master_vms),
        )
        assert self.worker_vms, "No compute VMs found in resource pool"
        assert self.master_vms, "No control-plane VMs found in resource pool"

    def test_is_node_ready_worker(self):
        """
        Verify _is_node_ready returns True for all running worker nodes.
        """
        for vm in self.worker_vms:
            status = self.vsphere.get_vm_power_status(vm)
            if status != VM_POWERED_ON:
                log.info("Skipping VM %s (status=%s)", vm.name, status)
                continue
            result = self.vsphere._is_node_ready(vm.name)
            log.info("_is_node_ready(%s) = %s", vm.name, result)
            assert result, (
                f"Worker node {vm.name} is powered on but _is_node_ready "
                f"returned False"
            )

    def test_is_node_ready_control_plane(self):
        """
        Verify _is_node_ready returns True for all running control-plane nodes.
        """
        for vm in self.master_vms:
            status = self.vsphere.get_vm_power_status(vm)
            if status != VM_POWERED_ON:
                log.info("Skipping VM %s (status=%s)", vm.name, status)
                continue
            result = self.vsphere._is_node_ready(vm.name)
            log.info("_is_node_ready(%s) = %s", vm.name, result)
            assert result, (
                f"Control-plane node {vm.name} is powered on but "
                f"_is_node_ready returned False"
            )

    def test_is_node_ready_catches_api_errors(self):
        """
        Verify _is_node_ready returns False (not raises) for a non-existent
        node name. This simulates the API-unavailable / node-not-found
        scenario that the method must handle gracefully.
        """
        result = self.vsphere._is_node_ready("nonexistent-node-xyz")
        log.info("_is_node_ready('nonexistent-node-xyz') = %s", result)
        assert result is False, (
            "_is_node_ready should return False for a nonexistent node, "
            "not raise an exception"
        )

    def test_start_vm_with_retry_worker(self):
        """
        Power-cycle a single worker VM using _start_vm_with_retry and
        confirm the VM comes back up with an IP and reaches Ready status.
        """

        vm = self.worker_vms[0]
        log.info(
            "Testing _start_vm_with_retry on worker VM %s",
            vm.name,
        )

        self.vsphere.stop_vms(vms=[vm])
        self.vsphere._start_vm_with_retry(vm)

        status = self.vsphere.get_vm_power_status(vm)
        assert status == VM_POWERED_ON, (
            f"VM {vm.name} should be powered on after _start_vm_with_retry, "
            f"got {status}"
        )

        log.info("Waiting for node %s to reach Ready status", vm.name)
        wait_for_nodes_status(
            node_names=[vm.name],
            status=NODE_READY,
            timeout=300,
        )
        log.info(
            "Worker VM %s successfully restarted via _start_vm_with_retry", vm.name
        )

    def test_wait_for_control_plane_stable(self):
        """
        Power-cycle a single control-plane VM and use
        _wait_for_control_plane_stable to verify it rejoins with proper
        etcd stabilization wait. This is the core fix that prevents etcd
        quorum loss when processing control-plane nodes sequentially.
        """
        vm = self.master_vms[0]
        log.info(
            "Testing _wait_for_control_plane_stable on control-plane VM %s",
            vm.name,
        )

        self.vsphere.stop_vms(vms=[vm])
        self.vsphere._start_vm_with_retry(vm)
        self.vsphere._wait_for_control_plane_stable(vm.name)

        assert self.vsphere._is_node_ready(vm.name), (
            f"Control-plane node {vm.name} should be Ready after "
            f"_wait_for_control_plane_stable"
        )
        log.info(
            "Control-plane VM %s stabilized successfully",
            vm.name,
        )

    def test_sequential_control_plane_restart(self):
        """
        Power-cycle each control-plane VM one at a time with
        _wait_for_control_plane_stable between each restart.
        This validates that sequential processing preserves etcd quorum
        by ensuring a previously restarted node is fully stabilized
        before the next one is power-cycled.
        """
        for i, vm in enumerate(self.master_vms):
            log.info(
                "Sequential control-plane restart: VM %s (%d/%d)",
                vm.name,
                i + 1,
                len(self.master_vms),
            )
            self.vsphere.stop_vms(vms=[vm])
            self.vsphere._start_vm_with_retry(vm)
            self.vsphere._wait_for_control_plane_stable(vm.name)

            assert self.vsphere._is_node_ready(vm.name), (
                f"Control-plane node {vm.name} not Ready after "
                f"stabilization (step {i + 1}/{len(self.master_vms)})"
            )
            log.info(
                "Control-plane VM %s stabilized (%d/%d)",
                vm.name,
                i + 1,
                len(self.master_vms),
            )

        log.info(
            "All %d control-plane VMs restarted sequentially with "
            "etcd stabilization — no quorum loss",
            len(self.master_vms),
        )

    def test_all_nodes_ready_after_operations(self):
        """
        Final sanity check: verify all worker and control-plane nodes
        are in Ready status.
        """
        from ocs_ci.ocs.node import wait_for_nodes_status

        worker_names = [n.name for n in get_nodes(constants.WORKER_MACHINE)]
        master_names = [n.name for n in get_nodes(constants.MASTER_MACHINE)]
        all_names = worker_names + master_names

        log.info("Verifying all nodes are Ready: %s", all_names)
        wait_for_nodes_status(all_names, NODE_READY, timeout=120)
        log.info("All %d nodes confirmed Ready", len(all_names))
