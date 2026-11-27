"""
Helper functions for Fusion Access Metro-DR (MDR) test cases.

Fusion Access for SAN uses IBM Storage Scale as the underlying storage
technology, therefore a Fusion Access cluster has **no** CephCluster or
StorageCluster object. Any generic MDR helper that reaches out to Ceph (for
example ``ceph_health_check``) must not be used here. Everything else in
:mod:`ocs_ci.helpers.dr_helpers` is reused as-is.

The module is built around :class:`FusionAccessDRApp`, a thin uniform wrapper
around the different DR workload classes (CNV ApplicationSet VMs, CNV
Discovered Apps VMs, Recipe protected Discovered Apps and plain busybox
Deployment apps). The wrapper hides the per-workload-type differences in
placement names, DRPC location and relocate arguments so the test code can
drive every application through the exact same calls.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.helpers import dr_helpers
from ocs_ci.helpers.cnv_helpers import (
    cal_md5sum_vm,
    check_fio_status,
    run_dd_io,
    run_fio,
)
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.dr.dr_workload import validate_data_integrity
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.node import get_nodes_in_statuses, get_node_objs
from ocs_ci.ocs.resources.drpc import DRPC
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running

logger = logging.getLogger(__name__)

# Application categories handled by the Fusion Access Metro-DR test cases
APP_TYPE_APPSET_VM = "appset-vm"
APP_TYPE_DISCOVERED_VM = "discovered-vm"
APP_TYPE_DISCOVERED_RECIPE = "discovered-recipe"
APP_TYPE_BUSYBOX_SUBSCRIPTION = "busybox-subscription"
APP_TYPE_BUSYBOX_APPSET = "busybox-appset"

# Application categories backed by a CNV VirtualMachine
VM_APP_TYPES = (APP_TYPE_APPSET_VM, APP_TYPE_DISCOVERED_VM)

# Application categories protected as ODF Discovered (imperative) Apps
DISCOVERED_APP_TYPES = (APP_TYPE_DISCOVERED_VM, APP_TYPE_DISCOVERED_RECIPE)

# Application categories deployed through OpenShift GitOps ApplicationSets
APPSET_APP_TYPES = (APP_TYPE_APPSET_VM, APP_TYPE_BUSYBOX_APPSET)

# Metro-DR replicates synchronously, so the DRPolicy scheduling interval - which
# expresses the RPO - has to be zero for every protected application.
ZERO_RPO_SCHEDULING_INTERVALS = ("0m", "0")

# systemd unit name used for the continuously running VM IO workload
CONTINUOUS_IO_SERVICE_NAME = "fusionaccess_dr_io"

# Number of 1KiB blocks written by dd for every data baseline file in a VM
DD_IO_BLOCK_COUNT = "10240"

# Resource kinds compared between relocation cycles to detect resource leaks
RESOURCE_LEAK_KINDS = (
    constants.PVC,
    constants.PV,
    constants.POD,
)

# Namespaces owned by Fusion Access that must stay healthy across relocations
FUSION_ACCESS_NAMESPACES = (defaults.FUSION_ACCESS_NAMESPACE,)


class FusionAccessDRApp:
    """
    Uniform view over a single DR protected application.

    The Fusion Access Metro-DR test cases protect four different kinds of
    application at the same time. Each underlying workload class exposes its
    placement name under a different attribute and needs slightly different
    arguments when a DR action is triggered. This wrapper normalises all of
    that so callers can treat every application identically.
    """

    def __init__(self, workload, app_type):
        """
        Args:
            workload (DRWorkload): Deployed workload instance
            app_type (str): One of the ``APP_TYPE_*`` module constants

        Raises:
            ValueError: If an unknown application type is passed

        """
        known_app_types = (
            APP_TYPE_APPSET_VM,
            APP_TYPE_DISCOVERED_VM,
            APP_TYPE_DISCOVERED_RECIPE,
            APP_TYPE_BUSYBOX_SUBSCRIPTION,
            APP_TYPE_BUSYBOX_APPSET,
        )
        if app_type not in known_app_types:
            raise ValueError(
                f"Unknown application type '{app_type}', expected one of {known_app_types}"
            )
        self.workload = workload
        self.app_type = app_type

    def __repr__(self):
        return f"{self.app_type}/{self.name}"

    @property
    def name(self):
        """
        str: Human readable application name used in log messages

        """
        return getattr(self.workload, "workload_name", None) or self.placement_name

    @property
    def namespace(self):
        """
        str: Namespace the application workload runs in

        """
        return self.workload.workload_namespace

    @property
    def is_vm(self):
        """
        bool: True when the application is backed by a CNV VirtualMachine

        """
        return self.app_type in VM_APP_TYPES

    @property
    def is_discovered_app(self):
        """
        bool: True when the application is protected as a Discovered App

        """
        return self.app_type in DISCOVERED_APP_TYPES

    @property
    def workload_type(self):
        """
        str: Workload type understood by :mod:`ocs_ci.helpers.dr_helpers`

        """
        if self.is_discovered_app:
            return constants.DISCOVERED_APPS
        if self.app_type in APPSET_APP_TYPES:
            return constants.APPLICATION_SET
        return constants.SUBSCRIPTION

    @property
    def placement_name(self):
        """
        str: Placement name of the application

        """
        if self.is_discovered_app:
            return self.workload.discovered_apps_placement_name
        if self.app_type == APP_TYPE_APPSET_VM:
            return self.workload.cnv_workload_placement_name
        if self.app_type == APP_TYPE_BUSYBOX_APPSET:
            return self.workload.appset_placement_name
        return self.workload.sub_placement_name

    @property
    def drpc_namespace(self):
        """
        str: Namespace holding the DRPC of the application on the hub cluster

        """
        if self.is_discovered_app:
            return constants.DR_OPS_NAMESPACE
        if self.app_type in APPSET_APP_TYPES:
            return constants.GITOPS_CLUSTER_NAMESPACE
        return self.namespace

    @property
    def drpc_name(self):
        """
        str: Name of the DRPC protecting the application

        """
        if self.is_discovered_app:
            return self.placement_name
        return f"{self.placement_name}-drpc"

    @property
    def vrg_name(self):
        """
        str: VolumeReplicationGroup name, only meaningful for Discovered Apps

        """
        return self.placement_name if self.is_discovered_app else ""

    @property
    def vm_obj(self):
        """
        obj: VirtualMachine object of the application, None for non VM apps

        """
        return getattr(self.workload, "vm_obj", None)

    @property
    def vm_username(self):
        """
        str: SSH user of the VM, None for non VM applications

        """
        return getattr(self.workload, "vm_username", None)


def get_app_drpc(app):
    """
    Build the DRPC object of the given application.

    Note that instantiating a DRPC switches the active context to the hub
    cluster, so callers that care about the current context must restore it.

    Args:
        app (FusionAccessDRApp): Application to fetch the DRPC for

    Returns:
        DRPC: DRPC object of the application

    """
    return DRPC(namespace=app.drpc_namespace, resource_name=app.drpc_name)


def get_app_primary_cluster(app):
    """
    Get the cluster the application is currently running on.

    Args:
        app (FusionAccessDRApp): Application to query

    Returns:
        str: Name of the current primary managed cluster

    """
    return dr_helpers.get_current_primary_cluster_name(
        namespace=app.namespace,
        workload_type=app.workload_type,
        discovered_apps=app.is_discovered_app,
        resource_name=app.drpc_name,
    )


def get_app_secondary_cluster(app):
    """
    Get the cluster the application would be relocated to.

    Args:
        app (FusionAccessDRApp): Application to query

    Returns:
        str: Name of the current secondary managed cluster

    """
    return dr_helpers.get_current_secondary_cluster_name(
        namespace=app.namespace,
        workload_type=app.workload_type,
        discovered_apps=app.is_discovered_app,
        resource_name=app.drpc_name,
    )


def switch_to_app_primary(app):
    """
    Switch the active context to the cluster the application runs on.

    Every command that talks to a workload - ``virtctl ssh`` into a VM or
    ``oc exec`` into a pod - runs against the active context, so the context
    has to follow the application as it moves between the sites.

    Args:
        app (FusionAccessDRApp): Application to switch the context to

    Returns:
        str: Name of the cluster the context was switched to

    """
    primary_cluster = get_app_primary_cluster(app)
    config.switch_to_cluster_by_name(primary_cluster)
    return primary_cluster


def start_continuous_vm_io(apps, runtime=86400, size="1G", rate="1m"):
    """
    Start a never ending FIO workload inside every VM based application.

    The IO is run from a systemd unit configured with ``Restart=always`` so it
    survives the VM restarts that happen on every relocation and keeps the
    cluster under load for the whole duration of the test. That is why this
    only has to be called once, before the first relocation: ``run_fio`` reads
    the guest OS from the VM spec, a query bound to the cluster the VM object
    was built against, so it must run while the VMs are still on that cluster.

    Args:
        apps (list): List of FusionAccessDRApp instances
        runtime (int): FIO runtime in seconds for a single service invocation
        size (str): Size of the FIO test file inside the VM
        rate (str): IO rate limit passed to FIO

    """
    restore_index = config.cur_index
    vm_apps = [app for app in apps if app.is_vm]
    logger.info(f"Starting continuous IO on {len(vm_apps)} VM based applications")
    for app in vm_apps:
        switch_to_app_primary(app)
        logger.info(f"Starting FIO service on VM of application {app}")
        run_fio(
            vm_obj=app.vm_obj,
            size=size,
            runtime=runtime,
            rate=rate,
            filename=f"/{CONTINUOUS_IO_SERVICE_NAME}.dat",
            fio_service_name=CONTINUOUS_IO_SERVICE_NAME,
        )
    config.switch_ctx(restore_index)


def verify_continuous_vm_io(apps, stage):
    """
    Verify the continuous FIO workload is still running inside every VM.

    The VM username is passed explicitly. Letting it be looked up from the VM
    spec would run an ``oc`` query bound to the cluster the VM object was built
    against, which after a relocation is the cluster the VM no longer runs on.

    Args:
        apps (list): List of FusionAccessDRApp instances
        stage (str): Test stage the check runs at, used in log messages

    Raises:
        AssertionError: If the IO workload is not running on one of the VMs

    """
    restore_index = config.cur_index
    for app in [app for app in apps if app.is_vm]:
        switch_to_app_primary(app)
        try:
            io_running = check_fio_status(
                vm_obj=app.vm_obj,
                fio_service_name=CONTINUOUS_IO_SERVICE_NAME,
                username=app.vm_username,
            )
        except CommandFailed as ex:
            # systemctl status exits non zero for a unit which is not active
            logger.error(
                f"Could not read the status of the {CONTINUOUS_IO_SERVICE_NAME} "
                f"service on VM {app.workload.vm_name} of application {app}: {ex}"
            )
            io_running = False
        logger.assertion(
            f"Continuous IO after {stage}: app={app}, vm={app.workload.vm_name}, "
            f"running={io_running}"
        )
        assert io_running, (
            f"Continuous IO is not running on VM {app.workload.vm_name} of "
            f"application {app} after {stage}"
        )
    config.switch_ctx(restore_index)


class DataBaseline:
    """
    Records the data written to the protected applications and its checksums.

    A new file is written to every VM based application at each stage of the
    test and its MD5 checksum is remembered. All previously recorded files are
    re-verified after every relocation, which makes a silent corruption of
    older data detectable and not only of the data written last.

    The busybox Deployment applications maintain their own hash file while they
    run, so for those applications only the namespace is tracked and the
    verification is delegated to
    :func:`ocs_ci.ocs.dr.dr_workload.validate_data_integrity`.
    """

    def __init__(self):
        # file path inside the VM -> {application name: md5 checksum}
        self.vm_checksums = {}

    @property
    def tracked_files(self):
        """
        list: File paths that have been written and checksummed so far

        """
        return list(self.vm_checksums)

    def add_data(self, apps, file_path, size=DD_IO_BLOCK_COUNT):
        """
        Write a new file to every VM based application and record its checksum.

        Args:
            apps (list): List of FusionAccessDRApp instances
            file_path (str): Full path of the file to write inside the VMs
            size (str): Number of 1KiB blocks written by dd inside the VM

        """
        restore_index = config.cur_index
        vm_apps = [app for app in apps if app.is_vm]
        logger.info(
            f"Writing {file_path} on {len(vm_apps)} VM based applications and "
            f"recording its checksum"
        )
        checksums = {}
        for app in vm_apps:
            switch_to_app_primary(app)
            checksums[app.name] = run_dd_io(
                vm_obj=app.vm_obj,
                file_path=file_path,
                size=size,
                username=app.vm_username,
                verify=True,
            )
            logger.info(
                f"Checksum recorded: app={app}, file={file_path}, "
                f"md5={checksums[app.name]}"
            )
        self.vm_checksums[file_path] = checksums
        config.switch_ctx(restore_index)

    def verify(self, apps, stage):
        """
        Re-verify every recorded checksum and the busybox hash files.

        Args:
            apps (list): List of FusionAccessDRApp instances
            stage (str): Test stage the check runs at, used in log messages

        Raises:
            AssertionError: On any checksum mismatch

        """
        restore_index = config.cur_index
        for app in apps:
            switch_to_app_primary(app)
            if not app.is_vm:
                logger.info(f"Verifying hash file of application {app} after {stage}")
                validate_data_integrity(app.namespace)
                continue
            for file_path, checksums in self.vm_checksums.items():
                current_md5 = cal_md5sum_vm(
                    vm_obj=app.vm_obj,
                    file_path=file_path,
                    username=app.vm_username,
                )
                expected_md5 = checksums[app.name]
                logger.assertion(
                    f"Data integrity after {stage}: app={app}, file={file_path}, "
                    f"expected={expected_md5}, actual={current_md5}, "
                    f"match={expected_md5 == current_md5}"
                )
                assert expected_md5 == current_md5, (
                    f"Data corruption detected after {stage} on application {app}: "
                    f"checksum of {file_path} changed from {expected_md5} to {current_md5}"
                )
        config.switch_ctx(restore_index)


def verify_app_resources_created(app, timeout=1800):
    """
    Wait for all workload resources of the application on the current cluster.

    Args:
        app (FusionAccessDRApp): Application to verify
        timeout (int): Time in seconds to wait for the resources

    """
    dr_helpers.wait_for_all_resources_creation(
        app.workload.workload_pvc_count,
        app.workload.workload_pod_count,
        app.namespace,
        timeout=timeout,
        discovered_apps=app.is_discovered_app,
        vrg_name=app.vrg_name,
    )
    if app.is_vm:
        dr_helpers.wait_for_cnv_workload(
            vm_name=app.workload.vm_name,
            namespace=app.namespace,
            phase=constants.STATUS_RUNNING,
        )


def verify_app_resources_deleted(app, timeout=1800):
    """
    Wait for all workload resources of the application to vanish from the
    current cluster.

    Args:
        app (FusionAccessDRApp): Application to verify
        timeout (int): Time in seconds to wait for the deletion

    """
    dr_helpers.wait_for_all_resources_deletion(
        namespace=app.namespace,
        timeout=timeout,
        discovered_apps=app.is_discovered_app,
        vrg_name=app.vrg_name,
        skip_vrg_check=app.is_discovered_app,
    )


def relocate_app(app):
    """
    Relocate a single application to its current secondary managed cluster.

    Args:
        app (FusionAccessDRApp): Application to relocate

    Returns:
        tuple: (old primary cluster name, new primary cluster name)

    """
    old_primary = get_app_primary_cluster(app)
    target_cluster = get_app_secondary_cluster(app)
    logger.info(f"Relocating application {app}: {old_primary} -> {target_cluster}")

    relocate_kwargs = dict(
        preferred_cluster=target_cluster,
        namespace=app.namespace,
        workload_type=app.workload_type,
        workload_placement_name=app.placement_name,
    )
    if app.is_discovered_app:
        # Discovered Apps are not reconciled by ACM, the resources on the old
        # primary have to be removed by the test, which dr_helpers.relocate()
        # does when it is handed the workload instance.
        relocate_kwargs.update(
            discovered_apps=True,
            old_primary=old_primary,
            workload_instance=app.workload,
        )
    dr_helpers.relocate(**relocate_kwargs)
    return old_primary, target_cluster


def relocate_dr_apps(apps, cycle):
    """
    Run one complete relocation cycle for every protected application.

    Each application is relocated to its current secondary cluster, the
    resources are verified to be gone from the cluster it came from and to be
    running on the cluster it moved to, and the DRPC is verified to settle in
    the ``Completed`` progression state.

    Args:
        apps (list): List of FusionAccessDRApp instances
        cycle (int): Relocation cycle number, used in log messages

    Returns:
        str: Name of the cluster the applications are running on afterwards

    """
    old_primary = get_app_primary_cluster(apps[0])
    new_primary = get_app_secondary_cluster(apps[0])
    logger.info(
        f"Relocation cycle {cycle}: relocating {len(apps)} applications from "
        f"{old_primary} to {new_primary}"
    )
    for app in apps:
        app_old_primary, app_new_primary = relocate_app(app)
        assert (app_old_primary, app_new_primary) == (old_primary, new_primary), (
            f"Application {app} is not placed like the other protected applications, "
            f"it was relocated from {app_old_primary} to {app_new_primary} while the "
            f"rest moved from {old_primary} to {new_primary}"
        )

    logger.info(
        f"Relocation cycle {cycle}: verifying resources are removed from {old_primary}"
    )
    config.switch_to_cluster_by_name(old_primary)
    for app in apps:
        verify_app_resources_deleted(app)

    logger.info(
        f"Relocation cycle {cycle}: verifying applications are running on {new_primary}"
    )
    config.switch_to_cluster_by_name(new_primary)
    for app in apps:
        verify_app_resources_created(app)

    verify_relocate_completed(apps, cycle)
    return new_primary


def verify_relocate_completed(apps, cycle):
    """
    Verify the DRPC of every application reports a completed relocation.

    Args:
        apps (list): List of FusionAccessDRApp instances
        cycle (int): Relocation cycle number, used in log messages

    Raises:
        AssertionError: If a DRPC does not reach the Completed progression

    """
    with config.RunWithAcmConfigContext():
        for app in apps:
            drpc_obj = get_app_drpc(app)
            logger.info(
                f"Waiting for DRPC {drpc_obj.resource_name} of application {app} to "
                f"complete relocation cycle {cycle}"
            )
            drpc_obj.wait_for_progression_status(
                status=constants.STATUS_COMPLETED, timeout=900
            )


def verify_rpo_zero(apps, stage):
    """
    Verify RPO=0 is preserved for every protected application.

    Metro-DR replicates synchronously, therefore a zero RPO is expressed by a
    DRPolicy scheduling interval of ``0m`` combined with a DRPC that reports
    both ``PeerReady`` and ``ClusterDataProtected``. A non-zero scheduling
    interval or a DRPC that never reaches those conditions means data is not
    protected synchronously anymore.

    Args:
        apps (list): List of FusionAccessDRApp instances
        stage (str): Test stage the check runs at, used in log messages

    Raises:
        AssertionError: If RPO=0 is not preserved for one of the applications

    """
    with config.RunWithAcmConfigContext():
        for app in apps:
            drpc_obj = get_app_drpc(app)
            scheduling_interval = drpc_obj.drpolicy_obj.get()["spec"][
                "schedulingInterval"
            ]
            logger.assertion(
                f"RPO check after {stage}: app={app}, drpolicy={drpc_obj.drpolicy}, "
                f"schedulingInterval={scheduling_interval}, "
                f"zero_rpo={scheduling_interval in ZERO_RPO_SCHEDULING_INTERVALS}"
            )
            assert scheduling_interval in ZERO_RPO_SCHEDULING_INTERVALS, (
                f"RPO=0 is not preserved after {stage} for application {app}: "
                f"DRPolicy {drpc_obj.drpolicy} has a scheduling interval of "
                f"{scheduling_interval} instead of 0m"
            )
            drpc_obj.wait_for_peer_ready_status()
            drpc_obj.wait_for_clusterdataprotected_status()


def collect_resource_snapshot(apps, cluster_names):
    """
    Count the workload resources of every application on both managed clusters.

    The counts are summed over the managed clusters so that the snapshot does
    not depend on which site currently hosts the applications. Comparing two
    snapshots taken at the same point of a relocation cycle therefore shows
    resources that were left behind on a site.

    Args:
        apps (list): List of FusionAccessDRApp instances
        cluster_names (list): Names of the managed clusters to inspect

    Returns:
        dict: Mapping of ``(namespace, kind)`` to the total resource count

    """
    restore_index = config.cur_index
    snapshot = {}
    for cluster_name in cluster_names:
        config.switch_to_cluster_by_name(cluster_name)
        for app in apps:
            for kind in RESOURCE_LEAK_KINDS:
                count = dr_helpers.get_resource_count(kind, app.namespace)
                key = (app.namespace, kind)
                snapshot[key] = snapshot.get(key, 0) + count
                logger.debug(
                    f"Resource count: cluster={cluster_name}, namespace={app.namespace}, "
                    f"kind={kind}, count={count}"
                )
    config.switch_ctx(restore_index)
    return snapshot


def verify_no_resource_leaks(baseline_snapshot, current_snapshot, stage):
    """
    Compare two resource snapshots and fail on any resource left behind.

    Args:
        baseline_snapshot (dict): Snapshot taken before the relocation cycles
        current_snapshot (dict): Snapshot taken after the relocation cycles
        stage (str): Test stage the check runs at, used in log messages

    Raises:
        AssertionError: If any namespace holds more resources than the baseline

    """
    leaked = {
        key: (baseline_snapshot.get(key, 0), count)
        for key, count in current_snapshot.items()
        if count > baseline_snapshot.get(key, 0)
    }
    logger.assertion(
        f"Resource leak check after {stage}: compared "
        f"{len(current_snapshot)} namespace/kind counters, leaked={len(leaked)}"
    )
    assert not leaked, (
        f"Resource leak detected after {stage}. The following "
        f"namespace/kind counters grew (expected, actual): {leaked}"
    )


def verify_system_stability(cluster_names, stage):
    """
    Verify both Fusion Access managed clusters are healthy.

    Fusion Access clusters have no CephCluster, so cluster health is asserted
    through the node readiness, the FusionAccess CR phase and the pods of the
    Fusion Access namespaces.

    Args:
        cluster_names (list): Names of the managed clusters to verify
        stage (str): Test stage the check runs at, used in log messages

    Raises:
        AssertionError: If one of the managed clusters is not healthy

    """
    # Imported here on purpose, ocs_ci.deployment imports back into
    # ocs_ci.helpers and a module level import would be circular.
    from ocs_ci.deployment.fusion_access import fusion_access_status_check

    restore_index = config.cur_index
    for cluster_name in cluster_names:
        config.switch_to_cluster_by_name(cluster_name)
        logger.info(f"Verifying stability of managed cluster {cluster_name}")

        all_nodes = get_node_objs()
        not_ready_nodes = get_nodes_in_statuses(
            [constants.NODE_NOT_READY, constants.NODE_NOT_READY_SCHEDULING_DISABLED],
            node_objs=all_nodes,
        )
        logger.assertion(
            f"Node readiness after {stage}: cluster={cluster_name}, "
            f"total={len(all_nodes)}, not_ready={len(not_ready_nodes)}"
        )
        assert not not_ready_nodes, (
            f"Managed cluster {cluster_name} has nodes which are not ready after "
            f"{stage}: {[node.name for node in not_ready_nodes]}"
        )

        fusion_access_status_check()

        for namespace in FUSION_ACCESS_NAMESPACES:
            pods_running = wait_for_pods_to_be_running(
                namespace=namespace,
                timeout=900,
                skip_for_status=[constants.STATUS_COMPLETED],
            )
            logger.assertion(
                f"Fusion Access pods after {stage}: cluster={cluster_name}, "
                f"namespace={namespace}, all_running={pods_running}"
            )
            assert pods_running, (
                f"Not all pods of namespace {namespace} are running on managed "
                f"cluster {cluster_name} after {stage}"
            )
    config.switch_ctx(restore_index)


def verify_workloads_healthy(apps, stage):
    """
    Verify every protected application is up and serving IO.

    Args:
        apps (list): List of FusionAccessDRApp instances
        stage (str): Test stage the check runs at, used in log messages

    """
    restore_index = config.cur_index
    for app in apps:
        switch_to_app_primary(app)
        verify_app_resources_created(app)
    config.switch_ctx(restore_index)
    verify_continuous_vm_io(apps, stage)
