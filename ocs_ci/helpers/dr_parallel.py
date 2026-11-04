"""
Generic helpers to run several DR workloads through the same test flow in
parallel (e.g. an RBD and a CephFS workload inside one RDR test).

Every workload is handled independently:

* each stage runs in its own config-safe thread, so switching the cluster
  context in one workload never affects another one (see
  ``MultiClusterConfig.cur_index``);
* when a workload fails (or skips) in a stage it is dropped from the following
  stages, while the remaining workloads continue;
* at the end a per-workload report is logged and the test is failed (or
  skipped) with a combined message.

Usage::

    runner = ParallelWorkloadRunner({"rbd": rbd_workload, "cephfs": fs_workload})
    runner.run_stage("create drpc", create_drpc)           # parallel
    runner.run_stage("verify alert", verify_alert, serial=True)  # one by one
    runner.report()
    runner.assert_all_passed()

Stage functions receive the workload object. Use a closure or
``functools.partial`` to pass extra arguments.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import pytest

from ocs_ci.framework import config, config_safe_thread_pool_task
from ocs_ci.helpers import dr_helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.node import get_node_objs, wait_for_nodes_status
from ocs_ci.ocs.resources.pod import wait_for_pods_to_be_running
from ocs_ci.utility.utils import ceph_health_check

logger = logging.getLogger(__name__)

PASS = "PASS"
FAIL = "FAIL"
SKIP = "SKIP"


@dataclass
class WorkloadResult:
    """
    Outcome of one workload in a parallel run.

    Attributes:
        name (str): Workload label, e.g. 'rbd' or 'cephfs'
        status (str): PASS, FAIL or SKIP
        failed_stage (str): Name of the stage in which the workload stopped
        error (BaseException): The exception which stopped the workload
        stages (dict): Stage name -> (status, duration in seconds)

    """

    name: str
    status: str = PASS
    failed_stage: str = ""
    error: BaseException = None
    stages: dict = field(default_factory=dict)


@dataclass
class InterfaceGroup:
    """
    Workloads of one PVC interface (RBD or CephFS) together with the DR details
    that are looked up once and reused by every stage of a parallel test.

    Attributes:
        name (str): Label used in logs/report ('rbd' or 'cephfs')
        pvc_interface (str): constants.CEPHBLOCKPOOL or constants.CEPHFILESYSTEM
        workloads (list): Deployed workload objects of this interface
        drpc_objs (list): DRPC object of each workload, same order
        primary (str): Primary cluster name
        secondary (str): Secondary cluster name
        scheduling_interval (int): DRPolicy scheduling interval in minutes
        cg_cephfs_enabled (bool): Consistency groups enabled for CephFS
        last_sync_times (list): Last lastGroupSyncTime value of each DRPC

    """

    name: str
    pvc_interface: str
    workloads: list = field(default_factory=list)
    drpc_objs: list = field(default_factory=list)
    primary: str = ""
    secondary: str = ""
    scheduling_interval: int = 0
    cg_cephfs_enabled: bool = False
    last_sync_times: list = field(default_factory=list)

    @property
    def is_cephfs(self):
        return self.pvc_interface == constants.CEPHFILESYSTEM


class ParallelWorkloadRunner:
    """
    Run test stages for several workloads in parallel with per-workload
    fault isolation and reporting.
    """

    def __init__(self, workloads):
        """
        Args:
            workloads (dict): Workload label -> workload object. The label is
                used in logs and in the report.

        """
        self.workloads = dict(workloads)
        self.results = {name: WorkloadResult(name) for name in self.workloads}

    @property
    def names(self):
        """
        Returns:
            list: Labels of the workloads that are still running (not failed/skipped)

        """
        return [n for n, r in self.results.items() if r.status == PASS]

    def run_stage(self, stage_name, func, serial=False):
        """
        Run ``func(workload)`` for every workload which is still healthy.

        Args:
            stage_name (str): Name used for logs and the report
            func (callable): Called as ``func(workload)`` once per workload
            serial (bool): Run the workloads one after another instead of in
                parallel. Use it for steps that share a single resource, e.g.
                the one Selenium/ACM UI session.

        Returns:
            dict: Workload label -> return value of ``func`` for workloads
                where the stage succeeded

        """
        active = self.names
        if not active:
            logger.warning(f"Stage '{stage_name}': no healthy workloads left")
            return {}
        logger.info(
            f"Stage '{stage_name}' on {active} ({'serial' if serial else 'parallel'})"
        )
        # Threads start in the context of the caller
        context_index = config.cur_index

        def _run(name):
            start = time.time()
            try:
                value = func(self.workloads[name])
            except pytest.skip.Exception as exc:
                self._record(name, stage_name, SKIP, start, exc)
                return name, None, False
            except BaseException as exc:  # noqa: B902 - isolate every failure
                logger.exception(
                    f"Workload '{name}' failed in stage '{stage_name}': {exc}"
                )
                self._record(name, stage_name, FAIL, start, exc)
                return name, None, False
            self._record(name, stage_name, PASS, start)
            return name, value, True

        outputs = {}
        if serial or len(active) == 1:
            results = [
                config_safe_thread_pool_task(context_index, _run, name)
                for name in active
            ]
        else:
            with ThreadPoolExecutor(max_workers=len(active)) as executor:
                futures = [
                    executor.submit(
                        config_safe_thread_pool_task, context_index, _run, name
                    )
                    for name in active
                ]
                results = [future.result() for future in futures]
        for name, value, ok in results:
            if ok:
                outputs[name] = value
        return outputs

    def fail_all(self, stage_name, error):
        """
        Mark every workload that is still healthy as failed. Use it when a
        step shared by all workloads (e.g. recovering the primary cluster)
        fails and none of them can continue.

        Args:
            stage_name (str): Name of the shared stage
            error (BaseException): The error that stopped it

        """
        start = time.time()
        for name in self.names:
            self._record(name, stage_name, FAIL, start, error)

    def _record(self, name, stage_name, status, start, error=None):
        result = self.results[name]
        result.stages[stage_name] = (status, round(time.time() - start, 1))
        if status != PASS:
            result.status = status
            result.failed_stage = stage_name
            result.error = error

    def report(self):
        """
        Log a per-workload summary and return it as text.

        Returns:
            str: The summary

        """
        lines = ["Parallel workload results:"]
        for result in self.results.values():
            line = f"  [{result.status}] {result.name}"
            if result.failed_stage:
                line += f" - {result.failed_stage}: {result.error!r}"
            lines.append(line)
            for stage, (status, duration) in result.stages.items():
                lines.append(f"      {status:4} {stage} ({duration}s)")
        summary = "\n".join(lines)
        logger.info(summary)
        return summary

    def add_user_properties(self, request, names=None):
        """
        Record workload outcomes as ``user_properties`` entries on the pytest
        item, so that they are visible in the test report.

        Args:
            request (pytest.FixtureRequest): The test's request fixture
            names (list): Workloads to record; all of them by default

        """
        for name in names or self.results:
            request.node.user_properties.append(
                (f"workload_{name}", self.results[name].status)
            )

    def assert_passed(self, name, owner=None):
        """
        Fail (or skip) the current test according to the outcome of one
        workload.

        Args:
            name (str): The workload label
            owner (str): Node ID of the test that executed the run, mentioned
                in the message so the full log can be found

        """
        result = self.results[name]
        where = f" (full run log: {owner})" if owner else ""
        if result.status == FAIL:
            pytest.fail(
                f"{name} failed in '{result.failed_stage}': {result.error!r}{where}"
            )
        if result.status == SKIP:
            pytest.skip(f"{name} skipped in '{result.failed_stage}': {result.error}")

    def assert_all_passed(self):
        """
        Fail the test when any workload failed; skip it when every workload
        skipped. Call it after ``report()`` so all workloads were given the
        chance to finish first.

        """
        failed = [r for r in self.results.values() if r.status == FAIL]
        if failed:
            details = "; ".join(
                f"{r.name} failed in '{r.failed_stage}': {r.error!r}" for r in failed
            )
            pytest.fail(
                f"{len(failed)}/{len(self.results)} workload(s) failed: {details}"
            )
        if all(r.status == SKIP for r in self.results.values()):
            skipped = "; ".join(f"{r.name}: {r.error}" for r in self.results.values())
            pytest.skip(f"All workloads skipped: {skipped}")


def run_stage_with_primary_cluster_down(
    runner, stage_name, func, nodes_multicluster, primary_cluster, wait_minutes
):
    """
    Run a stage for all healthy workloads while the primary cluster is down,
    then bring the primary cluster back and wait until it is healthy.

    The nodes are started only after the stage finished for EVERY workload, so
    one workload failing early never brings the primary cluster back while the
    others are still being failed over. The nodes are always started again,
    whatever happened in the stage. When the primary cluster does not recover,
    all the remaining workloads are marked as failed.

    Args:
        runner (ParallelWorkloadRunner): The runner
        stage_name (str): Name of the stage, for logs and report
        func (callable): Stage function, called as ``func(workload)``
        nodes_multicluster (list): The ``nodes_multicluster`` fixture value
        primary_cluster (str): Name of the primary cluster to bring down
        wait_minutes (int): Minutes to wait after the stage, before starting
            the nodes

    """
    config.switch_to_cluster_by_name(primary_cluster)
    primary_index = config.cur_index
    primary_nodes = get_node_objs()
    try:
        logger.info(f"Stopping nodes of primary cluster: {primary_cluster}")
        nodes_multicluster[primary_index].stop_nodes(primary_nodes)
        runner.run_stage(stage_name, func)
        if runner.names:
            logger.info(
                f"Waiting for {wait_minutes} minutes before starting nodes of "
                f"primary cluster: {primary_cluster}"
            )
            time.sleep(wait_minutes * 60)
    finally:
        nodes_multicluster[primary_index].start_nodes(primary_nodes)
    try:
        wait_for_nodes_status([node.name for node in primary_nodes])
        logger.info("Wait for all pods in openshift-storage to be running")
        assert wait_for_pods_to_be_running(
            timeout=720
        ), "Not all the pods reached running state"
        logger.info("Checking for Ceph Health OK")
        ceph_health_check()
    except Exception as exc:
        logger.exception("Primary cluster did not recover")
        runner.fail_all("recover primary cluster", exc)


@dataclass
class _ParallelRun:
    """
    The one execution shared by the pytest items of a parallel test.

    Attributes:
        runner (ParallelWorkloadRunner): The runner that executed the flow
        owner (str): Node ID of the item which executed it
        pending (set): Interfaces whose pytest item did not report yet

    """

    runner: ParallelWorkloadRunner
    owner: str
    pending: set


_PARALLEL_RUNS = {}
_INTERFACE_LABELS = {
    constants.CEPHBLOCKPOOL: constants.RBD_INTERFACE,
    constants.CEPHFILESYSTEM: constants.CEPHFS_INTERFACE,
}


def _run_key(item):
    """
    Key which groups the pytest items of one parallel run: same test function
    and same ``primary_cluster_down`` value, whatever the interface is.
    """
    callspec = getattr(item, "callspec", None)
    if callspec is None:
        return None
    return item.nodeid.split("[")[0], callspec.params.get("primary_cluster_down")


def _interface_label(item):
    """
    Returns:
        str: 'rbd' or 'cephfs' for the pytest item's ``pvc_interface`` parameter

    """
    return _INTERFACE_LABELS[item.callspec.params["pvc_interface"]]


def run_rbd_cephfs_test(request, execute):
    """
    Run a parallel RBD + CephFS test, but report every interface as a separate
    pytest test.

    The test is parametrized with ``primary_cluster_down`` and ``pvc_interface``
    so there is one pytest item per interface. The first item of a
    ``primary_cluster_down`` value executes the flow for all the interfaces
    selected in the session; the other items only report the outcome of their
    own interface. A failure of one interface therefore fails only its own
    item, and each interface appears as a separate result in junit,
    ReportPortal and the failed/passed test case lists.

    Args:
        request (pytest.FixtureRequest): The test's request fixture. The test
            must have the ``primary_cluster_down`` and ``pvc_interface``
            parameters.
        execute (callable): Called as ``execute(runner)`` by the first item;
            runs all the stages. Fixtures that are needed only for the
            execution should be fetched there with ``request.getfixturevalue``
            so that the other items do not set them up.

    """
    item = request.node
    key = _run_key(item)
    name = _interface_label(item)

    run = _PARALLEL_RUNS.get(key)
    if run is None:
        # Only the items that did not run yet: when an earlier item of this run
        # was skipped in setup, its interface is not executed again here.
        items = request.session.items
        selected = {
            _interface_label(i): i
            for i in items[items.index(item) :]
            if _run_key(i) == key
        }
        runner = ParallelWorkloadRunner(
            {
                label: InterfaceGroup(label, pvc_interface)
                for pvc_interface, label in _INTERFACE_LABELS.items()
                if label in selected
            }
        )
        run = _PARALLEL_RUNS[key] = _ParallelRun(runner, item.nodeid, set(selected))
        # The other items do nothing on the clusters, so their RDR health
        # check must not skip (and hide) the result of their interface.
        for sibling in selected.values():
            if sibling is not item:
                sibling.add_marker(pytest.mark.skip_rdr_health_check)
        logger.info(
            f"{item.nodeid} executes the parallel run for {sorted(selected)}; "
            "the other interfaces only report their own result"
        )
        try:
            execute(runner)
        except (Exception, pytest.fail.Exception, pytest.skip.Exception) as exc:
            logger.exception("Parallel run stopped unexpectedly")
            runner.fail_all("parallel run", exc)
        finally:
            runner.report()
    else:
        logger.info(
            f"The parallel run for this test was executed by {run.owner}; "
            f"reporting the result of '{name}'"
        )

    run.runner.add_user_properties(request, [name])
    run.pending.discard(name)
    if not run.pending:
        _PARALLEL_RUNS.pop(key, None)
    run.runner.assert_passed(name, run.owner)


def collect_dr_details(group, drpc_objs, **name_kwargs):
    """
    Fill in the DR details of an interface group: DRPC objects, primary and
    secondary cluster, scheduling interval and, for CephFS, wait for the
    replication destinations on the secondary cluster.

    Args:
        group (InterfaceGroup): The group to fill in
        drpc_objs (list): DRPC object of each workload of the group
        **name_kwargs: Keyword arguments for ``get_current_primary_cluster_name``
            and ``get_current_secondary_cluster_name`` (namespace, workload_type
            / discovered_apps, resource_name)

    """
    group.drpc_objs = drpc_objs
    group.primary = dr_helpers.get_current_primary_cluster_name(**name_kwargs)
    group.secondary = dr_helpers.get_current_secondary_cluster_name(**name_kwargs)
    group.scheduling_interval = int(
        drpc_objs[0].drpolicy_obj.get()["spec"]["schedulingInterval"][:-1]
    )
    if not group.is_cephfs:
        return
    group.cg_cephfs_enabled = dr_helpers.is_cg_cephfs_enabled()
    config.switch_to_cluster_by_name(group.secondary)
    for wl in group.workloads:
        if group.cg_cephfs_enabled:
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=wl.workload_namespace,
                should_exist=True,
            )
            dr_helpers.wait_for_resource_count(
                kind=constants.VOLUMESNAPSHOT,
                namespace=wl.workload_namespace,
                expected_count=wl.workload_pvc_count,
            )
        dr_helpers.wait_for_replication_destinations_creation(
            wl.workload_pvc_count, wl.workload_namespace
        )


def get_common_primary_and_wait_time(runner, primary_down):
    """
    Get the primary cluster shared by all healthy interfaces and the wait time
    (``2 x scheduling interval``, the larger one) used before DR actions.

    Args:
        runner (ParallelWorkloadRunner): The runner
        primary_down (bool): The primary cluster is going to be brought down

    Returns:
        tuple: (primary cluster name, wait time in minutes)

    Raises:
        pytest.fail.Exception: When the primary cluster is to be brought down
            but the interfaces run on different primary clusters

    """
    groups = [runner.workloads[name] for name in runner.names]
    if primary_down and len({g.primary for g in groups}) > 1:
        pytest.fail(
            "Workloads run on different primary clusters "
            f"({ {g.name: g.primary for g in groups} }); cannot bring down "
            "a single primary cluster for all of them"
        )
    return groups[0].primary, 2 * max(g.scheduling_interval for g in groups)


def run_failover_stage(
    runner, func, primary_down, nodes_multicluster, primary_cluster, wait_minutes
):
    """
    Run the failover stage; with ``primary_down`` the primary cluster is down
    during the stage (see :func:`run_stage_with_primary_cluster_down`).

    Args:
        runner (ParallelWorkloadRunner): The runner
        func (callable): Failover function, called as ``func(group)``
        primary_down (bool): Bring the primary cluster down during failover
        nodes_multicluster (list): The ``nodes_multicluster`` fixture value
        primary_cluster (str): Name of the primary cluster
        wait_minutes (int): Minutes to wait before starting the nodes again

    """
    if primary_down:
        run_stage_with_primary_cluster_down(
            runner, "failover", func, nodes_multicluster, primary_cluster, wait_minutes
        )
    else:
        runner.run_stage("failover", func)


def verify_cephfs_replication(group, new_primary, new_secondary):
    """
    Verify the CephFS replication resources after a DR action: they must be
    gone from the cluster where the workload now runs and exist on the cluster
    that is now the secondary. Does nothing for a non-CephFS group.

    Args:
        group (InterfaceGroup): The group to verify
        new_primary (str): Cluster where the workload runs now
        new_secondary (str): Cluster that is now the secondary

    """
    if not group.is_cephfs:
        return
    cg = group.cg_cephfs_enabled
    for wl in group.workloads:
        config.switch_to_cluster_by_name(new_primary)
        dr_helpers.wait_for_replication_destinations_deletion(wl.workload_namespace)
        if cg:
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=wl.workload_namespace,
                should_exist=False,
            )
        config.switch_to_cluster_by_name(new_secondary)
        dr_helpers.wait_for_replication_destinations_creation(
            wl.workload_pvc_count, wl.workload_namespace
        )
        if cg:
            dr_helpers.wait_for_resource_existence(
                kind=constants.REPLICATION_GROUP_DESTINATION,
                namespace=wl.workload_namespace,
                should_exist=True,
            )
            dr_helpers.wait_for_resource_count(
                kind=constants.VOLUMESNAPSHOT,
                namespace=wl.workload_namespace,
                expected_count=wl.workload_pvc_count,
            )
