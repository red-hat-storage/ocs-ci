"""
Aggressive PVC snapshot/restore background operations for Krkn/resiliency testing.

Exercises snapshot creation, restore, nesting, parallel operations, mount/integrity
checks, Vdbench I/O on restored PVCs, expansion, clone-from-restore, deletion, and
scalability while leaving running workload PVCs untouched (used only as snapshot sources).
"""

import logging
import os
import random
import shlex
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress
from typing import Any, Dict, List, Optional

from ocs_ci.helpers import helpers
from ocs_ci.ocs import constants
from ocs_ci.ocs.resources import pvc as pvc_helpers
from ocs_ci.ocs.resources.pvc import PVC

log = logging.getLogger(__name__)


def _exec_shell_on_pod(pod, command: str) -> str:
    """Run a shell command (pipes/redirections) inside a pod via ``oc rsh``."""
    return pod.exec_cmd_on_pod(f"sh -c {shlex.quote(command)}").strip()


_DEFAULT_AGGRESSIVE_SNAPSHOT_CONFIG = {
    "max_parallel": 5,
    "large_scale_count": 8,
    "max_snapshot_pool_size": 40,
    "max_restore_pool_size": 20,
    "repeated_snapshot_count": 3,
    "concurrent_create_count": 3,
    "concurrent_delete_count": 2,
    "actions_per_invocation": 3,
    "vdbench_on_restore_elapsed": 120,
    "vdbench_on_restore_interval": 30,
    "max_vdbench_on_restore_parallel": 2,
    "loop_interval": 60,
    "stop_join_timeout": 300,
}


class AggressiveSnapshotOperations:
    """Manager for aggressive PVC snapshot/restore stress operations."""

    def __init__(self, background_ops):
        self.bg_ops = background_ops
        self.namespace = background_ops.namespace
        self._pool_lock = threading.Lock()
        self._vdbench_lock = threading.Lock()
        self._snapshot_pool: List[Dict[str, Any]] = []
        self._restore_pool: List[Dict[str, Any]] = []
        self._active_vdbench_workloads: List[Any] = []
        self._shutdown_event = threading.Event()
        self._metrics = {"latencies": [], "successes": 0, "failures": 0}

    def run(self):
        """Entry point for the aggressive_snapshot_operation background task."""
        if self._shutdown_event.is_set():
            log.info("Skipping aggressive snapshot operation: shutdown requested")
            return

        log.info("Executing aggressive snapshot operation")
        if not self.bg_ops._namespace_exists():
            log.info(
                "Skipping aggressive snapshot operation: namespace %s no longer exists",
                self.namespace,
            )
            return

        cfg = self._get_config()
        workload_pvcs = self.bg_ops._get_all_workload_pvcs()
        with self._pool_lock:
            snap_size = len(self._snapshot_pool)
            restore_size = len(self._restore_pool)
        workload_summary = self._summarize_workloads()

        log.info(
            "Aggressive snapshot context: namespace=%s workloads=%s workload_pvcs=%s "
            "snapshot_pool=%s restore_pool=%s workload_types=%s",
            self.namespace,
            workload_summary["workload_count"],
            len(workload_pvcs),
            snap_size,
            restore_size,
            workload_summary["workload_types"] or "none",
        )
        if not workload_pvcs:
            log.warning(
                "No PVC-backed workloads found for snapshotting. Aggressive snapshot "
                "requires workloads with pvc/pvc_obj/pvc_objs (e.g. VDBENCH, FIO). "
                "RGW/MCG/Warp workloads are not PVC snapshot sources. %s",
                workload_summary["workload_details"] or "",
            )
        if not workload_pvcs and not snap_size and not restore_size:
            log.warning(
                "Skipping aggressive snapshot operation: no workload PVCs and empty "
                "snapshot/restore pools"
            )
            return

        create_actions = [
            self._single_snapshot,
            self._parallel_snapshots_multi_source,
            self._parallel_snapshots_same_source,
            self._repeated_snapshots,
            self._nested_snapshot,
            self._restore_random_snapshot,
            self._vdbench_on_restore,
            self._vdbench_on_restores_parallel,
            self._concurrent_create_delete,
            self._large_scale_parallel,
        ]
        pool_actions = [
            self._mount_verify_restores,
            self._integrity_verify_restores,
            self._expand_random_restore,
            self._clone_random_restore,
            self._delete_random_snapshots,
            self._delete_random_restores,
            self._cleanup_snapshots,
        ]
        actions = create_actions + pool_actions

        with self._pool_lock:
            snap_size = len(self._snapshot_pool)
            restore_size = len(self._restore_pool)

        if (
            snap_size >= cfg["max_snapshot_pool_size"]
            or restore_size >= cfg["max_restore_pool_size"]
        ):
            log.info(
                "Snapshot/restore pool at capacity (snapshots=%s, restores=%s), "
                "prioritizing cleanup",
                snap_size,
                restore_size,
            )
            selected = [self._cleanup_snapshots]
        elif snap_size == 0 and workload_pvcs:
            max_actions = min(cfg["actions_per_invocation"], len(actions))
            selected = [random.choice(create_actions)]
            remaining_slots = max_actions - len(selected)
            if remaining_slots > 0:
                selected.extend(
                    random.sample(pool_actions, min(remaining_slots, len(pool_actions)))
                )
            log.info(
                "Snapshot pool empty; forcing at least one create action: %s",
                selected[0].__name__,
            )
        else:
            count = random.randint(1, min(cfg["actions_per_invocation"], len(actions)))
            selected = random.sample(actions, count)

        log.info(
            "Selected aggressive snapshot actions (%s): %s",
            len(selected),
            [action.__name__ for action in selected],
        )

        for action in selected:
            if self._shutdown_event.is_set():
                log.info("Stopping aggressive snapshot actions: shutdown requested")
                break
            try:
                log.info("Running aggressive snapshot action: %s", action.__name__)
                action(workload_pvcs, cfg)
            except Exception as err:
                log.error(
                    "Aggressive snapshot action %s failed: %s", action.__name__, err
                )
                self._metrics["failures"] += 1

        self._log_metrics()

    def request_shutdown(self):
        """Signal the aggressive snapshot loop to stop and prepare for cleanup."""
        if not self._shutdown_event.is_set():
            log.info("Aggressive snapshot operation shutdown requested")
        self._shutdown_event.set()

    def is_shutdown_requested(self) -> bool:
        return self._shutdown_event.is_set()

    def has_tracked_resources(self) -> bool:
        with self._vdbench_lock:
            if self._active_vdbench_workloads:
                return True
        with self._pool_lock:
            return bool(self._snapshot_pool or self._restore_pool)

    def _sleep_interruptible(self, total_seconds: float) -> bool:
        """
        Sleep in short chunks so shutdown can interrupt long Vdbench runs.

        Returns:
            bool: False if shutdown was requested during the wait
        """
        deadline = time.time() + total_seconds
        while time.time() < deadline:
            if self._shutdown_event.is_set():
                return False
            time.sleep(min(5, max(0, deadline - time.time())))
        return True

    def _log_skip(self, action_name: str, reason: str):
        log.info("Skipping aggressive snapshot action %s: %s", action_name, reason)

    def _summarize_workloads(self) -> Dict[str, Any]:
        workload_types = []
        details = []
        for workload in self.bg_ops.workloads:
            wtype = type(workload).__name__
            workload_types.append(wtype)
            pvc_refs = self.bg_ops._collect_workload_pvc_refs(workload)
            pvc_names = [getattr(ref, "name", "?") for ref in pvc_refs]
            if pvc_names:
                details.append(f"{wtype}(pvcs={pvc_names})")
            else:
                details.append(f"{wtype}(has_pvc=False)")
        return {
            "workload_count": len(self.bg_ops.workloads),
            "workload_types": workload_types,
            "workload_details": ", ".join(details),
        }

    def _require_restore_entry(self, action_name: str) -> Optional[Dict[str, Any]]:
        entry = self._pick_random_restore_entry()
        if not entry:
            self._log_skip(action_name, "restore pool is empty")
        return entry

    def _require_workload_pvcs(
        self, action_name: str, workload_pvcs: List[PVC]
    ) -> bool:
        if workload_pvcs:
            return True
        self._log_skip(
            action_name,
            "no workload PVCs available as snapshot sources",
        )
        return False

    def _get_config(self) -> Dict[str, Any]:
        bg_config: Dict[str, Any] = {}
        with suppress(Exception):
            from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

            bg_config = KrknWorkloadConfig().get_background_cluster_operations_config()
        if not bg_config:
            with suppress(Exception):
                from ocs_ci.resiliency.resiliency_workload_config import (
                    ResiliencyWorkloadConfig,
                )

                bg_config = (
                    ResiliencyWorkloadConfig().get_background_operations_config()
                )

        user_cfg = bg_config.get("aggressive_snapshot_config", {})
        return {**_DEFAULT_AGGRESSIVE_SNAPSHOT_CONFIG, **user_cfg}

    def _single_snapshot(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        if not self._require_workload_pvcs("_single_snapshot", workload_pvcs):
            return
        self._create_and_track_snapshot(random.choice(workload_pvcs), is_nested=False)

    def _parallel_snapshots_multi_source(
        self, workload_pvcs: List[PVC], cfg: Dict[str, Any]
    ):
        if not self._require_workload_pvcs(
            "_parallel_snapshots_multi_source", workload_pvcs
        ):
            return
        count = min(cfg["max_parallel"], len(workload_pvcs))
        sources = random.sample(workload_pvcs, count)
        self._create_snapshots_parallel(sources)

    def _parallel_snapshots_same_source(
        self, workload_pvcs: List[PVC], cfg: Dict[str, Any]
    ):
        if not self._require_workload_pvcs(
            "_parallel_snapshots_same_source", workload_pvcs
        ):
            return
        source = random.choice(workload_pvcs)
        self._create_snapshots_parallel([source] * cfg["max_parallel"])

    def _repeated_snapshots(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        if not self._require_workload_pvcs("_repeated_snapshots", workload_pvcs):
            return
        source = random.choice(workload_pvcs)
        for _ in range(cfg["repeated_snapshot_count"]):
            self._create_and_track_snapshot(source, is_nested=False)

    def _nested_snapshot(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        with self._pool_lock:
            restore_entries = list(self._restore_pool)
        if not restore_entries:
            log.info(
                "No restored PVCs for nested snapshot; creating base snapshot+restore first"
            )
            if self._require_workload_pvcs("_nested_snapshot", workload_pvcs):
                snap = self._create_and_track_snapshot(
                    random.choice(workload_pvcs), is_nested=False
                )
                if snap:
                    self._restore_random_snapshot(workload_pvcs, cfg)
            return
        parent_entry = random.choice(restore_entries)
        parent_pvc = parent_entry["pvc"]
        parent_pvc.reload()
        if parent_pvc.status != constants.STATUS_BOUND:
            log.warning(
                "Parent restore %s not bound, skipping nested snapshot", parent_pvc.name
            )
            return
        self._create_and_track_snapshot(
            parent_pvc,
            is_nested=True,
            original_source=parent_entry.get("source_pvc"),
        )

    def _restore_random_snapshot(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._pick_random_snapshot_entry()
        if not entry:
            if workload_pvcs:
                self._create_and_track_snapshot(
                    random.choice(workload_pvcs), is_nested=False
                )
                entry = self._pick_random_snapshot_entry()
            if not entry:
                self._log_skip("_restore_random_snapshot", "snapshot pool is empty")
                return
        with self._pool_lock:
            if len(self._restore_pool) >= cfg["max_restore_pool_size"]:
                self._log_skip("_restore_random_snapshot", "restore pool at capacity")
                return
        self._create_and_track_restore(entry)

    def _mount_verify_restores(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._require_restore_entry("_mount_verify_restores")
        if not entry:
            return
        restored_pvc = entry["pvc"]
        if not self.bg_ops._can_attach_pod(restored_pvc):
            log.info(
                "Restore %s is not attachable (RWX), skipping mount verify",
                restored_pvc.name,
            )
            return
        test_pod = None
        try:
            test_pod = self.bg_ops._create_test_pod(restored_pvc)
            helpers.wait_for_resource_state(
                resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
            )
            self.bg_ops._verify_pod_data(test_pod)
            log.info("Mount verification passed for restore %s", restored_pvc.name)
        finally:
            with suppress(Exception):
                if test_pod:
                    test_pod.delete()
                    test_pod.ocp.wait_for_delete(
                        resource_name=test_pod.name, timeout=180
                    )

    def _integrity_verify_restores(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._require_restore_entry("_integrity_verify_restores")
        if not entry:
            return
        self._verify_restore_integrity(entry["source_pvc"], entry["pvc"])

    def _vdbench_on_restore(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        """Mount a restored PVC via Vdbench deployment and run a short I/O workload."""
        restored_pvc = self._get_or_create_restore_for_io(
            workload_pvcs, action_name="_vdbench_on_restore"
        )
        if restored_pvc is None:
            return
        self._run_vdbench_on_pvc(restored_pvc, cfg)

    def _vdbench_on_restores_parallel(
        self, workload_pvcs: List[PVC], cfg: Dict[str, Any]
    ):
        """Run Vdbench concurrently on multiple restored PVCs."""
        max_parallel = cfg.get("max_vdbench_on_restore_parallel", 2)
        restored_pvcs = self._get_or_create_restores_for_io(
            workload_pvcs, max_parallel, action_name="_vdbench_on_restores_parallel"
        )
        if not restored_pvcs:
            return

        log.info("Running Vdbench on %s restored PVCs in parallel", len(restored_pvcs))
        with ThreadPoolExecutor(max_workers=len(restored_pvcs)) as executor:
            futures = [
                executor.submit(self._run_vdbench_on_pvc, pvc, cfg)
                for pvc in restored_pvcs
            ]
            for future in as_completed(futures):
                with suppress(Exception):
                    future.result()

    def _get_or_create_restore_for_io(
        self,
        workload_pvcs: List[PVC],
        action_name: str = "_get_or_create_restore_for_io",
    ) -> Optional[PVC]:
        entry = self._pick_random_restore_entry()
        if entry:
            log.info("Pool hit: using existing restore %s", entry["pvc"].name)
            return entry["pvc"]
        snap_entry = self._pick_random_snapshot_entry()
        if not snap_entry and workload_pvcs:
            self._create_and_track_snapshot(random.choice(workload_pvcs), False)
            snap_entry = self._pick_random_snapshot_entry()
        if not snap_entry:
            self._log_skip(action_name, "no snapshot available to restore for IO")
            return None
        return self._create_and_track_restore(snap_entry)

    def _get_or_create_restores_for_io(
        self,
        workload_pvcs: List[PVC],
        count: int,
        action_name: str = "_get_or_create_restores_for_io",
    ) -> List[PVC]:
        with self._pool_lock:
            entries = list(self._restore_pool)
        restored_pvcs = (
            [entry["pvc"] for entry in random.sample(entries, min(count, len(entries)))]
            if entries
            else []
        )

        while len(restored_pvcs) < count:
            snap_entry = self._pick_random_snapshot_entry()
            if not snap_entry and workload_pvcs:
                self._create_and_track_snapshot(random.choice(workload_pvcs), False)
                snap_entry = self._pick_random_snapshot_entry()
            if not snap_entry:
                break
            created = self._create_and_track_restore(snap_entry)
            if created:
                restored_pvcs.append(created)
            else:
                break
        if not restored_pvcs:
            self._log_skip(
                action_name,
                "could not obtain any restored PVCs from pool or workload sources",
            )
        return restored_pvcs

    def _get_vdbench_settings(self) -> Dict[str, Any]:
        with suppress(Exception):
            from ocs_ci.krkn_chaos.krkn_workload_config import KrknWorkloadConfig

            return KrknWorkloadConfig().get_vdbench_config()
        with suppress(Exception):
            from ocs_ci.resiliency.resiliency_workload_config import (
                ResiliencyWorkloadConfig,
            )

            return ResiliencyWorkloadConfig().get_vdbench_config()
        return {}

    def _create_vdbench_config_file(
        self, restored_pvc: PVC, cfg: Dict[str, Any]
    ) -> str:
        from ocs_ci.helpers.vdbench_helpers import create_temp_config_file

        vdbench_config = self._get_vdbench_settings()
        elapsed = cfg.get(
            "vdbench_on_restore_elapsed", vdbench_config.get("elapsed", 120)
        )
        threads = vdbench_config.get("threads", 16)
        interval = cfg.get(
            "vdbench_on_restore_interval", vdbench_config.get("interval", 30)
        )

        if restored_pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
            block_config = vdbench_config.get("block", {})
            krkn_format = {
                "vdbench_config": {
                    "threads": threads,
                    "elapsed": elapsed,
                    "interval": interval,
                    "block": {
                        "size": block_config.get("size", "15g"),
                        "patterns": block_config.get(
                            "patterns",
                            [
                                {
                                    "name": "restore_random_write",
                                    "rdpct": 0,
                                    "seekpct": 100,
                                    "xfersize": "4k",
                                    "skew": 0,
                                },
                                {
                                    "name": "restore_random_read",
                                    "rdpct": 100,
                                    "seekpct": 100,
                                    "xfersize": "8k",
                                    "skew": 0,
                                },
                            ],
                        ),
                    },
                }
            }
        else:
            fs_config = vdbench_config.get("filesystem", {})
            krkn_format = {
                "vdbench_config": {
                    "threads": threads,
                    "elapsed": elapsed,
                    "interval": interval,
                    "filesystem": {
                        "size": fs_config.get("size", "10m"),
                        "depth": fs_config.get("depth", 4),
                        "width": fs_config.get("width", 5),
                        "files": fs_config.get("files", 10),
                        "file_size": fs_config.get("file_size", "1m"),
                        "openflags": fs_config.get("openflags", "o_direct"),
                        "group_all_fwds_in_one_rd": fs_config.get(
                            "group_all_fwds_in_one_rd", True
                        ),
                        "patterns": fs_config.get(
                            "patterns",
                            [
                                {
                                    "name": "restore_sequential_write",
                                    "rdpct": 0,
                                    "seekpct": 0,
                                    "xfersize": "1m",
                                    "skew": 0,
                                },
                                {
                                    "name": "restore_random_mixed",
                                    "rdpct": 50,
                                    "seekpct": 100,
                                    "xfersize": "256k",
                                    "skew": 0,
                                },
                            ],
                        ),
                    },
                }
            }

        return create_temp_config_file(krkn_format)

    def _run_vdbench_on_pvc(self, restored_pvc: PVC, cfg: Dict[str, Any]):
        from ocs_ci.resiliency.resiliency_workload import VdbenchWorkload

        config_file = None
        workload = None
        try:
            restored_pvc.reload()
            helpers.wait_for_resource_state(
                resource=restored_pvc, state=constants.STATUS_BOUND, timeout=120
            )
            config_file = self._create_vdbench_config_file(restored_pvc, cfg)
            workload = VdbenchWorkload(
                pvc=restored_pvc,
                vdbench_config_file=config_file,
                namespace=self.namespace,
            )
            with self._vdbench_lock:
                self._active_vdbench_workloads.append(workload)

            elapsed = cfg.get("vdbench_on_restore_elapsed", 120)
            interval = cfg.get("vdbench_on_restore_interval", 30)
            log.info(
                "Starting Vdbench on restored PVC %s (elapsed=%ss)",
                restored_pvc.name,
                elapsed,
            )
            workload.start_workload()
            run_seconds = elapsed + interval
            if not self._sleep_interruptible(run_seconds):
                log.info(
                    "Stopping Vdbench early on restored PVC %s (shutdown requested)",
                    restored_pvc.name,
                )
            workload.stop_workload()
            workload.cleanup_workload()
            if self._shutdown_event.is_set():
                log.info(
                    "Vdbench on restored PVC %s stopped for teardown",
                    restored_pvc.name,
                )
            else:
                log.info("Vdbench completed on restored PVC %s", restored_pvc.name)
        except Exception as err:
            self._metrics["failures"] += 1
            log.error("Vdbench on restore %s failed: %s", restored_pvc.name, err)
            with suppress(Exception):
                if workload:
                    workload.stop_workload()
                    workload.cleanup_workload()
        finally:
            if workload:
                with self._vdbench_lock:
                    if workload in self._active_vdbench_workloads:
                        self._active_vdbench_workloads.remove(workload)
            if config_file and os.path.exists(config_file):
                with suppress(Exception):
                    os.unlink(config_file)

    def _expand_random_restore(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._require_restore_entry("_expand_random_restore")
        if not entry:
            return
        restored_pvc = entry["pvc"]
        restored_pvc.reload()
        current_size = restored_pvc.size
        new_size = current_size + random.randint(1, 2)
        try:
            restored_pvc.resize_pvc(new_size, verify=True, timeout=300)
            log.info(
                "Expanded restore %s from %sGi to %sGi",
                restored_pvc.name,
                current_size,
                new_size,
            )
        except Exception as err:
            log.warning(
                "Restore expansion not supported or failed for %s: %s",
                restored_pvc.name,
                err,
            )

    def _clone_random_restore(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        """Mirror of clone module's _snapshot_random_clone: clone a restore, then delete."""
        entry = self._require_restore_entry("_clone_random_restore")
        if not entry:
            return
        restored_pvc = entry["pvc"]
        clone_pvc = None
        try:
            restored_pvc.reload()
            provisioner = (
                (getattr(restored_pvc, "provisioner", "") or "").strip().lower()
            )
            if "rbd" in provisioner:
                clone_yaml = constants.CSI_RBD_PVC_CLONE_YAML
            elif "cephfs" in provisioner:
                clone_yaml = constants.CSI_CEPHFS_PVC_CLONE_YAML
            else:
                self._log_skip(
                    "_clone_random_restore", f"unsupported provisioner {provisioner!r}"
                )
                return
            capacity = self.bg_ops._get_pvc_storage_capacity(restored_pvc)
            clone_pvc = pvc_helpers.create_pvc_clone(
                sc_name=restored_pvc.backed_sc,
                parent_pvc=restored_pvc.name,
                clone_yaml=clone_yaml,
                namespace=self.namespace,
                storage_size=capacity,
                access_mode=restored_pvc.get_pvc_access_mode,
                volume_mode=restored_pvc.get()["spec"]["volumeMode"],
            )
            helpers.wait_for_resource_state(
                resource=clone_pvc, state=constants.STATUS_BOUND, timeout=300
            )
            log.info(
                "Created clone %s from restored PVC %s",
                clone_pvc.name,
                restored_pvc.name,
            )
        except Exception as err:
            log.warning("Clone of restore %s failed: %s", restored_pvc.name, err)
        finally:
            with suppress(Exception):
                if clone_pvc:
                    clone_pvc.delete()
                    clone_pvc.ocp.wait_for_delete(
                        resource_name=clone_pvc.name, timeout=180
                    )

    def _delete_random_snapshots(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        with self._pool_lock:
            if not self._snapshot_pool:
                self._log_skip("_delete_random_snapshots", "snapshot pool is empty")
                return
            count = min(cfg["concurrent_delete_count"], len(self._snapshot_pool))
            entries = random.sample(self._snapshot_pool, count)
        for entry in entries:
            self._delete_tracked_snapshot(entry)

    def _delete_random_restores(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        with self._pool_lock:
            if not self._restore_pool:
                self._log_skip("_delete_random_restores", "restore pool is empty")
                return
            count = min(cfg["concurrent_delete_count"], len(self._restore_pool))
            entries = random.sample(self._restore_pool, count)
        for entry in entries:
            self._delete_tracked_restore(entry)

    def _concurrent_create_delete(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        if not self._require_workload_pvcs("_concurrent_create_delete", workload_pvcs):
            return

        def create_task():
            self._create_and_track_snapshot(
                random.choice(workload_pvcs), is_nested=False
            )

        def delete_task():
            # Prefer deleting a restore if present, else a snapshot
            restore_entry = self._pick_random_restore_entry()
            if restore_entry:
                self._delete_tracked_restore(restore_entry)
                return
            snap_entry = self._pick_random_snapshot_entry()
            if snap_entry:
                self._delete_tracked_snapshot(snap_entry)

        # Same ThreadPoolExecutor fan-out as clones using concurrent_* counts
        tasks = []
        for _ in range(cfg["concurrent_create_count"]):
            tasks.append(("create", create_task))
        for _ in range(cfg["concurrent_delete_count"]):
            tasks.append(("delete", delete_task))

        with ThreadPoolExecutor(max_workers=cfg["max_parallel"]) as executor:
            futures = {executor.submit(fn): name for name, fn in tasks}
            for future in as_completed(futures):
                action = futures[future]
                try:
                    future.result()
                except Exception as err:
                    log.error("Concurrent %s task failed: %s", action, err)
                    self._metrics["failures"] += 1

    def _large_scale_parallel(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        if not self._require_workload_pvcs("_large_scale_parallel", workload_pvcs):
            return
        count = cfg["large_scale_count"]
        sources = [random.choice(workload_pvcs) for _ in range(count)]
        log.info("Large-scale parallel snapshot creation: %s snapshots", count)
        self._create_snapshots_parallel(sources)

    def _cleanup_snapshots(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        with self._pool_lock:
            restores = list(self._restore_pool)
            snaps = list(self._snapshot_pool)
        log.info("Cleaning up %s restores and %s snapshots", len(restores), len(snaps))
        for entry in restores:
            self._delete_tracked_restore(entry)
        for entry in snaps:
            self._delete_tracked_snapshot(entry)
        self._verify_pool_reclamation()

    def _create_snapshots_parallel(self, sources: List[PVC]):
        with ThreadPoolExecutor(max_workers=min(len(sources), 8)) as executor:
            futures = [
                executor.submit(self._create_and_track_snapshot, src, False)
                for src in sources
            ]
            for future in as_completed(futures):
                with suppress(Exception):
                    future.result()

    def _create_and_track_snapshot(
        self,
        source_pvc: PVC,
        is_nested: bool,
        original_source: Optional[PVC] = None,
    ) -> Optional[Any]:
        start = time.time()
        try:
            source_pvc.reload()
            # Use prefixed snapshot names for safe orphan cleanup
            import uuid

            suffix = str(uuid.uuid4())[:8]
            snapshot_name = f"agg-snap-{source_pvc.name[:16]}-{suffix}"
            snapshot_obj = source_pvc.create_snapshot(
                snapshot_name=snapshot_name, wait=True, timeout=180
            )
            latency = time.time() - start
            entry = {
                "snapshot": snapshot_obj,
                "source_pvc": original_source or source_pvc,
                "is_nested": is_nested,
                "latency_seconds": latency,
                "created_at": time.time(),
            }
            with self._pool_lock:
                self._snapshot_pool.append(entry)
            self._metrics["latencies"].append(latency)
            self._metrics["successes"] += 1
            log.info(
                "Created snapshot %s from %s in %.2fs (nested=%s)",
                snapshot_obj.name,
                source_pvc.name,
                latency,
                is_nested,
            )
            return snapshot_obj
        except Exception as err:
            self._metrics["failures"] += 1
            log.error("Failed to create snapshot from %s: %s", source_pvc.name, err)
            return None

    def _delete_tracked_snapshot(self, entry: Dict[str, Any]):
        snapshot_obj = entry.get("snapshot")
        if snapshot_obj is None:
            return
        try:
            with suppress(Exception):
                snapshot_obj.reload()
            snapshot_obj.delete()
            snapshot_obj.ocp.wait_for_delete(
                resource_name=snapshot_obj.name, timeout=180
            )
            log.info("Deleted aggressive snapshot %s", snapshot_obj.name)
        except Exception as err:
            log.warning(
                "Failed to delete snapshot %s: %s",
                getattr(snapshot_obj, "name", "?"),
                err,
            )
        finally:
            with self._pool_lock:
                if entry in self._snapshot_pool:
                    self._snapshot_pool.remove(entry)

    def _pick_random_snapshot_entry(self) -> Optional[Dict[str, Any]]:
        with self._pool_lock:
            if not self._snapshot_pool:
                return None
            return random.choice(self._snapshot_pool)

    def _pick_random_restore_entry(self) -> Optional[Dict[str, Any]]:
        with self._pool_lock:
            if not self._restore_pool:
                return None
            return random.choice(self._restore_pool)

    def _get_restore_yaml(self, source_pvc: PVC) -> Optional[str]:
        provisioner = (getattr(source_pvc, "provisioner", "") or "").strip().lower()
        if "rbd" in provisioner:
            return constants.CSI_RBD_PVC_RESTORE_YAML
        if "cephfs" in provisioner:
            return constants.CSI_CEPHFS_PVC_RESTORE_YAML
        log.warning("Unsupported provisioner for aggressive restore: %r", provisioner)
        return None

    def _source_capacity(self, source_pvc: PVC) -> str:
        source_pvc.reload()
        capacity = source_pvc.data.get("status", {}).get("capacity", {}).get("storage")
        if capacity:
            return capacity
        return (
            source_pvc.data.get("spec", {})
            .get("resources", {})
            .get("requests", {})
            .get("storage", f"{source_pvc.size}Gi")
        )

    def _create_and_track_restore(
        self, snapshot_entry: Dict[str, Any]
    ) -> Optional[PVC]:
        snapshot_obj = snapshot_entry["snapshot"]
        source_pvc = snapshot_entry["source_pvc"]
        start = time.time()
        try:
            restore_yaml = self._get_restore_yaml(source_pvc)
            if not restore_yaml:
                return None
            capacity = self._source_capacity(source_pvc)
            restored_pvc = pvc_helpers.create_restore_pvc(
                sc_name=snapshot_obj.parent_sc,
                snap_name=snapshot_obj.name,
                namespace=self.namespace,
                size=capacity,
                pvc_name=f"agg-snap-restored-{snapshot_obj.name[:16]}",
                volume_mode=snapshot_obj.parent_volume_mode,
                restore_pvc_yaml=restore_yaml,
                access_mode=snapshot_obj.parent_access_mode,
            )
            helpers.wait_for_resource_state(
                resource=restored_pvc, state=constants.STATUS_BOUND, timeout=300
            )
            latency = time.time() - start
            entry = {
                "pvc": restored_pvc,
                "snapshot": snapshot_obj,
                "source_pvc": source_pvc,
                "latency_seconds": latency,
                "created_at": time.time(),
            }
            with self._pool_lock:
                self._restore_pool.append(entry)
            self._metrics["latencies"].append(latency)
            self._metrics["successes"] += 1
            log.info(
                "Restored PVC %s from snapshot %s in %.2fs",
                restored_pvc.name,
                snapshot_obj.name,
                latency,
            )
            return restored_pvc
        except Exception as err:
            self._metrics["failures"] += 1
            log.error(
                "Failed to restore from snapshot %s: %s",
                getattr(snapshot_obj, "name", "?"),
                err,
            )
            return None

    def _delete_tracked_restore(self, entry: Dict[str, Any]):
        restored_pvc = entry.get("pvc")
        if restored_pvc is None:
            return
        try:
            restored_pvc.reload()
            if restored_pvc.is_deleted:
                with self._pool_lock:
                    if entry in self._restore_pool:
                        self._restore_pool.remove(entry)
                return
            restored_pvc.delete()
            restored_pvc.ocp.wait_for_delete(
                resource_name=restored_pvc.name, timeout=180
            )
            log.info("Deleted aggressive restored PVC %s", restored_pvc.name)
        except Exception as err:
            log.warning(
                "Failed to delete restored PVC %s: %s",
                getattr(restored_pvc, "name", "?"),
                err,
            )
        finally:
            with self._pool_lock:
                if entry in self._restore_pool:
                    self._restore_pool.remove(entry)

    def _verify_restore_integrity(self, source_pvc: PVC, restored_pvc: PVC):
        if not self.bg_ops._can_attach_pod(restored_pvc):
            log.info(
                "Skipping integrity check for non-attachable restore %s",
                restored_pvc.name,
            )
            return

        source_pods = source_pvc.get_attached_pods()
        if not source_pods:
            log.warning(
                "No pod attached to source PVC %s; falling back to basic verify",
                source_pvc.name,
            )
            test_pod = None
            try:
                test_pod = self.bg_ops._create_test_pod(restored_pvc)
                helpers.wait_for_resource_state(
                    resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
                )
                self.bg_ops._verify_pod_data(test_pod)
            finally:
                with suppress(Exception):
                    if test_pod:
                        test_pod.delete()
            return

        source_pod = source_pods[0]
        vol_mode = source_pvc.get_pvc_vol_mode
        test_pod = None
        try:
            if vol_mode == constants.VOLUME_MODE_BLOCK:
                block_checksum_cmd = (
                    f"dd if={constants.VDBENCH_BLOCK_DEVICE_PATH} bs=1M count=1 "
                    "2>/dev/null | md5sum | awk '{print $1}'"
                )
                source_sum = _exec_shell_on_pod(source_pod, block_checksum_cmd)
                test_pod = self.bg_ops._create_test_pod(restored_pvc)
                helpers.wait_for_resource_state(
                    resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
                )
                clone_checksum_cmd = (
                    f"dd if={constants.RAW_BLOCK_DEVICE} bs=1M count=1 "
                    "2>/dev/null | md5sum | awk '{print $1}'"
                )
                restore_sum = _exec_shell_on_pod(test_pod, clone_checksum_cmd)
            else:
                source_mount = "/vdbench-data"
                restore_mount = "/mnt"
                sample_file = _exec_shell_on_pod(
                    source_pod,
                    f"find {source_mount} -type f 2>/dev/null | head -1",
                )
                if not sample_file:
                    log.warning(
                        "No files on source PVC %s for integrity check", source_pvc.name
                    )
                    test_pod = self.bg_ops._create_test_pod(restored_pvc)
                    helpers.wait_for_resource_state(
                        resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
                    )
                    self.bg_ops._verify_pod_data(test_pod)
                    return

                relative = sample_file.replace(source_mount, "").lstrip("/")
                source_sum = _exec_shell_on_pod(
                    source_pod,
                    f"md5sum {shlex.quote(sample_file)} | awk '{{print $1}}'",
                )
                test_pod = self.bg_ops._create_test_pod(restored_pvc)
                helpers.wait_for_resource_state(
                    resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
                )
                restore_path = f"{restore_mount}/{relative}"
                restore_sum = _exec_shell_on_pod(
                    test_pod,
                    f"md5sum {shlex.quote(restore_path)} | awk '{{print $1}}'",
                )

            if source_sum == restore_sum:
                log.info(
                    "Data integrity verified between %s and restore %s",
                    source_pvc.name,
                    restored_pvc.name,
                )
            else:
                log.warning(
                    "Checksum mismatch: source %s vs restore %s (%s != %s)",
                    source_pvc.name,
                    restored_pvc.name,
                    source_sum,
                    restore_sum,
                )
        except Exception as err:
            log.warning(
                "Integrity verification failed for restore %s: %s",
                restored_pvc.name,
                err,
            )
        finally:
            with suppress(Exception):
                if test_pod:
                    test_pod.delete()
                    test_pod.ocp.wait_for_delete(
                        resource_name=test_pod.name, timeout=180
                    )

    def _verify_pool_reclamation(self):
        with self._pool_lock:
            remaining_snaps = [
                e["snapshot"].name for e in self._snapshot_pool if e.get("snapshot")
            ]
            remaining_restores = [
                e["pvc"].name for e in self._restore_pool if e.get("pvc")
            ]
        if remaining_snaps or remaining_restores:
            log.warning(
                "Pools not fully reclaimed: snapshots=%s restores=%s",
                remaining_snaps,
                remaining_restores,
            )
        else:
            log.info("All aggressive snapshot/restore resources reclaimed")

    def _log_metrics(self):
        with self._pool_lock:
            snapshot_pool_size = len(self._snapshot_pool)
            restore_pool_size = len(self._restore_pool)
        latencies = self._metrics["latencies"]
        total = self._metrics["successes"] + self._metrics["failures"]
        if not latencies and total == 0:
            log.info(
                "Aggressive snapshot metrics: no snapshot/restore operations recorded "
                "yet (snapshot_pool=%s restore_pool=%s)",
                snapshot_pool_size,
                restore_pool_size,
            )
            return
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        success_rate = (self._metrics["successes"] / total * 100) if total else 0
        log.info(
            "Aggressive snapshot metrics: successes=%s failures=%s "
            "avg_latency=%.2fs success_rate=%.1f%% snapshot_pool=%s restore_pool=%s",
            self._metrics["successes"],
            self._metrics["failures"],
            avg_latency,
            success_rate,
            snapshot_pool_size,
            restore_pool_size,
        )

    def cleanup_all(self):
        self.request_shutdown()
        # stop vdbench workloads (same as clones)
        with self._vdbench_lock:
            vdbench_workloads = list(self._active_vdbench_workloads)
        for workload in vdbench_workloads:
            with suppress(Exception):
                workload.stop_workload()
                workload.cleanup_workload()

        with self._pool_lock:
            restores = list(self._restore_pool)
            snaps = list(self._snapshot_pool)
        for entry in restores:
            self._delete_tracked_restore(entry)
        for entry in snaps:
            self._delete_tracked_snapshot(entry)
        self._cleanup_orphan_aggressive_resources()
        self._verify_pool_reclamation()

    def _cleanup_orphan_aggressive_resources(self):
        """Best-effort sweep for leftover snapshot/test/vdbench resources."""
        from ocs_ci.ocs.ocp import OCP

        restored_pvc_names = set()

        # Sweep restored PVCs (only those created by this module)
        try:
            pvc_ocp = OCP(
                kind="PersistentVolumeClaim",
                namespace=self.namespace,
            )
            for item in pvc_ocp.get().get("items", []):
                name = item["metadata"]["name"]
                if name.startswith("agg-snap-restored-"):
                    restored_pvc_names.add(name)
                    with suppress(Exception):
                        pvc_ocp.delete(resource_name=name, wait=False)
                        log.info("Deleted orphan aggressive restored PVC %s", name)
        except Exception as err:
            log.warning("Failed to sweep orphan restored PVCs: %s", err)

        # Sweep VolumeSnapshots (only those created by this module)
        try:
            snapshot_ocp = OCP(kind="VolumeSnapshot", namespace=self.namespace)
            for item in snapshot_ocp.get().get("items", []):
                name = item["metadata"]["name"]
                # Only delete snapshots created by this aggressive snapshot module
                if name.startswith("agg-snap-"):
                    with suppress(Exception):
                        snapshot_ocp.delete(resource_name=name, wait=False)
                        log.info("Deleted orphan VolumeSnapshot %s", name)
        except Exception as err:
            log.warning("Failed to sweep orphan VolumeSnapshots: %s", err)

        # Sweep test pods
        try:
            pod_ocp = OCP(kind="Pod", namespace=self.namespace)
            for item in pod_ocp.get().get("items", []):
                name = item["metadata"]["name"]
                if name.startswith("test-pod-") and any(
                    restored in name for restored in restored_pvc_names
                ):
                    with suppress(Exception):
                        pod_ocp.delete(resource_name=name, wait=False)
                        log.info("Deleted orphan aggressive snapshot test pod %s", name)
        except Exception as err:
            log.warning("Failed to sweep orphan snapshot test pods: %s", err)

        # Sweep Vdbench deployments
        try:
            deploy_ocp = OCP(kind="Deployment", namespace=self.namespace)
            for item in deploy_ocp.get().get("items", []):
                name = item["metadata"]["name"]
                if not name.startswith("vdbench-workload-"):
                    continue
                volumes = (
                    item.get("spec", {})
                    .get("template", {})
                    .get("spec", {})
                    .get("volumes", [])
                )
                for volume in volumes:
                    claim = volume.get("persistentVolumeClaim", {}).get("claimName")
                    if claim and (
                        claim in restored_pvc_names
                        or claim.startswith("agg-snap-restored-")
                    ):
                        with suppress(Exception):
                            deploy_ocp.delete(resource_name=name, wait=False)
                            log.info(
                                "Deleted orphan Vdbench deployment %s on restored PVC %s",
                                name,
                                claim,
                            )
                        break
        except Exception as err:
            log.warning(
                "Failed to sweep orphan Vdbench deployments on restored PVCs: %s", err
            )
