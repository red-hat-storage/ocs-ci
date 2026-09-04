"""
Aggressive PVC clone background operations for Krkn/resiliency testing.

Exercises clone creation, nesting, parallel operations, mount/integrity checks,
Vdbench I/O on clones, expansion, snapshots, deletion, and scalability while
leaving running workload PVCs untouched (used only as read-only clone sources).
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


_DEFAULT_AGGRESSIVE_CLONE_CONFIG = {
    "max_parallel": 5,
    "large_scale_count": 8,
    "max_pool_size": 40,
    "repeated_clone_count": 3,
    "concurrent_create_count": 3,
    "concurrent_delete_count": 2,
    "actions_per_invocation": 3,
    "vdbench_on_clone_elapsed": 120,
    "vdbench_on_clone_interval": 30,
    "max_vdbench_on_clone_parallel": 2,
    "loop_interval": 60,
    "stop_join_timeout": 300,
}


class AggressiveCloneOperations:
    """Manager for aggressive PVC clone stress operations."""

    def __init__(self, background_ops):
        self.bg_ops = background_ops
        self.namespace = background_ops.namespace
        self._pool_lock = threading.Lock()
        self._vdbench_lock = threading.Lock()
        self._clone_pool: List[Dict[str, Any]] = []
        self._active_vdbench_workloads: List[Any] = []
        self._shutdown_event = threading.Event()
        self._metrics = {
            "latencies": [],
            "successes": 0,
            "failures": 0,
        }

    def run(self):
        """Entry point for the aggressive_clone_operation background task."""
        if self._shutdown_event.is_set():
            log.info("Skipping aggressive clone operation: shutdown requested")
            return

        log.info("Executing aggressive clone operation")
        if not self.bg_ops._namespace_exists():
            log.info(
                "Skipping aggressive clone operation: namespace %s no longer exists",
                self.namespace,
            )
            return

        cfg = self._get_config()
        workload_pvcs = self.bg_ops._get_all_workload_pvcs()
        with self._pool_lock:
            pool_size = len(self._clone_pool)
        workload_summary = self._summarize_workloads()

        log.info(
            "Aggressive clone context: namespace=%s workloads=%s workload_pvcs=%s "
            "clone_pool=%s workload_types=%s",
            self.namespace,
            workload_summary["workload_count"],
            len(workload_pvcs),
            pool_size,
            workload_summary["workload_types"] or "none",
        )
        if not workload_pvcs:
            log.warning(
                "No PVC-backed workloads found for cloning. Aggressive clone requires "
                "workloads with pvc/pvc_obj/pvc_objs (e.g. VDBENCH, FIO). "
                "RGW/MCG/Warp workloads are not PVC clone sources. %s",
                workload_summary["workload_details"] or "",
            )
        if not workload_pvcs and not self._clone_pool:
            log.warning(
                "Skipping aggressive clone operation: no workload PVCs and empty clone pool"
            )
            return

        create_actions = [
            self._single_clone,
            self._parallel_clones_multi_source,
            self._parallel_clones_same_source,
            self._repeated_clones,
            self._nested_clone,
            self._vdbench_on_clone,
            self._vdbench_on_clones_parallel,
            self._concurrent_create_delete,
            self._large_scale_parallel,
        ]
        pool_actions = [
            self._mount_verify_clones,
            self._integrity_verify_clones,
            self._expand_random_clone,
            self._snapshot_random_clone,
            self._delete_random_clones,
            self._cleanup_clones,
        ]
        actions = create_actions + pool_actions

        with self._pool_lock:
            pool_size = len(self._clone_pool)
        if pool_size >= cfg["max_pool_size"]:
            log.info("Clone pool at capacity (%s), prioritizing cleanup", pool_size)
            selected = [self._cleanup_clones]
        elif pool_size == 0 and workload_pvcs:
            # Always seed the pool first; random pool-only picks are no-ops otherwise.
            max_actions = min(cfg["actions_per_invocation"], len(actions))
            selected = [random.choice(create_actions)]
            remaining_slots = max_actions - len(selected)
            if remaining_slots > 0:
                selected.extend(
                    random.sample(
                        pool_actions,
                        min(remaining_slots, len(pool_actions)),
                    )
                )
            log.info(
                "Clone pool empty; forcing at least one create action: %s",
                selected[0].__name__,
            )
        else:
            count = random.randint(1, min(cfg["actions_per_invocation"], len(actions)))
            selected = random.sample(actions, count)

        log.info(
            "Selected aggressive clone actions (%s): %s",
            len(selected),
            [action.__name__ for action in selected],
        )

        for action in selected:
            if self._shutdown_event.is_set():
                log.info("Stopping aggressive clone actions: shutdown requested")
                break
            try:
                log.info("Running aggressive clone action: %s", action.__name__)
                action(workload_pvcs, cfg)
            except Exception as err:
                log.error("Aggressive clone action %s failed: %s", action.__name__, err)
                self._metrics["failures"] += 1

        self._log_metrics()

    def request_shutdown(self):
        """Signal the aggressive clone loop to stop and prepare for cleanup."""
        if not self._shutdown_event.is_set():
            log.info("Aggressive clone operation shutdown requested")
        self._shutdown_event.set()

    def is_shutdown_requested(self) -> bool:
        return self._shutdown_event.is_set()

    def has_tracked_resources(self) -> bool:
        with self._vdbench_lock:
            if self._active_vdbench_workloads:
                return True
        with self._pool_lock:
            return bool(self._clone_pool)

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
        log.info("Skipping aggressive clone action %s: %s", action_name, reason)

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

    def _require_pool_entry(self, action_name: str) -> Optional[Dict[str, Any]]:
        entry = self._pick_random_pool_entry()
        if not entry:
            self._log_skip(action_name, "clone pool is empty")
        return entry

    def _require_workload_pvcs(
        self, action_name: str, workload_pvcs: List[PVC]
    ) -> bool:
        if workload_pvcs:
            return True
        self._log_skip(
            action_name,
            "no workload PVCs available as clone sources",
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

        user_cfg = bg_config.get("aggressive_clone_config", {})
        return {**_DEFAULT_AGGRESSIVE_CLONE_CONFIG, **user_cfg}

    def _single_clone(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        if not self._require_workload_pvcs("_single_clone", workload_pvcs):
            return
        source = random.choice(workload_pvcs)
        self._create_and_track_clone(source, is_nested=False)

    def _parallel_clones_multi_source(
        self, workload_pvcs: List[PVC], cfg: Dict[str, Any]
    ):
        if not self._require_workload_pvcs(
            "_parallel_clones_multi_source", workload_pvcs
        ):
            return
        count = min(cfg["max_parallel"], len(workload_pvcs))
        sources = random.sample(workload_pvcs, count)
        self._create_clones_parallel(sources)

    def _parallel_clones_same_source(
        self, workload_pvcs: List[PVC], cfg: Dict[str, Any]
    ):
        if not self._require_workload_pvcs(
            "_parallel_clones_same_source", workload_pvcs
        ):
            return
        source = random.choice(workload_pvcs)
        self._create_clones_parallel([source] * cfg["max_parallel"])

    def _repeated_clones(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        if not self._require_workload_pvcs("_repeated_clones", workload_pvcs):
            return
        source = random.choice(workload_pvcs)
        for _ in range(cfg["repeated_clone_count"]):
            self._create_and_track_clone(source, is_nested=False)

    def _nested_clone(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        with self._pool_lock:
            pool_entries = list(self._clone_pool)
        if not pool_entries:
            log.info("No existing clones for nested clone; creating base clone first")
            if self._require_workload_pvcs("_nested_clone", workload_pvcs):
                self._create_and_track_clone(
                    random.choice(workload_pvcs), is_nested=False
                )
            return

        parent_entry = random.choice(pool_entries)
        parent_pvc = parent_entry["pvc"]
        parent_pvc.reload()
        if parent_pvc.status != constants.STATUS_BOUND:
            log.warning(
                "Parent clone %s not bound, skipping nested clone", parent_pvc.name
            )
            return
        self._create_and_track_clone(
            parent_pvc, is_nested=True, original_source=parent_entry.get("source_pvc")
        )

    def _mount_verify_clones(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._require_pool_entry("_mount_verify_clones")
        if not entry:
            return
        clone_pvc = entry["pvc"]
        if not self.bg_ops._can_attach_pod(clone_pvc):
            log.info(
                "Clone %s is not attachable (RWX), skipping mount verify",
                clone_pvc.name,
            )
            return
        test_pod = None
        try:
            test_pod = self.bg_ops._create_test_pod(clone_pvc)
            helpers.wait_for_resource_state(
                resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
            )
            self.bg_ops._verify_pod_data(test_pod)
            log.info("Mount verification passed for clone %s", clone_pvc.name)
        finally:
            with suppress(Exception):
                if test_pod:
                    test_pod.delete()
                    test_pod.ocp.wait_for_delete(
                        resource_name=test_pod.name, timeout=180
                    )

    def _integrity_verify_clones(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._require_pool_entry("_integrity_verify_clones")
        if not entry:
            return
        source_pvc = entry.get("source_pvc")
        clone_pvc = entry["pvc"]
        if source_pvc is None:
            log.warning("No source PVC recorded for clone %s", clone_pvc.name)
            return
        self._verify_clone_integrity(source_pvc, clone_pvc)

    def _vdbench_on_clone(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        """Mount a cloned PVC via Vdbench deployment and run a short I/O workload."""
        clone_pvc = self._get_or_create_clone_for_io(
            workload_pvcs, action_name="_vdbench_on_clone"
        )
        if clone_pvc is None:
            return
        self._run_vdbench_on_pvc(clone_pvc, cfg)

    def _vdbench_on_clones_parallel(
        self, workload_pvcs: List[PVC], cfg: Dict[str, Any]
    ):
        """Run Vdbench concurrently on multiple cloned PVCs."""
        max_parallel = cfg.get("max_vdbench_on_clone_parallel", 2)
        clone_pvcs = self._get_or_create_clones_for_io(
            workload_pvcs, max_parallel, action_name="_vdbench_on_clones_parallel"
        )
        if not clone_pvcs:
            return

        log.info("Running Vdbench on %s clone PVCs in parallel", len(clone_pvcs))
        with ThreadPoolExecutor(max_workers=len(clone_pvcs)) as executor:
            futures = [
                executor.submit(self._run_vdbench_on_pvc, pvc, cfg)
                for pvc in clone_pvcs
            ]
            for future in as_completed(futures):
                with suppress(Exception):
                    future.result()

    def _get_or_create_clone_for_io(
        self,
        workload_pvcs: List[PVC],
        action_name: str = "_get_or_create_clone_for_io",
    ) -> Optional[PVC]:
        entry = self._pick_random_pool_entry()
        if entry:
            log.info(
                "Using existing clone PVC %s from pool for %s",
                entry["pvc"].name,
                action_name,
            )
            return entry["pvc"]
        if not workload_pvcs:
            self._log_skip(
                action_name,
                "clone pool is empty and no workload PVCs available to create one",
            )
            return None
        log.info(
            "Clone pool empty for %s; creating new clone from workload PVC",
            action_name,
        )
        return self._create_and_track_clone(
            random.choice(workload_pvcs), is_nested=False
        )

    def _get_or_create_clones_for_io(
        self,
        workload_pvcs: List[PVC],
        count: int,
        action_name: str = "_get_or_create_clones_for_io",
    ) -> List[PVC]:
        with self._pool_lock:
            entries = list(self._clone_pool)
        clone_pvcs = (
            [entry["pvc"] for entry in random.sample(entries, min(count, len(entries)))]
            if entries
            else []
        )

        while len(clone_pvcs) < count and workload_pvcs:
            created = self._create_and_track_clone(
                random.choice(workload_pvcs), is_nested=False
            )
            if created:
                clone_pvcs.append(created)
            else:
                break
        if not clone_pvcs:
            self._log_skip(
                action_name,
                "could not obtain any clone PVCs from pool or workload sources",
            )
        return clone_pvcs

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

    def _create_vdbench_config_file(self, clone_pvc: PVC, cfg: Dict[str, Any]) -> str:
        from ocs_ci.helpers.vdbench_helpers import create_temp_config_file

        vdbench_config = self._get_vdbench_settings()
        elapsed = cfg.get(
            "vdbench_on_clone_elapsed", vdbench_config.get("elapsed", 120)
        )
        threads = vdbench_config.get("threads", 16)
        interval = cfg.get(
            "vdbench_on_clone_interval", vdbench_config.get("interval", 30)
        )

        if clone_pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
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
                                    "name": "clone_random_write",
                                    "rdpct": 0,
                                    "seekpct": 100,
                                    "xfersize": "4k",
                                    "skew": 0,
                                },
                                {
                                    "name": "clone_random_read",
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
                                    "name": "clone_sequential_write",
                                    "rdpct": 0,
                                    "seekpct": 0,
                                    "xfersize": "1m",
                                    "skew": 0,
                                },
                                {
                                    "name": "clone_random_mixed",
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

    def _run_vdbench_on_pvc(self, clone_pvc: PVC, cfg: Dict[str, Any]):
        from ocs_ci.resiliency.resiliency_workload import VdbenchWorkload

        config_file = None
        workload = None
        try:
            clone_pvc.reload()
            helpers.wait_for_resource_state(
                resource=clone_pvc, state=constants.STATUS_BOUND, timeout=120
            )
            config_file = self._create_vdbench_config_file(clone_pvc, cfg)
            workload = VdbenchWorkload(
                pvc=clone_pvc,
                vdbench_config_file=config_file,
                namespace=self.namespace,
            )
            with self._vdbench_lock:
                self._active_vdbench_workloads.append(workload)

            elapsed = cfg.get("vdbench_on_clone_elapsed", 120)
            interval = cfg.get("vdbench_on_clone_interval", 30)
            log.info(
                "Starting Vdbench on clone PVC %s (elapsed=%ss)",
                clone_pvc.name,
                elapsed,
            )
            workload.start_workload()
            run_seconds = elapsed + interval
            if not self._sleep_interruptible(run_seconds):
                log.info(
                    "Stopping Vdbench early on clone PVC %s (shutdown requested)",
                    clone_pvc.name,
                )
            workload.stop_workload()
            workload.cleanup_workload()
            if self._shutdown_event.is_set():
                log.info("Vdbench on clone PVC %s stopped for teardown", clone_pvc.name)
            else:
                log.info("Vdbench completed on clone PVC %s", clone_pvc.name)
        except Exception as err:
            self._metrics["failures"] += 1
            log.error("Vdbench on clone %s failed: %s", clone_pvc.name, err)
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

    def _expand_random_clone(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._require_pool_entry("_expand_random_clone")
        if not entry:
            return
        clone_pvc = entry["pvc"]
        clone_pvc.reload()
        current_size = clone_pvc.size
        new_size = current_size + random.randint(1, 2)
        try:
            clone_pvc.resize_pvc(new_size, verify=True, timeout=300)
            log.info(
                "Expanded clone %s from %sGi to %sGi",
                clone_pvc.name,
                current_size,
                new_size,
            )
        except Exception as err:
            log.warning(
                "Clone expansion not supported or failed for %s: %s",
                clone_pvc.name,
                err,
            )

    def _snapshot_random_clone(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        entry = self._require_pool_entry("_snapshot_random_clone")
        if not entry:
            return
        clone_pvc = entry["pvc"]
        snapshot_obj = None
        try:
            clone_pvc.reload()
            snapshot_obj = clone_pvc.create_snapshot(wait=True, timeout=180)
            log.info(
                "Created snapshot %s from clone PVC %s",
                snapshot_obj.name,
                clone_pvc.name,
            )
        except Exception as err:
            log.warning("Snapshot of clone %s failed: %s", clone_pvc.name, err)
        finally:
            with suppress(Exception):
                if snapshot_obj:
                    snapshot_obj.delete()
                    snapshot_obj.ocp.wait_for_delete(
                        resource_name=snapshot_obj.name, timeout=180
                    )

    def _delete_random_clones(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        with self._pool_lock:
            if not self._clone_pool:
                self._log_skip("_delete_random_clones", "clone pool is empty")
                return
            count = min(cfg["concurrent_delete_count"], len(self._clone_pool))
            entries = random.sample(self._clone_pool, count)
        log.info("Deleting %s random clone PVCs from pool", len(entries))
        for entry in entries:
            self._delete_tracked_clone(entry)

    def _concurrent_create_delete(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        if not self._require_workload_pvcs("_concurrent_create_delete", workload_pvcs):
            return

        def create_task():
            self._create_and_track_clone(random.choice(workload_pvcs), is_nested=False)

        def delete_task():
            entry = self._pick_random_pool_entry()
            if entry:
                self._delete_tracked_clone(entry)
            else:
                log.debug("Concurrent delete task: clone pool empty, nothing to delete")

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
        log.info("Large-scale parallel clone creation: %s clones", count)
        self._create_clones_parallel(sources)

    def _cleanup_clones(self, workload_pvcs: List[PVC], cfg: Dict[str, Any]):
        with self._pool_lock:
            entries = list(self._clone_pool)
        if not entries:
            self._log_skip("_cleanup_clones", "clone pool is empty")
            return
        log.info("Cleaning up %s aggressive clone PVCs", len(entries))
        for entry in entries:
            self._delete_tracked_clone(entry)
        self._verify_pool_reclamation()

    def _create_clones_parallel(self, sources: List[PVC]):
        with ThreadPoolExecutor(max_workers=min(len(sources), 8)) as executor:
            futures = [
                executor.submit(self._create_and_track_clone, src, False)
                for src in sources
            ]
            for future in as_completed(futures):
                with suppress(Exception):
                    future.result()

    def _create_and_track_clone(
        self,
        source_pvc: PVC,
        is_nested: bool,
        original_source: Optional[PVC] = None,
    ) -> Optional[PVC]:
        start = time.time()
        try:
            source_pvc.reload()
            clone_yaml = self._get_clone_yaml(source_pvc)
            if not clone_yaml:
                return None

            capacity = self.bg_ops._get_pvc_storage_capacity(source_pvc)
            clone_pvc = pvc_helpers.create_pvc_clone(
                sc_name=source_pvc.backed_sc,
                parent_pvc=source_pvc.name,
                clone_yaml=clone_yaml,
                namespace=self.namespace,
                storage_size=capacity,
                access_mode=source_pvc.get_pvc_access_mode,
                volume_mode=source_pvc.get()["spec"]["volumeMode"],
            )
            helpers.wait_for_resource_state(
                resource=clone_pvc, state=constants.STATUS_BOUND, timeout=300
            )
            latency = time.time() - start
            entry = {
                "pvc": clone_pvc,
                "source_pvc": original_source or source_pvc,
                "is_nested": is_nested,
                "latency_seconds": latency,
                "created_at": time.time(),
            }
            with self._pool_lock:
                self._clone_pool.append(entry)
            self._metrics["latencies"].append(latency)
            self._metrics["successes"] += 1
            log.info(
                "Created clone %s from %s in %.2fs (nested=%s)",
                clone_pvc.name,
                source_pvc.name,
                latency,
                is_nested,
            )
            return clone_pvc
        except Exception as err:
            self._metrics["failures"] += 1
            log.error("Failed to create clone from %s: %s", source_pvc.name, err)
            return None

    def _delete_tracked_clone(self, entry: Dict[str, Any]):
        clone_pvc = entry.get("pvc")
        if clone_pvc is None:
            return
        try:
            clone_pvc.reload()
            if clone_pvc.is_deleted:
                with self._pool_lock:
                    if entry in self._clone_pool:
                        self._clone_pool.remove(entry)
                return
            clone_pvc.delete()
            clone_pvc.ocp.wait_for_delete(resource_name=clone_pvc.name, timeout=180)
            log.info("Deleted aggressive clone PVC %s", clone_pvc.name)
        except Exception as err:
            log.warning(
                "Failed to delete clone %s: %s", getattr(clone_pvc, "name", "?"), err
            )
        finally:
            with self._pool_lock:
                if entry in self._clone_pool:
                    self._clone_pool.remove(entry)

    def _pick_random_pool_entry(self) -> Optional[Dict[str, Any]]:
        with self._pool_lock:
            if not self._clone_pool:
                return None
            return random.choice(self._clone_pool)

    def _get_clone_yaml(self, pvc_obj: PVC) -> Optional[str]:
        provisioner = (getattr(pvc_obj, "provisioner", "") or "").strip().lower()
        if "rbd" in provisioner:
            return constants.CSI_RBD_PVC_CLONE_YAML
        if "cephfs" in provisioner:
            return constants.CSI_CEPHFS_PVC_CLONE_YAML
        log.warning("Unsupported provisioner for aggressive clone: %r", provisioner)
        return None

    def _verify_clone_integrity(self, source_pvc: PVC, clone_pvc: PVC):
        if not self.bg_ops._can_attach_pod(clone_pvc):
            log.info(
                "Skipping integrity check for non-attachable clone %s", clone_pvc.name
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
                test_pod = self.bg_ops._create_test_pod(clone_pvc)
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
                test_pod = self.bg_ops._create_test_pod(clone_pvc)
                helpers.wait_for_resource_state(
                    resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
                )
                clone_checksum_cmd = (
                    f"dd if={constants.RAW_BLOCK_DEVICE} bs=1M count=1 "
                    "2>/dev/null | md5sum | awk '{print $1}'"
                )
                clone_sum = _exec_shell_on_pod(test_pod, clone_checksum_cmd)
            else:
                source_mount = "/vdbench-data"
                clone_mount = "/mnt"
                sample_file = _exec_shell_on_pod(
                    source_pod,
                    f"find {source_mount} -type f 2>/dev/null | head -1",
                )
                if not sample_file:
                    log.warning(
                        "No files on source PVC %s for integrity check", source_pvc.name
                    )
                    test_pod = self.bg_ops._create_test_pod(clone_pvc)
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
                test_pod = self.bg_ops._create_test_pod(clone_pvc)
                helpers.wait_for_resource_state(
                    resource=test_pod, state=constants.STATUS_RUNNING, timeout=120
                )
                clone_path = f"{clone_mount}/{relative}"
                clone_sum = _exec_shell_on_pod(
                    test_pod,
                    f"md5sum {shlex.quote(clone_path)} | awk '{{print $1}}'",
                )

            if source_sum == clone_sum:
                log.info(
                    "Data integrity verified between %s and clone %s",
                    source_pvc.name,
                    clone_pvc.name,
                )
            else:
                log.warning(
                    "Checksum mismatch: source %s vs clone %s (%s != %s)",
                    source_pvc.name,
                    clone_pvc.name,
                    source_sum,
                    clone_sum,
                )
        except Exception as err:
            log.warning(
                "Integrity verification failed for clone %s: %s", clone_pvc.name, err
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
            remaining = [e["pvc"].name for e in self._clone_pool if e.get("pvc")]
        if remaining:
            log.warning("Clone pool not fully reclaimed, remaining: %s", remaining)
        else:
            log.info("All aggressive clone PVCs reclaimed successfully")

    def _log_metrics(self):
        with self._pool_lock:
            pool_size = len(self._clone_pool)
        latencies = self._metrics["latencies"]
        total = self._metrics["successes"] + self._metrics["failures"]
        if not latencies and total == 0:
            log.info(
                "Aggressive clone metrics: no clone operations recorded yet "
                "(pool_size=%s)",
                pool_size,
            )
            return
        avg_latency = sum(latencies) / len(latencies) if latencies else 0.0
        success_rate = (self._metrics["successes"] / total * 100) if total else 0
        log.info(
            "Aggressive clone metrics: successes=%s failures=%s "
            "avg_latency=%.2fs success_rate=%.1f%% pool_size=%s",
            self._metrics["successes"],
            self._metrics["failures"],
            avg_latency,
            success_rate,
            pool_size,
        )

    def cleanup_all(self):
        """Stop workloads and delete all aggressive-clone resources in the namespace."""
        log.info(
            "Cleaning up aggressive clone operation resources in %s", self.namespace
        )
        self.request_shutdown()

        with self._vdbench_lock:
            vdbench_workloads = list(self._active_vdbench_workloads)
        for workload in vdbench_workloads:
            with suppress(Exception):
                workload.stop_workload()
                workload.cleanup_workload()

        with self._pool_lock:
            entries = list(self._clone_pool)
        for entry in entries:
            self._delete_tracked_clone(entry)

        self._cleanup_orphan_aggressive_resources()
        self._verify_pool_reclamation()

    def _cleanup_orphan_aggressive_resources(self):
        """Best-effort sweep for leftover clone/test/vdbench resources."""
        from ocs_ci.ocs.ocp import OCP

        clone_pvc_names = set()

        try:
            pvc_ocp = OCP(
                kind="PersistentVolumeClaim",
                namespace=self.namespace,
            )
            for item in pvc_ocp.get().get("items", []):
                name = item["metadata"]["name"]
                if name.startswith("clone-"):
                    clone_pvc_names.add(name)
                    with suppress(Exception):
                        pvc_ocp.delete(resource_name=name, wait=False)
                        log.info("Deleted orphan aggressive clone PVC %s", name)
        except Exception as err:
            log.warning("Failed to sweep orphan clone PVCs: %s", err)

        try:
            pod_ocp = OCP(kind="Pod", namespace=self.namespace)
            for item in pod_ocp.get().get("items", []):
                name = item["metadata"]["name"]
                if name.startswith("test-pod-clone-"):
                    with suppress(Exception):
                        pod_ocp.delete(resource_name=name, wait=False)
                        log.info("Deleted orphan aggressive clone test pod %s", name)
        except Exception as err:
            log.warning("Failed to sweep orphan clone test pods: %s", err)

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
                        claim in clone_pvc_names or claim.startswith("clone-")
                    ):
                        with suppress(Exception):
                            deploy_ocp.delete(resource_name=name, wait=False)
                            log.info(
                                "Deleted orphan Vdbench deployment %s on clone PVC %s",
                                name,
                                claim,
                            )
                        break
        except Exception as err:
            log.warning("Failed to sweep orphan Vdbench deployments on clones: %s", err)
