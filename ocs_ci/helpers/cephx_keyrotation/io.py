"""Background dd I/O helpers used by CephX CSI rotation tests."""

import logging
import shlex
import time
from threading import Thread

from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


class CephXIOHelper:
    """Background dd I/O helpers for CSI CephX rotation tests."""

    @staticmethod
    def _dd_io_stop_flag(file_path):
        """Return a pod-local stop-flag path for background dd I/O."""
        safe = file_path.strip("/").replace("/", "-")
        return f"/tmp/ocs-ci-stop-dd-{safe}"

    def start_dd_io_in_background(
        self, pod_obj, file_path, bs="4k", count=10000, loop=True
    ):
        """
        Start continuous ``dd`` I/O on a pod mount path in a background thread.

        Args:
            pod_obj: Pod object with ``exec_cmd_on_pod``.
            file_path (str): Destination file path on the mounted volume.
            bs (str): Block size for ``dd``.
            count (int): Block count per ``dd`` invocation.
            loop (bool): When True, repeat ``dd`` until stopped.

        Returns:
            Thread: Background I/O thread.
        """
        mount_dir = file_path.rsplit("/", 1)[0]
        stop_flag = self._dd_io_stop_flag(file_path)
        pod_obj.exec_cmd_on_pod(f"mkdir -p {mount_dir}", out_yaml_format=False)
        pod_obj.exec_cmd_on_pod(f"rm -f {stop_flag}", out_yaml_format=False)

        if loop:
            # Stop via stop-flag file — workload images often lack pkill/procps.
            dd_cmd = (
                f"while [ ! -f {stop_flag} ]; do "
                f"dd if=/dev/urandom of={file_path} bs={bs} count={count} "
                f"status=none conv=notrunc; "
                f"done; "
                f"rm -f {stop_flag}"
            )
        else:
            dd_cmd = (
                f"dd if=/dev/urandom of={file_path} bs={bs} count={count} "
                f"status=none"
            )

        def _run_dd():
            try:
                pod_obj.exec_cmd_on_pod(
                    command=f"bash -c '{dd_cmd}'",
                    timeout=7200,
                    out_yaml_format=False,
                )
            except Exception as exc:
                # Expected when stop_dd_io kills the in-flight dd/shell.
                log.info(
                    "Background dd I/O on %s:%s ended: %s",
                    pod_obj.name,
                    file_path,
                    exc,
                )

        thread = Thread(target=_run_dd, name=f"dd-io-{pod_obj.name}")
        thread.daemon = True
        thread.start()
        time.sleep(2)
        log.info(f"Started background dd I/O on {pod_obj.name}:{file_path}")
        return thread

    def stop_dd_io(self, pod_obj, file_path):
        """
        Stop background ``dd`` I/O started by :meth:`start_dd_io_in_background`.

        Uses a stop-flag file plus ``/proc`` cmdline matching so it works in
        minimal workload images that do not ship ``pkill``/``procps``.

        Skips the stop script's own PID when scanning ``/proc`` — the script
        text itself contains ``of=<file_path>``, so a naive match would
        SIGTERM the ``oc rsh`` shell (exit 143). Exit 143 from the stop
        command is still tolerated as a race with the background dd session.
        """
        stop_flag = self._dd_io_stop_flag(file_path)
        # shlex.quote keeps $pid expansion inside bash -c intact.
        # Exclude $$ so the stop shell does not match its own cmdline.
        stop_script = (
            f"touch {stop_flag}; "
            "self=$$; "
            "for pid in /proc/[0-9]*; do "
            "pidnum=${pid##*/}; "
            '[ "$pidnum" = "$self" ] && continue; '
            'cmd=$(tr "\\0" " " < "$pid/cmdline" 2>/dev/null) || continue; '
            f'case "$cmd" in '
            f'*dd*of={file_path}*) kill "$pidnum" 2>/dev/null || true ;; '
            "esac; "
            "done; "
            "true"
        )
        try:
            pod_obj.exec_cmd_on_pod(
                command=f"bash -c {shlex.quote(stop_script)}",
                out_yaml_format=False,
                timeout=60,
            )
        except CommandFailed as exc:
            # 143 = 128 + SIGTERM; expected if the stop rsh is terminated
            # while tearing down the background dd session.
            if "exit code 143" not in str(exc):
                raise
            log.info(
                "stop_dd_io on %s:%s returned exit 143 (SIGTERM); "
                "treating background dd as stopped",
                pod_obj.name,
                file_path,
            )
        log.info(f"Stopped background dd I/O on {pod_obj.name}:{file_path}")

    def verify_io_file_readable(self, pod_obj, file_path):
        """Assert *file_path* exists on the pod volume and is readable."""
        # oc rsh does not invoke a shell, so compound commands need bash -c.
        verify_script = (
            f"test -s {shlex.quote(file_path)} && "
            f"dd if={shlex.quote(file_path)} of=/dev/null bs=4k count=1 status=none"
        )
        pod_obj.exec_cmd_on_pod(
            command=f"bash -c {shlex.quote(verify_script)}",
            out_yaml_format=False,
        )
        log.info(f"Verified I/O file is readable on {pod_obj.name}:{file_path}")
