"""
CLI-side checks for VMs created through the OpenShift console (hybrid with UI).

"""

from __future__ import annotations

import logging
import re
import shlex
from typing import Optional, Tuple

import pexpect

from ocs_ci.ocs import constants
from ocs_ci.ocs.cnv.virtual_machine import VirtualMachine
from ocs_ci.ocs.ocp import OCP

logger = logging.getLogger(__name__)


def boot_volume_claim_name(vm_dict: dict) -> Optional[str]:
    """
    Return the backing PVC (or DV/PVC name) for the first boot volume.
    """
    spec = vm_dict.get("spec", {}).get("template", {}).get("spec", {})
    for vol in spec.get("volumes", []):
        pvc = vol.get("persistentVolumeClaim")
        if pvc and pvc.get("claimName"):
            return pvc["claimName"]
        dv = vol.get("dataVolume")
        if dv and dv.get("name"):
            return dv["name"]
    return None


def verify_cli_post_ui_vm(
    namespace: str, vm_name: str, timeout: int = 900
) -> VirtualMachine:
    """
    Assert VM is Running, a virt-launcher pod is Running, and boot PVC is Bound.
    """
    vm = VirtualMachine(vm_name=vm_name, namespace=namespace)
    logger.info("CLI: waiting for VM %s/%s Running", namespace, vm_name)
    vm.wait_for_vm_status(status=constants.VM_RUNNING, timeout=timeout)
    vm.vmi_obj.wait_for_vmi_to_be_running(timeout=timeout)

    logger.info(
        "CLI: waiting for virt-launcher pod Running (label vm.kubevirt.io/name)"
    )
    assert OCP(kind=constants.POD, namespace=namespace).wait_for_resource(
        condition=constants.STATUS_RUNNING,
        selector=f"vm.kubevirt.io/name={vm_name}",
        resource_count=1,
        timeout=timeout,
    ), "Virt launcher pod not Running"

    claim = boot_volume_claim_name(vm.get())
    assert claim, "Could not resolve boot PVC / DataVolume PVC name from VM spec"
    logger.info("CLI: waiting for PVC %s Bound", claim)
    assert OCP(kind=constants.PVC, namespace=namespace).wait_for_resource(
        condition=constants.STATUS_BOUND,
        resource_name=claim,
        timeout=timeout,
    ), f"PVC {claim} not Bound"
    return vm


def _virtctl_password_ssh_captured_output(
    namespace: str,
    vm_name: str,
    username: str,
    password: str,
    remote_bash_command: str,
    timeout: int = 600,
) -> str:
    """
    Run ``virtctl ssh`` and answer an SSH password prompt via pexpect.
    """
    script = f"bash -lc {shlex.quote(remote_bash_command)}"
    cmd = (
        f"virtctl -n {shlex.quote(namespace)} ssh "
        f"vmi/{shlex.quote(vm_name)} "
        f"--username={shlex.quote(username)} "
        "--local-ssh-opts='-o StrictHostKeyChecking=no "
        "-o PreferredAuthentications=password -o PubkeyAuthentication=no' "
        f"-c {shlex.quote(script)}"
    )
    logger.debug("virtctl ssh command: %s", cmd)
    child = pexpect.spawn(cmd, timeout=timeout, encoding="utf-8")
    patterns = ["password:", "Password:", pexpect.EOF]
    index = child.expect(patterns, timeout=120)
    if index in (0, 1):
        child.sendline(password)
    child.expect(pexpect.EOF, timeout=timeout)
    return (child.before or "") + (child.after or "")


def verify_guest_file_md5(
    namespace: str,
    vm_name: str,
    *,
    ssh_user: str = "fedora",
    ssh_password: str = "fedora",
    remote_path: str = "/tmp/ocs_ci_ui_md5_check.txt",
    test_payload: str = "ocs-ci-vm-ui-check\n",
    timeout: int = 900,
) -> Tuple[str, str]:
    """
    Write ``test_payload`` on the guest, print md5sum, and return (md5, line).

    Tries key-based ``VirtualMachine.run_ssh_cmd`` first (if accessCredentials
    exist); otherwise uses password auth via ``virtctl ssh`` + pexpect.
    """
    vm = VirtualMachine(vm_name=vm_name, namespace=namespace)
    remote_bash = (
        f"set -e; echo -n {shlex.quote(test_payload)} > {shlex.quote(remote_path)} "
        f"&& md5sum {shlex.quote(remote_path)}"
    )
    out = ""
    try:
        out = vm.run_ssh_cmd(remote_bash, username=ssh_user, use_sudo=False)
        logger.info("Guest md5 check via key-based virtctl ssh succeeded")
    except Exception as exc:
        logger.warning(
            "Key-based SSH failed (%s); falling back to password virtctl ssh", exc
        )
        out = _virtctl_password_ssh_captured_output(
            namespace, vm_name, ssh_user, ssh_password, remote_bash, timeout=timeout
        )

    m = re.search(r"^([0-9a-f]{32})\s+", out, flags=re.MULTILINE | re.IGNORECASE)
    assert m, f"md5sum not found in command output: {out!r}"
    digest = m.group(1).lower()
    logger.info("Recorded guest md5 %s for %s", digest, remote_path)
    return digest, out


def assert_md5_matches_local(digest: str, payload: str) -> None:
    import hashlib

    expected = hashlib.md5(payload.encode("utf-8")).hexdigest()
    assert digest == expected, f"md5 mismatch guest={digest} local={expected}"


def verify_hybrid_cli_for_vm(
    namespace: str,
    vm_name: str,
    test_payload: str = "ocs-ci-vm-ui-check",
) -> None:
    """
    Re-check Running (VM / pod / PVC) then guest file md5 against local digest.
    """
    verify_cli_post_ui_vm(namespace, vm_name)
    digest, _ = verify_guest_file_md5(namespace, vm_name, test_payload=test_payload)
    assert_md5_matches_local(digest, test_payload)
