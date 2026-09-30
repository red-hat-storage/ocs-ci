"""
Redfish helpers for Lenovo XCC / SE350 VirtualMedia ISO discovery boot.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import redfish
import urllib3

from ocs_ci.ocs.exceptions import CommandFailed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

OK_STATUS = {200, 201, 202, 204}
DEFAULT_SYSTEM_URI = "/redfish/v1/Systems/1"
DEFAULT_VM_COLLECTION = "/redfish/v1/Managers/1/VirtualMedia"


class RedfishClient:
    """Thin wrapper around a Redfish session for one BMC."""

    def __init__(self, bmc_ip, username, password, default_prefix="/redfish/v1"):
        self.bmc_ip = bmc_ip
        self.client = redfish.redfish_client(
            base_url=f"https://{bmc_ip}",
            username=username,
            password=password,
            default_prefix=default_prefix,
        )
        self.client.login(auth="session")

    def close(self):
        try:
            self.client.logout()
        except Exception as exc:
            logger.warning("[%s] logout failed: %s", self.bmc_ip, exc)

    def _check(self, response, action):
        if response.status not in OK_STATUS:
            raise CommandFailed(
                f"[{self.bmc_ip}] Failed to {action}. "
                f"Status: {response.status}, Error: {response.text}"
            )
        return response

    def find_free_cd_slot(self, vm_collection=DEFAULT_VM_COLLECTION):
        resp = self._check(self.client.get(vm_collection), "list virtual media")
        for member in resp.dict.get("Members", []):
            candidate = member["@odata.id"]
            vm = self.client.get(candidate)
            if vm.status not in OK_STATUS:
                continue
            media_types = vm.dict.get("MediaTypes", [])
            if ("CD" in media_types or "DVD" in media_types) and not vm.dict.get(
                "Inserted", False
            ):
                return candidate
        raise CommandFailed(f"[{self.bmc_ip}] No free CD/DVD VirtualMedia slot found")

    def find_cd_slot(self, vm_collection=DEFAULT_VM_COLLECTION):
        """Return first CD/DVD VirtualMedia URI (inserted or not)."""
        resp = self._check(self.client.get(vm_collection), "list virtual media")
        for member in resp.dict.get("Members", []):
            candidate = member["@odata.id"]
            vm = self.client.get(candidate)
            if vm.status not in OK_STATUS:
                continue
            media_types = vm.dict.get("MediaTypes", [])
            if "CD" in media_types or "DVD" in media_types:
                return candidate
        return None

    def attach_iso(self, iso_url, vm_uri=None):
        vm_uri = vm_uri or self.find_free_cd_slot()
        vm = self._check(self.client.get(vm_uri), "read virtual media slot")
        insert_uri = (
            vm.dict.get("Actions", {})
            .get("#VirtualMedia.InsertMedia", {})
            .get("target")
        )
        if insert_uri:
            payload = {
                "Image": iso_url,
                "Inserted": True,
                "WriteProtected": True,
                "TransferProtocol": "HTTPS",
                "VerifyCertificate": False,
            }
            self._check(self.client.post(insert_uri, body=payload), "mount ISO")
        else:
            payload = {"Image": iso_url, "Inserted": True, "WriteProtected": True}
            self._check(self.client.patch(vm_uri, body=payload), "mount ISO (PATCH)")

        time.sleep(5)
        vm = self._check(self.client.get(vm_uri), "verify virtual media")
        if not vm.dict.get("Inserted", False):
            raise CommandFailed(
                f"[{self.bmc_ip}] Mount verification failed - Inserted=False"
            )
        logger.info("[%s] ISO attached: %s", self.bmc_ip, vm.dict.get("Image"))
        return vm_uri

    def eject_iso(self, vm_uri=None):
        """Eject media from the CD/DVD slot (best-effort)."""
        vm_uri = vm_uri or self.find_cd_slot()
        if not vm_uri:
            logger.info("[%s] No CD/DVD VirtualMedia slot found to eject", self.bmc_ip)
            return

        vm = self.client.get(vm_uri)
        if vm.status not in OK_STATUS:
            logger.warning(
                "[%s] Failed to read VirtualMedia %s: %s",
                self.bmc_ip,
                vm_uri,
                vm.status,
            )
            return

        if not vm.dict.get("Inserted", False):
            logger.info("[%s] VirtualMedia already empty: %s", self.bmc_ip, vm_uri)
            return

        eject_uri = (
            vm.dict.get("Actions", {}).get("#VirtualMedia.EjectMedia", {}).get("target")
        )
        if eject_uri:
            self._check(self.client.post(eject_uri, body={}), "eject ISO")
        else:
            self._check(
                self.client.patch(vm_uri, body={"Inserted": False, "Image": None}),
                "eject ISO (PATCH)",
            )
        logger.info("[%s] ISO ejected from %s", self.bmc_ip, vm_uri)

    def set_boot_override_cd_once(self, system_uri=DEFAULT_SYSTEM_URI):
        payload = {
            "Boot": {
                "BootSourceOverrideTarget": "Cd",
                "BootSourceOverrideEnabled": "Once",
            }
        }
        self._check(
            self.client.patch(system_uri, body=payload), "set boot override to Cd"
        )

    def get_power_state(self, system_uri=DEFAULT_SYSTEM_URI):
        resp = self._check(self.client.get(system_uri), "get system state")
        return resp.dict.get("PowerState", "Unknown")

    def _reset(self, reset_type, system_uri=DEFAULT_SYSTEM_URI):
        reset_uri = f"{system_uri}/Actions/ComputerSystem.Reset"
        return self.client.post(reset_uri, body={"ResetType": reset_type})

    @staticmethod
    def _is_chassis_off_error(response):
        """True when XCC rejects reset because chassis/system is powered off."""
        if response.status not in (400, 409):
            return False
        text = (response.text or "").lower()
        return (
            "chassispowerstateonrequired" in text
            or "requires to be powered on" in text
            or "powered off" in text
        )

    def power_on_or_restart(self, system_uri=DEFAULT_SYSTEM_URI):
        """
        Power on if the host is off; ForceRestart if it is on.

        Lenovo XCC sometimes reports PowerState inconsistently with chassis
        power. If ForceRestart fails because the chassis is off, fall back to On.
        """
        power_state = self.get_power_state(system_uri)
        logger.info("[%s] PowerState=%s", self.bmc_ip, power_state)

        # Only restart when clearly On; any other state (Off, Unknown, ...) -> On
        if power_state == "On":
            response = self._reset("ForceRestart", system_uri=system_uri)
            if self._is_chassis_off_error(response):
                logger.warning(
                    "[%s] ForceRestart rejected (chassis off); falling back to On. "
                    "Body: %s",
                    self.bmc_ip,
                    response.text,
                )
                self._check(self._reset("On", system_uri=system_uri), "power on")
                logger.info(
                    "[%s] power on issued after ForceRestart fallback (reported %s)",
                    self.bmc_ip,
                    power_state,
                )
                return
            self._check(response, "force restart")
            logger.info("[%s] force restart issued (was %s)", self.bmc_ip, power_state)
            return

        self._check(self._reset("On", system_uri=system_uri), "power on")
        logger.info("[%s] power on issued (was %s)", self.bmc_ip, power_state)


def attach_iso_and_boot(bmc_ip, username, password, iso_url):
    """Attach discovery ISO and boot once from CD for a single BMC."""
    rf = RedfishClient(bmc_ip, username, password)
    try:
        rf.attach_iso(iso_url)
        rf.set_boot_override_cd_once()
        rf.power_on_or_restart()
    finally:
        rf.close()


def eject_iso(bmc_ip, username, password):
    """Eject VirtualMedia ISO without restarting the host."""
    rf = RedfishClient(bmc_ip, username, password)
    try:
        rf.eject_iso()
    finally:
        rf.close()


def eject_iso_and_restart(bmc_ip, username, password):
    """Eject VirtualMedia and power-cycle so the host boots from disk."""
    rf = RedfishClient(bmc_ip, username, password)
    try:
        rf.eject_iso()
        rf.power_on_or_restart()
    finally:
        rf.close()


def attach_iso_and_boot_nodes(servers, iso_url, max_workers=5):
    """
    Parallel attach+boot for multiple servers.

    Args:
        servers (dict): name -> server detail dict with
            mgmt_console / mgmt_username / mgmt_password
        iso_url (str): Assisted Installer discovery ISO URL
        max_workers (int): parallel Redfish workers
    """

    def _one(name, details):
        try:
            logger.info("[%s] Attaching discovery ISO via Redfish", name)
            attach_iso_and_boot(
                details["mgmt_console"],
                details["mgmt_username"],
                details["mgmt_password"],
                iso_url,
            )
            return name, True, "ok"
        except Exception as exc:
            logger.error("[%s] Redfish ISO boot failed: %s", name, exc)
            return name, False, str(exc)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_one, name, details): name
            for name, details in servers.items()
        }
        for future in as_completed(futures):
            results.append(future.result())

    failed = [(n, msg) for n, ok, msg in results if not ok]
    if failed:
        raise CommandFailed(f"Redfish ISO boot failed for: {failed}")
    logger.info("Redfish ISO boot succeeded for %s node(s)", len(results))


def eject_iso_nodes(servers, max_workers=5, restart=False):
    """
    Parallel eject of VirtualMedia ISOs.

    Args:
        servers (dict): name -> server detail dict
        max_workers (int): parallel Redfish workers
        restart (bool): if True, power-cycle after eject
    """

    def _one(name, details):
        try:
            if restart:
                eject_iso_and_restart(
                    details["mgmt_console"],
                    details["mgmt_username"],
                    details["mgmt_password"],
                )
            else:
                eject_iso(
                    details["mgmt_console"],
                    details["mgmt_username"],
                    details["mgmt_password"],
                )
            return name, True, "ok"
        except Exception as exc:
            logger.warning("[%s] Redfish eject failed: %s", name, exc)
            return name, False, str(exc)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_one, name, details) for name, details in servers.items()
        ]
        for future in as_completed(futures):
            future.result()
