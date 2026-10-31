"""
Redfish helpers for Lenovo XCC / SE350 VirtualMedia ISO discovery boot.

SE350 XCC does not behave like generic Redfish (Dell iDRAC, etc.):

* VirtualMedia.InsertMedia POST returns 404; mount with PATCH
  ``{Image, Inserted: true}``.
* PATCH 200 only means the BMC accepted the request. Poll until media is
  actually inserted; XCC can report ConnectedVia=URI too early, so require
  two consecutive good polls (interval is MOUNT_WAIT_SLEEP, no extra sleep
  after the second success).
* DELETE of the Redfish session (logout) unmounts URI VirtualMedia. After
  attach+boot, close HTTP without logout. After eject, logout is correct.
* ForceRestart plus immediate logout was observed to unmount minimal.iso
  and let LXPM mount local tdm_image.img. Hold the session after restart
  (POST_BOOT_SESSION_HOLD) and re-check media.
* Discovery boot must ForceRestart (BMC "Hard Reset"). Bare Power On is a
  no-op if the chassis is already On (BMC still logs the attempt). Prefer
  ForceRestart first; only if XCC says chassis-off: On → re-apply Cd Once
  → ForceRestart.
* XCC default SessionTimeout is 300s idle; an abandoned mount session that
  expires can unmount URI media before discovery finishes. Raise
  SessionTimeout to DISCOVERY_SESSION_TIMEOUT for the discovery window,
  then restore the previous value after eject (or via
  restore_session_timeout_nodes).
* All nodes must mount before any node restarts (threading.Barrier).
  One thread per BMC so the barrier cannot deadlock.

Public entry points used by baremetal deployment:
``attach_iso_and_boot_nodes``, ``eject_iso_nodes``,
``restore_session_timeout_nodes``.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import redfish
import urllib3

from ocs_ci.ocs.exceptions import APIRequestError
from ocs_ci.utility.assisted_installer import redact_iso_url

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)
# python-redfish logs every GET/PATCH as INFO (Attempt / Response Time).
logging.getLogger("redfish.rest.v1").setLevel(logging.WARNING)

OK_STATUS = {200, 201, 202, 204}
DEFAULT_SYSTEM_URI = "/redfish/v1/Systems/1"
DEFAULT_VIRTUAL_MEDIA_COLLECTION = "/redfish/v1/Managers/1/VirtualMedia"
DEFAULT_SESSION_SERVICE_URI = "/redfish/v1/SessionService"
MOUNT_WAIT_TIMEOUT = 300
MOUNT_WAIT_SLEEP = 10
# After ForceRestart, keep the session (no logout) and re-GET media so we
# fail if XCC dropped the ISO instead of returning success.
POST_BOOT_SESSION_HOLD = 120
POST_BOOT_SESSION_SLEEP = 15
# Chassis-off fallback only (ForceRestart rejected): after Power On, poll
# until PowerState=On before ForceRestart. This is a max wait, not a fixed
# sleep — return as soon as On. Immediate ForceRestart after On often fails
# again while XCC still reports off. Bare Power On must not be the final
# action: if the chassis is already On, Power On is a no-op (BMC still logs
# "Attempting to Power On") and the discovery ISO is never taken.
POWER_ON_BEFORE_RESTART_TIMEOUT = 120
POWER_ON_BEFORE_RESTART_SLEEP = 5
# Lenovo default SessionTimeout is 300s; discovery boot+wait often exceeds that.
DISCOVERY_SESSION_TIMEOUT = 1800  # 30 minutes
# Attach retries and peer barrier must share this budget so a slow BMC that
# needs attempt 2/3 does not break peers waiting at the barrier.
ATTACH_ISO_ATTEMPTS = 3
ATTACH_ISO_RETRY_SLEEP = 5
BARRIER_WAIT_MARGIN = 60
BARRIER_WAIT_TIMEOUT = (
    ATTACH_ISO_ATTEMPTS * (MOUNT_WAIT_TIMEOUT + ATTACH_ISO_RETRY_SLEEP)
    + BARRIER_WAIT_MARGIN
)

# bmc_ip -> SessionTimeout value before we raised it for discovery ISO.
_saved_session_timeouts = {}
_saved_session_timeouts_lock = threading.Lock()


def _node_tag(node_name, bmc_ip):
    """
    Hostname-first log tag; fall back to BMC IP alone.

    Args:
        node_name (str): server/hostname, or None/empty
        bmc_ip (str): BMC address

    Returns:
        str: ``hostname bmc=ip`` or ``ip``
    """
    if node_name:
        return f"{node_name} bmc={bmc_ip}"
    return str(bmc_ip)


class RedfishClient:
    """
    One Redfish session against one BMC (Lenovo XCC).

    Login uses session auth. Callers must close() explicitly: logout=True
    DELETEs the session (eject path); logout=False only drops HTTP so
    VirtualMedia stays mounted (attach+boot path).
    """

    def __init__(
        self,
        bmc_ip,
        username,
        password,
        default_prefix="/redfish/v1",  # gitleaks:allow - Redfish API path, not a secret
        node_name=None,
    ):
        """
        Open a Redfish session to the BMC.

        Args:
            bmc_ip (str): BMC address (mgmt_console)
            username (str): BMC username
            password (str): BMC password
            default_prefix (str): Redfish root path
            node_name (str): optional hostname for log tags
        """
        self.bmc_ip = bmc_ip
        self.node_name = node_name
        self.client = redfish.redfish_client(
            base_url=f"https://{bmc_ip}",
            username=username,
            password=password,
            default_prefix=default_prefix,
        )
        self.client.login(auth="session")

    @property
    def log_tag(self):
        """Hostname-first tag for INFO/WARNING lines."""
        return _node_tag(self.node_name, self.bmc_ip)

    def close(self, logout=True):
        """
        Close the Redfish client.

        Args:
            logout (bool): If True, DELETE the BMC session (use after eject).
                If False, abandon the session without DELETE so URI VirtualMedia
                stays mounted (use after attach+boot). XCC unmounts the discovery
                ISO on session DELETE and LXPM may then boot tdm_image.img.
        """
        try:
            if logout:
                self.client.logout()
                return
            # Abandon session without DELETE so VirtualMedia stays mounted.
            try:
                self.client.set_session_key(None)
                self.client.set_session_location(None)
            except Exception as exc:
                logger.debug(
                    "[%s] ignoring session-key clear failure during no-logout close: %s",
                    self.bmc_ip,
                    exc,
                )
            http = getattr(self.client, "_session", None)
            if http is not None:
                http.close()
            logger.debug(
                "[%s] Redfish HTTP closed without session logout "
                "(keep VirtualMedia mounted)",
                self.bmc_ip,
            )
        except Exception as exc:
            logger.warning("[%s] close failed: %s", self.log_tag, exc)

    def get_session_timeout(self):
        """
        Read SessionService.SessionTimeout (idle seconds).

        Returns:
            int or None: timeout in seconds, or None if unavailable
        """
        try:
            response = self.client.get(DEFAULT_SESSION_SERVICE_URI)
            if response.status not in OK_STATUS:
                logger.warning(
                    "[%s] GET SessionService failed: status=%s",
                    self.log_tag,
                    response.status,
                )
                return None
            value = response.dict.get("SessionTimeout")
            return int(value) if value is not None else None
        except Exception as exc:
            logger.warning("[%s] get SessionTimeout failed: %s", self.log_tag, exc)
            return None

    def set_session_timeout(self, seconds):
        """
        PATCH SessionService.SessionTimeout.

        Args:
            seconds (int): idle timeout in seconds (Lenovo: 30..86400)

        Returns:
            bool: True if the BMC accepted the change
        """
        try:
            response = self.client.patch(
                DEFAULT_SESSION_SERVICE_URI,
                body={"SessionTimeout": int(seconds)},
            )
            if response.status not in OK_STATUS:
                logger.warning(
                    "[%s] PATCH SessionTimeout=%s failed: status=%s",
                    self.log_tag,
                    seconds,
                    response.status,
                )
                return False
            return True
        except Exception as exc:
            logger.warning(
                "[%s] set SessionTimeout=%s failed: %s", self.log_tag, seconds, exc
            )
            return False

    def extend_session_timeout_for_discovery(self):
        """
        Raise SessionTimeout to DISCOVERY_SESSION_TIMEOUT if lower.

        Remembers the previous value once per BMC so it can be restored after
        discovery eject. Safe to call again on retry boots.
        """
        current = self.get_session_timeout()
        if current is None:
            logger.warning(
                "[%s] SessionTimeout unavailable; cannot extend for discovery",
                self.log_tag,
            )
            return
        with _saved_session_timeouts_lock:
            _saved_session_timeouts.setdefault(self.bmc_ip, current)
        if current >= DISCOVERY_SESSION_TIMEOUT:
            logger.debug(
                "[%s] SessionTimeout already %ss (>= %ss)",
                self.log_tag,
                current,
                DISCOVERY_SESSION_TIMEOUT,
            )
            return
        if self.set_session_timeout(DISCOVERY_SESSION_TIMEOUT):
            logger.info(
                "[%s] SessionTimeout %ss -> %ss for discovery ISO window",
                self.log_tag,
                current,
                DISCOVERY_SESSION_TIMEOUT,
            )

    def restore_session_timeout_after_discovery(self, log=True):
        """
        Restore SessionTimeout saved by extend_session_timeout_for_discovery.

        No-op if this BMC was never extended or already restored. The saved
        value is removed only after a successful restore (or when already at
        the original value) so a failed PATCH can be retried later.

        Args:
            log (bool): if True, log restore at INFO; if False, caller logs

        Returns:
            str or None: short status for combined log lines, or None if no-op
        """
        with _saved_session_timeouts_lock:
            original = _saved_session_timeouts.get(self.bmc_ip)
        if original is None:
            return None
        current = self.get_session_timeout()
        if current == original:
            with _saved_session_timeouts_lock:
                _saved_session_timeouts.pop(self.bmc_ip, None)
            logger.debug(
                "[%s] SessionTimeout already %ss; nothing to restore",
                self.log_tag,
                original,
            )
            return f"SessionTimeout already {original}s"
        if self.set_session_timeout(original):
            with _saved_session_timeouts_lock:
                _saved_session_timeouts.pop(self.bmc_ip, None)
            msg = (
                f"SessionTimeout restored "
                f"{current if current is not None else '?'}s -> {original}s"
            )
            if log:
                logger.info("[%s] %s", self.log_tag, msg)
            return msg
        return None

    def _check(self, response, action):
        """
        Raise if a Redfish HTTP status is not success.

        Args:
            response: redfish client response
            action (str): short description used in the error message

        Returns:
            the same response

        Raises:
            APIRequestError: status not in 200/201/202/204
        """
        if response.status not in OK_STATUS:
            raise APIRequestError(
                f"[{self.log_tag}] Failed to {action}. "
                f"Status: {response.status}, "
                f"Error: {redact_iso_url(response.text or '')}"
            )
        return response

    def _image_matches(self, image, iso_url):
        """
        True if VirtualMedia Image URL is the ISO we asked to mount.

        Query strings may differ between PATCH payload and GET; compare the
        path without ``?``.

        Args:
            image (str): Image field from VirtualMedia
            iso_url (str): ISO URL we PATCHed

        Returns:
            bool
        """
        if not image:
            return False
        return iso_url.split("?")[0] in image or image.split("?")[0] in iso_url

    def _virtual_media_status(self, media_uri, iso_url):
        """
        Snapshot of one VirtualMedia slot (GET).

        Used by both mount-wait and post-boot hold so those loops share the
        same Inserted / image / OEM reading.

        Args:
            media_uri (str): VirtualMedia resource URI
            iso_url (str): expected discovery ISO URL

        Returns:
            dict: Inserted, ConnectedVia, TransferPercentComplete, redacted
            Image, ImageName, OemMounted, image_ok

        Raises:
            APIRequestError: GET failed
        """
        media_resp = self._check(self.client.get(media_uri), "read virtual media")
        data = media_resp.dict or {}
        image = data.get("Image") or ""
        oem_mounted = any(
            isinstance(vendor, dict) and vendor.get("Mounted") is True
            for vendor in (data.get("Oem") or {}).values()
        )
        return {
            "Inserted": bool(data.get("Inserted")),
            "ConnectedVia": data.get("ConnectedVia"),
            "TransferPercentComplete": data.get("TransferPercentComplete"),
            "Image": redact_iso_url(image),
            "ImageName": data.get("ImageName"),
            "OemMounted": oem_mounted,
            "image_ok": self._image_matches(image, iso_url),
        }

    @staticmethod
    def _media_status_brief(status):
        """Short log line; omit the full ISO URL."""
        return (
            f"Inserted={status.get('Inserted')} "
            f"ConnectedVia={status.get('ConnectedVia')} "
            f"ImageName={status.get('ImageName')} "
            f"image_ok={status.get('image_ok')} "
            f"OemMounted={status.get('OemMounted')} "
            f"xfer={status.get('TransferPercentComplete')}"
        )

    def find_free_cd_slot(self, media_collection=DEFAULT_VIRTUAL_MEDIA_COLLECTION):
        """
        First CD/DVD slot that is not already Inserted.

        Args:
            media_collection (str): VirtualMedia collection URI

        Returns:
            str: slot URI

        Raises:
            APIRequestError: no empty CD/DVD slot
        """
        slot = self.find_cd_slot(media_collection, free_only=True)
        if not slot:
            raise APIRequestError(
                f"[{self.bmc_ip}] No free CD/DVD VirtualMedia slot found"
            )
        return slot

    def find_cd_slot(
        self, media_collection=DEFAULT_VIRTUAL_MEDIA_COLLECTION, free_only=False
    ):
        """
        First CD/DVD VirtualMedia URI on this BMC.

        Args:
            media_collection (str): VirtualMedia collection URI
            free_only (bool): if True, skip slots that already have media

        Returns:
            str or None: slot URI, or None if none found
        """
        resp = self._check(self.client.get(media_collection), "list virtual media")
        for member in resp.dict.get("Members", []):
            candidate = member["@odata.id"]
            media_resp = self.client.get(candidate)
            if media_resp.status not in OK_STATUS:
                continue
            media_types = media_resp.dict.get("MediaTypes", [])
            if "CD" not in media_types and "DVD" not in media_types:
                continue
            if free_only and media_resp.dict.get("Inserted", False):
                continue
            return candidate
        return None

    def attach_iso(self, iso_url, media_uri=None):
        """
        Mount the discovery ISO with PATCH (not InsertMedia POST).

        SE350 XCC returns 404 for VirtualMedia.InsertMedia. PATCH Image +
        Inserted is what actually mounts. Then wait until media is stable,
        not only until PATCH returns 200.

        Args:
            iso_url (str): Assisted Installer discovery ISO URL
            media_uri (str): optional slot URI; otherwise a free CD slot

        Returns:
            str: VirtualMedia URI used

        Raises:
            APIRequestError: PATCH failed or mount wait timed out
        """
        logger.debug(
            "[%s] attach_iso url=%s",
            self.bmc_ip,
            redact_iso_url(iso_url),
        )
        media_uri = media_uri or self.find_free_cd_slot()
        logger.debug("[%s] using VirtualMedia slot %s", self.bmc_ip, media_uri)
        payload = {"Image": iso_url, "Inserted": True, "WriteProtected": True}
        logger.debug("[%s] PATCH %s", self.bmc_ip, media_uri)
        self._check(self.client.patch(media_uri, body=payload), "mount ISO (PATCH)")
        self._wait_until_iso_mounted(media_uri, iso_url)
        return media_uri

    def _wait_until_iso_mounted(self, media_uri, iso_url):
        """
        Poll until the ISO is really mounted, not only PATCH-accepted.

        XCC can set ConnectedVia=URI before the CD is usable. Do not boot on
        the first good GET. Wait for the next poll (MOUNT_WAIT_SLEEP later)
        to still look mounted, then return immediately (no extra sleep).

        Args:
            media_uri (str): VirtualMedia slot
            iso_url (str): expected ISO URL

        Raises:
            APIRequestError: still not mounted after MOUNT_WAIT_TIMEOUT
        """
        deadline = time.time() + MOUNT_WAIT_TIMEOUT
        last = {}
        prev_ok = False
        while time.time() < deadline:
            last = self._virtual_media_status(media_uri, iso_url)
            really_mounted = (
                last["ConnectedVia"] == "URI"
                or last["TransferPercentComplete"] == 100
                or last["OemMounted"]
            )
            ok = last["Inserted"] and last["image_ok"] and really_mounted
            if ok and prev_ok:
                image_name = last.get("ImageName") or "ISO"
                logger.info("[%s] ISO mounted (%s)", self.log_tag, image_name)
                logger.debug(
                    "[%s] ISO mount detail %s",
                    self.log_tag,
                    self._media_status_brief(last),
                )
                return
            if ok:
                logger.debug(
                    "[%s] ISO looks mounted; confirming on next poll %s",
                    self.log_tag,
                    self._media_status_brief(last),
                )
            else:
                logger.debug(
                    "[%s] waiting for ISO mount %s",
                    self.log_tag,
                    self._media_status_brief(last),
                )
            prev_ok = ok
            time.sleep(MOUNT_WAIT_SLEEP)
        raise APIRequestError(
            f"[{self.log_tag}] Timed out waiting for ISO mount after "
            f"{MOUNT_WAIT_TIMEOUT}s (last={last})"
        )

    def eject_iso(self, media_uri=None):
        """
        Eject media from the CD/DVD slot (best-effort).

        No-op if there is no slot or the slot is already empty. Failures
        to read the slot are logged, not raised. Does not log success at
        INFO (caller combines with SessionTimeout restore).

        Args:
            media_uri (str): optional slot URI; otherwise first CD/DVD slot

        Returns:
            str: short status (``ISO ejected from ...``, ``already_empty``,
            ``no_slot``, or ``read_failed``)

        Raises:
            APIRequestError: eject PATCH failed
        """
        media_uri = media_uri or self.find_cd_slot()
        if not media_uri:
            logger.debug(
                "[%s] No CD/DVD VirtualMedia slot found to eject", self.log_tag
            )
            return "no_slot"

        media_resp = self.client.get(media_uri)
        if media_resp.status not in OK_STATUS:
            logger.warning(
                "[%s] Failed to read VirtualMedia %s: %s",
                self.log_tag,
                media_uri,
                media_resp.status,
            )
            return "read_failed"

        if not media_resp.dict.get("Inserted", False):
            logger.debug("[%s] VirtualMedia already empty: %s", self.log_tag, media_uri)
            return "already_empty"

        logger.debug("[%s] PATCH eject %s", self.log_tag, media_uri)
        self._check(
            self.client.patch(media_uri, body={"Inserted": False, "Image": None}),
            "eject ISO (PATCH)",
        )
        return f"ISO ejected from {media_uri}"

    def set_boot_override_cd_once(self, system_uri=DEFAULT_SYSTEM_URI):
        """
        Next boot from virtual CD, once (not persistent).

        Args:
            system_uri (str): ComputerSystem resource URI

        Raises:
            APIRequestError: PATCH failed
        """
        logger.debug(
            "[%s] set BootSourceOverrideTarget=Cd BootSourceOverrideEnabled=Once",
            self.bmc_ip,
        )
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
        """
        Current ComputerSystem PowerState.

        Args:
            system_uri (str): ComputerSystem resource URI

        Returns:
            str: e.g. On, Off, or Unknown

        Raises:
            APIRequestError: GET failed
        """
        resp = self._check(self.client.get(system_uri), "get system state")
        return resp.dict.get("PowerState", "Unknown")

    def _reset(self, reset_type, system_uri=DEFAULT_SYSTEM_URI):
        """
        POST ComputerSystem.Reset. Caller checks the response.

        Args:
            reset_type (str): On, ForceRestart, etc.
            system_uri (str): ComputerSystem resource URI

        Returns:
            redfish response (not status-checked)
        """
        reset_uri = f"{system_uri}/Actions/ComputerSystem.Reset"
        return self.client.post(reset_uri, body={"ResetType": reset_type})

    @staticmethod
    def _is_chassis_off_error(response):
        """
        True when XCC rejects reset because chassis/system is powered off.

        PowerState from GET can disagree with chassis power; ForceRestart
        then returns 400/409 with those message strings.

        Args:
            response: redfish reset response

        Returns:
            bool
        """
        if response.status not in (400, 409):
            return False
        text = (response.text or "").lower()
        return (
            "chassispowerstateonrequired" in text
            or "requires to be powered on" in text
            or "powered off" in text
        )

    def _wait_until_powered_on(self, system_uri=DEFAULT_SYSTEM_URI):
        """
        Poll PowerState until On or timeout.

        Args:
            system_uri (str): ComputerSystem resource URI

        Raises:
            APIRequestError: still not On after POWER_ON_BEFORE_RESTART_TIMEOUT
        """
        deadline = time.time() + POWER_ON_BEFORE_RESTART_TIMEOUT
        while time.time() < deadline:
            state = self.get_power_state(system_uri)
            if state == "On":
                return
            time.sleep(POWER_ON_BEFORE_RESTART_SLEEP)
        raise APIRequestError(
            f"[{self.log_tag}] PowerState did not become On within "
            f"{POWER_ON_BEFORE_RESTART_TIMEOUT}s "
            f"(last={self.get_power_state(system_uri)})"
        )

    def _power_on_then_force_restart_for_cd(self, system_uri=DEFAULT_SYSTEM_URI):
        """
        Cold-start path for discovery ISO when the chassis is off.

        Power On alone often does not boot VirtualMedia on SE350 (BMC event
        is "Power On" and the node never appears in AI). Working peers that
        were already On get "Hard Reset" via ForceRestart. Mirror that:
        On -> wait -> re-apply Cd Once -> ForceRestart.

        Args:
            system_uri (str): ComputerSystem resource URI

        Raises:
            APIRequestError: On / wait / ForceRestart failed
        """
        self._check(self._reset("On", system_uri=system_uri), "power on")
        logger.info(
            "[%s] Power On issued (chassis was off); waiting then ForceRestart "
            "so CD-once boot matches Hard Reset path",
            self.log_tag,
        )
        self._wait_until_powered_on(system_uri)
        # Power On can consume/clear BootSourceOverrideEnabled=Once.
        self.set_boot_override_cd_once(system_uri)
        self._check(self._reset("ForceRestart", system_uri=system_uri), "force restart")
        logger.info(
            "[%s] ForceRestart issued after Power On (CD-once re-applied)",
            self.log_tag,
        )

    def power_on_or_restart(self, system_uri=DEFAULT_SYSTEM_URI):
        """
        Boot the host so discovery ISO (Cd Once) is taken.

        Always end with ForceRestart (BMC "Hard Reset") when possible.
        A bare Power On is not enough for discovery: if the host is already
        On, Power On is a no-op (BMC may still log "Attempting to Power On"
        without rebooting), so the ISO is never taken. Peers that Hard-Reset
        after mount appear in Assisted Installer; Power-On-only nodes often
        do not.

        Strategy: try ForceRestart first (even if GET PowerState is not On;
        XCC PowerState can disagree with the live chassis). Only if
        ForceRestart is rejected as chassis-off: Power On, wait until On,
        re-apply Cd Once, then ForceRestart.

        Args:
            system_uri (str): ComputerSystem resource URI

        Raises:
            APIRequestError: reset POST failed
        """
        power_state = self.get_power_state(system_uri)
        logger.info(
            "[%s] power action for PowerState=%s (always prefer ForceRestart)",
            self.log_tag,
            power_state,
        )

        response = self._reset("ForceRestart", system_uri=system_uri)
        if self._is_chassis_off_error(response):
            logger.warning(
                "[%s] ForceRestart rejected (chassis off, reported PowerState=%s); "
                "using Power On + re-apply Cd Once + ForceRestart. Body: %s",
                self.log_tag,
                power_state,
                redact_iso_url(response.text or ""),
            )
            self._power_on_then_force_restart_for_cd(system_uri)
            return

        self._check(response, "force restart")
        logger.info(
            "[%s] ForceRestart issued (reported PowerState=%s)",
            self.log_tag,
            power_state,
        )

    def hold_session_after_boot(self, media_uri, iso_url):
        """
        Keep this Redfish session open after boot/restart and re-check media.

        Session DELETE unmounts URI ISO on XCC (seen on BMC events at the
        same second as restart). Hold POST_BOOT_SESSION_HOLD seconds, GET
        every POST_BOOT_SESSION_SLEEP, fail if Inserted/image is gone so we
        do not return success while LXPM has taken over.

        Args:
            media_uri (str): VirtualMedia slot that should still hold the ISO
            iso_url (str): expected discovery ISO URL

        Raises:
            APIRequestError: media disappeared during the hold
        """
        deadline = time.time() + POST_BOOT_SESSION_HOLD
        logger.debug(
            "[%s] holding Redfish session for %ss after boot",
            self.bmc_ip,
            POST_BOOT_SESSION_HOLD,
        )
        while time.time() < deadline:
            status = self._virtual_media_status(media_uri, iso_url)
            if not (status["Inserted"] and status["image_ok"]):
                raise APIRequestError(
                    f"[{self.log_tag}] VirtualMedia lost after boot/restart: "
                    f"{status}"
                )
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            time.sleep(min(POST_BOOT_SESSION_SLEEP, remaining))
        logger.info(
            "[%s] boot complete; media still mounted after %ss hold",
            self.log_tag,
            POST_BOOT_SESSION_HOLD,
        )


_EJECT_STATUS_TEXT = {
    "already_empty": "VirtualMedia already empty",
    "no_slot": "no CD/DVD VirtualMedia slot",
    "read_failed": "VirtualMedia read failed",
}


def _eject_iso_one(bmc_ip, username, password, restart=False, node_name=None):
    """
    Eject VirtualMedia on one BMC, restore SessionTimeout, then logout.

    Opposite of _attach_iso_and_boot_one: here we want the ISO gone.
    Logs one INFO line for eject + timeout restore.

    Args:
        bmc_ip (str): BMC address
        username (str): BMC username
        password (str): BMC password
        restart (bool): if True, power-cycle after eject
        node_name (str): hostname for log tags
    """
    rf = RedfishClient(bmc_ip, username, password, node_name=node_name)
    try:
        eject_status = rf.eject_iso()
        eject_msg = _EJECT_STATUS_TEXT.get(eject_status, eject_status)
        if restart:
            rf.power_on_or_restart()
        restore_msg = rf.restore_session_timeout_after_discovery(log=False)
        if restore_msg:
            logger.info("[%s] %s; %s", rf.log_tag, eject_msg, restore_msg)
        else:
            logger.info("[%s] %s", rf.log_tag, eject_msg)
    finally:
        rf.close(logout=True)


def _restore_session_timeout_one(bmc_ip, username, password, node_name=None):
    """
    Best-effort restore of SessionTimeout on one BMC (no VirtualMedia change).

    Args:
        bmc_ip (str): BMC address
        username (str): BMC username
        password (str): BMC password
        node_name (str): hostname for log tags

    Returns:
        bool: True if a restore action was attempted (saved timeout existed)
    """
    with _saved_session_timeouts_lock:
        needs_restore = bmc_ip in _saved_session_timeouts
    if not needs_restore:
        return False
    rf = RedfishClient(bmc_ip, username, password, node_name=node_name)
    try:
        msg = rf.restore_session_timeout_after_discovery(log=False)
        if msg:
            logger.info("[%s] %s", rf.log_tag, msg)
        return True
    finally:
        rf.close(logout=True)


def _attach_iso_and_boot_one(
    bmc_ip, username, password, iso_url, ready_barrier, node_name=None
):
    """
    Mount ISO and boot from CD in a single Redfish session (one BMC).

    Do not merge with _eject_iso_one: this path must close(logout=False).
    Logout DELETEs the session and XCC unmounts minimal.iso.

    Raises SessionTimeout for the discovery window before mount so an
    abandoned session does not expire at the default 300s and unmount URI
    media during boot/discovery.

    Wait on ready_barrier so every node has mounted before any restart.
    If this node fails, abort the barrier so peers do not hang.

    Args:
        bmc_ip (str): BMC address
        username (str): BMC username
        password (str): BMC password
        iso_url (str): discovery ISO URL
        ready_barrier (threading.Barrier): parties == number of BMCs
        node_name (str): hostname for log tags

    Raises:
        APIRequestError: mount failed, peer failed, or media lost after boot
    """
    rf = None
    tag = _node_tag(node_name, bmc_ip)
    try:
        # Construct inside try so login failures abort the peer barrier.
        rf = RedfishClient(bmc_ip, username, password, node_name=node_name)
        tag = rf.log_tag
        rf.extend_session_timeout_for_discovery()
        try:
            rf.eject_iso()
        except Exception as exc:
            logger.warning("[%s] pre-attach eject ignored: %s", tag, exc)

        last_err = None
        media_uri = None
        for attempt in range(1, ATTACH_ISO_ATTEMPTS + 1):
            try:
                if attempt > 1:
                    logger.info(
                        "[%s] attach attempt %s/%s",
                        tag,
                        attempt,
                        ATTACH_ISO_ATTEMPTS,
                    )
                else:
                    logger.debug(
                        "[%s] attach attempt %s/%s",
                        tag,
                        attempt,
                        ATTACH_ISO_ATTEMPTS,
                    )
                media_uri = rf.attach_iso(iso_url)
                break
            except Exception as exc:
                last_err = exc
                logger.warning("[%s] attach attempt %s failed: %s", tag, attempt, exc)
                try:
                    rf.eject_iso()
                except Exception as eject_exc:
                    logger.warning(
                        "[%s] eject after failed attach ignored: %s",
                        tag,
                        eject_exc,
                    )
                time.sleep(ATTACH_ISO_RETRY_SLEEP)
        else:
            raise last_err

        logger.debug("[%s] ISO mounted; waiting for peers before CD boot", tag)
        try:
            ready_barrier.wait(timeout=BARRIER_WAIT_TIMEOUT)
        except threading.BrokenBarrierError as exc:
            raise APIRequestError(
                f"[{tag}] Peer ISO mount failed; aborting CD boot"
            ) from exc

        rf.set_boot_override_cd_once()
        rf.power_on_or_restart()
        rf.hold_session_after_boot(media_uri, iso_url)
        logger.debug("[%s] boot sequence complete", tag)
    except Exception:
        try:
            ready_barrier.abort()
        except Exception as abort_exc:
            logger.debug(
                "[%s] ignoring ready_barrier.abort() failure after attach/boot error: %s",
                tag,
                abort_exc,
            )
        raise
    finally:
        # Never DELETE the session here — that unmounts minimal.iso on XCC and
        # LXPM then mounts local tdm_image.img instead.
        if rf is not None:
            rf.close(logout=False)


def _run_on_nodes(servers, worker, raise_on_fail=False):
    """
    Run worker(name, details) in parallel with one thread per BMC.

    max_workers must be len(servers). A smaller pool deadlocks
    threading.Barrier(parties=N) when N > max_workers.

    Args:
        servers (dict): hostname -> server detail dict
        worker (callable): returns (name, ok, msg)
        raise_on_fail (bool): raise APIRequestError if any worker failed
            (attach/boot must fail the deploy; eject is best-effort)

    Returns:
        list: worker results

    Raises:
        APIRequestError: raise_on_fail and at least one worker failed
    """
    if not servers:
        return []
    results = []
    with ThreadPoolExecutor(max_workers=len(servers)) as executor:
        futures = [
            executor.submit(worker, name, details) for name, details in servers.items()
        ]
        for future in as_completed(futures):
            results.append(future.result())
    failed = [(n, msg) for n, ok, msg in results if not ok]
    succeeded = [n for n, ok, _ in results if ok]
    logger.info(
        "Redfish node action summary: ok_count=%s failed_count=%s "
        "successes=%s failures=%s",
        len(succeeded),
        len(failed),
        succeeded,
        failed,
    )
    if raise_on_fail and failed:
        raise APIRequestError(f"Redfish ISO boot failed for: {failed}")
    return results


def attach_iso_and_boot_nodes(servers, iso_url):
    """
    Attach discovery ISO and boot all BMCs (parallel).

    Per node: extend SessionTimeout, mount + CD boot, barrier until all
    mounted, hold session after restart, close without logout. Caller should
    eject (restores timeout) or call restore_session_timeout_nodes.

    Args:
        servers (dict): name -> dict with mgmt_console / mgmt_username /
            mgmt_password (caller should already apply common BMC defaults)
        iso_url (str): Assisted Installer discovery ISO URL

    Raises:
        APIRequestError: any node failed to mount/boot
    """
    if not servers:
        return

    logger.info(
        "Redfish ISO boot starting for %s node(s) iso=%s",
        len(servers),
        redact_iso_url(iso_url),
    )
    ready_barrier = threading.Barrier(parties=len(servers))

    def _one(name, details):
        bmc = details.get("mgmt_console")
        try:
            logger.info(
                "[%s] Attaching discovery ISO via Redfish bmc=%s",
                name,
                bmc,
            )
            _attach_iso_and_boot_one(
                details["mgmt_console"],
                details["mgmt_username"],
                details["mgmt_password"],
                iso_url,
                ready_barrier,
                node_name=name,
            )
            return name, True, "ok"
        except Exception as exc:
            logger.error(
                "[%s] Redfish ISO boot failed: %s",
                _node_tag(name, bmc),
                exc,
            )
            return name, False, str(exc)

    _run_on_nodes(servers, _one, raise_on_fail=True)


def eject_iso_nodes(servers, restart=False):
    """
    Eject VirtualMedia ISOs on all BMCs (parallel).

    Restores SessionTimeout after eject (one INFO line per node), then
    logs out. Failures are logged; this does not raise.

    Args:
        servers (dict): name -> server detail dict with BMC fields
        restart (bool): if True, power-cycle after eject
    """

    def _one(name, details):
        try:
            _eject_iso_one(
                details["mgmt_console"],
                details["mgmt_username"],
                details["mgmt_password"],
                restart=restart,
                node_name=name,
            )
            return name, True, "ok"
        except Exception as exc:
            logger.warning(
                "[%s] Redfish eject failed: %s",
                _node_tag(name, details.get("mgmt_console")),
                exc,
            )
            return name, False, str(exc)

    _run_on_nodes(servers, _one, raise_on_fail=False)


def restore_session_timeout_nodes(servers):
    """
    Restore SessionTimeout on BMCs extended for discovery ISO (parallel).

    Safety net when discovery fails before eject, or eject skipped a node.
    No-op (no worker pool) if eject already restored every BMC.

    Args:
        servers (dict): name -> server detail dict with BMC fields
    """
    if not servers:
        return
    with _saved_session_timeouts_lock:
        pending = {
            name: details
            for name, details in servers.items()
            if details.get("mgmt_console") in _saved_session_timeouts
        }
    if not pending:
        return

    def _one(name, details):
        try:
            _restore_session_timeout_one(
                details["mgmt_console"],
                details["mgmt_username"],
                details["mgmt_password"],
                node_name=name,
            )
            return name, True, "ok"
        except Exception as exc:
            logger.warning(
                "[%s] SessionTimeout restore failed: %s",
                _node_tag(name, details.get("mgmt_console")),
                exc,
            )
            return name, False, str(exc)

    _run_on_nodes(pending, _one, raise_on_fail=False)


def attach_iso_and_boot(bmc_ip, username, password, iso_url):
    """
    Attach discovery ISO and boot once from CD for a single BMC.

    Compatibility wrapper for callers that use per-host Redfish helpers.
    """
    barrier = threading.Barrier(parties=1)
    _attach_iso_and_boot_one(bmc_ip, username, password, iso_url, barrier)


def eject_iso(bmc_ip, username, password):
    """Eject VirtualMedia ISO without restarting the host."""
    _eject_iso_one(bmc_ip, username, password, restart=False)


def eject_iso_and_restart(bmc_ip, username, password):
    """Eject VirtualMedia and power-cycle so the host boots from disk."""
    _eject_iso_one(bmc_ip, username, password, restart=True)
