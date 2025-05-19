import logging
import subprocess
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.utility.utils import remove_ceph_crashes, get_ceph_crashes
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.exceptions import (
    CommandFailed,
    CephHealthException,
    NoRunningCephToolBoxException,
)
from ocs_ci.utility.retry import retry

log = logging.getLogger(__name__)


class CephStatusTool:
    """
    Class to check the health of Ceph cluster.
    """

    def __init__(self):
        """
        Initialize the CephHealthCheck class.
        """
        self.ceph_health = ceph_health_check
        self.ceph_crashes = get_ceph_crashes
        self.remove_ceph_crashes = remove_ceph_crashes
        self.toolbox = pod.get_ceph_tools_pod()

    @retry(CommandFailed, tries=8, delay=3)
    def wait_till_ceph_status_became_healthy(self):
        """
        Get the status of the Ceph cluster.

        Returns:
            str: The status of the Ceph cluster.
        """

        log.info("Performing post-failure injection checks...")
        try:
            ceph_health_check(fix_ceph_health=True)
        except (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ) as e:
            log.error(f"Ceph health check failed after failure injection. : {e}")

    def check_ceph_crashes(self):
        """
        Check for any Ceph crashes.

        Returns:
            bool: True if crashes are found, False otherwise.
        """
        ceph_crashes = self.ceph_crashes(self.toolbox)
        if ceph_crashes:
            log.error(f"Ceph crash logs found: {ceph_crashes}")
            return True
        return False

    def archive_ceph_crashes(self):
        """
        Archive any existing Ceph crash logs.
        """
        log.info("Removing any existing Ceph crash logs...")
        self.remove_ceph_crashes(self.toolbox)
        log.info("Ceph crash logs archived successfully.")
        return True
