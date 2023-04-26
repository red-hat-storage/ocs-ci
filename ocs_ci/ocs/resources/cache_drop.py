# Builtin modules
import logging
import http.client

# OCS-CI modules
from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


class OSDCashDrop(OCP):
    """
    This module is for deploying pod which enable to drop the OSD's cache.

    Usage:
        import OSDCashDrop

        cd = OSDCashDrop()  # create new cache_drop object
        cd.deploy()         # deploy the cache_drop pod
        ....                # run test
        cd.drop_cache()     # drop the OSD's cache
        ....                # run the test again
        cd.cleanup()        # delete the cache_drop pod

    """

    def __init__(self):
        """
        Initialize the object parameters
        """
        super(OSDCashDrop, self).__init__(
            kind="POD",
            namespace=config.ENV_DATA["cluster_namespace"],
            resource_name="rook-ceph-osd-cache-drop",
        )
        self.crd = constants.RIPSAW_DROP_CACHE
        self.port = 9432  # this port number is hard coded in the pod

    def deploy(self):
        """
        Deploy the cache drop pod and wait until it is up

        """
        self.create(self.crd)
        self.wait_for_resource(condition=constants.STATUS_RUNNING, timeout=240)

    def cleanup(self):
        """
        Delete the pod from the cluster

        """
        self.delete(resource_name=self.resource_name)
        self.wait_for_delete(resource_name=self.resource_name)

    @property
    def ip(self):
        """
        return the cache drop IP

        """
        return self.get()["status"]["podIP"]

    def drop_cache(self):
        """
        Drop the OSD's cache by sending http request to the pod

        Raises:
            exception : if the request to drop the cache failed

        """
        log.info(f"ceph OSD cache drop pod: {self.ip}")
        conn = http.client.HTTPConnection(self.ip, port=self.port, timeout=30)
        log.info(f"requesting ceph to drop cache via {self.ip}:{self.port}")
        try:
            conn.request("GET", "/drop_osd_caches")
            rsp = conn.getresponse()
            if rsp.status != http.client.OK:
                log.error(f"HTTP ERROR {rsp.status}: {rsp.reason}")
                raise Exception(f"Ceph OSD cache drop {self.ip}:{self.port} Failed")
            else:
                log.info("The OSD cache was successfully dropped")
        except Exception as e:
            log.error(f"Can not connect to pod : {e}")
