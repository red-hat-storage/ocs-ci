import logging
import pytest

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    red_squad,
    tier1,
    rgw,
    skipif_no_lso,
    post_upgrade,
)
from ocs_ci.ocs.resources.pod import get_rgw_pods
from ocs_ci.ocs.ocp import OCP
import ocs_ci.ocs.resources.pod as pod
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import TimeoutSampler, run_cmd

log = logging.getLogger(__name__)

CEPH_OBJECT_STORE = OCP(
    kind="CephObjectStore", namespace=config.ENV_DATA["cluster_namespace"]
)
OCP_OBJ = OCP(kind=constants.POD, namespace=config.ENV_DATA["cluster_namespace"])
DEFAULT_READ_AFFINITY = "localize"


@rgw
@red_squad
@skipif_no_lso
class TestRGWReadAffinityMode:
    """
    Test the RGW Read Affinity value in an ODF cluster

    """

    @pytest.fixture(scope="class", autouse=True)
    def revert_read_affinity_mode(self, request):
        """
        Reverts the RGW readAffinity mode to localize state.
        """

        def teardown():
            patch = '\'{"spec": {"gateway": {"readAffinity": {"type": "localize"}}}}\''
            patch_cmd = (
                f"oc patch cephObjectStore/{CEPH_OBJECT_STORE.data['items'][0]['metadata'].get('name')} "
                f"-n openshift-storage  --type merge --patch {patch}"
            )
            run_cmd(patch_cmd)
            sample = TimeoutSampler(
                timeout=60,
                sleep=10,
                func=self.get_rgw_read_affinity_from_ceph,
            )
            sample.wait_for_func_value(DEFAULT_READ_AFFINITY)

        request.addfinalizer(teardown)

    def get_rgw_read_affinity_from_ceph(self):
        """
        Returns current readAffinity set in ceph cluster
        Returns :
            ceph_read_affinity_mode (String)
        """
        cmd = "ceph config show client.rgw.ocs.storagecluster.cephobjectstore.a"
        ceph_pod = pod.get_ceph_tools_pod()
        ceph_op = ceph_pod.exec_ceph_cmd(cmd)
        for val in ceph_op:
            if val["name"] == "rados_replica_read_policy":
                log.info(val["value"])
                return val["value"]
        else:
            return None

    @tier1
    @post_upgrade
    def test_rgw_read_affinity_mode(self):
        """
        Test default ReadAffinity mode of RGW in an ODF cluster
            step #1. Validate readAffinity mode is set to ""local"" for RGW
            step #2. Validate the status of RGW pod
            step #3. Validate readAffinity value from ceph cluster
            step #4: Change current mode to "balanced" and validate it along with RGW pod status
            step #5: Change current mode to "default" and validate it along with RGW pod status
        """
        rgw_pod_count = len(get_rgw_pods())
        # 1. Validate readAffinity mode is set to "localize" for RGW
        current_read_affinity = CEPH_OBJECT_STORE.data["items"][0]["spec"]["gateway"][
            "readAffinity"
        ]["type"]
        assert (
            DEFAULT_READ_AFFINITY == current_read_affinity
        ), f"Default ReadAffinity for RGW is {current_read_affinity}. Expected value is {DEFAULT_READ_AFFINITY}"
        # 2. Validate the status of RGW pod
        assert OCP_OBJ.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_count=rgw_pod_count,
            selector=constants.RGW_APP_LABEL,
        )
        # 3. Validate readAffinity value from ceph cluster
        sample = TimeoutSampler(
            timeout=60,
            sleep=10,
            func=self.get_rgw_read_affinity_from_ceph,
        )
        sample.wait_for_func_value(DEFAULT_READ_AFFINITY)

        # 4 and 5. Change current mode to all available mode and validate it along with RGW pod status
        READ_AFFINITY_LIST = ["balance", "default"]
        for val in READ_AFFINITY_LIST:
            patch = f'\'{{"spec": {{"gateway": {{"readAffinity": {{"type": "{val}"}}}}}}}}\''
            patch_cmd = (
                f"oc patch cephObjectStore/{CEPH_OBJECT_STORE.data['items'][0]['metadata'].get('name')} "
                f"-n openshift-storage  --type merge --patch {patch}"
            )
            run_cmd(patch_cmd)
            CEPH_OBJECT_STORE.reload_data()
            current_read_affinity = CEPH_OBJECT_STORE.data["items"][0]["spec"][
                "gateway"
            ]["readAffinity"]["type"]
            assert (
                current_read_affinity == val
            ), f"Failed to change ReadAffinity for RGW. Current value: {current_read_affinity}. Expected value: {val}"

            # Validate RGW POD status
            assert OCP_OBJ.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_count=rgw_pod_count,
                selector=constants.RGW_APP_LABEL,
            )
            sample = TimeoutSampler(
                timeout=60,
                sleep=10,
                func=self.get_rgw_read_affinity_from_ceph,
            )
            sample.wait_for_func_value(val)
