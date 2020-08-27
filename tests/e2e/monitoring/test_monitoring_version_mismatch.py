import logging
import pytest

from ocs_ci.ocs.constants import STATUS_RUNNING
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs.resources.pod import get_pod_obj
from ocs_ci.framework.testlib import E2ETest
from ocs_ci.utility import prometheus
from tests.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


class TestMonitoringVersionMismatch(E2ETest):
    """
    Test monitoring version mismatch
    """
    @pytest.fixture(autouse=True)
    def teardown(self, request):

        def finalizer():
            rook_ceph_version = self.get_rook_ceph_version(name=self.resource_name)
            if self.original_rook_ceph_version != rook_ceph_version:
                self.change_image_deployment(image_version=self.original_image, image_type=self.find_image)

        request.addfinalizer(finalizer)

    @pytest.mark.parametrize(
        argnames=['resource', 'alert', 'find_image'],
        argvalues=[
            pytest.param(
                *['rook-ceph-mon-a', 'CephMonVersionMismatch', 'mon:'], marks=pytest.mark.polarion_id("OCS-1571")
            ),
            pytest.param(
                *['rook-ceph-osd-1', 'CephOSDVersionMismatch', 'osd:'], marks=pytest.mark.polarion_id("OCS-1569")
            ),
        ]
    )
    def test_ceph_version_mismatch(self, resource, alert, find_image):
        """
        Test ceph version mismatch
        """
        self.resource_name = resource
        self.find_image = find_image
        # Get Image name
        self.original_image = self.find_image_name(resource_name=self.resource_name, find_image=find_image)

        # Check rook ceph version
        self.original_rook_ceph_version = self.get_rook_ceph_version(name=self.resource_name)

        # Change rook ceph version
        self.change_image_deployment(image_version='quay.io/ceph-ci/ceph:master', image_type=self.find_image)

        # Check rook ceph version changed
        rook_ceph_version = self.get_rook_ceph_version(name=self.resource_name)
        assert rook_ceph_version != self.original_rook_ceph_version,\
            f"{self.original_rook_ceph_version} is not changed"

        # Look for a Version Mismatch alert in OCP Alerting.
        prometheus_api = prometheus.PrometheusAPI()
        alert_records = prometheus_api.wait_for_alert(
            name=alert, state=None, timeout=15, sleep=3
        )
        assert len(alert_records) > 0, f'{alert} alert is not found'

        # Change to original rook ceph version
        self.change_image_deployment(image_version=self.original_image, image_type=self.find_image)

        # Look for an alert CephOSDVersionMismatch cleared.
        prometheus_api.check_alert_cleared(
            label=alert, measure_end_time=60, time_min=120
        )

    def change_image_deployment(self, image_version, image_type):
        """
        Change image deployment

        Args:
            image_version (str): Change to this version
            image_type (str): the type of deployment

        """
        ocp_obj = OCP(namespace='openshift-storage')
        ocp_obj.exec_oc_cmd(
            command=f'set image deployment/{self.resource_name} {image_type[:-1]}={image_version}'
        )

    def find_image_name(self, resource_name, find_image):
        """
        Find the Image name of the pod

        Args:
            resource_name (str): the type of deployment
            find_image (str): using find Image Name

        Returns:
            rook_ceph_version (str): the image name

        """
        ocp_obj = OCP(namespace='openshift-storage', kind='deployment')
        image = ocp_obj.describe(resource_name=resource_name)
        image_list = image.split()
        index = image_list.index(find_image)
        # The image name is two places after the "find_image" (in image_list)
        return image_list[index + 2]

    def get_rook_ceph_version(self, name):
        """
        Get Ceph version

        Args:
            name (str): name of pod

        Returns:
            rook_ceph_version (str): ceph version

        """
        pod_name = get_pod_name_by_pattern(
            namespace='openshift-storage', pattern=name
        )[0]
        pod_obj = get_pod_obj(namespace='openshift-storage', name=pod_name)
        wait_for_resource_state(
            resource=pod_obj, state=STATUS_RUNNING, timeout=180
        )
        rook_ceph_version = pod_obj.exec_cmd_on_pod(
            command='ceph -v', out_yaml_format=False
        )
        return rook_ceph_version
