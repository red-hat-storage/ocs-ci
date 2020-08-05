import logging
import pytest
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import (
    E2ETest, scale, ignore_leftovers, skipif_ocs_version
)
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.utility import utils

log = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def resize_pvc(request):
    # Setup scale environment in the cluster
    resize_pvc = FioPodScale(
        kind=constants.POD, pod_dict_path=constants.NGINX_POD_YAML,
        node_selector=constants.SCALE_NODE_SELECTOR
    )

    def teardown():
        resize_pvc.cleanup()
    request.addfinalizer(teardown)
    return resize_pvc


@scale
@skipif_ocs_version('<4.5')
@ignore_leftovers
@pytest.mark.parametrize(
    argnames=[
        "start_io", "pvc_size", "pvc_new_size",
    ],
    argvalues=[
        pytest.param(
            *[False, '10Gi', 20], marks=pytest.mark.polarion_id("OCS-2250")
        ),
        pytest.param(
            *[True, '50Gi', 60], marks=pytest.mark.polarion_id("OCS-2251")
        )
    ]
)
class TestPVCExpand(E2ETest):
    """
    Scale test case for PVC size expansion
    """

    def test_scale_pvc_expand(
        self, resize_pvc, start_io, pvc_size, pvc_new_size
    ):
        """
        Test case to scale pvc size expansion
        with and without IO running on the pods
        """
        # Create pvcs and scale pods
        logging.info('Create pvcs and scale pods')
        resize_pvc.create_scale_pods(
            scale_count=1500, pods_per_iter=10,
            io_runtime=3600, start_io=start_io, pvc_size=pvc_size
        )

        # Expand PVC to new size
        logging.info(f'Starting expanding PVC size to {pvc_new_size}Gi')
        resize_pvc.pvc_expansion(pvc_new_size=pvc_new_size)

        # Check ceph health status
        utils.ceph_health_check()
