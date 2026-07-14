# Module for testing functions designed for log improvements
import time

import pytest
import logging

from ocs_ci.framework.pytest_customization.marks import ignore_leftovers, libtest
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.ocp import OCP

log = logging.getLogger(__name__)


class TestWaitForResource:
    pvc_size = 1

    @pytest.fixture()
    def pvc(self, pvc_factory_class):
        self.pvc_obj = pvc_factory_class(
            interface=constants.CEPHFILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWX,
            size=self.pvc_size,
        )

    @pytest.fixture()
    def pod(self, pod_factory_class):
        self.pod_obj = pod_factory_class(
            pvc=self.pvc_obj,
            pod_dict_path=constants.PERF_POD_YAML,
        )

    @libtest
    @ignore_leftovers
    def test_wait_for_resource_oc_wait(self, pvc, pod):
        """
        Test the wait_for_resource_oc function to ensure it waits for a resource to be in different states
        with different search strategies.

        """
        label = "wait_for_resource_oc_wait"

        ocp_pod_obj = OCP(
            kind=constants.POD,
            namespace=self.pvc_obj.namespace,
        )
        ocp_pod_obj.get()
        ocp_pod_obj.add_label(resource_name=self.pod_obj.name, label=f'{label}=""')

        ocp_pvc_obj = OCP(
            kind=constants.PVC,
            namespace=self.pvc_obj.namespace,
        )
        ocp_pvc_obj.get()
        ocp_pvc_obj.add_label(resource_name=self.pvc_obj.name, label=f'{label}=""')

        # positive tests
        start_time = time.time()
        assert ocp_pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            resource_name=self.pod_obj.name,
            timeout=10,
        )
        log.info(
            f"Time taken for pod {self.pod_obj.name} to be found in running state: "
            f"{time.time() - start_time} seconds"
        )

        start_time = time.time()
        assert ocp_pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector=label,
            timeout=10,
        )
        log.info(
            f"Time taken for pod with label {label} to be found in running state: "
            f"{time.time() - start_time} seconds"
        )

        start_time = time.time()
        assert ocp_pvc_obj.wait_for_resource(
            condition=constants.STATUS_BOUND,
            resource_name=self.pvc_obj.name,
            timeout=10,
        )
        log.info(
            f"Time taken for pvc {self.pvc_obj.name} to be found in bound state: "
            f"{time.time() - start_time} seconds"
        )

        start_time = time.time()
        assert ocp_pvc_obj.wait_for_resource(
            condition=constants.STATUS_BOUND,
            selector=label,
            timeout=10,
        )
        log.info(
            f"Time taken for pvc with label {label} to be found in bound state: "
            f"{time.time() - start_time} seconds"
        )

        # negative tests
        with pytest.raises(TimeoutExpiredError, match=".*"):
            ocp_pod_obj.wait_for_resource(
                condition=constants.STATUS_FAILED,
                resource_name=self.pod_obj.name,
                timeout=5,
            )
        log.info(
            f"Expected exception raised. Pod {self.pod_obj.name} did not reach failed state within timeout"
        )

        with pytest.raises(TimeoutExpiredError, match=".*"):
            ocp_pod_obj.wait_for_resource(
                condition=constants.STATUS_FAILED,
                selector=label,
                timeout=5,
            )
        log.info(
            f"Expected exception raised. Pod with label {label} did not reach failed state within timeout"
        )

        with pytest.raises(TimeoutExpiredError, match=".*"):
            ocp_pvc_obj.wait_for_resource(
                condition=constants.STATUS_PENDING,
                resource_name=self.pvc_obj.name,
                timeout=5,
            )
        log.info(
            f"Expected exception raised. PVC {self.pvc_obj.name} did not reach released state within timeout"
        )

        with pytest.raises(TimeoutExpiredError, match=".*"):
            ocp_pvc_obj.wait_for_resource(
                condition=constants.STATUS_PENDING,
                selector=label,
                timeout=5,
            )
        log.info(
            f"Expected exception raised. PVC with label {label} did not reach released state within timeout"
        )

        # Clean up resources
        ocp_pod_obj.delete(resource_name=self.pod_obj.name, wait=True, force=True)
        ocp_pvc_obj.delete(resource_name=self.pvc_obj.name, wait=True, force=True)

        with pytest.raises(TimeoutExpiredError, match=".*"):
            ocp_pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=self.pod_obj.name,
                timeout=5,
            )
        log.info(
            f"Expected exception raised. Pod {self.pod_obj.name} does not exist after deletion"
        )

        with pytest.raises(TimeoutExpiredError, match=".*"):
            ocp_pvc_obj.wait_for_resource(
                condition=constants.STATUS_BOUND,
                resource_name=self.pvc_obj.name,
                timeout=5,
            )
        log.info(
            f"Expected exception raised. PVC {self.pvc_obj.name} does not exist after deletion"
        )
