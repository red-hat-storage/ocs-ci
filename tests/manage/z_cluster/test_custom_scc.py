import pytest
import logging

from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.helpers.helpers import create_resource
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.pod import (
    Pod,
    get_pods_having_label,
    wait_for_pods_to_be_running,
)
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.framework.pytest_customization.marks import bugzilla, polarion_id, tier2
from ocs_ci.helpers.sanity_helpers import Sanity

logger = logging.getLogger(__name__)


class TestSCC:
    @pytest.fixture(scope="function")
    def create_scc(self, request):
        scc_data = templating.load_yaml(constants.SCC_YAML)
        scc_obj = create_resource(**scc_data)
        logger.info("New scc-temp is created")

        def teardown():
            scc_obj.delete()
            logger.info("scc-temp is deleted")

        request.addfinalizer(teardown)
        return scc_obj

    @tier2
    @bugzilla("1938647")
    @polarion_id("OCS-4483")
    def test_custom_scc_with_pod_respin(self, create_scc):
        """
        Test if OCS deployments/pods get affected if custom scc is created
        """
        # Add new scc to system:authenticated
        OCP().exec_oc_cmd(
            command=f"adm policy add-scc-to-group {create_scc.name} system:authenticated"
        )

        # Delete csi-provisioner and noobaa db pods
        labels = [
            constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
            constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
            constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        ]
        pods = list()
        for label in labels:
            pods.extend(
                get_pods_having_label(
                    label=label, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
                )
            )
        pods = [Pod(**pod) for pod in pods]
        for pod in pods:
            pod.delete()

        # wait for the pods to reconcile
        try:
            wait_for_pods_to_be_running(pod_names=[pod.name for pod in pods])
        except TimeoutExpiredError as ex:
            logger.error("[Test Failed] Pods failed to reconcile!")
            raise ex
        logger.info("Pods reconciled successfully without any issues!!")

        # cluster sanity health check
        try:
            Sanity().health_check()
        except Exception as ex:
            logger.error("Failed at cluster health check!!")
            raise ex
