import logging
import pytest
from ocs_ci.helpers.helpers import is_rbd_default_storage_class
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.framework.pytest_customization.marks import (
    tier1,
    green_squad,
    polarion_id,
    baremetal_deployment_required,
    ui_deployment_required,
)
from ocs_ci.ocs.ui.helpers_ui import is_ui_deployment


log = logging.getLogger(__name__)


@green_squad
@baremetal_deployment_required
@ui_deployment_required
class TestRBDStorageClassAsDefaultStorageClass:
    @tier1
    @polarion_id("OCS-5459")
    def test_pvc_creation_without_storageclass_name(self, pvc_factory, pod_factory):
        """
        Test PVC creation without mentioning storageclass name in the spec.

        Steps:
            1. Create a PVC and don't provide any storage class name in the YAML file.
            2. Verify PVC has created and it has attached to the Default RBD  SC.
            3. Verify Storagecalss attched to PVC is the default RBD.
            4. Create a POD and attached the  above PVC to the Pod.
            5. Start IO on verify that IO is successful on the PV.
        """
        if not is_ui_deployment():
            pytest.skip("cluster is not deployed from UI. Skipping test.")

        pvc_data = templating.load_yaml(constants.CSI_PVC_YAML)
        pvc_data["metadata"]["name"] = create_unique_resource_name("test", "pvc")
        log.info("Removing 'storageClassName' Parameter from the PVC yaml file.")
        del pvc_data["spec"]["storageClassName"]

        pvc_obj = pvc_factory(custom_data=pvc_data, status=constants.STATUS_BOUND)
        log.info("Created PVC without providing storage class name")
        assert pvc_obj, "PVC creation failed."

        sc_attached_to_pvc = pvc_obj.get().get("spec").get("storageClassName")
        log.info("Verifying the storageclass attached to PVC is correct.")

        assert is_rbd_default_storage_class(
            sc_name=sc_attached_to_pvc
        ), f"RBD storageclass {sc_attached_to_pvc} is not default storageclass for Cluster."

        log.info("Attaching PVC to pod to start IO workload.")
        pod_obj = pod_factory(pvc=pvc_obj, status=constants.STATUS_RUNNING)
        pod_obj.run_io(direct=1, runtime=60, storage_type="fs", size="1G")

        # Wait for IO completion
        fio_result = pod_obj.get_fio_results()
        log.info("IO completed on all pods")
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on pod {pod_obj.name}. " f"FIO result: {fio_result}"
        )
