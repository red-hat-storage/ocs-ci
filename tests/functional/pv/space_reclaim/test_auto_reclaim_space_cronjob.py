import logging
import pytest
import yaml
from uuid import uuid4
import os
from tempfile import NamedTemporaryFile

from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
)
from ocs_ci.utility.utils import run_cmd
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.helpers import helpers, performance_lib
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import PVCNotCreated

logger = logging.getLogger(__name__)
ERRMSG = "Error in command"


# @tier2
# Test case is disabled
# Creating reclaim space cron job for namespace with prefix openshift-* has been deprecated since 4.16
# This will be implemented at storageclass level rather than at the namespace level in 4.17
@green_squad
@skipif_ocs_version("<4.14")
class TestReclaimSpaceCronJob(ManageTest):
    """
    Test that verifies automatic creation of Reclaim Space Cron Jobs for RBD PVCs in openshift-* namespaces
    The test also verifies that no Reclaim Space Cron Job is created automatically for CephFS PVC in this namespace
    """

    def setup(self):
        self.pvc_objs_created = []
        self.temp_files_list = []

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Cleanup the test environment

        """

        def finalizer():
            # pvc objects usually are deleted in the test (and the second delete has no effect),
            # if for some reason the pvcs were not deleted in the test - they will be deleted here
            for pvc_obj in self.pvc_objs_created:
                pvc_obj.delete()

            for temp_file in self.temp_files_list:
                if os.path.exists(temp_file):
                    run_cmd(f"rm {temp_file}")

            if self.delete_namespace:
                run_oc_command(cmd=f"delete namespace {self.namespace}")

        request.addfinalizer(finalizer)

    def __reclaim_space_cronjob(self):
        """
        Method which tests that cronjobs are created for RBD PVCs and not created for CEPHFS PVCs
        """
        num_of_pvcs = 10

        existing_pvcs_num = 0
        for line in run_oc_command("get pvc", namespace=self.namespace):
            if constants.STATUS_BOUND in line:
                existing_pvcs_num += 1

        logger.info(f"There are {existing_pvcs_num} existing PVCs")

        cron_jobs_result = run_oc_command(
            "get reclaimspacecronjob", namespace=self.namespace
        )
        existing_cronjobs_num = (
            len(cron_jobs_result) - 1
        )  # first line of the result is title
        logger.info(f"There are {existing_cronjobs_num} existing cronjobs")

        self.pvc_objs_created, _ = helpers.create_multiple_pvcs(
            sc_name=constants.DEFAULT_STORAGECLASS_RBD,
            namespace=self.namespace,
            number_of_pvc=num_of_pvcs,
            size="1Gi",
        )

        logger.info("Wait for all of the PVCs to be in Bound state")
        performance_lib.wait_for_resource_bulk_status(
            "pvc",
            num_of_pvcs + existing_pvcs_num,
            self.namespace,
            constants.STATUS_BOUND,
            num_of_pvcs * 2,
            5,
        )
        # in case of creation faliure, the wait_for_resource_bulk_status function
        # will raise an exception. so in this point the creation succeed

        result = performance_lib.wait_for_cronjobs(
            self.namespace,
            num_of_pvcs + existing_cronjobs_num,
            f"No reclaim space cron jobs exist in namespace {self.namespace}",
        )
        logger.info(f"Reclaim space jobs after PVC creation {result}")

        cronjob_names = []
        for cronjobs in result[1:]:
            cronjob_names.append(cronjobs.split()[0])

        for pvc_obj in self.pvc_objs_created:
            cronjob_for_pvc = [
                name for name in cronjob_names if name.startswith(f"{pvc_obj.name}-")
            ]
            assert (
                len(cronjob_for_pvc) == 1
            ), f"Expected exactly one reclaim space cron job for  pvc {pvc_obj.name}, {len(cronjob_for_pvc)} found."
        logger.info("Existence of reclaim space cron jobs for RBD PVCs was validated.")
        for pvc_obj in self.pvc_objs_created:
            pvc_obj.delete()

        logger.info(
            "Validating that all the cron jobs were deleted following deletion of the PVCs"
        )
        performance_lib.wait_for_cronjobs(
            self.namespace,
            existing_cronjobs_num,
            "After PVCs deletion some reclaimspacecronjobs were left",
        )

        # create CephFS PVC and test that no reclaim space job created for it
        try:
            pvc_obj = helpers.create_pvc(
                sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
                size="1Gi",
                namespace=self.namespace,
            )
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        except Exception as e:
            logger.exception(f"The PVC was not created, exception [{str(e)}]")
            raise PVCNotCreated("PVC did not reach BOUND state.")

        logger.info(f"PVC by name {pvc_obj.name} created")
        self.pvc_objs_created.append(pvc_obj)

        performance_lib.wait_for_cronjobs(
            self.namespace,
            existing_cronjobs_num,
            "After CephtFS PVC creation reclaim space cron job exists",
        )

        logger.info("No reclaim space cron job was created for CephFS PVCs")
        pvc_obj.delete()

    @pytest.mark.polarion_id("OCS-5400")
    def test_reclaim_space_cronjob(self):
        """
        Test case to check reclaim space cronjobs are created correctly for rbd pvcs in openshift-* namespace
        """
        namespace = f"openshift-{uuid4().hex}"
        self.namespace = namespace
        result = run_oc_command(cmd=f"create namespace {self.namespace}")
        assert ERRMSG not in result[0], (
            f"Failed to create namespace with name {namespace}" f"got result: {result}"
        )
        logger.info(f"Namespace {namespace} created")

        result = run_oc_command(cmd=f"get namespace {namespace} -o yaml")
        namespace_dict = yaml.safe_load("\n".join(result))

        schedule = namespace_dict["metadata"]["annotations"][
            "reclaimspace.csiaddons.openshift.io/schedule"
        ]
        assert (
            schedule == "@weekly"
        ), f"Namespace {namespace} created with schedule {schedule}, expected @weekly"
        logger.info(f"Existence of schedule {schedule} validated.")

        self.delete_namespace = True
        self.__reclaim_space_cronjob()

    @pytest.mark.polarion_id("OCS-5401")
    def test_reclaim_space_cronjob_on_existing_namespace(self):
        """
        Test case to check reclaim space cronjobs are created correctly for rbd pvcs in existing openshift-* namespaces
        ("openshift-monitoring" is taken as example)
        """
        self.namespace = "openshift-monitoring"
        self.delete_namespace = False
        self.__reclaim_space_cronjob()

    @pytest.mark.polarion_id("OCS-5402")
    def test_skip_reclaim_space(self):
        """
        Test case to check that no reclaim space job is created for rbd pvc
        in the openshift-* namespace if skipReclaimspaceSchedule value is True
        """
        namespace = f"openshift-{uuid4().hex}"
        self.namespace = namespace
        with open(
            os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, "ReclaimSpace_skip.yaml"),
            "r",
        ) as stream:
            try:
                namespace_yaml = yaml.safe_load(stream)
                namespace_yaml["metadata"]["name"] = namespace
            except yaml.YAMLError as exc:
                logger.error(f"Can not read template yaml file {exc}")

        temp_file = NamedTemporaryFile(
            mode="w+", prefix="namespace_skip_reclaim_space", suffix=".yaml"
        )
        with open(temp_file.name, "w") as f:
            yaml.dump(namespace_yaml, f)

        self.temp_files_list.append(temp_file.name)

        res = run_oc_command(cmd=f"create -f {temp_file.name}")
        assert ERRMSG not in res[0], (
            f"Failed to create namespace with name {namespace} " f"got result: {res}"
        )
        logger.info(f"Namespace {namespace} created")
        self.delete_namespace = True

        try:
            pvc_obj = helpers.create_pvc(
                sc_name=constants.DEFAULT_STORAGECLASS_RBD,
                size="1Gi",
                namespace=namespace,
            )
            helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND)
        except Exception as e:
            logger.exception(f"The PVC was not created, exception [{str(e)}]")
            raise PVCNotCreated("PVC did not reach BOUND state.")

        logger.info(f"PVC by name {pvc_obj.name} created")
        self.pvc_objs_created.append(pvc_obj)

        performance_lib.wait_for_cronjobs(
            self.namespace, 0, "For RBD PVC creation reclaim space cron job exists"
        )

        logger.info(
            "No reclaim space cron job was created for RBD PVC if skipReclaimspaceSchedule is True."
        )
        pvc_obj.delete()
