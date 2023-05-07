import logging
import os
import pytest
from tempfile import NamedTemporaryFile

from ocs_ci.framework.testlib import skipif_ocp_version, skipif_ocs_version, E2ETest
from ocs_ci.framework.pytest_customization.marks import system_test
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed

logger = logging.getLogger(__name__)
ERRMSG = "Error in command"


@system_test
@skipif_ocp_version("<4.13")
@skipif_ocs_version("<4.13")
class TestCRRsourcesValidation(E2ETest):
    """
    Test that check that csi addons resources are not editable after creation
    """

    def setup(self):
        self.temp_files_list = []
        self.object_name_to_delete = ""

    def cr_resource_not_editable(self, cr_object_kind, yaml_name, patches):
        """
        Test that cr object is not editable once created

        Args:
            cr_object_kind (str): cr object kind
            yaml_name (str): name of the yaml file from which the object is to be created
            patches (dict, of str: str): patches to be applied by 'oc patch' command

        """
        cr_resource_yaml = os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, yaml_name)
        res = run_oc_command(cmd=f"create -f {cr_resource_yaml}")
        assert (
            ERRMSG not in res[0]
        ), f"Failed to create resource {cr_object_kind} from yaml file {cr_resource_yaml}, got result {res}"

        cr_resource_name = res[0].split()[0]
        self.object_name_to_delete = cr_resource_name

        cr_resource_original_yaml = run_oc_command(f"get {cr_resource_name} -o yaml")

        editable_properties = {}
        cr_resource_prev_yaml = cr_resource_original_yaml
        for patch in patches:
            params = "'" + f"{patches[patch]}" + "'"
            command = f"oc -n openshift-storage patch {cr_resource_name} -p {params} --type merge"

            temp_file = NamedTemporaryFile(
                mode="w+", prefix="cr_resource_modification", suffix=".sh"
            )
            with open(temp_file.name, "w") as t_file:
                t_file.writelines(command)
            self.temp_files_list.append(temp_file.name)
            run_cmd(f"chmod 777 {temp_file.name}")
            logger.info(f"Trying to edit property {patch}")

            try:
                run_cmd(f"sh {temp_file.name}")
                cr_resource_modified_yaml = run_oc_command(
                    f"get {cr_resource_name} -o yaml"
                )

                if cr_resource_prev_yaml != cr_resource_modified_yaml:
                    editable_properties[patch] = cr_resource_modified_yaml
                    # reset prev yaml to the modified one to track further modifications
                    cr_resource_prev_yaml = cr_resource_modified_yaml
            except (
                CommandFailed
            ):  # some properties are not editable and CommandFailed exception is thrown
                continue  # just continue to the next property

        if editable_properties:
            err_msg = (
                f"{cr_object_kind} object has been edited but it should not be. \n"
                f"Changed properties: {list(editable_properties.keys())}"
            )
            logger.error(err_msg)

            detailed_err_msg = f"Original object yaml is {cr_resource_original_yaml}\n."
            for prop in editable_properties:
                detailed_err_msg += f"Changed property is {prop}. "
                detailed_err_msg += (
                    f"Edited object yaml is {editable_properties[prop]}\n"
                )
            logger.error(detailed_err_msg)

            raise Exception(err_msg)

    def test_network_fence_not_editable(self):
        """
        Test case to check that network fence object is not editable once created
        """

        patches = {  # dictionary: patch_name --> patch
            "apiVersion": {"apiVersion": "csiaddons.openshift.io/v1alpha2"},
            "kind": {"kind": "newNetworkFence"},
            "generation": '{"metadata": {"generation": 6456456 }}',
            "creationTime": '{"metadata": {"creationTimestamp": "2022-04-24T19:39:54Z" }}',
            "name": '{"metadata": {"name": "newName" }}',
            "uid": '{"metadata": {"uid": "897b3c9c-c1ce-40e3-95e6-7f3dadeb3e83" }}',
            "secret": '{"spec": {"secret": {"name": "new_secret"}}}',
            "cidrs": '{"spec": {"cidrs": {"10.90.89.66/32", "11.67.12.42/32"}}}',
            "fenceState": '{"spec": {"fenceState": "Unfenced"}}',
            "driver": '{"spec": {"driver": "example.new_driver"}}',
            "new_property": '{"spec": {"new_property": "new_value"}}',
        }

        self.cr_resource_not_editable("Network fence", "NetworkFence.yaml", patches)

    def test_reclaim_space_cron_job_editable(self):
        """
        Test case to check that reclaim space cron job object is not editable once created
        """

        patches = {  # dictionary: patch_name --> patch
            "apiVersion": {"apiVersion": "csiaddons.openshift.io/v1alpha2"},
            "kind": {"kind": "newReclainSpaceCronJob"},
            "generation": '{"metadata": {"generation": 6456456 }}',
            "creationTime": '{"metadata": {"creationTimestamp": "2022-04-24T19:39:54Z" }}',
            "name": '{"metadata": {"name": "newName" }}',
            "resourceVersion": '{"metadata": {"resourceVersion": "21332" }}',
            "uid": '{"metadata": {"uid": "897b3c9c-c1ce-40e3-95e6-7f3dadeb3e83" }}',
            "concurrencyPolicy": '{"spec": {"concurrencyPolicy": "Replace"}}',
            "failedJobsHistoryLimit": '{"spec": {"failedJobsHistoryLimit": 50}}',
            "backoffLimit": '{"spec": {"jobTemplate": {"spec": {"backOffLimit": 111}}}}',
            "retryDeadlineSeconds": '{"spec": {"jobTemplate": {"spec": {"retryDeadlineSeconds": 20}}}}',
            "persistentVolumeClaim": '{"spec": {"jobTemplate": {"spec": {"target" :{"persistentVolumeClaim": "pv"}}}}}',
            "successfulJobsHistoryLimit": '{"spec": {"successfulJobsHistoryLimit": 300}}',
            "new_property": '{"spec": {"new_property": "new_value"}}',
            "schedule": '{"spec": {"schedule": "@daily"}}',
        }

        self.cr_resource_not_editable(
            "Reclaim space cron job", "ReclaimSpaceCronJob.yaml", patches
        )

    @pytest.fixture(autouse=True)
    def teardown(self, request):
        """
        Cleanup the test environment

        """

        def finalizer():
            for temp_file in self.temp_files_list:
                if os.path.exists(temp_file):
                    run_cmd(f"rm {temp_file}")

            if self.object_name_to_delete != "":
                res = run_oc_command(cmd=f"delete {self.object_name_to_delete}")
                assert (
                    ERRMSG not in res[0]
                ), f"Failed to delete network fence resource with name: {self.object_name_to_delete}, got result: {res}"

        request.addfinalizer(finalizer)
