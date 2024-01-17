import logging
import os
import pytest
import yaml

from tempfile import NamedTemporaryFile
from ocs_ci.framework.pytest_customization.marks import bugzilla, green_squad
from ocs_ci.framework.testlib import (
    skipif_ocp_version,
    skipif_ocs_version,
    ManageTest,
    tier2,
)
from ocs_ci.helpers import helpers
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.exceptions import PVCNotCreated

logger = logging.getLogger(__name__)
ERRMSG = "Error in command"


@green_squad
@tier2
@skipif_ocp_version("<4.15")
@skipif_ocs_version("<4.15")
class TestCRRsourcesValidation(ManageTest):
    """
    Test that check that csi addons resources are not editable after creation
    """

    def setup(self):
        self.temp_files_list = []
        self.object_name_to_delete = ""
        self.pvc_objs_to_delete = []

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
                res = run_oc_command(
                    cmd=f"delete {self.object_name_to_delete}", namespace=self.namespace
                )
                assert ERRMSG not in res[0], (
                    f"Failed to delete {self.object_kind_to_delete} resource with name: {self.object_name_to_delete}, "
                    f"got result: {res}"
                )

            for pvc_obj in self.pvc_objs_to_delete:
                pvc_obj.delete()

        request.addfinalizer(finalizer)

    def cr_resource_not_editable(
        self,
        cr_object_kind,
        cr_resource_yaml,
        non_editable_patches,
        editable_patches,
        namespace="openshift-storage",
    ):
        """
        Test that cr object is not editable once created

        Args:
            cr_object_kind (str): cr object kind
            cr_resource_yaml (str): full path  of the yaml file from which the object is to be created
            non_editable_patches (dict, of str: str): patches to be applied by 'oc patch' command. These patches should
                    have no effect. If such a patch is applied successfully , the test should fail
            editable_patches (dict, of str: str): patches to be applied by 'oc patch' command. These patches should
                    have an effect. If such a patch is not applied successfully , the test should fail
            namespace (str): namespace in which CR object should be created

        """
        res = run_oc_command(cmd=f"create -f {cr_resource_yaml}", namespace=namespace)
        assert (
            ERRMSG not in res[0]
        ), f"Failed to create resource {cr_object_kind} from yaml file {cr_resource_yaml}, got result {res}"

        cr_resource_name = res[0].split()[0]
        self.object_name_to_delete = cr_resource_name
        self.object_kind_to_delete = cr_object_kind
        self.namespace = namespace

        cr_resource_original_yaml = run_oc_command(
            f"get {cr_resource_name} -o yaml", namespace=namespace
        )

        # test to verify that all non-editable properties are really not editable
        non_editable_properties_errors = {}
        cr_resource_prev_yaml = cr_resource_original_yaml
        for patch in non_editable_patches:
            params = "'" + f"{non_editable_patches[patch]}" + "'"
            command = (
                f"oc -n {namespace} patch {cr_resource_name} -p {params} --type merge"
            )

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
                    f"get {cr_resource_name} -o yaml", namespace=namespace
                )

                if cr_resource_prev_yaml != cr_resource_modified_yaml:
                    non_editable_properties_errors[patch] = cr_resource_modified_yaml
                    # reset prev yaml to the modified one to track further modifications
                    cr_resource_prev_yaml = cr_resource_modified_yaml
            except (
                CommandFailed
            ):  # some properties are not editable and CommandFailed exception is thrown
                logger.info(
                    f"Property {patch} is not editable, patch command failed, continue to the next"
                )
                continue  # just continue to the next property

        if non_editable_properties_errors:
            err_msg = (
                f"{cr_object_kind} object has been edited but it should not be. \n"
                f"Changed properties: {list(non_editable_properties_errors.keys())}"
            )
            logger.error(err_msg)

            detailed_err_msg = f"Original object yaml is {cr_resource_original_yaml}\n."
            for prop in non_editable_properties_errors:
                detailed_err_msg += (
                    f"Changed property is {prop}. \nEdited object yaml is "
                    f"{non_editable_properties_errors[prop]}\n"
                )
                logger.error(detailed_err_msg)

            raise Exception(err_msg)

        # test that all editable properties are really editable
        editable_properties_errors = {}
        cr_resource_prev_yaml = cr_resource_original_yaml
        for patch in editable_patches:
            params = "'" + f"{editable_patches[patch]}" + "'"
            command = (
                f"oc -n {namespace} patch {cr_resource_name} -p {params} --type merge"
            )

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
                    f"get {cr_resource_name} -o yaml", namespace=namespace
                )

                if cr_resource_prev_yaml == cr_resource_modified_yaml:
                    editable_properties_errors[patch] = cr_resource_modified_yaml
                else:
                    # reset prev yaml to the modified one to track further modifications
                    cr_resource_prev_yaml = cr_resource_modified_yaml
            except (
                CommandFailed
            ):  # some properties are not editable and CommandFailed exception is thrown
                logger.info(
                    f"Property {patch} should be editable, but patch command failed, continue to the next"
                )
                editable_properties_errors[patch] = cr_resource_modified_yaml
                continue  # just continue to the next property

        assert not editable_properties_errors, (
            f"{cr_object_kind} object has not been edited but it should be. \n"
            f"Unchanged properties: {list(editable_properties_errors.keys())}"
        )

    def test_network_fence_not_editable(self):
        """
        Test case to check that some properties of network fence object are not editable once the object is created
        """

        non_editable_patches = {  # dictionary: patch_name --> patch
            "apiVersion": {"apiVersion": "csiaddons.openshift.io/v1alpha2"},
            "kind": {"kind": "newNetworkFence"},
            "generation": '{"metadata": {"generation": 6456456 }}',
            "creationTime": '{"metadata": {"creationTimestamp": "2022-04-24T19:39:54Z" }}',
            "name": '{"metadata": {"name": "newName" }}',
            "uid": '{"metadata": {"uid": "897b3c9c-c1ce-40e3-95e6-7f3dadeb3e83" }}',
            "secret": '{"spec": {"secret": {"name": "new_secret"}}}',
            "cidrs": '{"spec": {"cidrs": {"10.90.89.66/32", "11.67.12.42/32"}}}',
            "driver": '{"spec": {"driver": "example.new_driver"}}',
            "new_property": '{"spec": {"new_property": "new_value"}}',
        }

        editable_patches = {  # dictionary: patch_name --> patch
            "fenceState": '{"spec": {"fenceState": "Unfenced"}}',
        }

        self.cr_resource_not_editable(
            "Network fence",
            os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, "NetworkFence.yaml"),
            non_editable_patches,
            editable_patches,
        )

    def test_reclaim_space_cron_job_editable(self):
        """
        Test case to check that some properties reclaim space cron job object are not editable once object is created
        """

        non_editable_patches = {  # dictionary: patch_name --> patch
            "persistentVolumeClaim": '{"spec": {"jobTemplate": {"spec": {"target" :{"persistentVolumeClaim": "pv"}}}}}',
        }

        self.cr_resource_not_editable(
            "Reclaim space cron job",
            os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, "ReclaimSpaceCronJob.yaml"),
            non_editable_patches,
            {},
        )

    def test_reclaim_space_job_editable(self):
        """
        Test case to check that some properties of reclaim space job object are not editable once object is created
        """

        non_editable_patches = {  # dictionary: patch_name --> patch
            "persistentVolumeClaim": '{"spec": {"target" :{"persistentVolumeClaim": "pv"}}}',
        }

        self.cr_resource_not_editable(
            "Reclaim space job",
            os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, "ReclaimSpaceJob.yaml"),
            non_editable_patches,
            {},
        )

    def test_volume_replication_class_editable(self):
        """
        Test case to check that some properties of volume replication class object are not editable
        once object is created
        """

        non_editable_patches = {  # dictionary: patch_name --> patch
            "provisioner": '{"spec": {"provisioner": "edited.provisioner.io"}}',
            "mirroringMode": '{"spec": {"parameters": {"mirroringMode": "clone"}}}',
            "replication-secret-name": '{"spec": {"parameters" : '
            '{"replication.storage.openshift.io/replication-secret-name": "my-secret"}}}',
            "replication-secret-namespace": '{"spec": {"parameters" :'
            '{"replication.storage.openshift.io/replication-secret-namespace": "my-name"}}}',
        }

        self.cr_resource_not_editable(
            "Volume Replication Class",
            os.path.join(
                constants.TEMPLATE_CSI_ADDONS_DIR, "volumeReplicationClass.yaml"
            ),
            non_editable_patches,
            {},
        )

    @bugzilla("2207780")
    def test_volume_replication_cr_editable(self):
        """
        Test case to check that some properties of volume replication class object are not editable
        once object is created
        """

        with open(
            os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, "volumeReplicationCR.yaml")
        ) as vrCR_yamml:
            yaml_dict = yaml.safe_load(vrCR_yamml)

        namespace = "default"
        try:
            pvc_obj = helpers.create_pvc(
                sc_name=constants.DEFAULT_STORAGECLASS_CEPHFS,
                size="1Gi",
                namespace=namespace,
            )
        except Exception as e:
            logger.exception(f"The PVC was not created, exception [{str(e)}]")
            raise PVCNotCreated("PVC did not reach BOUND state.")

        self.pvc_objs_to_delete.append(pvc_obj)
        yaml_dict["spec"]["dataSource"]["name"] = pvc_obj.name

        new_yaml_file = NamedTemporaryFile(
            mode="w+", prefix="volumeReplicationCR_modified", suffix=".yaml"
        )
        with open(new_yaml_file.name, "w") as y_file:
            y_file.write(yaml.safe_dump(yaml_dict))
        self.temp_files_list.append(new_yaml_file.name)

        non_editable_patches = {  # dictionary: patch_name --> patch
            "volume_replication_class": '{"spec": {"volumeReplicationClass": "volume-replication-class-new_sample"}}',
            "pvc_name": '{"spec": {"dataSource": {"name": "aaa"}}}',
        }

        self.cr_resource_not_editable(
            "Volume Replication CR",
            new_yaml_file.name,
            non_editable_patches,
            {},
            namespace=namespace,
        )
