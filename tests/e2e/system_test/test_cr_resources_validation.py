import logging
import os
import pytest
import yaml
from tempfile import NamedTemporaryFile

from ocs_ci.framework.testlib import E2ETest
from ocs_ci.helpers.performance_lib import run_oc_command
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)
ERRMSG = "Error in command"


class TestCRRsourcesValidation(E2ETest):
    """
    Tests that check that csi addons resources are not editable
    """

    def test_network_fence_not_editable(self):
        """
           Test that network fence object is not editable once created
         """
        network_fence_yaml = os.path.join(constants.TEMPLATE_CSI_ADDONS_DIR, "NetworkFence.yaml")
        res = run_oc_command(cmd=f"create -f {network_fence_yaml}")
        if ERRMSG in res[0]:
            err_msg = f"Failed to create resource from yaml file : {network_fence_yaml}, got result {res}"
            logger.error(err_msg)
            raise Exception(err_msg)

        network_fence_name = res[0].split()[0]

        res = run_oc_command(f"get {network_fence_name} -o yaml")
        network_fence_original_yaml = res

        patches = {  # dictionary: patch_name --> patch
            "generation": '{"metadata": {"generation": 6456456 }}',
            "creationTime": '{"metadata": {"creationTimestamp": "2022-04-24T19:39:54Z" }}',
            "secret": '{"spec": {"secret": {"name": "new_secret"}}}',
        }

        for patch in patches:
            params = "'" + f"{patches[patch]}" + "'"
            command = (f"oc -n openshift-storage patch {network_fence_name} -p {params} --type merge")

            temp_file = NamedTemporaryFile(mode="w+", prefix="network_fence_modification", suffix=".sh")
            with open(temp_file.name, "w") as t_file:
                t_file.writelines(command)
            run_cmd(f"chmod 777 {temp_file.name}")
            logger.info(f"Going to edit property {patch}")
            run_cmd(f"sh {temp_file.name}")

            res = run_oc_command(f"get {network_fence_name} -o yaml")
            network_fence_modified_yaml = res

            if (network_fence_original_yaml != network_fence_modified_yaml):
                run_oc_command(cmd=f"delete {network_fence_name}")
                err_msg = f"Network fence object has been edited but it should not. \n" \
                          f"Property {patch} was changed. \n" \
                          f"Original object yaml is {network_fence_original_yaml}\n." \
                          f"Edited object yaml is {network_fence_modified_yaml}"
                logger.error(err_msg)
                raise Exception(err_msg)

        res = run_oc_command(cmd=f"delete {network_fence_name}")
        if ERRMSG in res[0]:
            err_msg = f"Failed to delete network fence resource of name : {network_fence_name}, got result {res}"
            logger.error(err_msg)
            raise Exception(err_msg)
