import logging
import pytest
import time
import yaml
from tempfile import NamedTemporaryFile

from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import ManageTest, skipif_ocs_version, tier2, polarion_id
from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.ocs.exceptions import UnexpectedBehaviour
from ocs_ci.utility.utils import run_cmd


log = logging.getLogger(__name__)


@pytest.mark.parametrize(
    argnames="interface",
    argvalues=[
        pytest.param(*[constants.CEPHBLOCKPOOL]),
        pytest.param(*[constants.CEPHFILESYSTEM]),
    ],
)
@green_squad
@tier2
@skipif_ocs_version("<4.16")
class TestMigrateRWO2RWOP(ManageTest):
    """
    Tests changing access modes of the created pvc to ReadWriteOncePod access mode
    """

    def run_pv_patch(self, pv, namespace, patch):
        """
        Runs oc patch command on pv
        Args:
            pv(str) name of pv on which the patch will be run
            namespace (str) namespace in which the command is run
            patch (str) the patch to be run
        """
        patch_param = "'" + patch + "'"
        command = f"oc -n {namespace} patch pv {pv} -p {patch_param} --type merge"

        temp_file = NamedTemporaryFile(mode="w+", prefix="run_pv_patch", suffix=".sh")
        with open(temp_file.name, "w") as t_file:
            t_file.writelines(command)
        run_cmd(f"chmod 777 {temp_file.name}")
        run_cmd(f"sh {temp_file.name}")

    @polarion_id("OCS-5907")
    def test_pvc_migrate_rwo_to_rwop(self, pvc_factory, pod_factory, interface):
        """
        Tests that changing access mode from ReadWriteOnce to ReadWriteOncePod is successful
        1. Create PVC with RWO access mode
        2. Delete the PVC without deleting the PV
        3. Change the access mode of PV to RWOP
        4. Create another PVC with the same name and RWOP access mode on given PV
        5. Test that the second PVC behaves as indeed supposed in RWOP access mode , i.e it is possible to create
            one running pod this pvc, and the second pod created on this pvc will be in the 'Pending' state

        """
        pvc_obj = pvc_factory(
            interface=interface, access_mode=constants.ACCESS_MODE_RWO
        )
        pvc_obj.reload()

        assert pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_RWO, (
            f"PVC object {pvc_obj.name} has access mode {pvc_obj.get_pvc_access_mode}, "
            f"expected {constants.ACCESS_MODE_RWO}"
        )

        pvc_name = pvc_obj.name
        sc_name = pvc_obj.backed_sc
        namespace = pvc_obj.namespace
        pv = pvc_obj.backed_pv

        # ensures that pv is not deleted when pvc is deleted
        patch = '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
        self.run_pv_patch(pv, namespace, patch)

        pvc_obj.delete()
        pvc_obj.ocp.wait_for_delete(resource_name=pvc_name)

        # clean claimRef to ensure that another PVC can be bound
        patch = '{"spec":{"claimRef":{"uid":""}}}'
        self.run_pv_patch(pv, namespace, patch)

        # change the access mode
        patch = '{"spec":{"accessModes":["ReadWriteOncePod"]}}'
        self.run_pv_patch(pv, namespace, patch)

        pvc_obj2 = helpers.create_pvc(
            sc_name=sc_name,
            pvc_name=pvc_name,
            access_mode=constants.ACCESS_MODE_RWOP,
            volume_name=pv,
            namespace=namespace,
        )
        helpers.wait_for_resource_state(pvc_obj2, "Bound")
        log.info(f"pvc {pvc_obj2.name} reached Bound state")

        assert pvc_obj2.get_pvc_access_mode == constants.ACCESS_MODE_RWOP, (
            f"PVC object {pvc_obj2.name} has access mode {pvc_obj2.get_pvc_access_mode}, "
            f"expected {constants.ACCESS_MODE_RWOP}"
        )

        node0_name = node.get_worker_nodes()[0]

        pod_obj1 = pod_factory(pvc=pvc_obj2, node_name=node0_name)
        log.info(f"First pod with name {pod_obj1.name} created and running")

        # create second pod and validate that it is in the 'Pending' state
        pod_obj2 = helpers.create_pods(
            [pvc_obj2], pod_factory, interface, nodes=[node0_name]
        )[0]
        time.sleep(60)

        log.info(f"Second pod with name {pod_obj2.name} created")

        # Validating the pod status
        yaml_output = run_cmd(
            "oc get pod " + str(pod_obj2.name) + f" -n {namespace} -o yaml", timeout=60
        )
        results = yaml.safe_load(yaml_output)
        log.info(f"Status of the Pod : {results['status']['phase']}")
        if results["status"]["phase"] != "Pending":
            raise UnexpectedBehaviour(
                f"Pod {pod_obj2.name} using RWOP pvc {pvc_obj.name} is not in Pending state"
            )

        # return the original volume reclaim policy
        patch = '{"spec":{"persistentVolumeReclaimPolicy":"Delete"}}'
        self.run_pv_patch(pv, namespace, patch)

        pod_obj1.delete()
        pod_obj2.delete()
        pvc_obj2.delete()
