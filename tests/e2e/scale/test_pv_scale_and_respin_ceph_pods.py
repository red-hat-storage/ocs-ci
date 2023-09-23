"""
Scale TC to perform PVC Scale and Respin of Ceph pods in parallel
"""
import logging
import pytest
import threading
import pathlib

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import utils
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.helpers import helpers, disruption_helpers
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.framework.pytest_customization.marks import orange_squad
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers
from ocs_ci.framework.pytest_customization.marks import skipif_external_mode
from ocs_ci.helpers.storageclass_helpers import storageclass_name

log = logging.getLogger(__name__)


class BasePvcCreateRespinCephPods(E2ETest):
    """
    Base Class to create POD with PVC and respin ceph Pods
    """

    kube_job_pvc_list, kube_job_pod_list = ([], [])

    def create_pvc_pod(self, obj_name, number_of_pvc, size):
        """
        Function to create multiple PVC of different type and create pod using kube_job

        Args:
            obj_name (str): Kube Job Object name prefix
            number_of_pvc (int): pvc count to be created for each type
            size (str): size of each pvc to be created eg: '10Gi'
        """
        log.info(
            f"Start creating {number_of_pvc * 4} PVC of 4 types RBD, FS with RWO & RWX"
        )
        cephfs_sc_obj = storageclass_name(constants.OCS_COMPONENTS_MAP["cephfs"])
        rbd_sc_obj = storageclass_name(constants.OCS_COMPONENTS_MAP["blockpools"])

        # Get pvc_dict_list, append all the pvc.yaml dict to pvc_dict_list
        rbd_pvc_dict_list, rbd_rwx_pvc_dict_list, cephfs_pvc_dict_list = ([], [], [])
        access_modes = [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        rbd_pvc_dict_list.extend(
            scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
                no_of_pvc=int(number_of_pvc),
                access_mode=constants.ACCESS_MODE_RWO,
                sc_name=rbd_sc_obj,
                pvc_size=size,
                max_pvc_size=number_of_pvc,
            )
        )
        rbd_rwx_pvc_dict_list.extend(
            scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
                no_of_pvc=int(number_of_pvc),
                access_mode=constants.ACCESS_MODE_RWX,
                sc_name=rbd_sc_obj,
                pvc_size=size,
                max_pvc_size=number_of_pvc,
            )
        )
        for mode in access_modes:
            cephfs_pvc_dict_list.extend(
                scale_lib.construct_pvc_creation_yaml_bulk_for_kube_job(
                    no_of_pvc=int(number_of_pvc),
                    access_mode=mode,
                    sc_name=cephfs_sc_obj,
                    pvc_size=size,
                    max_pvc_size=number_of_pvc,
                )
            )

        # kube_job for cephfs and rbd PVC creations
        lcl = locals()
        tmp_path = pathlib.Path(utils.ocsci_log_path())
        lcl[f"rbd-pvc-{obj_name}"] = ObjectConfFile(
            name=f"rbd-pvc-{obj_name}",
            obj_dict_list=rbd_pvc_dict_list,
            project=self.namespace,
            tmp_path=tmp_path,
        )
        lcl[f"rbd-rwx-pvc-{obj_name}"] = ObjectConfFile(
            name=f"rbd-rwx-pvc-{obj_name}",
            obj_dict_list=rbd_rwx_pvc_dict_list,
            project=self.namespace,
            tmp_path=tmp_path,
        )
        lcl[f"cephfs-pvc-{obj_name}"] = ObjectConfFile(
            name=f"cephfs-pvc-{obj_name}",
            obj_dict_list=cephfs_pvc_dict_list,
            project=self.namespace,
            tmp_path=tmp_path,
        )

        # Create kube_job for PVC creations
        lcl[f"rbd-pvc-{obj_name}"].create(namespace=self.namespace)
        lcl[f"rbd-rwx-pvc-{obj_name}"].create(namespace=self.namespace)
        lcl[f"cephfs-pvc-{obj_name}"].create(namespace=self.namespace)

        # Check all the PVC reached Bound state
        rbd_pvc_name = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=lcl[f"rbd-pvc-{obj_name}"],
            namespace=self.namespace,
            no_of_pvc=int(number_of_pvc),
            timeout=60,
        )
        rbd_rwx_pvc_name = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=lcl[f"rbd-rwx-pvc-{obj_name}"],
            namespace=self.namespace,
            no_of_pvc=int(number_of_pvc),
            timeout=60,
        )
        fs_pvc_name = scale_lib.check_all_pvc_reached_bound_state_in_kube_job(
            kube_job_obj=lcl[f"cephfs-pvc-{obj_name}"],
            namespace=self.namespace,
            no_of_pvc=int(number_of_pvc * 2),
            timeout=60,
        )

        # Construct pod yaml file for kube_job
        pod_data_list = list()
        pod_data_list.extend(
            scale_lib.attach_multiple_pvc_to_pod_dict(
                pvc_list=rbd_pvc_name,
                namespace=self.namespace,
                pvcs_per_pod=1,
            )
        )
        pod_data_list.extend(
            scale_lib.attach_multiple_pvc_to_pod_dict(
                pvc_list=rbd_rwx_pvc_name,
                namespace=self.namespace,
                raw_block_pv=True,
                pvcs_per_pod=1,
            )
        )
        pod_data_list.extend(
            scale_lib.attach_multiple_pvc_to_pod_dict(
                pvc_list=fs_pvc_name,
                namespace=self.namespace,
                pvcs_per_pod=1,
            )
        )

        # Create kube_job for pod creation
        lcl[f"pod-{obj_name}"] = ObjectConfFile(
            name=f"pod-{obj_name}",
            obj_dict_list=pod_data_list,
            project=self.namespace,
            tmp_path=tmp_path,
        )
        lcl[f"pod-{obj_name}"].create(namespace=self.namespace)

        # Check all the POD reached Running state
        pod_running_list = scale_lib.check_all_pod_reached_running_state_in_kube_job(
            kube_job_obj=lcl[f"pod-{obj_name}"],
            namespace=self.namespace,
            no_of_pod=len(pod_data_list),
            timeout=90,
        )
        self.pod_count = self.pod_count + len(pod_data_list)

        # Update list with all the kube_job object created, list will be used in cleanup
        self.kube_job_pvc_list.append(lcl[f"rbd-pvc-{obj_name}"])
        self.kube_job_pvc_list.append(lcl[f"rbd-rwx-pvc-{obj_name}"])
        self.kube_job_pvc_list.append(lcl[f"cephfs-pvc-{obj_name}"])
        self.kube_job_pod_list.append(lcl[f"pod-{obj_name}"])

    def respin_ceph_pod(self, resource_to_delete):
        """
        Function to respin ceph pods one by one,
        delete_resource functions checks for the deleted pod back up and running

        Args:
            resource_to_delete (str): Ceph resource type to be deleted.
            eg: mgr/mon/osd/mds/cephfsplugin/rbdplugin/cephfsplugin_provisioner/rbdplugin_provisioner
        """
        disruption = disruption_helpers.Disruptions()
        disruption.set_resource(resource=resource_to_delete)
        no_of_resource = disruption.resource_count
        for i in range(0, no_of_resource):
            disruption.delete_resource(resource_id=i)
            # Validate storage pods are running
            assert pod.wait_for_storage_pods(), "ODF Pods are not in good shape"
            # Validate cluster health ok and all pods are running
            assert utils.ceph_health_check(
                delay=180
            ), "Ceph health in bad state after node reboots"

    def cleanup(self):
        """
        Function to clean_up the namespace, PVC and POD kube objects.
        """
        # Delete all pods, pvcs and namespaces
        for job in self.kube_job_pod_list:
            job.delete(namespace=self.namespace)

        for job in self.kube_job_pvc_list:
            job.delete(namespace=self.namespace)

        ocp = OCP(kind=constants.NAMESPACE)
        ocp.delete(resource_name=self.namespace)
        self.kube_job_pod_list.clear()
        self.kube_job_pvc_list.clear()


@orange_squad
@scale
@ignore_leftovers
@skipif_external_mode
@pytest.mark.parametrize(
    argnames="resource_to_delete",
    argvalues=[
        pytest.param(
            *["mgr"],
            marks=[
                pytest.mark.polarion_id("OCS-766"),
                pytest.mark.skip(reason="Skipped due to bz 2130867"),
            ],
        ),
        pytest.param(*["mon"], marks=[pytest.mark.polarion_id("OCS-764")]),
        pytest.param(*["osd"], marks=[pytest.mark.polarion_id("OCS-765")]),
        pytest.param(*["mds"], marks=[pytest.mark.polarion_id("OCS-613")]),
        pytest.param(
            *["cephfsplugin_provisioner"], marks=[pytest.mark.polarion_id("OCS-2641")]
        ),
        pytest.param(
            *["rbdplugin_provisioner"], marks=[pytest.mark.polarion_id("OCS-2639")]
        ),
        pytest.param(*["rbdplugin"], marks=[pytest.mark.polarion_id("OCS-2643")]),
        pytest.param(*["cephfsplugin"], marks=[pytest.mark.polarion_id("OCS-2642")]),
    ],
)
class TestPVSTOcsCreatePVCsAndRespinCephPods(BasePvcCreateRespinCephPods):
    """
    Class for PV scale Create Cluster with 1000 PVC, then Respin ceph pods parallel
    """

    pod_count = 0

    @pytest.fixture()
    def setup_fixture(self, teardown_factory, request):
        proj_obj = helpers.create_project()
        self.namespace = proj_obj.namespace

        def finalizer():
            self.cleanup()

        request.addfinalizer(finalizer)

    def test_pv_scale_out_create_pvcs_and_respin_ceph_pods(
        self,
        setup_fixture,
        resource_to_delete,
    ):
        pvc_count_each_itr = 10
        scale_pod_count = 120
        size = "10Gi"
        iteration = 1
        kube_obj_name = helpers.create_unique_resource_name("obj", "kube")

        # First Iteration call to create PVC and POD
        self.create_pvc_pod(f"{kube_obj_name}-{iteration}", pvc_count_each_itr, size)
        # Re-spin the ceph pods one by one in parallel with PVC and POD creation
        while True:
            if scale_pod_count <= self.pod_count:
                log.info(f"Create {scale_pod_count} pvc and pods")
                break
            else:
                iteration += 1
                thread1 = threading.Thread(
                    target=self.respin_ceph_pod, args=(resource_to_delete,)
                )
                thread2 = threading.Thread(
                    target=self.create_pvc_pod,
                    args=(
                        f"{kube_obj_name}-{iteration}",
                        pvc_count_each_itr,
                        size,
                    ),
                )
                thread1.start()
                thread2.start()
            thread1.join()
            thread2.join()

        assert utils.ceph_health_check(
            delay=180
        ), "Ceph health in bad state after pod respins"
