import logging
import pathlib

from datetime import datetime, timedelta

from ocs_ci.ocs import constants, workload as _workload
from ocs_ci.ocs.exceptions import CommandFailed, UnsupportedWorkloadError
from ocs_ci.utility.utils import ocsci_log_path
from ocs_ci.ocs.scale_noobaa_lib import (
    construct_obc_creation_yaml_bulk_for_kube_job,
    check_all_obc_reached_bound_state_in_kube_job,
)
from ocs_ci.utility import templating
from ocs_ci.ocs.scale_lib import (
    construct_pvc_creation_yaml_bulk_for_kube_job,
    check_all_pvc_reached_bound_state_in_kube_job,
    check_all_pod_reached_running_state_in_kube_job,
)
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.ocs.pgsql import Postgresql
from ocs_ci.ocs.couchbase import CouchBase
from ocs_ci.ocs.cosbench import Cosbench
from ocs_ci.helpers.helpers import create_unique_resource_name
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.ocs.resources.pvc import get_pvc_objs, delete_pvcs
from ocs_ci.ocs.resources.pod import delete_pods
from ocs_ci.ocs.resources import pod
import ocs_ci.ocs.exceptions as ex
from ocs_ci.deployment.deployment import setup_persistent_monitoring
from ocs_ci.ocs.registry import (
    change_registry_backend_to_ocs,
    check_if_registry_stack_exists,
)
from ocs_ci.utility.deployment_openshift_logging import install_logging
from ocs_ci.ocs.monitoring import check_if_monitoring_stack_exists

log = logging.getLogger(__name__)

supported_app_workloads = ["pgsql", "couchbase", "cosbench"]
supported_ocp_workloads = ["logging", "monitoring", "registry"]

STAGE_4_PREFIX = "stage-4-cycle-"


class Longevity(object):
    """
    This class consists of the library functions and params required for Longevity testing
    """

    def __init__(self):
        """
        Initializer function
        """
        lcl = locals()
        self.tmp_path = pathlib.Path(ocsci_log_path())
        self.pvc_size = None
        self.pvc_count = None
        self.pod_count = None
        self.num_of_obcs = None

    def construct_stage_builder_bulk_pvc_creation_yaml(self, num_of_pvcs, pvc_size):
        """
        This function constructs pvc.yamls to create bulk of pvc's using kube_jobs.
        It constructs yamls with the specified number of PVCs of each supported
        type and access mode.

        Eg: num_of_pvcs = 30
        The function creates a total of 120 PVCs of below types and access modes
        RBD-Filesystemvolume  -> 30 RWO PVCs
        CEPHFS                -> 30 RWO PVCs, 30 RWX PVCs
        RBD-Block             -> 30 RWX PVCs

        Args:
            num_of_pvcs(int): Bulk PVC count
            pvc_size (str): size of all pvcs to be created with Gi suffix (e.g. 10Gi).
            If None, random size pvc will be created

        Returns:
             all_pvc_dict_list (list): List of all PVC.yaml dicts

        """
        all_pvc_dict_list = []
        # Construct bulk pvc creation yaml for ceph-rbd with access mode RWO
        log.info(
            "Constructing bulk pvc creation yaml for ceph-rbd with access mode RWO"
        )
        ceph_rbd_rwo_dict_list = construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=num_of_pvcs,
            access_mode=constants.ACCESS_MODE_RWO,
            sc_name=constants.CEPHBLOCKPOOL_SC,
            pvc_size=pvc_size,
        )
        all_pvc_dict_list.append(ceph_rbd_rwo_dict_list)
        # Construct bulk pvc creation yaml for ceph-rbd with access mode RWX
        log.info(
            "Constructing bulk pvc creation yaml for ceph-rbd with access mode RWX"
        )
        ceph_rbd_rwx_dict_list = construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=num_of_pvcs,
            access_mode=constants.ACCESS_MODE_RWX,
            sc_name=constants.CEPHBLOCKPOOL_SC,
            pvc_size=pvc_size,
        )
        all_pvc_dict_list.append(ceph_rbd_rwx_dict_list)
        # Construct bulk pvc creation yaml for cephfs with access mode RWO
        log.info("Constructing bulk pvc creation yaml for cephfs with access mode RWO")
        cephfs_rwo_dict_list = construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=num_of_pvcs,
            access_mode=constants.ACCESS_MODE_RWO,
            sc_name=constants.CEPHFILESYSTEM_SC,
            pvc_size=pvc_size,
        )
        all_pvc_dict_list.append(cephfs_rwo_dict_list)
        # Construct bulk pvc creation yaml for cephfs with access mode RWX
        log.info("Constructing bulk pvc creation yaml for cephfs with access mode RWX")
        cephfs_rwx_dict_list = construct_pvc_creation_yaml_bulk_for_kube_job(
            no_of_pvc=num_of_pvcs,
            access_mode=constants.ACCESS_MODE_RWX,
            sc_name=constants.CEPHFILESYSTEM_SC,
            pvc_size=pvc_size,
        )
        all_pvc_dict_list.append(cephfs_rwx_dict_list)

        return all_pvc_dict_list

    def construct_stage_builder_kube_job(
        self, obj_dict_list, namespace, kube_job_name="job_profile"
    ):
        """
        This function constructs kube object config file for the kube object

        Args:
            obj_dict_list (list): List of dictionaries with kube objects,
            the return value of construct_stage_builder_bulk_pvc_creation_yaml()
            namespace (str): Namespace where the object has to be deployed
            name (str): Name of this object config file

        Returns:
             pvc_dict_list (list): List of all PVC.yaml dicts

        """
        # Construct kube object config file for each kube object in the list
        kube_job_obj_list = []
        log.info("Constructing kube jobs ...")
        for i in range(0, len(obj_dict_list)):
            kube_job_obj = ObjectConfFile(
                name=f"{kube_job_name}-{i}",
                obj_dict_list=obj_dict_list[i],
                project=namespace,
                tmp_path=self.tmp_path,
            )
            kube_job_obj_list.append(kube_job_obj)

        return kube_job_obj_list

    def create_stage_builder_kube_job(self, kube_job_obj_list, namespace):
        """
        Create kube jobs

        Args:
            kube_job_list (list): List of kube jobs
            namespace (str): Namespace where the job has to be created

        """
        # Create kube jobs
        log.info("Creating kube jobs ...")
        for kube_job_obj in kube_job_obj_list:
            kube_job_obj.create(namespace=namespace)

    def get_resource_yaml_dict_from_kube_job_obj(
        self, kube_job_obj, namespace, resource_name="PVC"
    ):
        """
        Get the resource (PVC/POD) yaml dict from the kube job object

        Args:
            kube_job_obj (obj): Kube job object
            namespace (str): Namespace where the job is created

        Returns:
            res_yaml_dict (list): List of all resource yaml dicts

        """
        log.info(
            f"Get {resource_name} yaml dict from the kube job object: {kube_job_obj.name}"
        )
        get_kube_job_obj = kube_job_obj.get(namespace=namespace)
        res_yaml_dict = get_kube_job_obj["items"]

        return res_yaml_dict

    def validate_pvc_in_kube_job_reached_bound_state(
        self, kube_job_obj_list, namespace, pvc_count
    ):
        """
        Validate PVCs in the kube job list reached BOUND state

        Args:
            kube_job_obj_list (list): List of Kube job objects
            namespace (str): Namespace where the Kube job/PVCs are created
            pvc_count (int): Bulk PVC count; If not specified the count will be
            fetched from the kube job pvc yaml dict

        Returns:
            pvc_bound_list (list): List of all PVCs in Bound state

        Raises:
        AssertionError: If not all PVCs reached to Bound state

        """
        pvc_bound_list_of_list = []
        log.info("validate that all the pvcs in the kube job list reached BOUND state")
        for i in range(0, len(kube_job_obj_list)):
            pvc_count = (
                pvc_count
                if pvc_count
                else len(
                    self.get_pvc_yaml_dict_from_kube_job_obj(
                        kube_job_obj_list[i], namespace=namespace
                    )
                )
            )
            pvc_bound = check_all_pvc_reached_bound_state_in_kube_job(
                kube_job_obj=kube_job_obj_list[i],
                namespace=namespace,
                no_of_pvc=pvc_count,
            )
            pvc_bound_list_of_list.append(pvc_bound)
            log.info(
                f"Kube job : {kube_job_obj_list[i].name} -> {len(pvc_bound_list_of_list[i])} PVCs in BOUND state"
            )
        pvc_bound_list = [item for elem in pvc_bound_list_of_list for item in elem]
        log.info(f"All Kube jobs -> {len(pvc_bound_list)} PVCs in BOUND state")

        return pvc_bound_list

    def delete_stage_builder_kube_job(self, kube_job_obj_list, namespace):
        """
        Delete the stage builder kube jobs

        Args:
            kube_job_obj_list (list): List of kube jobs to delete
            namespace (str): Namespace where the job is created

        """
        # Delete stage builder kube jobs
        log.info("Deleting stage builder kube jobs ...")
        for kube_job_obj in kube_job_obj_list:
            kube_job_obj.delete(namespace=namespace)

    def create_stagebuilder_all_pvc_types(
        self, num_of_pvc, namespace, pvc_size, kube_job_name="all_pvc_job_profile"
    ):
        """
        Create stagebuilder PVCs with all supported PVC types and access modes

        Args:
            num_of_pvc(int): Bulk PVC count
            namespace (str): Namespace where the Kube job/PODs are to be created
            pvc_size (str): size of all pvcs to be created with Gi suffix (e.g. 10Gi).
            If None, random size pvc will be created

        Returns:
            pvc_bound_list (list): List of all PVCs in Bound state
            pvc_job_file_list (list): List of all PVC.yaml dicts

        Raises:
        AssertionError: If not all PVCs reached Bound state

        """
        log.info("Creating stagebuilder pods with all pvc types and access modes")
        # Construct bulk PVC creation yaml for kube job
        pvc_dict_list = self.construct_stage_builder_bulk_pvc_creation_yaml(
            num_of_pvcs=num_of_pvc, pvc_size=pvc_size
        )
        # Construct kube job with the pvc dict list
        pvc_job_file_list = self.construct_stage_builder_kube_job(
            obj_dict_list=pvc_dict_list,
            namespace=namespace,
            kube_job_name=kube_job_name,
        )
        # Create stage builder for PVC kube job
        self.create_stage_builder_kube_job(
            kube_job_obj_list=pvc_job_file_list, namespace=namespace
        )
        # Validate PVCs in kube job reached BOUND state
        pvc_bound_list = self.validate_pvc_in_kube_job_reached_bound_state(
            kube_job_obj_list=pvc_job_file_list,
            namespace=namespace,
            pvc_count=num_of_pvc,
        )

        return pvc_bound_list, pvc_job_file_list

    def construct_stage_builder_bulk_pod_creation_yaml(self, pvc_list, namespace):
        """
        This function constructs bulks pod.yamls to create bulk pods using kube_jobs.

        Args:
            pvc_list (list): List of PVCs
            namespace (str): Namespace where the resource has to be created

        Returns:
            pods_dict_list (list): List of all Pod.yaml dict list

        """
        pods_dict_list = []
        log.info("Constructing bulk pod creation yaml for the list of PVCs ")
        # Get pvc objs from namespace
        pvc_objs = get_pvc_objs(pvc_names=pvc_list, namespace=namespace)
        for pvc_obj in pvc_objs:
            if pvc_obj.backed_sc == constants.DEFAULT_STORAGECLASS_RBD:
                pod_dict = templating.load_yaml(constants.CSI_RBD_POD_YAML)
                if pvc_obj.get_pvc_vol_mode == "Block":
                    temp_dict = [
                        {
                            "devicePath": constants.RAW_BLOCK_DEVICE,
                            "name": pod_dict.get("spec")
                            .get("containers")[0]
                            .get("volumeMounts")[0]
                            .get("name"),
                        }
                    ]
                    del pod_dict["spec"]["containers"][0]["volumeMounts"]
                    pod_dict["spec"]["containers"][0]["volumeDevices"] = temp_dict
            elif pvc_obj.backed_sc == constants.DEFAULT_STORAGECLASS_CEPHFS:
                pod_dict = templating.load_yaml(constants.CSI_CEPHFS_POD_YAML)
            pod_name = create_unique_resource_name("test", "pod")
            pod_dict["metadata"]["name"] = pod_name
            pod_dict["metadata"]["namespace"] = namespace
            pod_dict["spec"]["volumes"][0]["persistentVolumeClaim"][
                "claimName"
            ] = pvc_obj.name
            pods_dict_list.append(pod_dict)

        return [pods_dict_list]

    def validate_pods_in_kube_job_reached_running_state(
        self, kube_job_obj, namespace, pod_count=None, timeout=30
    ):
        """
        Validate PODs in the kube job list reached RUNNING state

        Args:
            kube_job_obj_list (list): List of Kube job objects
            namespace (str): Namespace where the Kube job/PVCs are created
            pod_count (int): Bulk PODs count; If not specified the count will be
            fetched from the kube job pod yaml dict

        Returns:
            running_pods_list (list): List of all PODs in RUNNING state

        Raises:
        AssertionError: If not all PODs reached to Running state

        """
        log.info(
            "validate that all the pods in the kube job list reached RUNNING state"
        )
        pod_count = (
            pod_count
            if pod_count
            else len(
                self.get_resource_yaml_dict_from_kube_job_obj(
                    kube_job_obj, namespace=namespace, resource_name="POD"
                )
            )
        )
        running_pods_list = check_all_pod_reached_running_state_in_kube_job(
            kube_job_obj=kube_job_obj,
            namespace=namespace,
            no_of_pod=pod_count,
            timeout=timeout,
        )
        log.info(f"Total number of PODs in Running state: {len(running_pods_list)}")

        return running_pods_list

    def create_stagebuilder_pods_with_all_pvc_types(
        self, num_of_pvc, namespace, pvc_size
    ):
        """
        Create stagebuilder pods with all supported PVC types and access modes

        It first constructs bulk pvc.yamls with the specified number of PVCs of each
        supported type, access modes and then creates bulk pvc's using the kube_jobs.
        Once all the PVCs in the kube_jobs reaches BOUND state it then constructs bulk
        pod.yamls for each of these PVCs using kube_job.

        Eg: num_of_pvc = 30
        The function creates a total of 120 PVCs of below types and access modes
        RBD-Filesystemvolume  -> 30 RWO PVCs
        CEPHFS                -> 30 RWO PVCs, 30 RWX PVCs
        RBD-Block             -> 30 RWX PVCs
        and then creates pods for each of these PVCs. So, it will create 150 PODs

        Args:
            num_of_pvc(int): Bulk PVC count
            namespace (str): Namespace where the Kube job/PVCs/PODs are to be created
            pvc_size (str): size of all pvcs to be created with Gi suffix (e.g. 10Gi).
            If None, random size pvc will be created

        Returns:
             pod_pvc_job_file_list (list): List of all POD.yaml and PVC.yaml dicts

        """
        # Create stage builder PVCs of all supported types and access modes
        pvc_bound_list, pvc_job_file_list = self.create_stagebuilder_all_pvc_types(
            num_of_pvc=num_of_pvc,
            namespace=namespace,
            pvc_size=pvc_size,
            kube_job_name="all_pvc_for_pod_attach_job_profile",
        )
        # Construct bulk POD creation yaml for kube job
        pods_dict_list = self.construct_stage_builder_bulk_pod_creation_yaml(
            pvc_list=pvc_bound_list, namespace=namespace
        )
        # Construct kube job with the pods dict list
        pod_job_file_list = self.construct_stage_builder_kube_job(
            obj_dict_list=pods_dict_list,
            namespace=namespace,
            kube_job_name="all_pods_job_profile",
        )
        # Create stage builder for POD kube job
        self.create_stage_builder_kube_job(
            kube_job_obj_list=pod_job_file_list, namespace=namespace
        )
        # Validate PODs in kube job reached RUNNING state
        self.validate_pods_in_kube_job_reached_running_state(
            kube_job_obj=pod_job_file_list[0], namespace=namespace
        )
        pod_pvc_job_file_list = pod_job_file_list + pvc_job_file_list

        return pod_pvc_job_file_list

    def create_stagebuilder_obc(
        self, num_of_obcs, namespace, sc_name=constants.NOOBAA_SC
    ):
        """
        Create stagebuilder OBC

        It first constructs bulk obc.yamls with the specified number of OBCs and
        then creates bulk obc's using the kube_jobs.

        Args:
            namespace(str): Namespace uses to create bulk of obc
            sc_name (str): storage class name using for obc creation; By default uses
            Noobaa storage class 'openshift-storage.noobaa.io'
            num_of_obcs (str): Bulk obc count

        Returns:
             obc_job_file (list): List of all OBC.yaml dicts

        """
        log.info("Creating stagebuilder OBCs")
        # Construct bulk OBC creation yaml for kube job
        obc_dict_list = construct_obc_creation_yaml_bulk_for_kube_job(
            no_of_obc=num_of_obcs,
            sc_name=sc_name,
            namespace=namespace,
        )
        # Construct kube job with the OBCs dict list
        obc_job_file = self.construct_stage_builder_kube_job(
            obj_dict_list=[obc_dict_list],
            namespace=namespace,
            kube_job_name="obc_job_profile",
        )
        # Create stage builder for OBC kube job
        self.create_stage_builder_kube_job(
            kube_job_obj_list=obc_job_file, namespace=namespace
        )
        # Validate OBCs in kube job reached BOUND state
        self.validate_obcs_in_kube_job_reached_running_state(
            kube_job_obj=obc_job_file[0],
            namespace=namespace,
            num_of_obc=num_of_obcs,
        )

        return obc_job_file

    def validate_obcs_in_kube_job_reached_running_state(
        self, kube_job_obj, namespace, num_of_obc
    ):
        """
        Validate that OBCs in the kube job list reached BOUND state

        Args:
            kube_job_obj (obj): Kube Job Object
            namespace (str): Namespace of OBC's created
            num_of_obc (int): Bulk OBCs count; If not specified the count will be
            fetched from the kube job obc yaml dict

        Returns:
        obc_bound_list (list): List of all OBCs which is in Bound state.

        Raises:
        AssertionError: If not all OBC reached to Bound state

        """
        log.info("validate that all the OBCs in the kube job list reached BOUND state")
        num_of_obc = (
            num_of_obc
            if num_of_obc
            else len(
                self.get_resource_yaml_dict_from_kube_job_obj(
                    kube_job_obj, namespace=namespace, resource_name="OBC"
                )
            )
        )
        # Check all the OBCs to reach Bound state
        obc_bound_list = check_all_obc_reached_bound_state_in_kube_job(
            kube_job_obj=kube_job_obj,
            namespace=namespace,
            no_of_obc=num_of_obc,
        )
        log.info(f"Number of OBCs in Bound state {len(obc_bound_list)}")

        return obc_bound_list

    def create_restore_verify_snapshots(
        self,
        multi_snapshot_factory,
        snapshot_restore_factory,
        pod_factory,
        pvc_objs,
        namespace,
        file_name,
    ):
        """
        Creates snapshots from each PVC in the provided list of PVCs,
        Restores new PVCs out of the created snapshots
        and
        Verifies data integrity by checking the existence and md5sum of file in the restored PVC.

        Args:
            multi_snapshot_factory : Fixture to create a VolumeSnapshot of each PVC in the provided list of PVCs.
            snapshot_restore_factory : Fixture to create a new PVCs out of the VolumeSnapshot provided.
            pod_factory : Fixture to create new PODs.
            pvc_objs (list) : List of PVC objects for which snapshots are to be created.
            namespace (str) : Namespace in which the PVCs are created.
            file_name (str) : Name of the file on which FIO is performed.

        Returns:
            tuple: A tuple of size 2 containing a list of restored PVC objects and a list of the pods attached to the
                    restored PVCs, respectively.

        """
        # Create Snapshots
        log.info("Started creation of snapshots of the PVCs.")
        snapshots = multi_snapshot_factory(
            pvc_obj=pvc_objs, snapshot_name_suffix=namespace
        )
        log.info(
            "Created snapshots from all the PVCs and snapshots are in Ready state."
        )

        # Restore Snapshots
        log.info("Started restoration of the snapshots created.")
        restored_pvc_objs = list()
        for snapshot_no in range(len(snapshots)):
            restored_pvc_objs.append(
                snapshot_restore_factory(
                    snapshot_obj=snapshots[snapshot_no],
                    volume_mode=pvc_objs[snapshot_no].get_pvc_vol_mode,
                    access_mode=pvc_objs[snapshot_no].get_pvc_access_mode,
                    timeout=600,
                )
            )
        log.info("Restoration complete - Created new PVCs from all the snapshots.")

        # Attach PODs to restored PVCs
        restored_pod_objs = list()
        for restored_pvc_obj in restored_pvc_objs:
            if restored_pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
                restored_pod_objs.append(
                    pod_factory(
                        pvc=restored_pvc_obj,
                        raw_block_pv=True,
                        status=constants.STATUS_RUNNING,
                        pod_dict_path=constants.CSI_RBD_RAW_BLOCK_POD_YAML,
                    )
                )
            else:
                restored_pod_objs.append(
                    pod_factory(pvc=restored_pvc_obj, status=constants.STATUS_RUNNING)
                )

        # Verify that the fio exists and md5sum matches
        pod.verify_data_integrity_for_multi_pvc_objs(
            restored_pod_objs, pvc_objs, file_name
        )

        return restored_pvc_objs, restored_pod_objs

    def expand_verify_pvcs(self, pvc_objs, pod_objs, pvc_size_new, file_name, fio_size):
        """
        Expands size of each PVC in the provided list of PVCs,
        Verifies data integrity by checking the existence and md5sum of file in the expanded PVC
        and
        Runs FIO on expanded PVCs and verifies results.

        Args:
            pvc_objs (list) : List of PVC objects which are to be expanded.
            pod_objs (list) : List of POD objects attached to the PVCs.
            pvc_size_new (int) : Size of the expanded PVC in GB.
            file_name (str) : Name of the file on which FIO is performed.
            fio_size (int) : Size in MB of FIO.

        """
        # Expand original PVCs
        log.info("Started expansion of the PVCs.")
        for pvc_obj in pvc_objs:
            log.info(f"Expanding size of PVC {pvc_obj.name} to {pvc_size_new}G")
            pvc_obj.resize_pvc(pvc_size_new, True)
        log.info("Successfully expanded the PVCs.")

        # Verify that the fio exists and md5sum matches
        for pod_no in range(len(pod_objs)):
            pod_obj = pod_objs[pod_no]
            if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
                pod.verify_data_integrity_after_expansion_for_block_pvc(
                    pod_obj, pvc_objs[pod_no], fio_size
                )
            else:
                pod.verify_data_integrity(pod_obj, file_name, pvc_objs[pod_no].md5sum)

        # Run IO to utilize 50% of volume
        log.info("Run IO on all pods to utilise 50% of the expanded PVC used space")
        expanded_file_name = "fio_50"
        for pod_obj in pod_objs:
            log.info(f"Running IO on pod {pod_obj.name}")
            log.info(f"File created during IO {expanded_file_name}")
            fio_size = int(0.50 * pvc_size_new * 1000)
            storage_type = (
                "block"
                if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK
                else "fs"
            )
            pod_obj.wl_setup_done = True
            pod_obj.wl_obj = _workload.WorkLoad(
                "test_workload_fio",
                pod_obj.get_storage_path(storage_type),
                "fio",
                storage_type,
                pod_obj,
                1,
            )
            pod_obj.run_io(
                storage_type=storage_type,
                size=f"{fio_size}M",
                runtime=20,
                fio_filename=expanded_file_name,
                end_fsync=1,
            )

        log.info("Started IO on all pods to utilise 50% of PVCs")

        for pod_obj in pod_objs:
            # Wait for IO to finish
            pod_obj.get_fio_results(3600)
            log.info(f"IO finished on pod {pod_obj.name}")
            is_block = (
                True
                if pod_obj.pvc.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK
                else False
            )
            expanded_file_name_pod = (
                expanded_file_name
                if not is_block
                else pod_obj.get_storage_path(storage_type="block")
            )

            # Verify presence of the file
            expanded_file_path = (
                expanded_file_name_pod
                if is_block
                else pod.get_file_path(pod_obj, expanded_file_name_pod)
            )
            log.info(f"Actual file path on the pod {expanded_file_path}")
            assert pod.check_file_existence(
                pod_obj, expanded_file_path
            ), f"File {expanded_file_name_pod} does not exist"
            log.info(f"File {expanded_file_name_pod} exists in {pod_obj.name}")

    def stage_0(
        self, num_of_pvc, num_of_obc, namespace, pvc_size, ignore_teardown=True
    ):
        """
        This function creates the initial soft configuration required to start
        longevity testing

        Args:
            num_of_pvc (int): Bulk PVC count
            num_of_obc (int): Bulk OBC count
            namespace (str): Namespace where the Kube job/PVCs/PODsOBCs are to be created
            pvc_size (str): size of all pvcs to be created with Gi suffix (e.g. 10Gi).
            If None, random size pvc will be created

        Returns:
             kube_job_file_list (list): List of all PVC, POD, OBC yaml dicts

        """
        # Create bulk PVCs of all types
        _, pvc_job_file_list = self.create_stagebuilder_all_pvc_types(
            num_of_pvc=num_of_pvc, namespace=namespace, pvc_size=pvc_size
        )
        # Create bulk PVCs of all types and attach each PVC to a Pod
        pod_pvc_job_file_list = self.create_stagebuilder_pods_with_all_pvc_types(
            num_of_pvc=num_of_pvc, namespace=namespace, pvc_size=pvc_size
        )
        # Create bulk OBCs
        obc_job_file = self.create_stagebuilder_obc(
            namespace=namespace, num_of_obcs=num_of_obc
        )
        kube_job_file_list = pvc_job_file_list + pod_pvc_job_file_list + obc_job_file
        # For longevity Stage-0 we would want these resources to be keep running forever
        # Hence, ignore deletion of created resources
        if ignore_teardown:
            self.delete_stage_builder_kube_job(
                kube_job_obj_list=kube_job_file_list, namespace=namespace
            )

        return kube_job_file_list

    def stage_4(
        self,
        project_factory,
        multi_pvc_factory,
        pod_factory,
        multi_pvc_clone_factory,
        multi_snapshot_factory,
        snapshot_restore_factory,
        teardown_factory,
        num_of_pvcs=30,
        pvc_size=2,
        run_time=1440,
        pvc_size_new=4,
    ):
        """
        Function to handle automation of Longevity Stage 4 i.e.
            1. Creation / Deletion of PODs, PVCs of different types + fill data upto 25% of mount point space.
            2. Creation / Deletion of Clones of the given PVCs.
            3. Creation / Deletion of VolumeSnapshots of the given PVCs.
            4. Restore the created VolumeSnapshots into a new set of PVCs.
            5. Expansion of size of the original PVCs.


        Args:
            project_factory : Fixture to create a new Project.
            multi_pvc_factory : Fixture to create multiple PVCs of different access modes and interface types.
            pod_factory : Fixture to create new PODs.
            multi_pvc_clone_factory : Fixture to create a clone from each PVC in the provided list of PVCs.
            multi_snapshot_factory : Fixture to create a VolumeSnapshot of each PVC in the provided list of PVCs.
            snapshot_restore_factory : Fixture to create a new PVCs out of the VolumeSnapshot provided.
            teardown_factory : Fixture to tear down a resource that was created during the test.
            num_of_pvcs (int) : Total Number of PVCs we want to create for each operation (clone, snapshot, expand).
            pvc_size (int) : Size of each PVC in GB.
            run_time (int) : Total Run Time in minutes.
            pvc_size_new (int) : Size of the expanded PVC in GB.

        """
        end_time = datetime.now() + timedelta(minutes=run_time)
        cycle_no = 0

        while datetime.now() < end_time:
            cycle_no += 1
            log.info(f"#################[STARTING CYCLE:{cycle_no}]#################")

            for concurrent in (False, True):
                current_ops = (
                    "CONCURRENT-OPERATION" if concurrent else "SEQUENTIAL-OPERATION"
                )
                log.info(f"#################[{current_ops}]#################")

                namespace = f"{STAGE_4_PREFIX}{cycle_no}-{current_ops.lower()}"
                project = project_factory(project_name=namespace)
                executor = ThreadPoolExecutor(max_workers=1)
                operation_pvc_dict = dict()
                operation_pod_dict = dict()
                fio_size = int(0.25 * pvc_size * 1000)
                file_name = "fio_25"

                for operation in ("clone", "snapshot", "expand"):
                    pvc_objs = list()
                    for interface in (
                        constants.CEPHFILESYSTEM,
                        constants.CEPHBLOCKPOOL,
                    ):
                        if interface == constants.CEPHFILESYSTEM:
                            access_modes = [
                                constants.ACCESS_MODE_RWO,
                                constants.ACCESS_MODE_RWX,
                            ]
                            num_of_pvc = num_of_pvcs // 2
                        else:
                            access_modes = [
                                constants.ACCESS_MODE_RWO,
                                constants.ACCESS_MODE_RWO
                                + "-"
                                + constants.VOLUME_MODE_BLOCK,
                                constants.ACCESS_MODE_RWX
                                + "-"
                                + constants.VOLUME_MODE_BLOCK,
                            ]
                            num_of_pvc = num_of_pvcs - num_of_pvcs // 2

                        # Create PVCs
                        if num_of_pvc > 0:
                            pvc_objs_tmp = multi_pvc_factory(
                                interface=interface,
                                size=pvc_size,
                                project=project,
                                access_modes=access_modes,
                                status=constants.STATUS_BOUND,
                                num_of_pvc=num_of_pvc,
                                wait_each=True,
                            )
                            log.info(
                                f"PVCs of interface:{interface} for operation:{operation} were successfully created."
                            )
                            pvc_objs.extend(pvc_objs_tmp)
                        else:
                            log.error(
                                f"Num of PVCs of interface - {interface} = {num_of_pvc}. So no PVCs created."
                            )

                    # Create PODs
                    pod_objs = list()
                    for pvc_obj in pvc_objs:
                        if pvc_obj.get_pvc_vol_mode == constants.VOLUME_MODE_BLOCK:
                            pod_objs.append(
                                pod_factory(
                                    pvc=pvc_obj,
                                    raw_block_pv=True,
                                    status=constants.STATUS_RUNNING,
                                    pod_dict_path=constants.PERF_BLOCK_POD_YAML,
                                )
                            )
                        else:
                            pod_objs.append(
                                pod_factory(
                                    pvc=pvc_obj,
                                    status=constants.STATUS_RUNNING,
                                    pod_dict_path=constants.PERF_POD_YAML,
                                )
                            )

                    log.info(
                        f"PODs for operation:{operation} were successfully created."
                    )

                    # Run IO to utilize 25% of volume
                    log.info("Run IO on all pods to utilise 25% of PVC used space")
                    for pod_obj in pod_objs:
                        log.info(f"Running IO on pod {pod_obj.name}")
                        log.info(f"File created during IO {file_name}")
                        storage_type = (
                            "block"
                            if pod_obj.pvc.get_pvc_vol_mode
                            == constants.VOLUME_MODE_BLOCK
                            else "fs"
                        )
                        pod_obj.wl_setup_done = True
                        pod_obj.wl_obj = _workload.WorkLoad(
                            "test_workload_fio",
                            pod_obj.get_storage_path(storage_type),
                            "fio",
                            storage_type,
                            pod_obj,
                            1,
                        )
                        pod_obj.run_io(
                            storage_type=storage_type,
                            size=f"{fio_size}M",
                            runtime=20,
                            fio_filename=file_name,
                            end_fsync=1,
                        )

                    log.info(
                        "Waiting for IO to complete on all pods to utilise 25% of PVC used space"
                    )

                    for pod_obj in pod_objs:
                        # Wait for IO to finish
                        pod_obj.get_fio_results(3600)
                        log.info(f"IO finished on pod {pod_obj.name}")
                        is_block = (
                            True
                            if pod_obj.pvc.get_pvc_vol_mode
                            == constants.VOLUME_MODE_BLOCK
                            else False
                        )
                        file_name_pod = (
                            file_name
                            if not is_block
                            else pod_obj.get_storage_path(storage_type="block")
                        )
                        # Verify presence of the file
                        file_path = (
                            file_name_pod
                            if is_block
                            else pod.get_file_path(pod_obj, file_name_pod)
                        )
                        log.info(f"Actual file path on the pod {file_path}")
                        assert pod.check_file_existence(
                            pod_obj, file_path
                        ), f"File {file_name_pod} does not exist"
                        log.info(f"File {file_name_pod} exists in {pod_obj.name}")

                        if operation == "expand" and is_block:
                            # Read IO from block PVCs using dd and calculate md5sum.
                            # This dd command reads the data from the device, writes it to
                            # stdout, and reads md5sum from stdin.
                            pod_obj.pvc.md5sum = pod_obj.exec_sh_cmd_on_pod(
                                command=(
                                    f"dd iflag=direct if={file_path} bs=10M "
                                    f"count={fio_size // 10} | md5sum"
                                )
                            )
                            log.info(f"md5sum of {file_name_pod}: {pod_obj.pvc.md5sum}")
                        else:
                            # Calculate md5sum of the file
                            pod_obj.pvc.md5sum = pod.cal_md5sum(pod_obj, file_name_pod)

                    operation_pvc_dict[operation] = pvc_objs
                    operation_pod_dict[operation] = pod_objs
                    log.info(
                        f"PVCs and PODs for operation:{operation} were successfully created + 25% FIO."
                    )

                if not concurrent:
                    cloned_pvcs, cloned_pod_objs = multi_pvc_clone_factory(
                        pvc_obj=operation_pvc_dict["clone"],
                        wait_each=True,
                        attach_pods=True,
                        verify_data_integrity=True,
                        file_name=file_name,
                    )

                    (
                        restored_pvc_objs,
                        restored_pod_objs,
                    ) = self.create_restore_verify_snapshots(
                        multi_snapshot_factory,
                        snapshot_restore_factory,
                        pod_factory,
                        operation_pvc_dict["snapshot"],
                        namespace,
                        file_name,
                    )

                    self.expand_verify_pvcs(
                        operation_pvc_dict["expand"],
                        operation_pod_dict["expand"],
                        pvc_size_new,
                        file_name,
                        fio_size,
                    )

                else:
                    stage4_executor1 = ThreadPoolExecutor(max_workers=1)
                    stage4_thread1 = stage4_executor1.submit(
                        multi_pvc_clone_factory,
                        pvc_obj=operation_pvc_dict["clone"],
                        wait_each=True,
                        attach_pods=True,
                        verify_data_integrity=True,
                        file_name=file_name,
                    )

                    stage4_executor2 = ThreadPoolExecutor(max_workers=1)
                    stage4_thread2 = stage4_executor2.submit(
                        self.create_restore_verify_snapshots,
                        multi_snapshot_factory,
                        snapshot_restore_factory,
                        pod_factory,
                        operation_pvc_dict["snapshot"],
                        namespace,
                        file_name,
                    )

                    stage4_executor3 = ThreadPoolExecutor(max_workers=1)
                    stage4_thread3 = stage4_executor3.submit(
                        self.expand_verify_pvcs,
                        operation_pvc_dict["expand"],
                        operation_pod_dict["expand"],
                        pvc_size_new,
                        file_name,
                        fio_size,
                    )

                    cloned_pvcs, cloned_pod_objs = stage4_thread1.result()
                    restored_pvc_objs, restored_pod_objs = stage4_thread2.result()
                    stage4_thread3.result()

                total_pvcs = (
                    operation_pvc_dict["clone"]
                    + operation_pvc_dict["snapshot"]
                    + operation_pvc_dict["expand"]
                    + cloned_pvcs
                    + restored_pvc_objs
                )
                total_pods = (
                    operation_pod_dict["clone"]
                    + operation_pod_dict["snapshot"]
                    + operation_pod_dict["expand"]
                    + cloned_pod_objs
                    + restored_pod_objs
                )

                # PVC and PV Teardown
                pv_objs = list()
                for pvc_obj in total_pvcs:
                    teardown_factory(pvc_obj)
                    pv_objs.append(pvc_obj.backed_pv_obj.name)
                    teardown_factory(pvc_obj.backed_pv_obj)

                # POD Teardown
                for pod_obj in total_pods:
                    teardown_factory(pod_obj)

                # Delete PODs
                pod_delete = executor.submit(delete_pods, total_pods)
                pod_delete.result()

                log.info("Verified: Pods are deleted.")

                # Delete PVCs
                pvc_delete = executor.submit(delete_pvcs, total_pvcs)
                res = pvc_delete.result()
                if not res:
                    raise ex.UnexpectedBehaviour("Deletion of PVCs failed")
                log.info("PVC deletion was successful.")

            log.info(f"#################[ENDING CYCLE:{cycle_no}]#################")


def start_app_workload(
    request, workloads_list=None, run_time=10, run_in_bg=True, delay=600
):
    """
    This function reads the list of app workloads to run and
    starts running those iterating over the workload in the list for a
    specified duration

    Usage:
    start_app_workload(workloads_list=['pgsql', 'couchbase', 'cosbench'], run_time=60,
    run_in_bg=True)

    Args:
        workloads_list (list): The list of app workloads to run
        run_time (int): The amount of time the workloads should run (in minutes)
        run_in_bg (bool): Runs the workload in background starting a thread
        delay (int): Delay in seconds before starting the next cycle

    Raise:
        UnsupportedWorkloadError: When the workload is not found in the supported_app_workloads list

    """
    threads = []
    workloads = []

    def factory(
        workloads_list=workloads_list,
        run_time=run_time,
        run_in_bg=run_in_bg,
        delay=delay,
    ):

        log.info(f"workloads_list: {workloads_list}")
        log.info(f"supported app workloads list: {supported_app_workloads}")
        support_check = all(item in supported_app_workloads for item in workloads_list)
        if not support_check:
            raise UnsupportedWorkloadError("Found Unsupported app workloads list")
        log.info("APP Workloads support check is Successful")
        cycle_count = 1
        end_time = datetime.now() + timedelta(minutes=run_time)
        while datetime.now() < end_time:
            log.info(f"Current time is {datetime.now()}")
            log.info(f"End time is {end_time}")
            log.info(
                f"##############[STARTING CYCLE:{cycle_count}]####################"
            )
            for workload in workloads_list:
                if workload == "pgsql":
                    pgsql = Postgresql()
                    workloads.append(pgsql)
                    if run_in_bg:
                        log.info(f"Starting {workload} workload in background")
                        executor = ThreadPoolExecutor(max_workers=1)
                        thread1 = executor.submit(pgsql.pgsql_full)
                        threads.append(thread1)
                        continue
                    else:
                        log.info(f"Starting {workload} workload in foreground")
                        pgsql.pgsql_full()
                        pgsql.cleanup()
                elif workload == "couchbase":
                    cb = CouchBase()
                    workloads.append(cb)
                    if run_in_bg:
                        log.info(f"Starting {workload} workload in background")
                        executor1 = ThreadPoolExecutor(max_workers=1)
                        thread2 = executor1.submit(cb.couchbase_full)
                        threads.append(thread2)
                        continue
                    else:
                        log.info(f"Starting {workload} workload in foreground")
                        cb.couchbase_full()
                        cb.cleanup()
                elif workload == "cosbench":
                    cos = Cosbench()
                    workloads.append(cos)
                    if run_in_bg:
                        log.info(f"Starting {workload} workload in background")
                        executor2 = ThreadPoolExecutor(max_workers=1)
                        thread3 = executor2.submit(cos.cosbench_full)
                        threads.append(thread3)
                        continue
                    else:
                        log.info(f"Starting {workload} workload in foreground")
                        cos.cosbench_full()
                        cos.cleanup()
            if run_in_bg:
                for t in threads:
                    t.result()
                cleanup()
            threads.clear()
            workloads.clear()
            log.info(
                f"##############[COMPLETED CYCLE:{cycle_count}]####################"
            )
            cycle_count += 1
            log.info(
                f"###########[SLEEPING FOR {delay} SECONDS BEFORE STARTING NEXT CYCLE]###########"
            )

    def cleanup():
        for workload in workloads:
            try:
                workload.cleanup()
            except CommandFailed as ef:
                log.info("Workload already cleaned")
                if "does not exist on " not in str(ef):
                    raise ef

    request.addfinalizer(cleanup)
    return factory


def start_ocp_workload(workloads_list, run_in_bg=True):
    """
    This function reads the list of OCP workloads to run and
     starts running those iterating over the elements in the list.

     Usage:
     start_ocp_workload(workloads_list=['logging','registry'], run_in_bg=True)

     Args:
         workloads_list (list): The list of ocp workloads to run
         run_in_bg (bool): Runs the workload in background starting a thread

    Raise:
        UnsupportedWorkloadError: When the workload is not found in the supported_ocp_workloads list

    """
    threads = []
    log.info(f"workloads_list: {workloads_list}")
    log.info(f"supported ocp workloads list: {supported_ocp_workloads}")
    support_check = all(item in supported_ocp_workloads for item in workloads_list)
    if not support_check:
        raise UnsupportedWorkloadError("Found Unsupported ocp workloads list")
    log.info("OCP Workloads support check is Successful")

    for workload in workloads_list:
        if workload == "monitoring":
            if not check_if_monitoring_stack_exists():
                if run_in_bg:
                    log.info(f"Starting {workload} workload in background")
                    executor = ThreadPoolExecutor(max_workers=1)
                    thread1 = executor.submit(setup_persistent_monitoring)
                    threads.append(thread1)
                    continue
                else:
                    log.info(f"Starting {workload} workload in foreground")
                    setup_persistent_monitoring()
        elif workload == "registry":
            if not check_if_registry_stack_exists():
                if run_in_bg:
                    log.info(f"Starting {workload} workload in background")
                    executor2 = ThreadPoolExecutor(max_workers=1)
                    thread2 = executor2.submit(change_registry_backend_to_ocs)
                    threads.append(thread2)
                    continue
                else:
                    log.info(f"Starting {workload} workload in foreground")
                    change_registry_backend_to_ocs()
        elif workload == "logging":
            if run_in_bg:
                log.info(f"Starting {workload} workload in background")
                executor3 = ThreadPoolExecutor(max_workers=1)
                thread3 = executor3.submit(install_logging)
                threads.append(thread3)
                continue
            else:
                log.info(f"Starting {workload} workload in foreground")
                install_logging()
    if run_in_bg:
        for t in threads:
            t.result()
