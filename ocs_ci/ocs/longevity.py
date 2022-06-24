import time
import logging
import pathlib

from datetime import datetime, timedelta

from ocs_ci.ocs import constants
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
from ocs_ci.ocs.resources.pvc import get_pvc_objs
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

    def stage_2(
        self,
        multi_pvc_pod_lifecycle_factory,
        multi_obc_lifecycle_factory,
        num_of_pvcs=100,
        pvc_size=2,
        num_of_obcs=20,
        run_time=1440,
        measure=True,
        delay=600,
    ):
        """
        Function to handle automation of Longevity Stage 2 Sequential Steps i.e. Creation / Deletion of PVCs, PODs and
        OBCs and measurement of creation / deletion times of the mentioned resources.

        Args:
            multi_pvc_pod_lifecycle_factory : Fixture to create/delete multiple pvcs and pods and
                                                measure pvc creation/deletion time and pod attach time.
            multi_obc_lifecycle_factory : Fixture to create/delete multiple obcs and
                                            measure their creation/deletion time.
            num_of_pvcs (int) : Total Number of PVCs / PODs we want to create.
            pvc_size (int) : Size of each PVC in GB.
            num_of_obcs (int) : Number of OBCs we want to create of each type. (Total OBCs = num_of_obcs * 5)
            run_time (int) : Total Run Time in minutes.
            measure (bool) : True if we want to measure the performance metrics, False otherwise.
            delay (int) : Delay time (in seconds) between sequential and bulk operations as well as between cycles.

        """
        end_time = datetime.now() + timedelta(minutes=run_time)
        cycle_no = 0

        while datetime.now() < end_time:
            cycle_no += 1
            log.info(f"#################[STARTING CYCLE:{cycle_no}]#################")

            for bulk in (False, True):
                current_ops = "BULK-OPERATION" if bulk else "SEQUENTIAL-OPERATION"
                log.info(f"#################[{current_ops}]#################")
                multi_pvc_pod_lifecycle_factory(
                    num_of_pvcs=num_of_pvcs,
                    pvc_size=pvc_size,
                    bulk=bulk,
                    namespace=f"stage-2-cycle-{cycle_no}-{current_ops.lower()}",
                    measure=measure,
                )
                multi_obc_lifecycle_factory(
                    num_of_obcs=num_of_obcs, bulk=bulk, measure=False
                )

                # Delay between Sequential and Bulk Operations
                if not bulk:
                    log.info(
                        f"#################[WAITING FOR {delay} SECONDS AFTER {current_ops}.]#################"
                    )
                    time.sleep(delay)

            log.info(f"#################[ENDING CYCLE:{cycle_no}]#################")

            log.info(
                f"#################[WAITING FOR {delay} SECONDS AFTER {cycle_no} CYCLE.]#################"
            )
            time.sleep(delay)


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
