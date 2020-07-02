import logging
import threading
import random

from tests import helpers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.framework import config
from ocs_ci.ocs.resources import pod, pvc
from ocs_ci.ocs import constants, cluster, machine, node
from ocs_ci.ocs.exceptions import (
    UnavailableResourceException, UnexpectedBehaviour, CephHealthException,
    UnsupportedPlatformError
)

logger = logging.getLogger(__name__)


class FioPodScale(object):
    """
    FioPodScale Class with required scale library functions and params
    """
    def __init__(
        self, kind='deploymentconfig', pod_dict_path=constants.FEDORA_DC_YAML,
        node_selector=constants.SCALE_NODE_SELECTOR
    ):
        """
        Initializer function

        Args:
            kind (str): Kind of service POD or DeploymentConfig
            pod_dict_path (yaml): Pod yaml
            node_selector (dict): Pods will be created in this node_selector
            Example, {'nodetype': 'app-pod'}

        """
        self._kind = kind
        self._pod_dict_path = pod_dict_path
        self._node_selector = node_selector
        self._set_dc_deployment()
        self.namespace_list = list()

    @property
    def kind(self):
        return self._kind

    @property
    def pod_dict_path(self):
        return self._pod_dict_path

    @property
    def node_selector(self):
        return self._node_selector

    def _set_dc_deployment(self):
        """
        Set dc_deployment True or False based on Kind
        """
        self.dc_deployment = True if self.kind == 'deploymentconfig' else False

    def create_and_set_namespace(self):
        """
        Create and set namespace for the pods to be created
        Create sa_name if Kind if DeploymentConfig
        """
        self.namespace_list.append(helpers.create_project())
        self.namespace = self.namespace_list[-1].namespace
        if self.dc_deployment:
            self.sa_name = helpers.create_serviceaccount(self.namespace)
            self.sa_name = self.sa_name.name
            helpers.add_scc_policy(
                sa_name=self.sa_name, namespace=self.namespace
            )
        else:
            self.sa_name = None

    def create_multi_pvc_pod(
        self, pods_per_iter=5, io_runtime=3600, start_io=False
    ):
        """
        Function to create PVC of different type and attach them to PODs and start IO.

        Args:
            pods_per_iter (int): Number of PVC-POD to be created per PVC type
            Example, If 2 then 8 PVC+POD will be created with 2 each of 4 PVC types
            io_runtime (sec): Fio run time in seconds
            start_io (bool): If True start IO else don't

        Returns:
            pod_objs (obj): Objs of all the PODs created
            pvc_objs (obj): Objs of all the PVCs created

        """
        rbd_sc = helpers.default_storage_class(constants.CEPHBLOCKPOOL)
        cephfs_sc = helpers.default_storage_class(constants.CEPHFILESYSTEM)
        pvc_size = f"{random.randrange(15, 105, 5)}Gi"
        fio_size = get_size_based_on_cls_usage()
        fio_rate = get_rate_based_on_cls_iops()
        logging.info(f"Create {pods_per_iter * 4} PVCs and PODs")
        # Create PVCs
        cephfs_pvcs = helpers.create_multiple_pvc_parallel(
            sc_obj=cephfs_sc, namespace=self.namespace, number_of_pvc=pods_per_iter,
            size=pvc_size, access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        rbd_pvcs = helpers.create_multiple_pvc_parallel(
            sc_obj=rbd_sc, namespace=self.namespace, number_of_pvc=pods_per_iter,
            size=pvc_size, access_modes=[constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]
        )
        # Appending all the pvc_obj and pod_obj to list
        pvc_objs, pod_objs = ([] for i in range(2))
        pvc_objs.extend(cephfs_pvcs + rbd_pvcs)

        # Create pods with above pvc list
        cephfs_pods = helpers.create_pods_parallel(
            cephfs_pvcs, self.namespace, constants.CEPHFS_INTERFACE,
            pod_dict_path=self.pod_dict_path, sa_name=self.sa_name,
            dc_deployment=self.dc_deployment, node_selector=self.node_selector
        )
        rbd_rwo_pvc, rbd_rwx_pvc = ([] for i in range(2))
        for pvc_obj in rbd_pvcs:
            if pvc_obj.get_pvc_access_mode == constants.ACCESS_MODE_RWX:
                rbd_rwx_pvc.append(pvc_obj)
            else:
                rbd_rwo_pvc.append(pvc_obj)
        rbd_rwo_pods = helpers.create_pods_parallel(
            rbd_rwo_pvc, self.namespace, constants.CEPHBLOCKPOOL,
            pod_dict_path=self.pod_dict_path, sa_name=self.sa_name,
            dc_deployment=self.dc_deployment, node_selector=self.node_selector
        )
        rbd_rwx_pods = helpers.create_pods_parallel(
            rbd_rwx_pvc, self.namespace, constants.CEPHBLOCKPOOL,
            pod_dict_path=self.pod_dict_path, sa_name=self.sa_name,
            dc_deployment=self.dc_deployment, raw_block_pv=True,
            node_selector=self.node_selector
        )
        temp_pod_objs = list()
        temp_pod_objs.extend(cephfs_pods + rbd_rwo_pods)

        # Appending all the pod_obj to list
        pod_objs.extend(temp_pod_objs + rbd_rwx_pods)

        # Start IO
        import time
        if start_io:
            threads = list()
            for pod_obj in temp_pod_objs:
                process = threading.Thread(
                    target=pod_obj.run_io, kwargs={
                        'storage_type': 'fs', 'size': fio_size,
                        'runtime': io_runtime, 'rate': fio_rate
                    }
                )
                process.start()
                threads.append(process)
                time.sleep(30)
            for pod_obj in rbd_rwx_pods:
                process = threading.Thread(
                    target=pod_obj.run_io, kwargs={
                        'storage_type': 'block', 'size': fio_size,
                        'runtime': io_runtime, 'rate': fio_rate
                    }
                )
                process.start()
                threads.append(process)
                time.sleep(30)
            for process in threads:
                process.join()

        return pod_objs, pvc_objs

    def create_scale_pods(
        self, scale_count=1500, pods_per_iter=5, io_runtime=None, start_io=None
    ):
        """
        Main Function with scale pod creation flow and checks to add nodes.
        For other platforms will not be considering the instance_type param

        Args:
            scale_count (int): Scale pod+pvc count
            io_runtime (sec): Fio run time in seconds
            start_io (bool): If True start IO else don't
            pods_per_iter (int): Number of PVC-POD to be created per PVC type
            Example, If 5 then 20 PVC+POD will be created with 5 each of 4 PVC types
            Test value in-between 5-15

        """
        self.ms_name, all_pod_obj = ([] for i in range(2))
        if not 5 <= pods_per_iter <= 15:
            raise UnexpectedBehaviour("Pods_per_iter value should be in-between 5-15")

        # Check for expected worker count
        expected_worker_count = get_expected_worker_count(scale_count)
        if check_and_add_enough_worker(expected_worker_count):
            if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
                for obj in machine.get_machineset_objs():
                    if 'app' in obj.name:
                        self.ms_name.append(obj.name)
            else:
                self.ms_name = []

        # Create namespace
        self.create_and_set_namespace()

        # Continue to iterate till the scale pvc limit is reached
        while True:
            if scale_count <= len(all_pod_obj):
                logger.info(f"Scaled {scale_count} pvc and pods")

                if cluster.validate_pg_balancer():
                    logging.info("OSD consumption and PG distribution is good to continue")
                else:
                    raise UnexpectedBehaviour("Unequal PG distribution to OSDs")

                break
            else:
                logger.info(f"Scaled PVC and POD count {len(all_pod_obj)}")
                pod_obj, pvc_obj = self.create_multi_pvc_pod(
                    pods_per_iter, io_runtime, start_io
                )
                all_pod_obj.extend(pod_obj)
                try:
                    # Check enough resources available in the dedicated app workers
                    check_enough_resource_available_in_workers(self.ms_name, self.pod_dict_path)

                    # Check for ceph cluster OSD utilization
                    if not cluster.validate_osd_utilization(osd_used=75):
                        logging.info("Cluster OSD utilization is below 75%")
                    elif not cluster.validate_osd_utilization(osd_used=83):
                        logger.warning("Cluster OSD utilization is above 75%")
                    else:
                        raise CephHealthException("Cluster OSDs are near full")

                    # Check for 500 pods per namespace
                    pod_objs = pod.get_all_pods(namespace=self.namespace_list[-1].namespace)
                    if len(pod_objs) >= 500:
                        self.create_and_set_namespace()

                except UnexpectedBehaviour:
                    logging.error(
                        f"Scaling of cluster failed after {len(all_pod_obj)} pod creation"
                    )
                    raise UnexpectedBehaviour(
                        "Scaling PVC+POD failed analyze setup and log for more details"
                    )

    def cleanup(self):
        """
        Function to tear down
        """
        # Delete all pods, pvcs and namespaces
        for namespace in self.namespace_list:
            delete_objs_parallel(
                obj_list=pod.get_all_pods(namespace=namespace.namespace),
                namespace=namespace.namespace, kind=self.kind
            )
            delete_objs_parallel(
                obj_list=pvc.get_all_pvc_objs(namespace=namespace.namespace),
                namespace=namespace.namespace, kind=constants.PVC
            )
            ocp = OCP(kind=constants.NAMESPACE)
            ocp.delete(resource_name=namespace.namespace)

        # Remove scale label from worker nodes in cleanup
        scale_workers = machine.get_labeled_nodes(constants.SCALE_LABEL)
        helpers.remove_label_from_worker_node(
            node_list=scale_workers, label_key='scale-label'
        )

        # Delete machineset which will delete respective nodes too for aws-ipi platform
        if self.ms_name:
            for name in self.ms_name:
                machine.delete_custom_machineset(name)


def delete_objs_parallel(obj_list, namespace, kind):
    """
    Function to delete objs specified in list

    Args:
        obj_list(list): List can be obj of pod, pvc, etc
        namespace(str): Namespace where the obj belongs to
        kind(str): Obj Kind

    """
    ocp = OCP(kind=kind, namespace=namespace)
    threads = list()
    for obj in obj_list:
        process1 = threading.Thread(
            target=ocp.delete, kwargs={'resource_name': f"{obj.name}"}
        )
        process2 = threading.Thread(
            target=ocp.wait_for_delete, kwargs={'resource_name': f"{obj.name}"}
        )
        process1.start()
        process2.start()
        threads.append(process1)
        threads.append(process2)
    for process in threads:
        process.join()


def check_enough_resource_available_in_workers(ms_name=None, pod_dict_path=None):
    """
    Function to check if there is enough resource in worker, if not add worker
    for automation supported platforms

    Args:
        ms_name (list): Require param in-case of aws platform to increase the worker
        pod_dict_path (str): Pod dict path for nginx pod.

    """
    # Check for enough worker nodes
    if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
        if pod_dict_path == constants.NGINX_POD_YAML:
            # Below expected count value is kind of hardcoded based on the manual
            # execution result i.e. With m5.4xlarge instance and nginx pod
            # TODO: Revisit the expected_count value once there is support for
            # TODO: more pod creation in one worker node
            if add_worker_based_on_pods_count_per_node(
                machineset_name=ms_name, node_count=1, expected_count=200,
                role_type='app,worker'
            ):
                logging.info("Nodes added for app pod creation")
            else:
                logging.info("Existing resource are enough to create more pods")
        else:
            if add_worker_based_on_cpu_utilization(
                machineset_name=ms_name, node_count=1, expected_percent=59,
                role_type='app,worker'
            ):
                logging.info("Nodes added for app pod creation")
            else:
                logging.info("Existing resource are enough to create more pods")
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'vsphere':
        raise UnsupportedPlatformError("Unsupported Platform")
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'baremetal':
        raise UnsupportedPlatformError("Unsupported Platform")
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'azure':
        raise UnsupportedPlatformError("Unsupported Platform")


def add_worker_based_on_cpu_utilization(
    node_count, expected_percent, role_type=None, machineset_name=None
):
    """
    Function to evaluate CPU utilization of nodes and add node if required.

    Args:
        machineset_name (list): Machineset_names to add more nodes if required.
        node_count (int): Additional nodes to be added
        expected_percent (int): Expected utilization precent
        role_type (str): To add type to the nodes getting added

    Returns:
        bool: True if Nodes gets added, else false.

    """
    # Check for CPU utilization on each nodes
    if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
        app_nodes = node.get_typed_nodes(node_type=role_type)
        uti_dict = node.get_node_resource_utilization_from_oc_describe(node_type=role_type)
        uti_high_nodes, uti_less_nodes = ([] for i in range(2))
        for node_obj in app_nodes:
            utilization_percent = uti_dict[f"{node_obj.name}"]['cpu']
            if utilization_percent > expected_percent:
                uti_high_nodes.append(node_obj.name)
            else:
                uti_less_nodes.append(node_obj.name)
        if len(uti_less_nodes) <= 1:
            for name in machineset_name:
                count = machine.get_replica_count(machine_set=name)
                machine.add_node(machine_set=name, count=(count + node_count))
                machine.wait_for_new_node_to_be_ready(name)
            return True
        else:
            logging.info(f"Enough resource available for more pod creation {uti_dict}")
            return False
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'vsphere':
        raise UnsupportedPlatformError("Unsupported Platform to add worker")
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'baremetal':
        raise UnsupportedPlatformError("Unsupported Platform to add worker")
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'azure':
        raise UnsupportedPlatformError("Unsupported Platform to add worker")


def add_worker_based_on_pods_count_per_node(
    node_count, expected_count, role_type=None, machineset_name=None
):
    """
    Function to evaluate number of pods up in node and add new node accordingly.

    Args:
        machineset_name (list): Machineset_names to add more nodes if required.
        node_count (int): Additional nodes to be added
        expected_count (int): Expected pod count in one node
        role_type (str): To add type to the nodes getting added

    Returns:
        bool: True if Nodes gets added, else false.

    """
    # Check for POD running count on each nodes
    if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
        app_nodes = node.get_typed_nodes(node_type=role_type)
        pod_count_dict = node.get_running_pod_count_from_node(node_type=role_type)
        high_count_nodes, less_count_nodes = ([] for i in range(2))
        for node_obj in app_nodes:
            count = pod_count_dict[f"{node_obj.name}"]
            if count >= expected_count:
                high_count_nodes.append(node_obj.name)
            else:
                less_count_nodes.append(node_obj.name)
        if len(less_count_nodes) <= 1:
            for name in machineset_name:
                count = machine.get_replica_count(machine_set=name)
                machine.add_node(machine_set=name, count=(count + node_count))
                machine.wait_for_new_node_to_be_ready(name)
            return True
        else:
            logging.info(f"Enough pods can be created with available nodes {pod_count_dict}")
            return False
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'vsphere':
        raise UnsupportedPlatformError("Unsupported Platform to add worker")
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'baremetal':
        raise UnsupportedPlatformError("Unsupported Platform to add worker")
    elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'azure':
        raise UnsupportedPlatformError("Unsupported Platform to add worker")


def get_size_based_on_cls_usage(custom_size_dict=None):
    """
    Function to check cls capacity suggest IO write to cluster

    Args:
        custom_size_dict (dict): Dictionary of size param to be used during IO run.
        Example, size_dict = {'usage_below_60': '2G', 'usage_60_70': '512M',
        'usage_70_80': '10M', 'usage_80_85': '512K', 'usage_above_85': '10K'}
        Warning, Make sure dict key is same as above example.

    Returns:
        size (str): IO size to be considered for cluster env

    """
    osd_dict = cluster.get_osd_utilization()
    logger.info(f"Printing OSD utilization from cluster {osd_dict}")
    if custom_size_dict:
        size_dict = custom_size_dict
    else:
        size_dict = {
            'usage_below_40': '1G', 'usage_40_60': '128M', 'usage_60_70': '10M',
            'usage_70_80': '5M', 'usage_80_85': '512K', 'usage_above_85': '10K'
        }
    temp = 0
    for k, v in osd_dict.items():
        if temp <= v:
            temp = v
    if temp <= 40:
        size = size_dict['usage_below_40']
    elif 40 < temp <= 50:
        size = size_dict['usage_40_50']
    elif 60 < temp <= 70:
        size = size_dict['usage_60_70']
    elif 70 < temp <= 80:
        size = size_dict['usage_70_80']
    elif 80 < temp <= 85:
        size = size_dict['usage_80_85']
    else:
        size = size_dict['usage_above_85']
        logging.warning(f"One of the OSD is near full {temp}% utilized")
    return size


def get_rate_based_on_cls_iops(custom_iops_dict=None, osd_size=2048):
    """
    Function to check ceph cluster iops and suggest rate param for fio.

    Args:
        osd_size (int): Size of the OSD in GB
        custom_iops_dict (dict): Dictionary of rate param to be used during IO run.
        Example, iops_dict = {'usage_below_40%': '16k', 'usage_40%_60%': '8k',
        'usage_60%_80%': '4k', 'usage_80%_95%': '2K'}
        Warning, Make sure dict key is same as above example.

    Returns:
        rate_param (str): Rate parm for fio based on ceph cluster IOPs

    """
    # Check for IOPs limit percentage of cluster and accordingly suggest fio rate param
    cls_obj = cluster.CephCluster()
    iops = cls_obj.get_iops_percentage(osd_size=osd_size)
    logger.info(f"Printing iops from cluster {iops}")
    if custom_iops_dict:
        iops_dict = custom_iops_dict
    else:
        iops_dict = {
            'usage_below_40%': '8k', 'usage_40%_60%': '8k',
            'usage_60%_80%': '4k', 'usage_80%_95%': '2K'
        }
    if (iops * 100) <= 40:
        rate_param = iops_dict['usage_below_40%']
    elif 40 < (iops * 100) <= 60:
        rate_param = iops_dict['usage_40%_60%']
    elif 60 < (iops * 100) <= 80:
        rate_param = iops_dict['usage_60%_80%']
    elif 80 < (iops * 100) <= 95:
        rate_param = iops_dict['usage_80%_95%']
    else:
        logging.warning(f"Cluster iops utilization is more than {iops * 100} percent")
        raise UnavailableResourceException("Overall Cluster utilization is more than 95%")
    return rate_param


def get_expected_worker_count(scale_count=1500):
    """
    Function to get expected worker count based on platform to scale pods in cluster

    Args:
        scale_count (int): Scale count of the PVC+POD to be created

    Returns:
        expected_worker_count (int): Expected worker count to scale required number of pod

    """
    # Get expected worker count based on dict in constants.py
    worker_count_dict = constants.SCALE_WORKER_DICT
    if scale_count in worker_count_dict:
        if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
            expected_worker_count = worker_count_dict[scale_count]['aws']
        elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'vsphere':
            expected_worker_count = worker_count_dict[scale_count]['vmware']
        elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'baremetal':
            expected_worker_count = worker_count_dict[scale_count]['bm']
        elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'azure':
            expected_worker_count = worker_count_dict[scale_count]['azure']
        else:
            raise UnsupportedPlatformError("Unsupported Platform")
        return expected_worker_count
    else:
        raise UnexpectedBehaviour("Scale_count value is not matching the dict key")


def check_and_add_enough_worker(worker_count):
    """
    Function to check if there is enough workers available to scale pods.
    IF there is no enough worker then worker will be added based on supported platforms
    Function also adds scale label to the respective worker nodes.

    Args:
        worker_count (int): Expected worker count to be present in the setup

    Returns:
        book: True is there is enough worker count else raise exception.

    """
    # Check either to use OCS workers for scaling app pods
    # Further continue to label the worker with scale label else not
    if config.RUN.get('use_ocs_worker_for_scale'):
        worker_list = helpers.get_worker_nodes()
        if not machine.get_labeled_nodes(constants.SCALE_LABEL):
            helpers.label_worker_node(
                node_list=worker_list, label_key='scale-label', label_value='app-scale'
            )
    else:
        worker_list = helpers.get_worker_nodes()
        if not machine.get_labeled_nodes(constants.SCALE_LABEL):
            for worker in worker_list:
                if worker in machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL):
                    worker_list.remove(worker)
            helpers.label_worker_node(
                node_list=worker_list, label_key='scale-label', label_value='app-scale'
            )
    scale_worker_list = machine.get_labeled_nodes(constants.SCALE_LABEL)
    logging.info(f"Print existing scale worker {scale_worker_list}")

    # Check if there is enough nodes to continue scaling of app pods
    if len(scale_worker_list) >= worker_count:
        logging.info(
            f"Setup has expected worker count {worker_count} "
            "to continue scale of pods"
        )
        return True
    else:
        logging.info(
            "There is no enough worker in the setup, will add enough worker "
            "for the automation supported platforms"
        )
        # Add enough worker for AWS
        if config.ENV_DATA['deployment_type'] == 'ipi' and config.ENV_DATA['platform'].lower() == 'aws':
            # Create machineset for app worker nodes on each aws zone
            # Each zone will have one app worker node
            ms_name = list()
            for obj in machine.get_machineset_objs():
                if 'app' in obj.name:
                    ms_name.append(obj.name)
            if not ms_name:
                if len(machine.get_machineset_objs()) == 3:
                    for zone in ['a', 'b', 'c']:
                        ms_name.append(
                            machine.create_custom_machineset(instance_type='m5.4xlarge', zone=zone)
                        )
                else:
                    ms_name.append(
                        machine.create_custom_machineset(instance_type='m5.4xlarge', zone='a')
                    )
                for ms in ms_name:
                    machine.wait_for_new_node_to_be_ready(ms)
            if len(ms_name) == 3:
                exp_count = int(worker_count / 3)
            else:
                exp_count = worker_count
            for name in ms_name:
                machine.add_node(machine_set=name, count=exp_count)
            for ms in ms_name:
                machine.wait_for_new_node_to_be_ready(ms)
            worker_list = helpers.get_worker_nodes()
            for worker in worker_list:
                if worker in machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL):
                    worker_list.remove(worker)
                elif worker in machine.get_labeled_nodes(constants.SCALE_LABEL):
                    worker_list.remove(worker)
            helpers.label_worker_node(
                node_list=worker_list, label_key='scale-label', label_value='app-scale'
            )
            return True
        elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'vsphere':
            raise UnsupportedPlatformError("Unsupported Platform to add worker")
        elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'baremetal':
            raise UnsupportedPlatformError("Unsupported Platform to add worker")
        elif config.ENV_DATA['deployment_type'] == 'upi' and config.ENV_DATA['platform'].lower() == 'azure':
            raise UnsupportedPlatformError("Unsupported Platform to add worker")
        else:
            raise UnavailableResourceException(
                "There is no enough worker nodes to continue app pod scaling"
            )
