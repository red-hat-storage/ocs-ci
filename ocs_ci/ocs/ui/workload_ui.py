import copy
import logging
import random
import tempfile
import time
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import CommandFailed, UnableUpgradeConnectionException
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.deployment import Deployment
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.utility.retry import retry


logger = logging.getLogger(__name__)


class SingletonMeta(type):
    """
    Singleton class to ensure only one instance of WorkloadUi is created.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class WorkloadUi(metaclass=SingletonMeta):
    """Class to handle workload UI related operations"""

    with open(constants.BUSYBOX_TEMPLATE) as file_stream:
        busy_box_depl = yaml.safe_load(file_stream)

    deployment_list = list()

    def get_busybox_depl_name(self):
        """
        Retrieves the name of the BusyBox deployment.

        Returns:
            str: Name of the BusyBox deployment.
        """
        return self.busy_box_depl["metadata"]["name"]

    def set_busybox_depl_name(self, name):
        """
        Sets the name of the BusyBox deployment in the original YAML.

        Args:
            name (str): New name for the BusyBox deployment.
        """
        self.busy_box_depl["metadata"]["name"] = name

    def deploy_busybox(
        self,
        node: str = None,
        namespace=config.ENV_DATA["cluster_namespace"],
        depl_name="busybox-ui-test",
        pvc_name: str = None,
    ) -> tuple:
        """
        Deploys a busybox container to a randomly selected worker node.

        node (str): Name of the node where the busybox container is to be deployed.
        If not specified, a random worker node is selected.
        namespace (str): Namespace where the busybox container is to be deployed.
        depl_name (str): Name of the deployment to be created. Defaults to None.
        pvc_name (str): Name of the PVC to be attached by the busybox container. Defaults to None.

        Returns:
            tuple: The name of the node where the busybox container is deployed, and a deployment name if deployed,
            otherwise None.
        """
        if not node:
            node = random.choice(get_worker_nodes())
            logger.info(f"selected node {node} for deployment")

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=True
        ) as temp_file:

            bb_dict = copy.deepcopy(self.busy_box_depl)

            bb_dict["spec"]["template"]["spec"]["nodeName"] = node
            bb_dict["metadata"]["namespace"] = namespace
            if depl_name:
                bb_dict["metadata"]["name"] = depl_name
            if pvc_name:
                bb_dict["spec"]["template"]["spec"].setdefault("volumes", [])
                bb_dict["spec"]["template"]["spec"]["volumes"].append(
                    {
                        "name": "unique-pvc",
                        "persistentVolumeClaim": {"claimName": pvc_name},
                    }
                )
                bb_dict["spec"]["template"]["spec"]["containers"][0].setdefault(
                    "volumeMounts", []
                )
                bb_dict["spec"]["template"]["spec"]["containers"][0][
                    "volumeMounts"
                ].append({"name": "unique-pvc", "mountPath": "/tmp"})

            yaml.dump(bb_dict, temp_file, default_flow_style=False)
            temp_file.flush()
            occli = OCP()
            occli.apply(temp_file.name)

        busy_box_scaled_up = self.wait_busy_box_scaled_up(60, depl_name, namespace)
        deployment = Deployment(
            **OCP(
                kind="deployment",
                namespace=namespace,
                resource_name=depl_name,
            ).get()
        )
        self.deployment_list.append(deployment)
        return node, deployment if busy_box_scaled_up else None

    def wait_busy_box_scaled_up(
        self, timeout: int, depl_name: str, namespace: str
    ) -> bool:
        """
        Waits for the 'busybox' deployment to be scaled up within the specified timeout.

        Args:
            timeout (int): The maximum time to wait for the deployment to be scaled up, in seconds.
            depl_name (str): Name of the deployment to be scaled up.
            namespace (str): Namespace where the deployment is located.
        Returns:
            bool: True if the deployment is successfully scaled up within the timeout, False otherwise.

        """
        busy_box_ocp_inst = OCP(
            kind="deployment",
            namespace=namespace,
            resource_name=depl_name,
        )
        start_time = time.perf_counter()
        while time.perf_counter() - start_time < timeout:
            time.sleep(1)
            if busy_box_ocp_inst.get().get("spec").get("replicas") > 0:
                logger.info(f"deployment '{depl_name}' " f"successfully deployed")
                return True
        else:
            logger.error(f"failed to deploy and scale up '{depl_name}' deployment")
            return False

    def delete_busybox(self, depl_name: str, force: bool = False):
        """
        Deletes the BusyBox deployment from cluster.

        Args:
            force (bool, optional): If True, force deletion even if the deployment is not found. Defaults to False.
            depl_name ( str): Name of the deployment to be deleted. Defaults to None.
        Returns:
            dict: The deletion result if the BusyBox deployment exists and is successfully deleted.
                  Returns None otherwise.
        """

        for depl_obj in self.deployment_list:
            if depl_name == depl_obj.name:
                bb_depl = depl_obj
                break
        else:
            logger.error(f"no deployment with the given name {depl_name} found")
            return None
        res = bb_depl.delete(wait=True, force=force)
        self.deployment_list.remove(bb_depl)
        return res

    def delete_all_busy_box_deployments(self):
        """
        Deletes all BusyBox deployments from cluster.
        """
        for depl in WorkloadUi().deployment_list:
            self.delete_busybox(depl.name, force=True)


class PvcCapacityDeployment:
    """
    Class to store PVC capacity and deployment name to avoid unnecessary queries to a cluster
    and speed-up the test
    """

    def __init__(self, pvc_obj: PVC, capacity_float: float, deployment: str = None):
        self.pvc_obj = pvc_obj
        self.capacity_size = capacity_float
        self.deployment = deployment

    def __str__(self):
        return f"pvc_obj: {self.pvc_obj}, capacity_size: {self.capacity_size}, deployment: {self.deployment}"


class PvcCapacityDeploymentList(list, metaclass=SingletonMeta):
    """
    Subclass of list to store a list of PvcCapacityDeployment objects and provide additional methods.
    """

    def add_instance(self, pvc_obj: PVC, capacity_float: float, deployment: str = None):
        """
        Add a PvcCapacityDeployment object to the list.

        Args:
            pvc_obj (PVC): The PVC object.
            capacity_float (float): The capacity in float.
            deployment (str, optional): The deployment name. Defaults to None.
        """
        pvc_capacity_deployment = PvcCapacityDeployment(
            pvc_obj, capacity_float, deployment
        )
        self.append(pvc_capacity_deployment)

    def get_pvc_capacity_deployment(self, index: int) -> PvcCapacityDeployment:
        """
        Get the PvcCapacityDeployment object at the specified index.

        Args:
            index (int): The index of the PvcCapacityDeployment object to retrieve.

        Returns:
            PvcCapacityDeployment: The PvcCapacityDeployment object at the specified index.
        """
        return self[index]

    def set_pvc_capacity_deployment(
        self, index: int, pvc_obj: PVC, capacity_float: float, deployment: str = None
    ):
        """
        Set the PvcCapacityDeployment object at the specified index.

        Args:
            index (int): The index of the PvcCapacityDeployment object to set.
            pvc_obj (PVC): The PVC object.
            capacity_float (float): The capacity in float.
            deployment (str, optional): The deployment name. Defaults to None.
        """
        pvc_capacity_deployment = PvcCapacityDeployment(
            pvc_obj, capacity_float, deployment
        )
        self[index] = pvc_capacity_deployment

    def get_pvc_capacity_deployment_by_deployment(self, deployment: str):
        """
        Get the PvcCapacityDeployment object by deployment.

        Args:
            deployment (str): The deployment name.

        Returns:
            PvcCapacityDeployment: The PvcCapacityDeployment object that matches the pvc_obj and deployment.
        """
        for pvc_capacity_deployment in self:
            if pvc_capacity_deployment.deployment.name == deployment:
                return pvc_capacity_deployment
        raise ValueError(
            f"PvcCapacityDeployment with deployment {deployment} not found."
        )

    def get_deployment_names_list(self):
        """
        Get the list of deployments.

        Returns:
            list: The list of deployments.
        """
        return [
            pvc_capacity_deployment.deployment.name for pvc_capacity_deployment in self
        ]

    def get_pvc_names_list(self):
        """
        Get the list of pvcs.

        Returns:
            list: The list of pvcs.
        """
        return [
            pvc_capacity_deployment.pvc_obj.name for pvc_capacity_deployment in self
        ]

    def _delete_pvc_capacity_deployment_from_list(self, deployment_name: str):
        """
        Delete the PvcCapacityDeployment object by deployment name.
        ! deletes deployment only from the list, not from the cluster !


        Args:
            deployment_name (str): The deployment name.
        """
        pvc_capacity_deployment = self.get_pvc_capacity_deployment_by_deployment(
            deployment_name
        )
        self.remove(pvc_capacity_deployment)
        logger.info(f"Deleted entry {str(pvc_capacity_deployment)} from the list")

    def delete_deployment(self, deployment: Deployment):
        """
        Deletes the deployment from the cluster and from the list.
        :param deployment:
        """
        logger.info(f"Delete deployment {deployment.name}")
        for pvc_capacity_deployment in self:
            if pvc_capacity_deployment.deployment == deployment:
                pvc_capacity_deployment.deployment.delete(True, False)
                self._delete_pvc_capacity_deployment_from_list(deployment.name)
                break
        else:
            raise ValueError(
                f"PvcCapacityDeployment with deployment {deployment.name} not found."
            )

    def delete_pvc(self, pvc: PVC):
        """
        Delete the PVC and remove the PvcCapacityDeployment object
        :param pvc:
        """
        logger.info(f"Delete pvc {pvc.name}")
        for pvc_capacity_deployment in self:
            if pvc_capacity_deployment.pvc_obj == pvc:
                pv_obj = pvc.backed_pv_obj
                pvc.delete(force=True, wait=False)

                # remove finalizers from the pvc to be able to delete mounted pvc
                params = '{"metadata": {"finalizers":null}}'
                try:
                    OCP().exec_oc_cmd(
                        f"patch pvc {pvc.name} -p '{params}' -n {pvc.namespace}"
                    )
                except CommandFailed as ex:
                    if "not found" in str(ex):
                        logger.info(f"pvc '{pvc.name}' already deleted")
                if not pv_obj.is_deleted:
                    logger.info(
                        f"PVC deletion did not delete PV on cluster. Delete pv {pv_obj.name}"
                    )
                    pv_obj.delete(wait=False)

                self._delete_pvc_capacity_deployment_from_list(
                    pvc_capacity_deployment.deployment.name
                )
                break
        else:
            raise ValueError(f"PvcCapacityDeployment with pvc {pvc.name} not found.")


def compare_mem_usage(
    mem_float: float, mem_str: str, deviation_accepted: int = 10
) -> bool:
    """
    Compare memory usage in bytes and in string format
    :param mem_float: memory in float format
    :param mem_str: memory in string format such as "1.5 GiB" or "1.5 GB"
    :param deviation_accepted: deviation accepted in percentage

    :return: True if the deviation is within the acceptable range, False otherwise
    """

    def string_to_bytes(s):
        size_units = {
            "B": 1,
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
            "KiB": 1024,
            "MiB": 1024**2,
            "GiB": 1024**3,
            "TiB": 1024**4,
        }
        size, unit = s.split()
        return float(size) * size_units[unit]

    # Convert string b to bytes and calculate the deviation as a percentage
    float_value = string_to_bytes(mem_str)
    float_value_a = string_to_bytes(f"{mem_float} GiB")

    deviation = abs(float_value / float_value_a - 1) * 100
    # Check if the deviation is within the acceptable range (10%)
    return deviation <= deviation_accepted


def wait_for_container_status_ready(pod: Pod):
    """
    Wait for container of the pod move to Running state
    :param pod: a pod object of the pod which container is to be checked
    :return: status of the container
    """
    logger.info(f"Wait for container of the pod {pod.name} move to Running state")

    def do_wait_for_container_status_ready(pod_obj: Pod, timeout=300):
        logger.info(f"Waiting for container status ready for {timeout}s")
        start_time = time.time()
        while (
            pod_obj.get()["status"]["containerStatuses"][0]["ready"] is False
            and time.time() - start_time < timeout
        ):
            logger.info("Waiting for container status ready")
            time.sleep(5)
        return pod_obj.get()["status"]["containerStatuses"][0]["ready"]

    retry(
        CommandFailed,
        text_in_exception="can't read container state of busybox deployment",
        func=do_wait_for_container_status_ready,
    )(pod)


@retry(UnableUpgradeConnectionException, tries=3, delay=10, backoff=1)
def fill_attached_pv(data_struct: PvcCapacityDeployment, pod: Pod) -> bool:
    """
    Fill the attached PV with data until it is full or disc quota is exceeded.
    A PvcCapacityDeployment object will be updated with capacity of copied data if the disc quota is exceeded.

    :param data_struct: a PvcCapacityDeployment object consisting of a PVC and a capacity and a Deployment
    :param pod: a Pod object to fill the PV attached to it

    :return: True if the PV is filled, False otherwise
    """
    logger.info(f"Run IO on pod {pod.name}")
    chunk_size = 64
    count = data_struct.capacity_size * 1024 // chunk_size

    try:
        pod.exec_cmd_on_pod(
            f"dd if=/dev/urandom of=/tmp/testfile bs={chunk_size}M count={count}"
        )
        return True
    except CommandFailed as ex:
        if "No space left on device" in str(ex):
            logger.info("dd failed as expected. No space left on device")
            return True
        elif "Disk quota exceeded" in str(ex):
            # get the size that was copied from the Error message
            size_copied = str(ex).partition("(")[2].partition(")")[0]
            # extract a number from the string like 2.6 from "2.6GiB"
            data_struct.capacity_size = float(
                "".join(filter(lambda x: x.isdigit() or x == ".", size_copied))
            )
            logger.info(
                f"dd failure is acceptable. Disk quota exceeded. Copied {size_copied}"
            )
            return True
        elif "error: unable to upgrade connection: container not found" in str(ex):
            logger.info("Container not found")
            raise UnableUpgradeConnectionException
        else:
            raise


def divide_capacity(total, num):
    """
    Get n random numbers that sum up to a total. For example:  10 = 3 + 4 + 3

    :param total: number to divide
    :param num: number of parts
    :return: list of numbers
    """
    numbers = []
    for _ in range(num - 1):
        number = random.randint(1, total - sum(numbers) - (num - len(numbers) - 1))
        numbers.append(number)
    numbers.append(total - sum(numbers))
    return numbers
