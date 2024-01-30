"""
Virtual machine instance class
"""
import logging

from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.cnv.virtctl import Virtctl
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.ocs.resources.pod import get_pods_having_label
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


class VirtualMachineInstance:
    """
    Virtual Machine Instance class for managing VMIs.
    """

    def __init__(
        self,
        vmi_name,
        namespace=None,
    ):
        """
        Initialize the VirtualMachineInstance object.

        Args:
            vmi_name (str): Name of the VirtualMachineInstance.
            namespace (str): Namespace for the VirtualMachineInstance.

        """
        self._namespace = namespace
        self._vmi_name = vmi_name
        self.ocp = OCP(
            kind=constants.VIRTUAL_MACHINE_INSTANCE,
            namespace=namespace,
        )
        self._virtctl = Virtctl(namespace=self._namespace)

    def get(self, out_yaml_format=True):
        """
        Get information about the VirtualMachineInstance.

        Args:
            out_yaml_format (bool): True to get the output in YAML format.

        Returns:
            dict: Information about the VirtualMachineInstance.

        """
        return self.ocp.get(
            resource_name=self._vmi_name, out_yaml_format=out_yaml_format
        )

    def is_vmi_running(self):
        """
        Check if the VirtualMachineInstance is in the 'Running' phase.

        Returns:
            bool: True if the VirtualMachineInstance is in 'Running' phase, False otherwise.

        """
        return self.get().get("status").get("phase") == "Running"

    def node(self):
        """
        Get the node information for the VirtualMachineInstance.

        Returns:
            str: Node name.

        """
        return self.get().get("status").get("nodeName")

    def pause(self, wait=True):
        """
        Pause the VirtualMachineInstance.

        Args:
            wait (bool): True to wait for the VirtualMachineInstance to reach the 'Paused' status.

        """
        self._virtctl.pause_vmi(vm_name=self._vmi_name)
        logger.info(f"Successfully Paused VMI: {self._vmi_name}")
        if wait:
            self.wait_for_vmi_condition_pause_status(pause=True)
            logger.info(f"VMI: {self._vmi_name} reached Paused state")

    def unpause(self, wait=True):
        """
        Unpause the VirtualMachineInstance.

        Args:
            wait (bool): True to wait for the VirtualMachineInstance to reach the 'Running' status.

        """
        self._virtctl.unpause_vmi(vm_name=self._vmi_name)
        logger.info(f"Successfully UnPaused VMI: {self._vmi_name}")
        if wait:
            self.wait_for_vmi_condition_pause_status(pause=False)
            logger.info(f"VMI: {self._vmi_name} reached Running state")

    def virt_launcher_pod(self):
        """
        Get the name of the Virt Launcher Pod associated with the VirtualMachineInstance.

        Returns:
            str: Virt Launcher Pod name.

        """
        selector = f"vm.kubevirt.io/name={self._vmi_name}"
        virt_launcher_pod = Pod(**get_pods_having_label(selector, self._namespace)[0])
        return virt_launcher_pod.name

    def wait_for_vmi_to_be_running(self, timeout=600):
        """
        Wait for the VirtualMachineInstance to reach the 'Running' status.

        Args:
            timeout (int): Timeout value in seconds.

        """
        self.ocp.wait_for_resource(
            resource_name=self._vmi_name,
            column="PHASE",
            condition=constants.VM_RUNNING,
            timeout=timeout,
        )

    def get_vmi_active_condition(self):
        """
        Get the active condition of the VirtualMachineInstance.

        Returns:
            list: Active condition of the VirtualMachineInstance.

        """
        out = self.get().get("status").get("conditions")
        return [
            condition
            for condition in out
            if condition.get("lastTransitionTime") is not None
        ]

    def get_vmi_pause_condition(self):
        """
        Get the pause condition of the VirtualMachineInstance.

        Returns:
            dict: Pause condition of the VirtualMachineInstance, or None if not found.

        """
        for pause_condition in self.get_vmi_active_condition():
            if (
                pause_condition.get("reason") == "PausedByUser"
                and pause_condition.get("status") == "True"
            ):
                return pause_condition

    def wait_for_vmi_condition_pause_status(self, pause=True):
        """
        Wait for the VirtualMachineInstance to reach the specified pause status.

        Args:
            pause (bool): True to wait for 'Paused' status, False for 'Running' status.

        """
        condition = "PausedByUser" if pause else None
        for sample in TimeoutSampler(
            timeout=600, sleep=10, func=self.get_vmi_pause_condition
        ):
            if not sample or (condition and sample.get("reason") == condition):
                return

    def wait_for_vmi_delete(self, timeout=600):
        """
        Wait for the deletion of the VirtualMachineInstance.

        Args:
            timeout (int): Timeout value in seconds.

        """
        self.ocp.wait_for_delete(resource_name=self._vmi_name, timeout=timeout)

    def wait_for_virt_launcher_pod_delete(self):
        """
        Wait for the deletion of the Virt Launcher Pod associated with the VirtualMachineInstance.

        """
        pod_obj = OCP(kind=constants.POD, namespace=self._namespace)
        virt_launcher_pod = self.virt_launcher_pod()
        pod_obj.wait_for_delete(virt_launcher_pod)
