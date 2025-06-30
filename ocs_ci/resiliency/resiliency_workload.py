"""
This module defines a base class and specific implementations for simulating
various I/O workloads in a Kubernetes-based OpenShift environment.

These workloads are intended to be used in resiliency and stress testing scenarios,
to validate cluster and storage behavior under varying load and failure conditions.
"""

import logging
import os

from abc import ABC, abstractmethod
from jinja2 import Environment, FileSystemLoader
import fauxfactory

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd
from ocs_ci.workloads.vdbench import VdbenchWorkload as VdbenchWorkloadImpl

log = logging.getLogger(__name__)


class Workload(ABC):
    """
    Abstract base class representing a generic workload.

    This class defines a common interface for workloads that simulate
    I/O or stress operations on Kubernetes PVCs.

    Args:
        namespace (str): Kubernetes namespace where the workload runs.
        image (str, optional): Container image used by the workload.
    """

    def __init__(self, namespace="default", image=None):
        self.namespace = namespace
        self.image = image
        template_path = os.path.join(constants.RESILIENCY_DIR, "workloads")
        self.workload_env = Environment(loader=FileSystemLoader(template_path))

    @abstractmethod
    def start_workload(self):
        """Start the workload."""
        pass

    @abstractmethod
    def scale_up_pods(self, desired_count):
        """Scale up the workload pods."""
        pass

    @abstractmethod
    def scale_down_pods(self, desired_count):
        """Scale down the workload pods."""
        pass

    @abstractmethod
    def stop_workload(self):
        """Stop the workload."""
        pass

    @abstractmethod
    def cleanup_workload(self):
        """Cleanup all workload resources."""
        pass


class FioWorkload(Workload):
    """
    FIO workload implementation supporting both Block and Filesystem PVCs.

    Args:
        pvc (OCS): PVC object to attach the workload to.
        fio_args (dict, optional): Dictionary of FIO parameters.
    """

    def __init__(self, pvc, fio_args=None):
        super().__init__(namespace=pvc.namespace)
        self.pvc = pvc
        self.pvc.reload()
        self.deployment_name = f"fio-app-{fauxfactory.gen_alpha(8).lower()}"
        self.volume_mode = self.pvc.data["spec"]["volumeMode"]
        self.fio_output_file = "/tmp/fio_output.txt"
        self.output_file = f"/tmp/{self.deployment_name}.yaml"
        self.fio_args = fio_args or {}
        self._render_template()

    def _render_template(self):
        """
        Render the FIO workload YAML using Jinja2 template engine.

        This method creates a YAML definition for a FIO deployment, customized
        using the PVC and user-provided FIO arguments, and writes it to a file.

        Raises:
            Exception: If the template rendering or file writing fails.
        """
        try:
            template = self.workload_env.get_template("fio_workload_template.yaml")
            rendered = template.render(
                fio_name=self.deployment_name,
                namespace=self.namespace,
                pvc_claim_name=self.pvc.name,
                volume_mode=self.volume_mode,
                fio_output_file=self.fio_output_file,
                rw=self.fio_args.get("rw", "randwrite"),
                ioengine=self.fio_args.get("ioengine", "libaio"),
                direct=self.fio_args.get("direct", 1),
                size=self.fio_args.get("size", "4G"),
                bs=self.fio_args.get("bs", "256k"),
                numjobs=self.fio_args.get("numjobs", 4),
                runtime=self.fio_args.get("runtime", 120),
            )
            with open(self.output_file, "w") as f:
                f.write(rendered)
            log.info("Rendered FIO workload template: %s", self.output_file)

            # Log the file content
            with open(self.output_file, "r") as f:
                log.info("FIO workload template content:\n%s", f.read())
        except Exception as e:
            log.error("Failed to render FIO workload template: %s", e)
            raise

    def start_workload(self):
        """
        Start the FIO workload by creating the deployment resource.
        """
        log.info("Starting FIO workload: %s", self.deployment_name)
        self._apply_yaml("create")

    def stop_workload(self):
        """
        Stop the FIO workload by deleting the deployment.
        """
        log.info("Stopping FIO workload: %s", self.deployment_name)
        self._apply_yaml("delete", ignore_errors=True)
        log.info("Successfully stopped FIO workload.")

    def scale_up_pods(self, desired_count):
        """
        Scale up the FIO workload pods.

        Args:
            desired_count (int): Desired number of pods.
        """
        self._scale_pods(desired_count, "up")

    def scale_down_pods(self, desired_count):
        """
        Scale down the FIO workload pods.

        Args:
            desired_count (int): Desired number of pods.
        """
        self._scale_pods(desired_count, "down")

    def cleanup_workload(self):
        """
        Cleanup all FIO workload resources.
        """
        log.info("Cleaning up FIO workload: %s", self.deployment_name)
        self.stop_workload()

    def get_fio_results(self):
        """
        Retrieve the output of the FIO workload from the pod logs.

        Returns:
            str: FIO command output captured from within the pod.

        Raises:
            Exception: If pod name retrieval or output fetching fails.
        """
        try:
            log.info("Fetching FIO results for: %s", self.deployment_name)
            cmd_get_pod = (
                f"oc -n {self.namespace} get pod -l app={self.deployment_name} "
                "-o jsonpath='{.items[0].metadata.name}'"
            )
            pod_name = run_cmd(cmd_get_pod).strip().strip("'")
            log.debug("Found FIO pod: %s", pod_name)

            cmd_read_output = (
                f"oc -n {self.namespace} rsh {pod_name} cat {self.fio_output_file}"
            )
            result = run_cmd(cmd_read_output)
            log.info("Fetched FIO results from pod.")
            log.info(result)
            return result
        except Exception as e:
            log.error("Failed to fetch FIO output: %s", e)
            return ""

    def _apply_yaml(self, action, ignore_errors=False, *args):
        """
        Apply or delete the FIO workload YAML.

        Args:
            action (str): Either 'create' or 'delete'.
            ignore_errors (bool): If True, suppress exceptions on failure.
            *args: Additional command-line arguments (e.g., '--force').

        Raises:
            ValueError: If an invalid action is passed.
        """
        if action not in ["create", "delete"]:
            raise ValueError("Action must be 'create' or 'delete'")

        extra_args = " ".join(args)
        cmd = f"oc {action} -f {self.output_file} {extra_args}".strip()

        try:
            run_cmd(cmd)
            log.info("Successfully %sed workload.", action)
        except Exception as e:
            log.error("Failed to %s workload: %s", action, e)
            if not ignore_errors:
                raise

    def _scale_pods(self, replicas, direction="up"):
        """
        Scale the number of replicas in the FIO workload deployment.

        Args:
            replicas (int): Number of pods to scale to.
            direction (str): Description for log output ("up" or "down").
        """
        log.info(
            "Scaling %s FIO pods to %d in namespace %s",
            direction,
            replicas,
            self.namespace,
        )
        try:
            run_cmd(
                f"oc -n {self.namespace} scale deployment {self.deployment_name} --replicas={replicas}"
            )
            log.info("Successfully scaled FIO pods.")
        except Exception as e:
            log.error("Failed to scale FIO pods: %s", e)
            raise


class SmallFilesWorkload(Workload):
    """
    Placeholder class for simulating SmallFiles workload.

    Args:
        namespace (str): Kubernetes namespace.
        image (str): Container image to use.
    """

    def __init__(self, namespace="default", image="smallfiles-image:latest"):
        super().__init__(namespace, image)

    def start_workload(self):
        log.info(f"Starting SmallFiles workload in namespace: {self.namespace}.")

    def scale_up_pods(self, desired_count):
        log.info(f"Scaling up SmallFiles pods to {desired_count}.")

    def scale_down_pods(self, desired_count):
        log.info(f"Scaling down SmallFiles pods to {desired_count}.")

    def stop_workload(self):
        log.info("Stopping SmallFiles workload.")

    def cleanup_workload(self):
        log.info("Cleaning up SmallFiles workload.")


class VdbenchWorkload(Workload):
    """
    Placeholder class for simulating Vdbench workload.

    Args:
        namespace (str): Kubernetes namespace.
        image (str): Container image to use.
    """

    def __init__(self, pvc, vdbench_config_file, namespace=None, image=None):
        super().__init__(namespace=namespace or pvc.namespace, image=image)
        self.pvc = pvc
        self.vdbench_config_file = vdbench_config_file

        self.workload_impl = VdbenchWorkloadImpl(
            pvc=pvc,
            vdbench_config_file=vdbench_config_file,
            namespace=namespace,
            image=image,
        )

        log.info(
            f"Initialized Vdbench workload for resiliency testing: {self.workload_impl.deployment_name}"
        )

    def start_workload(self):
        """Start the Vdbench workload."""
        log.info(f"Starting Vdbench workload in namespace: {self.namespace}")
        self.workload_impl.start_workload()

    def scale_up_pods(self, desired_count):
        """Scale up Vdbench pods to desired count."""
        log.info(f"Scaling up Vdbench pods to {desired_count}")
        self.workload_impl.scale_up_pods(desired_count)

    def scale_down_pods(self, desired_count):
        """Scale down Vdbench pods to desired count."""
        log.info(f"Scaling down Vdbench pods to {desired_count}")
        self.workload_impl.scale_down_pods(desired_count)

    def stop_workload(self):
        """Stop the Vdbench workload."""
        log.info("Stopping Vdbench workload")
        self.workload_impl.stop_workload()

    def cleanup_workload(self):
        """Cleanup Vdbench workload resources."""
        log.info("Cleaning up Vdbench workload")
        self.workload_impl.cleanup_workload()

    def pause_workload(self):
        """Pause the Vdbench workload."""
        log.info("Pausing Vdbench workload")
        self.workload_impl.pause_workload()

    def resume_workload(self):
        """Resume the Vdbench workload."""
        log.info("Resuming Vdbench workload")
        self.workload_impl.resume_workload()

    def get_workload_status(self):
        """Get workload status."""
        return self.workload_impl.get_workload_status()


def workload_object(workload_type, namespace):
    """
    Factory method to create a workload object based on type.

    Args:
        workload_type (str): Type of workload (e.g., "FIO").
        namespace (str): Kubernetes namespace for workload.

    Returns:
        Workload: Instance of the appropriate workload class.

    Raises:
        ValueError: If the workload type is not supported.
    """
    mapping = {
        "FIO": FioWorkload,
        "VDBENCH": VdbenchWorkload,
    }

    if workload_type.upper() not in mapping:
        raise ValueError(f"Unknown workload type: {workload_type}")

    return mapping[workload_type.upper()]
