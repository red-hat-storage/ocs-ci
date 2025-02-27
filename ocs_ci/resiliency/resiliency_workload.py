import logging
import os

from abc import ABC, abstractmethod
from jinja2 import Environment, FileSystemLoader
import fauxfactory

from ocs_ci.ocs import constants
from ocs_ci.utility.utils import run_cmd

log = logging.getLogger(__name__)


class Workload(ABC):
    """
    Abstract Base Class for all workloads.
    """

    def __init__(self, namespace="default", image=None):
        self.namespace = namespace
        self.image = image
        self.template_dir = os.path.join(constants.RESILIENCY_DIR, "workloads")
        self.workload_env = Environment(loader=FileSystemLoader(self.template_dir))

    @abstractmethod
    def start_workload(self):
        pass

    @abstractmethod
    def scale_up_pods(self, desired_count):
        pass

    @abstractmethod
    def scale_down_pods(self, desired_count):
        pass

    @abstractmethod
    def stop_workload(self):
        pass

    @abstractmethod
    def cleanup_workload(self):
        pass


class FioWorkload(Workload):
    """
    FIO-specific implementation of Workload.
    """

    def __init__(self, pvc):
        super().__init__(namespace=pvc.namespace)
        self.pvc = pvc
        self.pvc.reload()
        self.deployment_name = f"fio-app-{fauxfactory.gen_alpha(8).lower()}"
        self.volume_mode = self.pvc.data["spec"]["volumeMode"]
        self.template_file = (
            "fio_fs_workload_template.yaml"
            if self.volume_mode == "Filesystem"
            else "fio_block_workload_template.yaml"
        )
        self.template = self.workload_env.get_template(self.template_file)
        self.output_file = f"/tmp/{fauxfactory.gen_alpha(8).lower()}.yaml"
        self.render_template()

    def start_workload(self):
        log.info("Starting FIO workload.")
        run_cmd(f"oc create -f {self.output_file}")
        log.info("FIO workload started.")

    def scale_up_pods(self, desired_count):
        log.info(f"Scaling up FIO pods to {desired_count}.")
        # Implement logic to scale up pods

    def scale_down_pods(self, desired_count):
        log.info(f"Scaling down FIO pods to {desired_count}.")
        # Implement logic to scale down pods

    def stop_workload(self):
        log.info("Stopping FIO workload.")
        run_cmd(f"oc delete -f {self.output_file}")
        log.info("FIO workload stopped.")

    def cleanup_workload(self):
        log.info("Cleaning up FIO workload.")
        # Implement cleanup logic, e.g., deleting all pods in the workload

    def render_template(self):
        rendered_yaml = self.template.render(
            fio_name=self.deployment_name,
            namespace=self.namespace,
            pvc_claim_name=self.pvc.name,
        )
        with open(self.output_file, "w") as f:
            f.write(rendered_yaml)
        log.info("Rendered FIO workload template.")


class SmallFilesWorkload(Workload):
    """
    SmallFiles-specific implementation of Workload.
    """

    def __init__(self, namespace="default", image="smallfiles-image:latest"):
        super().__init__(namespace, image)

    def start_workload(self):
        log.info(f"Starting SmallFiles workload in namespace: {self.namespace}.")
        # Implement pod creation logic

    def scale_up_pods(self, desired_count):
        log.info(f"Scaling up SmallFiles pods to {desired_count}.")
        # Implement logic to scale up pods

    def scale_down_pods(self, desired_count):
        log.info(f"Scaling down SmallFiles pods to {desired_count}.")
        # Implement logic to scale down pods

    def stop_workload(self):
        log.info("Stopping SmallFiles workload.")
        # Implement pod deletion logic

    def cleanup_workload(self):
        log.info("Cleaning up SmallFiles workload.")
        # Implement cleanup logic


class VdbenchWorkload(Workload):
    """
    Vdbench-specific implementation of Workload.
    """

    def __init__(self, namespace="default", image="vdbench-image:latest"):
        super().__init__(namespace, image)

    def start_workload(self):
        log.info(f"Starting Vdbench workload in namespace: {self.namespace}.")
        # Implement pod creation logic

    def scale_up_pods(self, desired_count):
        log.info(f"Scaling up Vdbench pods to {desired_count}.")
        # Implement logic to scale up pods

    def scale_down_pods(self, desired_count):
        log.info(f"Scaling down Vdbench pods to {desired_count}.")
        # Implement logic to scale down pods

    def stop_workload(self):
        log.info("Stopping Vdbench workload.")
        # Implement pod deletion logic

    def cleanup_workload(self):
        log.info("Cleaning up Vdbench workload.")
        # Implement cleanup logic
