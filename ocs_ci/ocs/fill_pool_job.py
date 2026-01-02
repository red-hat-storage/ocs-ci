import logging

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import catch_exceptions
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
from ocs_ci.utility.utils import exec_cmd


log = logging.getLogger(__name__)


class FillPoolJob(object):
    """
    Fill Pool Job operations (assumes a Job manifest).
    """

    def __init__(self):
        self.name = "<unknown>"
        self.job_obj = None
        self.pod_obj = None
        self.pvc_obj = None
        self.namespace = None

    def create(
        self,
        name=None,
        block_size="1M",
        cpu_request="100m",
        mem_request="128Mi",
        cpu_limit="500m",
        mem_limit="256Mi",
        fill_mode="zero",
        base_yaml_path=constants.FILL_POOL_JOB_YAML,
        pvc_name=None,
        sc_name=constants.DEFAULT_STORAGECLASS_RBD,
        storage="50Gi",
        pvc_base_yaml_path=constants.FILL_POOL_PVC_YAML,
        wait_for_resource=True,
    ):
        """
        Create a Job that fills up cluster storage by writing data to a PVC.
        Assumes manifest is a Job (pod spec under spec.template.spec).
        """
        self.name = name or create_unique_resource_name("fill-pool", "job")
        sc_name = sc_name or constants.DEFAULT_STORAGECLASS_RBD
        proj_obj = helpers.create_project()
        self.namespace = proj_obj.namespace

        if fill_mode not in ["zero", "random"]:
            raise ValueError("fill_mode must be either 'zero' or 'random'")

        input_source = "/dev/zero" if fill_mode == "zero" else "/dev/urandom"

        # Load Job manifest and apply metadata
        job_data = templating.load_yaml(base_yaml_path)
        job_data.setdefault("metadata", {})
        job_data["metadata"]["name"] = self.name
        job_data["metadata"]["namespace"] = self.namespace

        # Assume Job: pod spec under spec.template.spec
        template = job_data["spec"]["template"]
        template.setdefault("metadata", {})
        template["metadata"]["namespace"] = self.namespace
        pod_spec = template["spec"]

        container = pod_spec["containers"][0]
        volume = pod_spec["volumes"][0]

        # Prepare PVC name and update volume claim
        pvc_name = pvc_name or create_unique_resource_name("fill-pool", "pvc")
        if "persistentVolumeClaim" in volume:
            volume["persistentVolumeClaim"]["claimName"] = pvc_name

        # Update BLOCK_SIZE env variable if present
        for env_var in container.get("env", []):
            if env_var.get("name") == "BLOCK_SIZE":
                env_var["value"] = block_size

        # Update resources
        container["resources"] = {
            "requests": {"cpu": cpu_request, "memory": mem_request},
            "limits": {"cpu": cpu_limit, "memory": mem_limit},
        }

        # Ensure the container will run the dd command
        dd_cmd = (
            f'echo "Filling PVC with {fill_mode} data..."; '
            f"dd if={input_source} of=/mnt/fill/testfile bs=${{BLOCK_SIZE:-{block_size}}}"
        )
        container["command"] = ["sh", "-c", dd_cmd]
        container.pop("args", None)

        # Prepare PVC manifest
        pvc_data = templating.load_yaml(pvc_base_yaml_path)
        pvc_data.setdefault("metadata", {})
        pvc_data["metadata"]["name"] = pvc_name
        pvc_data["metadata"]["namespace"] = self.namespace
        pvc_data["spec"]["storageClassName"] = sc_name
        pvc_data["spec"]["resources"]["requests"]["storage"] = storage

        # Create PVC resource
        ocs_obj = helpers.create_resource(**pvc_data)
        self.pvc_obj = PVC(**ocs_obj.data)

        # Create Job resource
        self.job_obj = helpers.create_resource(**job_data)
        # Get Pod created by the Job
        label = f"job-name={self.name}"
        pods = get_pods_having_label(label, namespace=self.namespace)
        if pods:
            self.pod_obj = Pod(**pods[0])

        # Wait for Pod to be Running if we wrapped it
        if wait_for_resource and self.pod_obj:
            self.pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=self.pod_obj.name,
                timeout=180,
                sleep=10,
            )

    def cleanup(self):
        """
        Cleanup resources: Job, Pod, PVC, and Namespace.
        """
        log.info("Cleaning up Fill Pool Job resources...")

        if self.job_obj:
            job_name = getattr(self.job_obj, "name", "<unknown>")
            log.info(f"Deleting Job {job_name}")
            try:
                self.job_obj.delete()
            except Exception as e:
                log.warning(f"Failed to delete Job {job_name}: {e}")

        # Delete Pod if it still exists
        if self.pod_obj:
            pod_name = getattr(self.pod_obj, "name", "<unknown>")
            log.info(f"Deleting Pod {pod_name}")
            try:
                self.pod_obj.delete()
            except Exception as e:
                log.warning(f"Failed to delete Pod {pod_name}: {e}")

        if self.pvc_obj:
            pvc_name = getattr(self.pvc_obj, "name", "<unknown>")
            log.info(f"Deleting PVC {pvc_name}")
            try:
                self.pvc_obj.delete()
            except Exception as e:
                log.warning(f"Failed to delete PVC {pvc_name}: {e}")
        if self.namespace:
            log.info(f"Deleting Namespace {self.namespace}")
            catch_exceptions(CommandFailed)(exec_cmd)(
                f"oc delete project {self.namespace}"
            )
