import logging

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import catch_exceptions
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.resources.pod import Pod
from ocs_ci.utility.utils import run_cmd


log = logging.getLogger(__name__)


class FillPoolPod(object):
    """
    Fill Pool Pod operations
    """

    def __init__(self):
        """
        Initialize the FillPoolPod object

        """
        self.name = "<unknown>"
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
        base_yaml_path=constants.FILL_POOL_POD_YAML,
        pvc_name=None,
        sc_name=constants.DEFAULT_STORAGECLASS_RBD,
        storage="50Gi",
        pvc_base_yaml_path=constants.FILL_POOL_PVC_YAML,
        wait_for_resource=True,
    ):
        """
        Create a Pod that fills up the cluster storage by writing data to a PVC.
        Also creates the linked PVC.

        Args:
            name (str): Name of the Pod to create.
            block_size (str): Block size for the dd command.
            cpu_request (str): CPU request for the Pod.
            mem_request (str): Memory request for the Pod.
            cpu_limit (str): CPU limit for the Pod.
            mem_limit (str): Memory limit for the Pod.
            fill_mode (str): Mode of filling data, either 'zero' or 'random'.
            base_yaml_path (str): Path to the base Pod YAML manifest.
            pvc_name (str): Name of the PVC to create and attach to the Pod.
            sc_name (str): StorageClass name for the PVC.
            storage (str): Storage size for the PVC.
            pvc_base_yaml_path (str): Path to the base PVC YAML manifest.
            wait_for_resource (bool): Whether to wait for the Pod to be running.

        """
        self.name = name or create_unique_resource_name("fill-pool-pod", "pod")
        sc_name = sc_name or constants.DEFAULT_STORAGECLASS_RBD
        proj_obj = helpers.create_project()
        self.namespace = proj_obj.namespace

        if fill_mode not in ["zero", "random"]:
            raise ValueError("fill_mode must be either 'zero' or 'random'")

        input_source = "/dev/zero" if fill_mode == "zero" else "/dev/urandom"

        # Load base Pod manifest
        pod_data = templating.load_yaml(base_yaml_path)
        # Apply overrides
        pod_data["metadata"]["name"] = self.name
        pod_data["metadata"]["namespace"] = self.namespace
        container = pod_data["spec"]["containers"][0]
        volume = pod_data["spec"]["volumes"][0]

        # Update PVC name in Pod
        pvc_name = pvc_name or create_unique_resource_name("fill-pool-pvc", "pvc")
        volume["persistentVolumeClaim"]["claimName"] = pvc_name

        # Update BLOCK_SIZE env variable
        for env_var in container.get("env", []):
            if env_var["name"] == "BLOCK_SIZE":
                env_var["value"] = block_size

        # Update resources
        container["resources"] = {
            "requests": {"cpu": cpu_request, "memory": mem_request},
            "limits": {"cpu": cpu_limit, "memory": mem_limit},
        }

        # Update command dynamically
        container["command"] = [
            "sh",
            "-c",
            f'echo "Filling PVC with {fill_mode} data..."; '
            f"dd if={input_source} of=/mnt/fill/testfile bs=${{BLOCK_SIZE:-{block_size}}}",
        ]

        pvc_name = pvc_name or create_unique_resource_name("fill-pool-pvc", "pvc")
        sc_name = sc_name or constants.DEFAULT_STORAGECLASS_RBD
        # Load base PVC manifest
        pvc_data = templating.load_yaml(pvc_base_yaml_path)
        # Apply overrides
        pvc_data["metadata"]["name"] = pvc_name
        pvc_data["metadata"]["namespace"] = pod_data["metadata"]["namespace"]
        pvc_data["spec"]["storageClassName"] = sc_name
        pvc_data["spec"]["resources"]["requests"]["storage"] = storage

        # Create PVC resource in OpenShift
        ocs_obj = helpers.create_resource(**pvc_data)
        self.pvc_obj = PVC(**ocs_obj.data)
        # Create Pod resource in OpenShift
        ocs_obj = helpers.create_resource(**pod_data)
        self.pod_obj = Pod(**ocs_obj.data)
        if wait_for_resource:
            # Wait for Pod to be in Running state
            self.pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                resource_name=self.pod_obj.name,
                timeout=180,
                sleep=10,
            )

    def cleanup(self):
        """
        Cleanup the Fill Pool Pod resources: Pod, PVC, and Namespace.

        """
        log.info("Cleaning up Fill Pool Pod resources")
        if not self.pod_obj and not self.pvc_obj and not self.namespace:
            log.info("No resources to clean up.")
            return

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
            catch_exceptions(CommandFailed)(run_cmd)(
                f"oc delete project {self.namespace}"
            )
