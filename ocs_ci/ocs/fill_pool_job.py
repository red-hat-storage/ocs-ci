import logging

from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import catch_exceptions
from ocs_ci.utility import templating
from ocs_ci.helpers import helpers
from ocs_ci.ocs.resources.pvc import PVC
from ocs_ci.ocs.resources.pod import Pod, get_pods_having_label
from ocs_ci.utility.utils import convert_device_size, exec_cmd


log = logging.getLogger(__name__)

# RBD discards actual zero writes (discard_on_zeroed_write_same). /dev/urandom is
# CPU-bound under the default CPU cap. fio libaio with scrambled buffers is stored
# as real used capacity and can issue multiple outstanding IOs.
FILL_MODES = ("zero", "random", "incompressible")

# fio 3.21 in fedora:fio is OOMKilled at 1Gi. incompressible mode raises the
# caller-supplied resources to at least these values; larger requests are kept.
INCOMPRESSIBLE_MIN_CPU_REQUEST = "500m"
INCOMPRESSIBLE_MIN_CPU_LIMIT = "2"
INCOMPRESSIBLE_MIN_MEM_REQUEST = "512Mi"
INCOMPRESSIBLE_MIN_MEM_LIMIT = "2Gi"


def _fio_size_from_pvc_storage(storage):
    """92% of a Gi PVC, so fio --size fits on the filesystem (fill_fs OOMs this image)."""
    text = str(storage).strip().lower()
    if not text.endswith("gi"):
        raise ValueError(f"incompressible fill expects storage in Gi, got {storage!r}")
    gib = float(text[:-2])
    return f"{max(1, int(gib * 1024 * 0.92))}M"


def _cpu_millicores(quantity):
    """Convert a Kubernetes CPU quantity to millicores."""
    text = str(quantity).strip().lower()
    if text.endswith("m"):
        return float(text[:-1])
    return float(text) * 1000


def _memory_bytes(quantity):
    """Convert a Kubernetes binary memory quantity (Ki/Mi/Gi/Ti) to bytes."""
    return convert_device_size(str(quantity).strip(), "BY", convert_size=1024)


def _at_least(value, floor, to_number):
    """Return value when it already meets the floor; otherwise return floor."""
    return value if to_number(value) >= to_number(floor) else floor


def _apply_incompressible_resource_floors(
    cpu_request, cpu_limit, mem_request, mem_limit
):
    """
    Raise incompressible FillPoolJob CPU/memory to the measured fio floors.

    Callers may pass larger values; those are kept. If a floored request would
    exceed its limit, the limit is raised to match so the pod spec stays valid.
    """
    cpu_request = _at_least(
        cpu_request, INCOMPRESSIBLE_MIN_CPU_REQUEST, _cpu_millicores
    )
    cpu_limit = _at_least(cpu_limit, INCOMPRESSIBLE_MIN_CPU_LIMIT, _cpu_millicores)
    mem_request = _at_least(mem_request, INCOMPRESSIBLE_MIN_MEM_REQUEST, _memory_bytes)
    mem_limit = _at_least(mem_limit, INCOMPRESSIBLE_MIN_MEM_LIMIT, _memory_bytes)
    if _cpu_millicores(cpu_request) > _cpu_millicores(cpu_limit):
        cpu_limit = cpu_request
    if _memory_bytes(mem_request) > _memory_bytes(mem_limit):
        mem_limit = mem_request
    return cpu_request, cpu_limit, mem_request, mem_limit


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

        Args:
            fill_mode (str): How to generate write data:
                'zero' - dd from /dev/zero (does not increase Ceph used-raw on RBD).
                'random' - dd from /dev/urandom (slow; CPU-bound).
                'incompressible' - fio libaio write (queue depth 16). Prefer this
                to increase Ceph used-raw capacity on RBD.
            cpu_request (str): CPU request. Incompressible mode raises this to
                at least 500m.
            cpu_limit (str): CPU limit. Incompressible mode raises this to
                at least 2.
            mem_request (str): Memory request (Ki/Mi/Gi/Ti). Incompressible
                mode raises this to at least 512Mi.
            mem_limit (str): Memory limit (Ki/Mi/Gi/Ti). Incompressible mode
                raises this to at least 2Gi (fio 3.21 OOMKills below that).
        """
        self.name = name or create_unique_resource_name("fill-pool", "job")
        sc_name = sc_name or constants.DEFAULT_STORAGECLASS_RBD
        proj_obj = helpers.create_project()
        self.namespace = proj_obj.namespace

        if fill_mode not in FILL_MODES:
            raise ValueError(f"fill_mode must be one of {FILL_MODES}")

        if fill_mode == "incompressible":
            cpu_request, cpu_limit, mem_request, mem_limit = (
                _apply_incompressible_resource_floors(
                    cpu_request, cpu_limit, mem_request, mem_limit
                )
            )

        log.info(
            f"Creating FillPoolJob {self.name} fill_mode={fill_mode} "
            f"storage={storage} block_size={block_size} "
            f"cpu={cpu_request}/{cpu_limit} memory={mem_request}/{mem_limit}"
        )

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
        container["image"] = constants.FEDORA_FIO_IMAGE

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

        fill_cmd = self._build_fill_command(fill_mode, block_size, storage)
        container["command"] = ["sh", "-c", fill_cmd]
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

    @staticmethod
    def _build_fill_command(fill_mode, block_size, storage="50Gi"):
        """
        Build the container shell command for the given fill mode.

        incompressible: fio libaio, iodepth 16, scrambled buffers, explicit --size.
        ENOSPC is treated as success for every mode.
        """
        bs = f"${{BLOCK_SIZE:-{block_size}}}"
        enospc_handler = (
            "EXIT_STATUS=$?; "
            "if [ $EXIT_STATUS -ne 0 ] && grep -qE "
            "'No space left on device|ENOSPC|OS error: 28' /tmp/fill_err /tmp/fill_out; then "
            "  cat /tmp/fill_out /tmp/fill_err; "
            "  echo 'Capacity reached. Exiting successfully.'; exit 0; "
            "fi; "
            "cat /tmp/fill_out /tmp/fill_err; exit $EXIT_STATUS"
        )
        if fill_mode == "incompressible":
            fio_size = _fio_size_from_pvc_storage(storage)
            return (
                'echo "Filling PVC with incompressible fio data..."; '
                "touch /tmp/fill_out /tmp/fill_err; "
                "fio --name=fill --filename=/mnt/fill/testfile --rw=write "
                f"--bs={bs} --size={fio_size} --direct=1 --ioengine=libaio "
                "--iodepth=16 --scramble_buffers=1 --end_fsync=1 "
                "--output=/tmp/fill_out 2>/tmp/fill_err; "
                f"{enospc_handler}"
            )
        input_source = "/dev/zero" if fill_mode == "zero" else "/dev/urandom"
        return (
            f'echo "Filling PVC with {fill_mode} data..."; '
            "touch /tmp/fill_out /tmp/fill_err; "
            f"dd if={input_source} of=/mnt/fill/testfile bs={bs} "
            f">/tmp/fill_out 2>/tmp/fill_err; "
            f"{enospc_handler}"
        )

    def wait_for_completion(self, timeout=3600, sleep=30):
        """
        Wait for the Fill Pool Job Pod to complete.
        """
        if not self.pod_obj:
            raise RuntimeError("Fill Pool Job Pod object is not available")

        log.info(f"Waiting for Fill Pool Job Pod {self.pod_obj.name} to complete...")
        self.pod_obj.ocp.wait_for_resource(
            condition=constants.STATUS_COMPLETED,
            resource_name=self.pod_obj.name,
            timeout=timeout,
            sleep=sleep,
        )
        log.info(f"Fill Pool Job Pod {self.pod_obj.name} has completed.")

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
