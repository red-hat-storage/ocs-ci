import logging

from ocs_ci.helpers import helpers
from ocs_ci.ocs.constants import CEPHFS_STRESS_YAML, STATUS_RUNNING
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import pod


logger = logging.getLogger(__name__)


def create_cephfs_stress_project(project_name):
    """
    Create a new CephFS stress test project

    Args:
       project_name (str): Project name to be created

    Returns:
        ocs_ci.ocs.ocp.OCP: Project object

    """
    proj_obj = helpers.create_project(project_name=project_name)
    return proj_obj


def create_cephfs_stress_pod(
    base_dir=None,
    num_files=None,
    files_size=None,
    operations=None,
    base_file_count=None,
    multiplication_factor=None,
    threads=None,
):
    """
    Creates a CephFS stress pod, utilizing smallfiles to generate numerous files and directories.
    The pod is configured with various parameters to stress CephFS, it
    gradually increases load on CephFS in incremental stages.

    Args:
        base_dir (str, optional): Directory used by smallfile to perform file and directory
        operations (e.g., append, stat, chmod, ls-l, etc.)
        num_files (str, optional): Total number of files to create
        files_size (str, optional): Size of each file in KB
        operations (str, optional): File operations to perform (e.g., create, read, delete),
        Pass as a comma-separated string
        base_file_count (str, optional): Base file count, to multiply with scaling factor
        multiplication_factor (str, optional): Dynamic scaling of file creation
          - base_file_count * MULTIPLICATION_FACTORS
        threads (str, optional): Number of threads to use for the operation.

    Returns:
        pod_obj: The created Pod object after it's in a running state

    Raises:
        AssertionError: If the pod creation fails

    """
    cephfs_stress_pod_data = templating.load_yaml(CEPHFS_STRESS_YAML)
    namespace = create_cephfs_stress_project(project_name="cephfs-stress-project")
    cephfs_stress_pod_data["metadata"]["namespace"] = namespace

    if base_dir:
        cephfs_stress_pod_data["spec"]["containers"][0]["env"][0]["value"] = str(
            base_dir
        )
    if num_files:
        cephfs_stress_pod_data["spec"]["containers"][0]["env"][1]["value"] = str(
            num_files
        )
    if files_size:
        cephfs_stress_pod_data["spec"]["containers"][0]["env"][2]["value"] = str(
            files_size
        )
    if operations:
        cephfs_stress_pod_data["spec"]["containers"][0]["env"][3]["value"] = str(
            operations
        )
    if base_file_count:
        cephfs_stress_pod_data["spec"]["containers"][0]["env"][4]["value"] = str(
            base_file_count
        )
    if multiplication_factor:
        cephfs_stress_pod_data["spec"]["containers"][0]["env"][5]["value"] = str(
            multiplication_factor
        )
    if threads:
        cephfs_stress_pod_data["spec"]["containers"][0]["env"][7]["value"] = str(
            threads
        )

    cephfs_stress_pod_obj = pod.Pod(**cephfs_stress_pod_data)
    logger.info("Creating Cephfs stress pod")
    created_resource = cephfs_stress_pod_obj.create()
    assert created_resource, f"Failed to create Pod {cephfs_stress_pod_obj.name}"

    logger.info("Waiting for Cephfs stress pod to start")
    helpers.wait_for_resource_state(
        cephfs_stress_pod_obj, state=STATUS_RUNNING, timeout=300
    )

    return cephfs_stress_pod_obj
