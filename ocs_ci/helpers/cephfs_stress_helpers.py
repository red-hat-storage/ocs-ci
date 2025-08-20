import logging

from ocs_ci.helpers import helpers
from ocs_ci.ocs.constants import SMALLFILE_STRESS_YAML, STATUS_RUNNING
from ocs_ci.utility import templating
from ocs_ci.ocs.resources import pod


logger = logging.getLogger(__name__)


def create_cephfs_stress_project(project_name):
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

    cephfs_stress_pod_data = templating.load_yaml(SMALLFILE_STRESS_YAML)
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
