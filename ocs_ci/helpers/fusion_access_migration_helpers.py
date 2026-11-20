"""
Helpers for the Fusion Access migration-tool Job workflow.

Extracts ``resources.yaml`` and ``migration-job.yaml`` from a build-provided
ODF CLI / migration-tool image, then applies them to the cluster.

The image must be supplied via ``DEPLOYMENT.migration_tool_image`` (build team
provides the value for custom builds; do not hardcode a release tag).
"""

import logging
import os
import tempfile

import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants
from ocs_ci.ocs.exceptions import ResourceNotFoundError
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.job import wait_for_job_completion
from ocs_ci.utility.utils import create_directory_path, exec_cmd

logger = logging.getLogger(__name__)

MIGRATION_TOOL_RESOURCES_PATH = "/migration-tool/resources.yaml"
MIGRATION_TOOL_JOB_PATH = "/migration-tool/migration-job.yaml"


def get_migration_tool_image():
    """
    Resolve the migration-tool container image from config.

    Returns:
        str: Image pullspec

    Raises:
        ValueError: If ``DEPLOYMENT.migration_tool_image`` is not set
    """
    image = (config.DEPLOYMENT.get("migration_tool_image") or "").strip()
    if not image:
        raise ValueError(
            "DEPLOYMENT.migration_tool_image is required. Provide the image "
            "pullspec from the build team via --ocsci-conf "
            "(see conf/ocsci/fusion_access_migration.yaml)."
        )
    return image


def export_cluster_pull_secret(dest_path=None):
    """
    Export the cluster ``pull-secret`` from ``openshift-config`` as a
    dockerconfig JSON file for ``oc image extract --registry-config``.

    Args:
        dest_path (str): Destination file path. Defaults to a file under
            ``constants.DATA_DIR``.

    Returns:
        str: Absolute path to the written pull-secret JSON
    """
    if dest_path is None:
        create_directory_path(constants.DATA_DIR)
        dest_path = os.path.join(constants.DATA_DIR, "cluster-pull-secret.json")

    logger.info(
        f"Exporting pull-secret from {constants.OPENSHIFT_CONFIG_NAMESPACE} "
        f"to {dest_path}"
    )
    exec_cmd(
        f"oc get secret pull-secret -n {constants.OPENSHIFT_CONFIG_NAMESPACE} "
        f"-ojson | jq -r '.data.\".dockerconfigjson\"|@base64d' > {dest_path}",
        shell=True,
    )
    if not os.path.isfile(dest_path) or os.path.getsize(dest_path) == 0:
        raise FileNotFoundError(f"Failed to export pull-secret to {dest_path}")
    return dest_path


def extract_migration_tool_manifest(
    image, manifest_path_in_image, dest_dir, registry_config, insecure=False
):
    """
    Extract a single file from the migration-tool image via ``oc image extract``.

    Args:
        image (str): Migration-tool image pullspec
        manifest_path_in_image (str): Absolute path inside the image
            (e.g. ``/migration-tool/resources.yaml``)
        dest_dir (str): Local directory to extract into
        registry_config (str): Path to dockerconfig / pull-secret JSON
        insecure (bool): Pass ``--insecure`` to ``oc image extract``

    Returns:
        str: Absolute path to the extracted file

    Raises:
        FileNotFoundError: If the extracted file is missing or empty
    """
    create_directory_path(dest_dir)
    basename = os.path.basename(manifest_path_in_image)
    dest_file = os.path.join(dest_dir, basename)

    cmd = (
        f"oc image extract --filter-by-os linux/amd64 "
        f"--registry-config {registry_config} {image} --confirm "
        f"--path {manifest_path_in_image}:{dest_dir}"
    )
    if insecure:
        cmd = f"{cmd} --insecure"

    logger.info(f"Extracting '{manifest_path_in_image}' from image '{image}'")
    exec_cmd(cmd, timeout=300)

    if not os.path.isfile(dest_file) or os.path.getsize(dest_file) == 0:
        raise FileNotFoundError(
            f"Expected extracted file at {dest_file} after "
            f"'oc image extract' from {image}, but it is missing or empty"
        )
    logger.info(f"Extracted manifest: {dest_file}")
    return dest_file


def apply_manifest(manifest_path):
    """
    Apply a YAML manifest with ``oc apply -f``.

    Args:
        manifest_path (str): Path to a YAML file
    """
    logger.info(f"Applying manifest: {manifest_path}")
    exec_cmd(f"oc apply -f {manifest_path}", timeout=300)


def get_jobs_from_manifest(manifest_path):
    """
    Parse Job resources from a multi-doc YAML manifest.

    Args:
        manifest_path (str): Path to YAML (e.g. migration-job.yaml)

    Returns:
        list[tuple]: List of ``(job_name, namespace)`` pairs
    """
    jobs = []
    with open(manifest_path) as f:
        for doc in yaml.safe_load_all(f):
            if not doc or doc.get("kind") != "Job":
                continue
            metadata = doc.get("metadata") or {}
            name = metadata.get("name")
            if not name:
                continue
            namespace = metadata.get("namespace", "default")
            jobs.append((name, namespace))
    return jobs


def run_fusion_access_migration_tool(
    image=None, work_dir=None, wait_for_jobs=True, job_timeout=1800, insecure=False
):
    """
    End-to-end: export pull-secret, extract migration manifests, apply them,
    optionally wait for Job completion.

    Args:
        image (str): Migration-tool image. Defaults to
            ``DEPLOYMENT.migration_tool_image``.
        work_dir (str): Directory for extracted manifests. Defaults to a
            temporary directory under the cluster path / DATA_DIR.
        wait_for_jobs (bool): Wait for Jobs defined in migration-job.yaml
        job_timeout (int): Timeout in seconds per Job
        insecure (bool): Pass ``--insecure`` to ``oc image extract``

    Returns:
        dict: Paths and discovered jobs, e.g.::

            {
                "image": "...",
                "pull_secret": "...",
                "resources_yaml": "...",
                "migration_job_yaml": "...",
                "jobs": [("job-name", "namespace"), ...],
            }
    """
    image = image or get_migration_tool_image()
    if work_dir is None:
        cluster_path = config.ENV_DATA.get("cluster_path") or constants.DATA_DIR
        work_dir = tempfile.mkdtemp(
            prefix="fa-migration-tool-", dir=os.path.abspath(cluster_path)
        )
    create_directory_path(work_dir)

    pull_secret = export_cluster_pull_secret(
        dest_path=os.path.join(work_dir, "pull-secret.json")
    )

    resources_yaml = extract_migration_tool_manifest(
        image=image,
        manifest_path_in_image=MIGRATION_TOOL_RESOURCES_PATH,
        dest_dir=work_dir,
        registry_config=pull_secret,
        insecure=insecure,
    )
    migration_job_yaml = extract_migration_tool_manifest(
        image=image,
        manifest_path_in_image=MIGRATION_TOOL_JOB_PATH,
        dest_dir=work_dir,
        registry_config=pull_secret,
        insecure=insecure,
    )

    apply_manifest(resources_yaml)
    apply_manifest(migration_job_yaml)

    jobs = get_jobs_from_manifest(migration_job_yaml)
    if not jobs:
        logger.warning(
            f"No Job resources found in {migration_job_yaml}; "
            "skipping job completion wait"
        )
    elif wait_for_jobs:
        for job_name, namespace in jobs:
            logger.info(
                f"Waiting for migration Job '{job_name}' in namespace "
                f"'{namespace}' (timeout={job_timeout}s)"
            )
            # Confirm the Job exists shortly after apply before the long wait
            job_ocp = OCP(kind="Job", namespace=namespace, resource_name=job_name)
            try:
                job_ocp.get()
            except Exception as e:
                raise ResourceNotFoundError(
                    f"Migration Job '{job_name}' not found in '{namespace}' "
                    f"after apply: {e}"
                ) from e
            wait_for_job_completion(
                job_name=job_name,
                namespace=namespace,
                timeout=job_timeout,
            )
            logger.info(f"Migration Job '{job_name}' completed successfully")

    return {
        "image": image,
        "pull_secret": pull_secret,
        "resources_yaml": resources_yaml,
        "migration_job_yaml": migration_job_yaml,
        "jobs": jobs,
        "work_dir": work_dir,
    }
