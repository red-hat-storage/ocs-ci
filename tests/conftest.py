import json
import logging
import os
import time

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    deployment, destroy, ignore_leftovers
)
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.exceptions import CommandFailed, CephHealthException
from ocs_ci.ocs.openshift_ops import OCP
from ocs_ci.ocs.parallel import parallel
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.utils import create_oc_resource, apply_oc_resource
from ocs_ci.utility import templating, system
from ocs_ci.utility.aws import AWS
from ocs_ci.utility.environment_check import (
    get_status_before_execution, get_status_after_execution
)
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    destroy_cluster, run_cmd, get_openshift_installer, get_openshift_client,
    is_cluster_running
)
from tests import helpers

log = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def polarion_testsuite_properties(record_testsuite_property):
    """
    Configures polarion testsuite properties for junit xml
    """
    polarion_project_id = config.REPORTING['polarion']['project_id']
    record_testsuite_property('polarion-project-id', polarion_project_id)
    jenkins_build_url = config.RUN.get('jenkins_build_url')
    if jenkins_build_url:
        record_testsuite_property(
            'polarion-custom-description', jenkins_build_url
        )


def cluster_teardown():
    log.info("Destroying the test cluster")
    destroy_cluster(config.ENV_DATA['cluster_path'])
    log.info("Destroying the test cluster complete")


@pytest.fixture(scope="session", autouse=True)
def cluster(request):
    log.info("Running OCS basic installation")
    cluster_path = config.ENV_DATA['cluster_path']
    deploy = config.RUN['cli_params']['deploy']
    teardown = config.RUN['cli_params']['teardown']
    # Add a finalizer to teardown the cluster after test execution is finished
    if teardown:
        request.addfinalizer(cluster_teardown)
        log.info("Will teardown cluster because --teardown was provided")
    # Test cluster access and if exist just skip the deployment.
    if is_cluster_running(cluster_path):
        log.info("The installation is skipped because the cluster is running")
        return
    elif teardown and not deploy:
        log.info("Attempting teardown of non-accessible cluster: %s", cluster_path)
        return
    elif not deploy and not teardown:
        msg = "The given cluster can not be connected to: {}. ".format(cluster_path)
        msg += "Provide a valid --cluster-path or use --deploy to deploy a new cluster"
        pytest.fail(msg)
    elif not system.is_path_empty(cluster_path) and deploy:
        msg = "The given cluster path is not empty: {}. ".format(cluster_path)
        msg += "Provide an empty --cluster-path and --deploy to deploy a new cluster"
        pytest.fail(msg)
    else:
        log.info("A testing cluster will be deployed and cluster information stored at: %s", cluster_path)

    # Generate install-config from template
    log.info("Generating install-config")
    run_cmd(f"mkdir -p {cluster_path}")
    pull_secret_path = os.path.join(
        constants.TOP_DIR,
        "data",
        "pull-secret"
    )

    # TODO: check for supported platform and raise the exception if not
    # supported. Currently we support just AWS.

    _templating = templating.Templating()
    install_config_str = _templating.render_template(
        "install-config.yaml.j2", config.ENV_DATA
    )
    # Log the install config *before* adding the pull secret, so we don't leak
    # sensitive data.
    log.info(f"Install config: \n{install_config_str}")
    # Parse the rendered YAML so that we can manipulate the object directly
    install_config_obj = yaml.safe_load(install_config_str)
    with open(pull_secret_path, "r") as f:
        # Parse, then unparse, the JSON file.
        # We do this for two reasons: to ensure it is well-formatted, and
        # also to ensure it ends up as a single line.
        install_config_obj['pullSecret'] = json.dumps(json.loads(f.read()))
    install_config_str = yaml.safe_dump(install_config_obj)
    install_config = os.path.join(cluster_path, "install-config.yaml")
    with open(install_config, "w") as f:
        f.write(install_config_str)

    # Download installer
    installer = get_openshift_installer(
        config.DEPLOYMENT['installer_version']
    )
    # Download client
    get_openshift_client()

    # Deploy cluster
    log.info("Deploying cluster")
    run_cmd(
        f"{installer} create cluster "
        f"--dir {cluster_path} "
        f"--log-level debug"
    )

    # Test cluster access
    if not OCP.set_kubeconfig(
        os.path.join(cluster_path, config.RUN.get('kubeconfig_location'))
    ):
        pytest.fail("Cluster is not available!")

    # TODO: Create cluster object, add to config.ENV_DATA for other tests to
    # utilize.
    # Determine worker pattern and create ebs volumes
    with open(os.path.join(cluster_path, "terraform.tfvars")) as f:
        tfvars = json.load(f)

    cluster_id = tfvars['cluster_id']
    worker_pattern = f'{cluster_id}-worker*'
    log.info(f'Worker pattern: {worker_pattern}')
    create_ebs_volumes(worker_pattern, region_name=config.ENV_DATA['region'])

    # render templates and create resources
    create_oc_resource('common.yaml', cluster_path, _templating, config.ENV_DATA)
    run_cmd(
        f'oc label namespace {config.ENV_DATA["cluster_namespace"]} '
        f'"openshift.io/cluster-monitoring=true"'
    )
    run_cmd(
        f"oc policy add-role-to-user view "
        f"system:serviceaccount:openshift-monitoring:prometheus-k8s "
        f"-n {config.ENV_DATA['cluster_namespace']}"
    )
    apply_oc_resource(
        'csi-nodeplugin-rbac_rbd.yaml',
        cluster_path,
        _templating,
        config.ENV_DATA,
        template_dir="ocs-deployment/csi/rbd/"
    )
    apply_oc_resource(
        'csi-provisioner-rbac_rbd.yaml',
        cluster_path,
        _templating,
        config.ENV_DATA,
        template_dir="ocs-deployment/csi/rbd/"
    )
    apply_oc_resource(
        'csi-nodeplugin-rbac_cephfs.yaml',
        cluster_path,
        _templating,
        config.ENV_DATA,
        template_dir="ocs-deployment/csi/cephfs/"
    )
    apply_oc_resource(
        'csi-provisioner-rbac_cephfs.yaml',
        cluster_path,
        _templating,
        config.ENV_DATA,
        template_dir="ocs-deployment/csi/cephfs/"
    )
    # Increased to 15 seconds as 10 is not enough
    # TODO: do the sampler function and check if resource exist
    wait_time = 15
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    create_oc_resource(
        'operator-openshift-with-csi.yaml', cluster_path, _templating, config.ENV_DATA
    )
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    run_cmd(
        f"oc wait --for condition=ready pod "
        f"-l app=rook-ceph-operator "
        f"-n {config.ENV_DATA['cluster_namespace']} "
        f"--timeout=120s"
    )
    run_cmd(
        f"oc wait --for condition=ready pod "
        f"-l app=rook-discover "
        f"-n {config.ENV_DATA['cluster_namespace']} "
        f"--timeout=120s"
    )
    create_oc_resource('cluster.yaml', cluster_path, _templating, config.ENV_DATA)

    POD = ocp.OCP(kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace'])
    CFS = ocp.OCP(
        kind=constants.CEPHFILESYSTEM, namespace=config.ENV_DATA['cluster_namespace']
    )

    # Check for the Running status of Ceph Pods
    run_cmd(
        f"oc wait --for condition=ready pod "
        f"-l app=rook-ceph-agent "
        f"-n {config.ENV_DATA['cluster_namespace']} "
        f"--timeout=120s"
    )
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mon',
        resource_count=3, timeout=600
    )
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-mgr',
        timeout=600
    )
    assert POD.wait_for_resource(
        condition='Running', selector='app=rook-ceph-osd',
        resource_count=3, timeout=600
    )

    create_oc_resource('toolbox.yaml', cluster_path, _templating, config.ENV_DATA)
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    create_oc_resource(
        'storage-manifest.yaml', cluster_path, _templating, config.ENV_DATA
    )
    create_oc_resource(
        "service-monitor.yaml", cluster_path, _templating, config.ENV_DATA
    )
    create_oc_resource(
        "prometheus-rules.yaml", cluster_path, _templating, config.ENV_DATA
    )
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)

    # Create MDS pods for CephFileSystem
    fs_data = templating.load_yaml_to_dict(constants.CEPHFILESYSTEM_YAML)
    fs_data['metadata']['namespace'] = config.ENV_DATA['cluster_namespace']

    ceph_obj = OCS(**fs_data)
    ceph_obj.create()
    assert POD.wait_for_resource(
        condition=constants.STATUS_RUNNING, selector='app=rook-ceph-mds',
        resource_count=2, timeout=600
    )

    # Check for CephFilesystem creation in ocp
    cfs_data = CFS.get()
    cfs_name = cfs_data['items'][0]['metadata']['name']

    if helpers.validate_cephfilesystem(cfs_name):
        log.info(f"MDS deployment is successful!")
        defaults.CEPHFILESYSTEM_NAME = cfs_name
    else:
        log.error(
            f"MDS deployment Failed! Please check logs!"
        )

    # Verify health of ceph cluster
    # TODO: move destroy cluster logic to new CLI usage pattern?
    log.info("Done creating rook resources, waiting for HEALTH_OK")
    assert ceph_health_check(namespace=config.ENV_DATA['cluster_namespace'])


def create_ebs_volumes(
    worker_pattern,
    size=100,
    region_name=None,
):
    """
    Create volumes on workers

    Args:
        worker_pattern (string): Worker name pattern e.g.:
            cluster-55jx2-worker*
        size (int): Size in GB (default: 100)
        region_name (str): Region name (default: config.ENV_DATA['region'])
    """
    aws = AWS(region_name)
    region_name = region_name or config.ENV_DATA['region']
    worker_instances = aws.get_instances_by_name_pattern(worker_pattern)
    with parallel() as p:
        for worker in worker_instances:
            log.info(
                f"Creating and attaching {size} GB volume to {worker['name']}"
            )
            p.spawn(
                aws.create_volume_and_attach,
                availability_zone=worker['avz'],
                instance_id=worker['id'],
                name=f"{worker['name']}_extra_volume",
                size=size,
            )


@retry((CephHealthException, CommandFailed), tries=20, delay=30, backoff=1)
def ceph_health_check(namespace=None):
    """
    Exec `ceph health` cmd on tools pod to determine health of cluster.

    Args:
        namespace (str): Namespace of OCS
            (default: config.ENV_DATA['cluster_namespace'])

    Raises:
        CephHealthException: If the ceph health returned is not HEALTH_OK
        CommandFailed: If the command to retrieve the tools pod name or the
            command to get ceph health returns a non-zero exit code
    Returns:
        boolean: True if HEALTH_OK

    """
    namespace = namespace or config.ENV_DATA['cluster_namespace']
    run_cmd(
        f"oc wait --for condition=ready pod "
        f"-l app=rook-ceph-tools "
        f"-n {namespace} "
        f"--timeout=120s"
    )
    tools_pod = run_cmd(
        f"oc -n {namespace} get pod -l 'app=rook-ceph-tools' "
        f"-o jsonpath='{{.items[0].metadata.name}}'"
    )
    health = run_cmd(f"oc -n {namespace} exec {tools_pod} ceph health")
    if health.strip() == "HEALTH_OK":
        log.info("HEALTH_OK, install successful.")
        return True
    else:
        raise CephHealthException(
            f"Ceph cluster health is not OK. Health: {health}"
        )


@pytest.fixture(scope='class')
def environment_checker(request):
    return
    node = request.node
    # List of marks for which we will ignore the leftover checker
    marks_to_ignore = [m.mark for m in [deployment, destroy, ignore_leftovers]]
    for mark in node.iter_markers():
        if mark in marks_to_ignore:
            return

    request.addfinalizer(get_status_after_execution)
    get_status_before_execution()
