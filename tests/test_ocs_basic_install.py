import json
import logging
import os
import random
import time
from copy import deepcopy

import ocs.defaults as default
from oc.openshift_ops import OCP
from ocs.exceptions import CommandFailed, CephHealthException
from ocs.utils import create_oc_resource
from ocsci.enums import TestStatus
from utility import templating
from utility.aws import AWS
from utility.retry import retry
from utility.utils import run_cmd, get_openshift_installer, get_openshift_client
from ocs.parallel import parallel

log = logging.getLogger(__name__)


def run(**kwargs):
    log.info("Running OCS basic installation")
    test_data = kwargs.get('test_data')
    cluster_path = test_data.get('cluster-path')
    # Test cluster access and if exist just skip the deployment.
    if cluster_path and OCP.set_kubeconfig(
        os.path.join(cluster_path, default.KUBECONFIG_LOCATION)
    ):
        return TestStatus.SKIPPED
    config = kwargs.get('config')
    cluster_conf = kwargs.get('cluster_conf', {})

    env_data = deepcopy(default.ENV_DATA)
    custom_env_data = cluster_conf.get('env_data', {})
    # Generate install-config from template
    log.info("Generating install-config")
    # TODO: determine better place to create cluster directories - (log dir?)
    cluster_dir_parent = "/tmp"
    cluster_name = test_data.get('cluster-name')
    base_cluster_name = test_data.get('cluster-name', default.CLUSTER_NAME)
    cid = random.randint(10000, 99999)
    if not (cluster_name and cluster_path):
        cluster_name = f"{base_cluster_name}-{cid}"
    if not cluster_path:
        cluster_path = os.path.join(cluster_dir_parent, cluster_name)
    run_cmd(f"mkdir -p {cluster_path}")
    pull_secret_path = os.path.join(templating.TOP_DIR, "data", "pull-secret")
    with open(pull_secret_path, "r") as f:
        pull_secret = f.readline()
    custom_env_data.update(
        {
            'pull_secret': pull_secret,
            'cluster_name': cluster_name,
        }
    )
    if custom_env_data:
        env_data.update(custom_env_data)

    # TODO: check for supported platform and raise the exception if not
    # supported. Currently we support just AWS.

    _templating = templating.Templating()
    template = _templating.render_template(
        "install-config.yaml.j2", env_data
    )
    log.info(f"Install config: \n{template}")
    install_config = os.path.join(cluster_path, "install-config.yaml")
    with open(install_config, "w") as f:
        f.write(template)

    # Download installer and client
    installer = get_openshift_installer(
        version=config.get('installer-version', default.INSTALLER_VERSION)
    )
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
        os.path.join(cluster_path, default.KUBECONFIG_LOCATION)
    ):
        return TestStatus.FAILED

    # TODO: Create cluster object, add to test_data for other tests to utilize
    # Determine worker pattern and create ebs volumes
    with open(os.path.join(cluster_path, "terraform.tfvars")) as f:
        tfvars = json.load(f)

    cluster_id = tfvars['cluster_id']
    worker_pattern = f'{cluster_id}-worker*'
    log.info(f'Worker pattern: {worker_pattern}')
    create_ebs_volumes(worker_pattern, region_name=env_data['region'])

    # render templates and create resources
    create_oc_resource('common.yaml', cluster_path, _templating, env_data)
    run_cmd(
        f'oc label namespace {env_data["cluster_namespace"]} '
        f'"openshift.io/cluster-monitoring=true"'
    )
    run_cmd(
        f"oc policy add-role-to-user view "
        f"system:serviceaccount:openshift-monitoring:prometheus-k8s "
        f"-n {env_data['cluster_namespace']}"
    )
    create_oc_resource(
        'operator-openshift.yaml', cluster_path, _templating, env_data
    )
    # Increased to 10 seconds as 5 is not enough
    # TODO: do the sampler function and check if resource exist
    wait_time = 10
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    run_cmd(
        f"oc wait --for condition=ready pod "
        f"-l app=rook-ceph-operator "
        f"-n {env_data['cluster_namespace']} "
        f"--timeout=120s"
    )
    run_cmd(
        f"oc wait --for condition=ready pod "
        f"-l app=rook-ceph-agent "
        f"-n {env_data['cluster_namespace']} "
        f"--timeout=120s"
    )
    run_cmd(
        f"oc wait --for condition=ready pod "
        f"-l app=rook-discover "
        f"-n {env_data['cluster_namespace']} "
        f"--timeout=120s"
    )
    create_oc_resource('cluster.yaml', cluster_path, _templating, env_data)
    create_oc_resource('toolbox.yaml', cluster_path, _templating, env_data)
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    create_oc_resource(
        'storage-manifest.yaml', cluster_path, _templating, env_data
    )
    create_oc_resource(
        "service-monitor.yaml", cluster_path, _templating, env_data
    )
    create_oc_resource(
        "prometheus-rules.yaml", cluster_path, _templating, env_data
    )

    # Verify health of ceph cluster
    # TODO: move destroy cluster logic to new CLI usage pattern?
    log.info("Done creating rook resources, waiting for HEALTH_OK")
    rc = ceph_health_check(namespace=env_data['cluster_namespace'])

    return rc


def create_ebs_volumes(
    worker_pattern,
    size=100,
    region_name=default.AWS_REGION
):
    """
    Create volumes on workers

    Args:
        worker_pattern (string): Worker name pattern e.g.:
            cluster-55jx2-worker*
        size (int): Size in GB (default: 100)
        region_name (str): Region name (default: default.AWS_REGION)
    """
    aws = AWS(region_name)
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
def ceph_health_check(namespace=default.ROOK_CLUSTER_NAMESPACE):
    """
    Exec `ceph health` cmd on tools pod to determine health of cluster.

    Args:
        namespace (str): Namespace of OCS (default:
            default.ROOK_CLUSER_NAMESPACE)

    Raises:
        CephHealthException: If the ceph health returned is not HEALTH_OK
        CommandFailed: If the command to retrieve the tools pod name or the
            command to get ceph health returns a non-zero exit code
    Returns:
        0 if HEALTH_OK

    """
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
        return 0
    else:
        raise CephHealthException(
            f"Ceph cluster health is not OK. Health: {health}"
        )
