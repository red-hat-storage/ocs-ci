import json
import logging
import os
import platform
import random
import time

import ocs.defaults as default
from oc.openshift_ops import OCP
from ocs.exceptions import CommandFailed, CephHealthException
from ocs.exceptions import UnsupportedOSType
from ocs.utils import create_oc_resource
from ocsci.enums import TestStatus
from utility import templating
from utility.aws import AWS
from utility.retry import retry
from utility.utils import run_cmd, download_file

log = logging.getLogger(__name__)


def run(**kwargs):
    log.info("Running OCS basic installation")
    config = kwargs.get('config')
    test_data = kwargs.get('test_data')
    cluster_conf = kwargs.get('cluster_conf')

    workers = masters = aws_region = None
    if cluster_conf:
        cluster_details = cluster_conf.get('aws', {}).get('cluster', {})
        workers = cluster_details.get('workers')
        masters = cluster_details.get('masters')
        aws_region = cluster_details.get('region', default.AWS_REGION)

    # Generate install-config from template
    log.info("Generating install-config")
    # TODO: determine better place to create cluster directories - (log dir?)
    cluster_dir_parent = "/tmp"
    cluster_name = test_data.get('cluster-name')
    cluster_path = test_data.get('cluster-path')
    cid = random.randint(10000, 99999)
    if not (cluster_name and cluster_path):
        cluster_name = f"{default.CLUSTER_NAME}-{cid}"
    if not cluster_path:
        cluster_path = os.path.join(cluster_dir_parent, cluster_name)
    # Test cluster access and if exist just skip the deployment.
    if OCP.set_kubeconfig(
        os.path.join(cluster_path, default.KUBECONFIG_LOCATION)
    ):
        return TestStatus.SKIPPED
    run_cmd(f"mkdir -p {cluster_path}")
    pull_secret_path = os.path.join(templating.TOP_DIR, "data", "pull-secret")
    with open(pull_secret_path, "r") as f:
        pull_secret = f.readline()

    data = {
        "cluster_name": cluster_name,
        "pull_secret": pull_secret,
    }
    if workers:
        data.update({'worker_replicas': workers})
    if masters:
        data.update({'master_replicas': masters})
    if aws_region:
        data.update({'region': aws_region})

    _templating = templating.Templating()
    template = _templating.render_template("install-config.yaml.j2", data)
    log.info(f"Install config: \n{template}")
    install_config = os.path.join(cluster_path, "install-config.yaml")
    with open(install_config, "w") as f:
        f.write(template)

    # Download installer
    installer_filename = "openshift-install"
    tarball = f"{installer_filename}.tar.gz"
    if os.path.isfile(installer_filename):
        log.info("Installer exists, skipping download")
    else:
        log.info("Downloading openshift installer")
        ver = config.get('installer-version', default.INSTALLER_VERSION)
        if platform.system() == "Darwin":
            os_type = "mac"
        elif platform.system() == "Linux":
            os_type = "linux"
        else:
            raise UnsupportedOSType
        url = (
            f"https://mirror.openshift.com/pub/openshift-v4/clients/ocp/"
            f"{ver}/openshift-install-{os_type}-{ver}.tar.gz"
        )
        download_file(url, tarball)
        run_cmd(f"tar xzvf {tarball}")

    # Deploy cluster
    log.info("Deploying cluster")
    run_cmd(
        f"./openshift-install create cluster "
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
    region_name = aws_region if aws_region else default.AWS_REGION
    create_ebs_volumes(worker_pattern, region_name=region_name)

    # Use Rook to install Ceph cluster
    # retrieve rook config from cluster_conf
    rook_data = {}
    if cluster_conf:
        rook_data = cluster_conf.get('rook', {})

    # render templates and create resources
    create_oc_resource('common.yaml', rook_data, cluster_path, _templating)
    run_cmd(
        'oc label namespace openshift-storage '
        '"openshift.io/cluster-monitoring=true"'
    )
    run_cmd(
        "oc policy add-role-to-user view "
        "system:serviceaccount:openshift-monitoring:prometheus-k8s "
        "-n openshift-storage"
    )
    create_oc_resource(
        'operator-openshift.yaml', rook_data, cluster_path, _templating
    )
    wait_time = 5
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    run_cmd(
        "oc wait --for condition=ready pod "
        "-l app=rook-ceph-operator "
        "-n openshift-storage "
        "--timeout=120s"
    )
    run_cmd(
        "oc wait --for condition=ready pod "
        "-l app=rook-ceph-agent "
        "-n openshift-storage "
        "--timeout=120s"
    )
    run_cmd(
        "oc wait --for condition=ready pod "
        "-l app=rook-discover "
        "-n openshift-storage "
        "--timeout=120s"
    )
    create_oc_resource('cluster.yaml', rook_data, cluster_path, _templating)
    create_oc_resource('toolbox.yaml', rook_data, cluster_path, _templating)
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    create_oc_resource(
        'storage-manifest.yaml', rook_data, cluster_path, _templating
    )
    create_oc_resource(
        "service-monitor.yaml", rook_data, cluster_path, _templating
    )
    create_oc_resource(
        "prometheus-rules.yaml", rook_data, cluster_path, _templating
    )

    # Verify health of ceph cluster
    # TODO: move destroy cluster logic to new CLI usage pattern?
    log.info("Done creating rook resources, waiting for HEALTH_OK")
    rc = ceph_health_check()

    # Destroy cluster (if configured)
    destroy_cmd = (
        f"./openshift-install destroy cluster "
        f"--dir {cluster_path} "
        f"--log-level debug"
    )
    if config.get("destroy-cluster"):
        log.info("Destroying cluster")
        run_cmd(destroy_cmd)
        # TODO: destroy volumes created
        os.remove(installer_filename)
        os.remove(tarball)
    else:
        log.info(f"Cluster directory is located here: {cluster_path}")
        log.info(
            f"Skipping cluster destroy. "
            f"To manually destroy the cluster execute the following cmd:\n"
            f"{destroy_cmd}"
        )

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
    for worker in worker_instances:
        log.info(
            f"Creating and attaching {size} GB volume to {worker['name']}"
        )
        aws.create_volume_and_attach(
            availability_zone=worker['avz'],
            instance_id=worker['id'],
            name=f"{worker['name']}_extra_volume",
            size=size,
        )


@retry((CephHealthException, CommandFailed), tries=20, delay=30, backoff=1)
def ceph_health_check():
    """
    Exec `ceph health` cmd on tools pod to determine health of cluster.

    Raises:
        CephHealthException: If the ceph health returned is not HEALTH_OK
        CommandFailed: If the command to retrieve the tools pod name or the
            command to get ceph health returns a non-zero exit code
    Returns:
        0 if HEALTH_OK

    """
    # TODO: grab namespace-name from rook data, default to openshift-storage
    namespace = "openshift-storage"
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
