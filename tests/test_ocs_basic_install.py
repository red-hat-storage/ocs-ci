import json
import logging
import os
import platform
import random
import shlex
import subprocess
import time

import ocs.defaults as default
import requests
import yaml
from jinja2 import Environment, FileSystemLoader
from ocs.exceptions import CommandFailed

from ocs.exceptions import CommandFailed, CephHealthException
from ocs.exceptions import UnsupportedOSType
from utility.aws import AWS
from utility.retry import retry

log = logging.getLogger(__name__)

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TOP_DIR = os.path.dirname(THIS_DIR)


def run(**kwargs):
    log.info("Running OCS basic installation")
    config = kwargs.get('config')
    test_data = kwargs.get('test_data')
    cluster_conf = kwargs.get('cluster_conf')

    workers, masters = None, None
    if cluster_conf:
        workers = cluster_conf.get('aws').get('cluster').get('workers')
        masters = cluster_conf.get('aws').get('cluster').get('masters')

    # Generate install-config from template
    log.info("Generating install-config")
    # TODO: determine better place to create cluster directories - (log dir?)
    cluster_dir_parent = "/tmp"
    base_name = test_data.get('cluster-name', 'ocs-ci-cluster')
    cid = random.randint(10000, 99999)
    cluster_name = f'{base_name}-{cid}'

    cluster_path = os.path.join(cluster_dir_parent, cluster_name)
    run_cmd(f"mkdir {cluster_path}")

    pull_secret_path = os.path.join(TOP_DIR, "data", "pull-secret")
    with open(pull_secret_path, "r") as f:
        pull_secret = f.readline()

    data = {"cluster_name": cluster_name,
            "pull_secret": pull_secret}
    if workers:
        data.update({'worker_replicas': workers})
    if masters:
        data.update({'master_replicas': masters})
    template = render_template("install-config.yaml.j2", data)
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
    log.info("Testing access to cluster")
    os.environ['KUBECONFIG'] = f"{cluster_path}/auth/kubeconfig"
    run_cmd("oc cluster-info")

    # TODO: Create cluster object, add to test_data for other tests to utilize
    # Determine worker pattern and create eb2 volumes
    with open(os.path.join(cluster_path, "terraform.tfvars")) as f:
        tfvars = json.load(f)

    cluster_id = tfvars['cluster_id']
    worker_pattern = f'{cluster_id}-worker*'
    log.info(f'Worker pattern: {worker_pattern}')
    create_eb2_volumes(worker_pattern)

    # Use Rook to install Ceph cluster
    # retrieve rook config from cluster_conf
    rook_data = {}
    if cluster_conf:
        rook_data = cluster_conf.get('rook', {})

    # render templates and create resources
    create_rook_resource('common.yaml', rook_data, cluster_path)
    run_cmd(
        'oc label namespace openshift-storage '
        '"openshift.io/cluster-monitoring=true"'
    )
    run_cmd(
        "oc policy add-role-to-user view "
        "system:serviceaccount:openshift-monitoring:prometheus-k8s "
        "-n openshift-storage"
    )
    create_rook_resource('operator-openshift.yaml', rook_data, cluster_path)
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
    create_rook_resource('cluster.yaml', rook_data, cluster_path)
    create_rook_resource('toolbox.yaml', rook_data, cluster_path)
    log.info(f"Waiting {wait_time} seconds...")
    time.sleep(wait_time)
    create_rook_resource('storage-manifest.yaml', rook_data, cluster_path)
    create_rook_resource("service-monitor.yaml", rook_data, cluster_path)
    create_rook_resource("prometheus-rules.yaml", rook_data, cluster_path)

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


def run_cmd(cmd, **kwargs):
    """
    Run an arbitrary command locally

    Args:
        cmd: command to run

<<<<<<< HEAD
    Raises:
        CommandFailed: In case the command execution fails
=======
    Returns:
        decoded stdout of command
>>>>>>> da29d2d... Verify health of ceph cluster after deployment
    """
    log.info(f"Executing command: {cmd}")
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    r = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        **kwargs
    )
    log.debug(f"CMD output: {r.stdout.decode()}")
    if r.stderr:
        log.error(f"CMD error:: {r.stderr.decode()}")
    if r.returncode:
        raise CommandFailed(
            f"Error during execution of command: {cmd}"
        )
    return r.stdout.decode()


def download_file(url, filename):
    """
    Download a file from a specified url

    Args:
        url: URL of the file to download
        filename: Name of the file to write the download to


    """
    with open(filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    assert r.ok


def to_nice_yaml(a, indent=2, *args, **kw):
    """Make verbose, human readable yaml"""
    # TODO: elaborate more in docstring on what this actually does
    transformed = yaml.dump(
        a,
        Dumper=yaml.Dumper,
        indent=indent,
        allow_unicode=True,
        default_flow_style=False,
        **kw
    )
    return transformed


def render_template(template_path, data):
    """
    Render a template with the given data.

    Args:
        template_path: location of the j2 template
        data: the data to be formatted into the template

    Returns: rendered template

    """
    j2_env = Environment(
        loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')),
        trim_blocks=True
    )
    j2_env.filters['to_nice_yaml'] = to_nice_yaml
    j2_template = j2_env.get_template(template_path)
    return j2_template.render(**data)


def load_config_data(data_path):
    """
    Loads YAML data from the specified path

    Args:
        data_path: location of the YAML data file

    Returns: loaded YAML data

    """
    with open(data_path, "r") as data_descriptor:
        return yaml.load(data_descriptor, Loader=yaml.FullLoader)


def create_rook_resource(template_name, rook_data, cluster_path):
    """
    Create a rook resource after rendering the specified template with
    the rook data from cluster_conf.

    Args:
        template_name: name of the ocs-deployment config template.
        rook_data: rook specific config from cluster_conf
        cluster_path: path to cluster directory, where files will be written
    """
    base_name = template_name.split('.')[0]
    template_path = os.path.join('ocs-deployment', template_name)
    template = render_template(
        template_path,
        rook_data.get(base_name, {})
    )
    cfg_file = os.path.join(cluster_path, template_name)
    with open(cfg_file, "w") as f:
        f.write(template)
    log.info(f"Creating rook resource from {template_name}")
    run_cmd(f"oc create -f {cfg_file}")


def create_eb2_volumes(worker_pattern, size=100):
    """
    Create volumes on workers

    Args:
        worker_pattern (string): worker name pattern e.g.:
            cluster-55jx2-worker*
        size (int): size in GB (default: 100)
    """
    aws = AWS()
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
        CephHealthException: if the ceph health returned is not HEALTH_OK
        CommandFailed: if the command to retrieve the tools pod name or the
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

