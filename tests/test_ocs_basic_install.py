import logging
import os
import platform
import random
import subprocess
import sys

import requests
import yaml
from ocs.exceptions import UnsupportedOSType
from jinja2 import Environment, FileSystemLoader
from ocs.exceptions import CommandFailed

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
    cluster_dir_parent = "/tmp"  # TODO: determine better place to create cluster directories, perhaps in project dir?
    if test_data.get('cluster-name'):
        cluster_name = test_data.get('cluster-name')
    else:
        cid = random.randint(10000, 99999)
        cluster_name = f'ocs-ci-cluster-{cid}'
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
        ver = config.get('installer-version', '4.1.0-rc.0')
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
    run_cmd(f"./openshift-install create cluster --dir {cluster_path} --log-level debug")

    # Test cluster access
    log.info("Testing access to cluster")
    os.environ['KUBECONFIG'] = f"{cluster_path}/auth/kubeconfig"
    run_cmd("oc cluster-info")

    # TODO: Create cluster object, add to test_data for other tests to utilize
    # TODO: Use Rook to install ceph on the cluster

    # Destroy cluster (if configured)
    destroy_cmd = f"./openshift-install destroy cluster --dir {cluster_path} --log-level debug"
    if config.get("destroy-cluster"):
        log.info("Destroying cluster")
        # run this twice to ensure all resources are destroyed
        run_cmd(destroy_cmd)
        run_cmd(destroy_cmd)
        log.info(f"Removing cluster directory: {cluster_path}")
        os.remove(cluster_path)
        os.remove(installer_filename)
        os.remove(tarball)
    else:
        log.info(f"Cluster directory is located here: {cluster_path}")
        log.info(f"Skipping cluster destroy. To manually destroy the cluster execute the following cmd: {destroy_cmd}")

    return 0


def run_cmd(cmd, **kwargs):
    """
    Run an arbitrary command locally

    Args:
        cmd: command to run

    Raises:
        CommandFailed: In case the command execution fails
    """
    log.info(f"Executing command: {cmd}")
    r = subprocess.run(cmd.split(), stdout=sys.stdout, stderr=sys.stderr, **kwargs)
    if r.returncode != 0:
        raise CommandFailed(f"Error during execution of command: {cmd}")


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


def render_template(template_path, data):
    """
    Render a template with the given data.

    Args:
        template_path: location of the j2 template
        data: the data to be formatted into the template

    Returns: rendered template

    """
    j2_env = Environment(loader=FileSystemLoader(os.path.join(TOP_DIR, 'templates')), trim_blocks=True)
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
