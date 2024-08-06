import datetime
import json
import logging
import os
import pickle
import re
import time
import traceback
import subprocess
import shlex
from subprocess import TimeoutExpired
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from gevent import sleep
from libcloud.common.exceptions import BaseHTTPError
from libcloud.common.types import LibcloudError
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider
from paramiko.ssh_exception import SSHException

from ocs_ci.framework import config as ocsci_config
from ocs_ci.ocs import constants
from ocs_ci.ocs.external_ceph import RolesContainer, Ceph, CephNode
from ocs_ci.ocs.clients import WinNode
from ocs_ci.ocs.exceptions import CommandFailed, ExternalClusterDetailsException
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.openstack import CephVMNode
from ocs_ci.ocs.parallel import parallel
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.utility import templating, version
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import (
    create_directory_path,
    mirror_image,
    run_cmd,
    get_oadp_version,
    get_acm_version,
)
from ocs_ci.utility.version import (
    get_dr_hub_operator_version,
    get_dr_cluster_operator_version,
    get_odf_multicluster_orchestrator_version,
    get_ocp_gitops_operator_version,
    get_submariner_operator_version,
    get_volsync_operator_version,
)


log = logging.getLogger(__name__)


def create_ceph_nodes(cluster_conf, inventory, osp_cred, run_id, instances_name=None):
    osp_glbs = osp_cred.get("globals")
    os_cred = osp_glbs.get("openstack-credentials")
    params = dict()
    ceph_cluster = cluster_conf.get("ceph-cluster")
    if ceph_cluster.get("inventory"):
        inventory_path = os.path.abspath(ceph_cluster.get("inventory"))
        with open(inventory_path, "r") as inventory_stream:
            inventory = yaml.safe_load(inventory_stream)
    params["cloud-data"] = inventory.get("instance").get("setup")
    params["username"] = os_cred["username"]
    params["password"] = os_cred["password"]
    params["auth-url"] = os_cred["auth-url"]
    params["auth-version"] = os_cred["auth-version"]
    params["tenant-name"] = os_cred["tenant-name"]
    params["service-region"] = os_cred["service-region"]
    params["keypair"] = os_cred.get("keypair", None)
    ceph_nodes = dict()
    if inventory.get("instance").get("create"):
        if ceph_cluster.get("image-name"):
            params["image-name"] = ceph_cluster.get("image-name")
        else:
            params["image-name"] = (
                inventory.get("instance").get("create").get("image-name")
            )
        params["cluster-name"] = ceph_cluster.get("name")
        params["vm-size"] = inventory.get("instance").get("create").get("vm-size")
        if params.get("root-login") is False:
            params["root-login"] = False
        else:
            params["root-login"] = True
        with parallel() as p:
            for node in range(1, 100):
                node = "node" + str(node)
                if not ceph_cluster.get(node):
                    break
                node_dict = ceph_cluster.get(node)
                node_params = params.copy()
                node_params["role"] = RolesContainer(node_dict.get("role"))
                role = node_params["role"]
                user = os.getlogin()
                if instances_name:
                    node_params["node-name"] = "{}-{}-{}-{}-{}".format(
                        node_params.get("cluster-name", "ceph"),
                        instances_name,
                        run_id,
                        node,
                        "+".join(role),
                    )
                else:
                    node_params["node-name"] = "{}-{}-{}-{}-{}".format(
                        node_params.get("cluster-name", "ceph"),
                        user,
                        run_id,
                        node,
                        "+".join(role),
                    )
                if node_dict.get("no-of-volumes"):
                    node_params["no-of-volumes"] = node_dict.get("no-of-volumes")
                    node_params["size-of-disks"] = node_dict.get("disk-size")
                if node_dict.get("image-name"):
                    node_params["image-name"] = node_dict.get("image-name")
                if node_dict.get("cloud-data"):
                    node_params["cloud-data"] = node_dict.get("cloud-data")
                p.spawn(setup_vm_node, node, ceph_nodes, **node_params)
    log.info("Done creating nodes")
    return ceph_nodes


def setup_vm_node(node, ceph_nodes, **params):
    ceph_nodes[node] = CephVMNode(**params)


def get_openstack_driver(yaml):
    OpenStack = get_driver(Provider.OPENSTACK)
    glbs = yaml.get("globals")
    os_cred = glbs.get("openstack-credentials")
    username = os_cred["username"]
    password = os_cred["password"]
    auth_url = os_cred["auth-url"]
    auth_version = os_cred["auth-version"]
    tenant_name = os_cred["tenant-name"]
    service_region = os_cred["service-region"]
    driver = OpenStack(
        username,
        password,
        ex_force_auth_url=auth_url,
        ex_force_auth_version=auth_version,
        ex_tenant_name=tenant_name,
        ex_force_service_region=service_region,
        ex_domain_name="redhat.com",
    )
    return driver


def cleanup_ceph_nodes(osp_cred, pattern=None, timeout=300):
    user = os.getlogin()
    name = pattern if pattern else "-{user}-".format(user=user)
    driver = get_openstack_driver(osp_cred)
    timeout = datetime.timedelta(seconds=timeout)
    with parallel() as p:
        for node in driver.list_nodes():
            if name in node.name:
                for ip in node.public_ips:
                    log.info("removing ip %s from node %s", ip, node.name)
                    driver.ex_detach_floating_ip_from_node(node, ip)
                starttime = datetime.datetime.now()
                log.info(
                    "Destroying node {node_name} with {timeout} timeout".format(
                        node_name=node.name, timeout=timeout
                    )
                )
                while True:
                    try:
                        p.spawn(node.destroy)
                        break
                    except AttributeError:
                        if datetime.datetime.now() - starttime > timeout:
                            raise RuntimeError(
                                "Failed to destroy node {node_name} with {timeout} timeout:\n{stack_trace}".format(
                                    node_name=node.name,
                                    timeout=timeout,
                                    stack_trace=traceback.format_exc(),
                                )
                            )
                        else:
                            sleep(1)
                sleep(5)
    with parallel() as p:
        for fips in driver.ex_list_floating_ips():
            if fips.node_id is None:
                log.info("Releasing ip %s", fips.ip_address)
                driver.ex_delete_floating_ip(fips)
    with parallel() as p:
        errors = {}
        for volume in driver.list_volumes():
            if volume.name is None:
                log.info("Volume has no name, skipping")
            elif name in volume.name:
                log.info("Removing volume %s", volume.name)
                sleep(10)
                try:
                    volume.destroy()
                except BaseHTTPError as e:
                    log.error(e, exc_info=True)
                    errors.update({volume.name: e.message})
        if errors:
            for vol, err in errors.items():
                log.error("Error destroying {vol}: {err}".format(vol=vol, err=err))
            raise RuntimeError(
                "Encountered errors during volume deletion. Volume names and messages have been logged."
            )


def keep_alive(ceph_nodes):
    for node in ceph_nodes:
        node.exec_command(cmd="uptime", check_ec=False)


def setup_repos(ceph, base_url, installer_url=None):
    repos = ["MON", "OSD", "Tools", "Calamari", "Installer"]
    base_repo = generate_repo_file(base_url, repos)
    base_file = ceph.write_file(
        sudo=True, file_name="/etc/yum.repos.d/rh_ceph.repo", file_mode="w"
    )
    base_file.write(base_repo)
    base_file.flush()
    if installer_url is not None:
        installer_repos = ["Agent", "Main", "Installer"]
        inst_repo = generate_repo_file(installer_url, installer_repos)
        log.info("Setting up repo on %s", ceph.hostname)
        inst_file = ceph.write_file(
            sudo=True, file_name="/etc/yum.repos.d/rh_ceph_inst.repo", file_mode="w"
        )
        inst_file.write(inst_repo)
        inst_file.flush()


def check_ceph_healthly(ceph_mon, num_osds, num_mons, mon_container=None, timeout=300):
    """
    Function to check ceph is in healthy state

    Args:
        ceph_mon (CephNode): monitor node
        num_osds (int): number of osds in cluster
        num_mons (int): number of mons in cluster
        mon_container (str): monitor container name if monitor is placed in
            the container
        timeout: 300 seconds(default) max time to check if cluster is not
            healthy within timeout period return 1

    Returns:
        int: returns 0 when ceph is in healthy state otherwise returns 1

    """

    timeout = datetime.timedelta(seconds=timeout)
    starttime = datetime.datetime.now()
    lines = None
    pending_states = ["peering", "activating", "creating"]
    valid_states = ["active+clean"]

    while datetime.datetime.now() - starttime <= timeout:
        if mon_container:
            out, err = ceph_mon.exec_command(
                cmd="sudo docker exec {container} ceph -s".format(
                    container=mon_container
                )
            )
        else:
            out, err = ceph_mon.exec_command(cmd="sudo ceph -s")
        lines = out.read().decode()

        if not any(state in lines for state in pending_states):
            if all(state in lines for state in valid_states):
                break
        sleep(5)
    log.info(lines)
    if not all(state in lines for state in valid_states):
        log.error("Valid States are not found in the health check")
        return 1
    match = re.search(r"(\d+)\s+osds:\s+(\d+)\s+up,\s+(\d+)\s+in", lines)
    all_osds = int(match.group(1))
    up_osds = int(match.group(2))
    in_osds = int(match.group(3))
    if num_osds != all_osds:
        log.error("Not all osd's are up. %s / %s" % (num_osds, all_osds))
        return 1
    if up_osds != in_osds:
        log.error("Not all osd's are in. %s / %s" % (up_osds, all_osds))
        return 1

    # attempt luminous pattern first, if it returns none attempt jewel pattern
    match = re.search(r"(\d+) daemons, quorum", lines)
    if not match:
        match = re.search(r"(\d+) mons at", lines)
    all_mons = int(match.group(1))
    if all_mons != num_mons:
        log.error("Not all monitors are in cluster")
        return 1
    if "HEALTH_ERR" in lines:
        log.error("HEALTH in ERROR STATE")
        return 1
    return 0


def generate_repo_file(base_url, repos):
    return Ceph.generate_repository_file(base_url, repos)


def get_iso_file_url(base_url):
    return Ceph.get_iso_file_url(base_url)


def create_ceph_conf(
    fsid,
    mon_hosts,
    pg_num="128",
    pgp_num="128",
    size="2",
    auth="cephx",
    pnetwork="172.16.0.0/12",
    jsize="1024",
):
    fsid = "fsid = " + fsid + "\n"
    mon_init_memb = "mon initial members = "
    mon_host = "mon host = "
    public_network = "public network = " + pnetwork + "\n"
    auth = "auth cluster required = cephx\nauth service \
            required = cephx\nauth client required = cephx\n"
    jsize = "osd journal size = " + jsize + "\n"
    size = "osd pool default size = " + size + "\n"
    pgnum = "osd pool default pg num = " + pg_num + "\n"
    pgpnum = "osd pool default pgp num = " + pgp_num + "\n"
    for mhost in mon_hosts:
        mon_init_memb = mon_init_memb + mhost.shortname + ","
        mon_host = mon_host + mhost.internal_ip + ","
    mon_init_memb = mon_init_memb[:-1] + "\n"
    mon_host = mon_host[:-1] + "\n"
    conf = "[global]\n"
    conf = (
        conf
        + fsid
        + mon_init_memb
        + mon_host
        + public_network
        + auth
        + size
        + jsize
        + pgnum
        + pgpnum
    )
    return conf


def setup_deb_repos(node, ubuntu_repo):
    node.exec_command(cmd="sudo rm -f /etc/apt/sources.list.d/*")
    repos = ["MON", "OSD", "Tools"]
    for repo in repos:
        cmd = (
            "sudo echo deb "
            + ubuntu_repo
            + "/{0}".format(repo)
            + " $(lsb_release -sc) main"
        )
        node.exec_command(cmd=cmd + " > " + "/tmp/{0}.list".format(repo))
        node.exec_command(
            cmd="sudo cp /tmp/{0}.list /etc/apt/sources.list.d/".format(repo)
        )
    ds_keys = [
        "https://www.redhat.com/security/897da07a.txt",
        "https://www.redhat.com/security/f21541eb.txt",
        # 'https://prodsec.redhat.com/keys/00da75f2.txt',
        # TODO: replace file file.rdu.redhat.com/~kdreyer with prodsec.redhat.com when it's back
        "http://file.rdu.redhat.com/~kdreyer/keys/00da75f2.txt",
        "https://www.redhat.com/security/data/fd431d51.txt",
    ]

    for key in ds_keys:
        wget_cmd = "sudo wget -O - " + key + " | sudo apt-key add -"
        node.exec_command(cmd=wget_cmd)
    node.exec_command(cmd="sudo apt-get update")


def setup_deb_cdn_repo(node, build=None):
    user = "redhat"
    passwd = "OgYZNpkj6jZAIF20XFZW0gnnwYBjYcmt7PeY76bLHec9"
    num = build.split(".")[0]
    cmd = (
        "umask 0077; echo deb https://{user}:{passwd}@rhcs.download.redhat.com/{num}-updates/Tools "
        "$(lsb_release -sc) main | tee /etc/apt/sources.list.d/Tools.list".format(
            user=user, passwd=passwd, num=num
        )
    )
    node.exec_command(sudo=True, cmd=cmd)
    node.exec_command(
        sudo=True,
        cmd="wget -O - https://www.redhat.com/security/fd431d51.txt | apt-key add -",
    )
    node.exec_command(sudo=True, cmd="apt-get update")


def setup_cdn_repos(ceph_nodes, build=None):
    repos_13x = [
        "rhel-7-server-rhceph-1.3-mon-rpms",
        "rhel-7-server-rhceph-1.3-osd-rpms",
        "rhel-7-server-rhceph-1.3-calamari-rpms",
        "rhel-7-server-rhceph-1.3-installer-rpms",
        "rhel-7-server-rhceph-1.3-tools-rpms",
    ]

    repos_20 = [
        "rhel-7-server-rhceph-2-mon-rpms",
        "rhel-7-server-rhceph-2-osd-rpms",
        "rhel-7-server-rhceph-2-tools-rpms",
        "rhel-7-server-rhscon-2-agent-rpms",
        "rhel-7-server-rhscon-2-installer-rpms",
        "rhel-7-server-rhscon-2-main-rpms",
    ]

    repos_30 = [
        "rhel-7-server-rhceph-3-mon-rpms",
        "rhel-7-server-rhceph-3-osd-rpms",
        "rhel-7-server-rhceph-3-tools-rpms",
        "rhel-7-server-extras-rpms",
    ]

    repos = None
    if build.startswith("1"):
        repos = repos_13x
    elif build.startswith("2"):
        repos = repos_20
    elif build.startswith("3"):
        repos = repos_30
    with parallel() as p:
        for node in ceph_nodes:
            p.spawn(set_cdn_repo, node, repos)


def set_cdn_repo(node, repos):
    for repo in repos:
        node.exec_command(
            sudo=True, cmd="subscription-manager repos --enable={r}".format(r=repo)
        )
    # node.exec_command(sudo=True, cmd='subscription-manager refresh')


def update_ca_cert(node, cert_url, timeout=120):
    if node.pkg_type == "deb":
        cmd = "cd /usr/local/share/ca-certificates/ && {{ sudo curl -OL {url} ; cd -; }}".format(
            url=cert_url
        )
        node.exec_command(cmd=cmd, timeout=timeout)
        node.exec_command(cmd="sudo update-ca-certificates", timeout=timeout)
    else:
        cmd = "cd /etc/pki/ca-trust/source/anchors && {{ sudo curl -OL {url} ; cd -; }}".format(
            url=cert_url
        )
        node.exec_command(cmd=cmd, timeout=timeout)
        node.exec_command(cmd="sudo update-ca-trust extract", timeout=timeout)


def write_docker_daemon_json(json_text, node):
    """
    Write given string to /etc/docker/daemon/daemon
    Args:
        json_text: json string
        node (ceph.ceph.CephNode): Ceph node object
    """
    node.write_docker_daemon_json(json_text)


def search_ethernet_interface(ceph_node, ceph_node_list):
    """
    Search interface on the given node node which allows every node in the cluster accesible by it's shortname.

    Args:
        ceph_node (ceph.ceph.CephNode): node where check is performed
        ceph_node_list(list): node list to check
    """
    return ceph_node.search_ethernet_interface(ceph_node_list)


def open_firewall_port(ceph_node, port, protocol):
    """
    Opens firewall ports for given node
    Args:
        ceph_node (ceph.ceph.CephNode): ceph node
        port (str): port
        protocol (str): protocol
    """
    ceph_node.open_firewall_port(port, protocol)


def config_ntp(ceph_node):
    ceph_node.exec_command(
        cmd="sudo sed -i '/server*/d' /etc/ntp.conf", long_running=True
    )
    ceph_node.exec_command(
        cmd="echo 'server clock.corp.redhat.com iburst' | sudo tee -a /etc/ntp.conf",
        long_running=True,
    )
    ceph_node.exec_command(cmd="sudo ntpd -gq", long_running=True)
    ceph_node.exec_command(cmd="sudo systemctl enable ntpd", long_running=True)
    ceph_node.exec_command(cmd="sudo systemctl start ntpd", long_running=True)


def get_ceph_versions(ceph_nodes, containerized=False):
    """
    Log and return the ceph or ceph-ansible versions for each node in the cluster.

    Args:
        ceph_nodes: nodes in the cluster
        containerized: is the cluster containerized or not

    Returns:
        A dict of the name / version pair for each node or container in the cluster
    """
    versions_dict = {}

    for node in ceph_nodes:
        try:
            if node.role == "installer":
                if node.pkg_type == "rpm":
                    out, rc = node.exec_command(cmd="rpm -qa | grep ceph-ansible")
                else:
                    out, rc = node.exec_command(cmd="dpkg -s ceph-ansible")
                output = out.read().decode().rstrip()
                log.info(output)
                versions_dict.update({node.shortname: output})

            else:
                if containerized:
                    containers = []
                    if node.role == "client":
                        pass
                    else:
                        out, rc = node.exec_command(
                            sudo=True, cmd='docker ps --format "{{.Names}}"'
                        )
                        output = out.read().decode()
                        containers = [
                            container
                            for container in output.split("\n")
                            if container != ""
                        ]
                        log.info("Containers: {}".format(containers))

                    for container_name in containers:
                        out, rc = node.exec_command(
                            sudo=True,
                            cmd="sudo docker exec {container} ceph --version".format(
                                container=container_name
                            ),
                        )
                        output = out.read().decode().rstrip()
                        log.info(output)
                        versions_dict.update({container_name: output})

                else:
                    out, rc = node.exec_command(cmd="ceph --version")
                    output = out.read().decode().rstrip()
                    log.info(output)
                    versions_dict.update({node.shortname: output})

        except CommandFailed:
            log.info("No ceph versions on {}".format(node.shortname))

    return versions_dict


def hard_reboot(gyaml, name=None):
    user = os.getlogin()
    if name is None:
        name = "ceph-" + user
    driver = get_openstack_driver(gyaml)
    for node in driver.list_nodes():
        if node.name.startswith(name):
            log.info("Hard-rebooting %s" % node.name)
            driver.ex_hard_reboot_node(node)

    return 0


def node_power_failure(gyaml, sleep_time=300, name=None):
    user = os.getlogin()
    if name is None:
        name = "ceph-" + user
    driver = get_openstack_driver(gyaml)
    for node in driver.list_nodes():
        if node.name.startswith(name):
            log.info("Doing power-off on %s" % node.name)
            driver.ex_stop_node(node)
            time.sleep(20)
            op = driver.ex_get_node_details(node)
            if op.state == "stopped":
                log.info("Node stopped successfully")
            time.sleep(sleep_time)
            log.info("Doing power-on on %s" % node.name)
            driver.ex_start_node(node)
            time.sleep(20)
            op = driver.ex_get_node_details(node)
            if op.state == "running":
                log.info("Node restarted successfully")
            time.sleep(20)
    return 0


def get_root_permissions(node, path):
    """
    Transfer ownership of root to current user for the path given. Recursive.
    Args:
        node(ceph.ceph.CephNode):
        path: file path
    """
    node.obtain_root_permissions(path)


def get_public_network():
    """
    Get the configured public network subnet for nodes in the cluster.

    Returns:
        (str) public network subnet
    """
    return "10.0.144.0/22"  # TODO: pull from configuration file


@retry(LibcloudError, tries=5, delay=15)
def create_nodes(conf, inventory, osp_cred, run_id, instances_name=None):
    log.info("Destroying existing osp instances")
    cleanup_ceph_nodes(osp_cred, instances_name)
    ceph_cluster_dict = {}
    log.info("Creating osp instances")
    for cluster in conf.get("globals"):
        ceph_vmnodes = create_ceph_nodes(
            cluster, inventory, osp_cred, run_id, instances_name
        )
        ceph_nodes = []
        clients = []
        for node in ceph_vmnodes.values():
            if node.role == "win-iscsi-clients":
                clients.append(
                    WinNode(
                        ip_address=node.ip_address, private_ip=node.get_private_ip()
                    )
                )
            else:
                ceph = CephNode(
                    username="cephuser",
                    password="cephuser",
                    root_password="passwd",
                    root_login=node.root_login,
                    role=node.role,
                    no_of_volumes=node.no_of_volumes,
                    ip_address=node.ip_address,
                    private_ip=node.get_private_ip(),
                    hostname=node.hostname,
                    ceph_vmnode=node,
                )
                ceph_nodes.append(ceph)
        cluster_name = cluster.get("ceph-cluster").get("name", "ceph")
        ceph_cluster_dict[cluster_name] = Ceph(cluster_name, ceph_nodes)
    # TODO: refactor cluster dict to cluster list
    log.info("Done creating osp instances")
    log.info("Waiting for Floating IPs to be available")
    log.info("Sleeping 15 Seconds")
    time.sleep(15)
    for cluster_name, cluster in ceph_cluster_dict.items():
        for instance in cluster:
            instance.connect()
    return ceph_cluster_dict, clients


def store_cluster_state(ceph_cluster_object, ceph_clusters_file_name):
    cn = open(ceph_clusters_file_name, "w+b")
    pickle.dump(ceph_cluster_object, cn)
    cn.close()
    log.info("ceph_clusters_file %s", ceph_clusters_file_name)


def create_oc_resource(
    template_name,
    cluster_path,
    _templating,
    template_data=None,
    template_dir="ocs-deployment",
):
    """
    Create an oc resource after rendering the specified template with
    the rook data from cluster_conf.

    Args:
        template_name (str): Name of the ocs-deployment config template
        cluster_path (str): Path to cluster directory, where files will be
            written
        _templating (Templating): Object of Templating class used for
            templating
        template_data (dict): Data for render template (default: {})
        template_dir (str): Directory under templates dir where template
            exists (default: ocs-deployment)
    """
    if template_data is None:
        template_data = {}
    template_path = os.path.join(template_dir, template_name)
    template = _templating.render_template(template_path, template_data)
    cfg_file = os.path.join(cluster_path, template_name)
    with open(cfg_file, "w") as f:
        f.write(template)
    log.info(f"Creating rook resource from {template_name}")
    occli = OCP()
    occli.create(cfg_file)


def get_pod_name_by_pattern(
    pattern="client", namespace=None, filter=None, cluster_kubeconfig=""
):
    """
    In a given namespace find names of the pods that match
    the given pattern

    Args:
        pattern (str): name of the pod with given pattern
        namespace (str): Namespace value
        filter (str): pod name to filter from the list
        cluster_kubeconfig (str): Path to kubeconfig file

    Returns:
        pod_list (list): List of pod names matching the pattern

    """
    namespace = namespace if namespace else ocsci_config.ENV_DATA["cluster_namespace"]

    ocp_obj = OCP(
        kind="pod", namespace=namespace, cluster_kubeconfig=cluster_kubeconfig
    )
    pod_names = ocp_obj.exec_oc_cmd("get pods -o name", out_yaml_format=False)
    pod_names = pod_names.split("\n")
    pod_list = []
    for name in pod_names:
        if filter is not None and re.search(filter, name):
            log.info(f"Pod name filtered {name}")
        elif re.search(pattern, name):
            (_, name) = name.split("/")
            log.info(f"pod name match found appending {name}")
            pod_list.append(name)
    return pod_list


def get_namespce_name_by_pattern(
    pattern="client",
    filter=None,
):
    """
    Find namespace names that match the given pattern

    Args:
        pattern (str): name of the namespace with given pattern
        filter (str): namespace name to filter from the list

    Returns:
        list: Namespace names matching the pattern

    """
    ocp_obj = OCP(kind="namespace")
    namespace_names = ocp_obj.exec_oc_cmd(
        "get namespace -o name", out_yaml_format=False
    )
    namespace_names = namespace_names.split("\n")
    namespace_list = []
    for namespace_name in namespace_names:
        if filter is not None and filter == namespace_name:
            log.info(f"Namespace name filtered {namespace_name}")
        elif re.search(pattern, namespace_name):
            (_, name) = namespace_name.split("/")
            log.info(f"namespace name match found appending {namespace_name}")
            namespace_list.append(name)
    return namespace_list


def get_rook_version():
    """
    Get the rook image information from rook-ceph-operator pod

    Returns:
        str: rook version

    """
    namespace = ocsci_config.ENV_DATA["cluster_namespace"]
    rook_operator = get_pod_name_by_pattern("rook-ceph-operator", namespace)
    out = run_cmd(
        f"oc -n {namespace} get pods {rook_operator[0]} -o yaml",
    )
    version = yaml.safe_load(out)
    rook_version = version["spec"]["containers"][0]["image"]
    return rook_version


def setup_ceph_toolbox(force_setup=False, storage_cluster=None):
    """
    Setup ceph-toolbox - also checks if toolbox exists, if it exists it
    behaves as noop.

    Args:
        force_setup (bool): force setup toolbox pod

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    storage_cluster = (
        storage_cluster if storage_cluster else constants.DEFAULT_STORAGE_CLUSTER
    )
    if ocsci_config.ENV_DATA["mcg_only_deployment"]:
        log.info("Skipping Ceph toolbox setup due to running in MCG only mode")
        return
    namespace = ocsci_config.ENV_DATA["cluster_namespace"]
    ceph_toolbox = get_pod_name_by_pattern("rook-ceph-tools", namespace)
    # setup toolbox for external mode
    # Refer bz: 1856982 - invalid admin secret
    if len(ceph_toolbox) == 1:
        log.info("Ceph toolbox already exists, skipping")
        if force_setup:
            log.info("Running force setup for Ceph toolbox!")
        else:
            return
    external_mode = ocsci_config.DEPLOYMENT.get("external_mode")

    if ocs_version == version.VERSION_4_2:
        tool_box_data = templating.load_yaml(constants.TOOL_POD_YAML)
        tool_box_data["spec"]["template"]["spec"]["containers"][0][
            "image"
        ] = get_rook_version()
        rook_toolbox = OCS(**tool_box_data)
        rook_toolbox.create()
    else:
        if external_mode:
            toolbox = templating.load_yaml(constants.TOOL_POD_YAML)
            toolbox["spec"]["template"]["spec"]["containers"][0][
                "image"
            ] = get_rook_version()
            toolbox["metadata"]["name"] += "-external"
            keyring_dict = ocsci_config.EXTERNAL_MODE.get("admin_keyring")
            if ocs_version >= version.VERSION_4_10:
                toolbox["spec"]["template"]["spec"]["containers"][0]["command"] = [
                    "/bin/bash"
                ]
                toolbox["spec"]["template"]["spec"]["containers"][0]["args"][0] = "-m"
                toolbox["spec"]["template"]["spec"]["containers"][0]["args"][1] = "-c"
                toolbox["spec"]["template"]["spec"]["containers"][0]["tty"] = True
            env = toolbox["spec"]["template"]["spec"]["containers"][0]["env"]
            # replace secret
            env = [item for item in env if not (item["name"] == "ROOK_CEPH_SECRET")]
            env.append({"name": "ROOK_CEPH_SECRET", "value": keyring_dict["key"]})
            toolbox["spec"]["template"]["spec"]["containers"][0]["env"] = env
            # add ceph volumeMounts
            ceph_volume_mount_path = {"mountPath": "/etc/ceph", "name": "ceph-config"}
            ceph_volume = {"name": "ceph-config", "emptyDir": {}}
            toolbox["spec"]["template"]["spec"]["containers"][0]["volumeMounts"].append(
                ceph_volume_mount_path
            )
            toolbox["spec"]["template"]["spec"]["volumes"].append(ceph_volume)
            if ocs_version >= version.VERSION_4_16:
                toolbox["spec"]["template"]["spec"][
                    "serviceAccount"
                ] = "rook-ceph-default"
                toolbox["spec"]["template"]["spec"][
                    "serviceAccountName"
                ] = "rook-ceph-default"
            rook_toolbox = OCS(**toolbox)
            rook_toolbox.create()
            return
        if (
            ocsci_config.ENV_DATA.get("platform").lower()
            == constants.FUSIONAAS_PLATFORM
            and ocsci_config.ENV_DATA["cluster_type"].lower()
            == constants.MS_CONSUMER_TYPE
        ):
            log.warning(
                f"Skipping toolbox creation on {constants.MS_CONSUMER_TYPE} cluster on "
                f"{constants.FUSIONAAS_PLATFORM} platform."
            )
            return

        # for OCS >= 4.3 there is new toolbox pod deployment done here:
        # https://github.com/openshift/ocs-operator/pull/207/
        log.info("starting ceph toolbox pod")
        cmd = (
            f"oc patch storagecluster {storage_cluster} -n openshift-storage --type "
            'json --patch  \'[{ "op": "replace", "path": '
            '"/spec/enableCephTools", "value": true }]\''
        )
        run_cmd(cmd)
        toolbox_pod = OCP(kind=constants.POD, namespace=namespace)
        toolbox_pod.wait_for_resource(
            condition="Running",
            selector="app=rook-ceph-tools",
            resource_count=1,
            timeout=120,
        )


def apply_oc_resource(
    template_name,
    cluster_path,
    _templating,
    template_data=None,
    template_dir="ocs-deployment",
):
    """
    Apply an oc resource after rendering the specified template with
    the rook data from cluster_conf.

    Args:
        template_name (str): Name of the ocs-deployment config template
        cluster_path (str): Path to cluster directory, where files will be
            written
        _templating (Templating): Object of Templating class used for
            templating
        template_data (dict): Data for render template (default: {})
        template_dir (str): Directory under templates dir where template
            exists (default: ocs-deployment)
    """
    if template_data is None:
        template_data = {}
    template_path = os.path.join(template_dir, template_name)
    template = _templating.render_template(template_path, template_data)
    cfg_file = os.path.join(cluster_path, template_name)
    with open(cfg_file, "w") as f:
        f.write(template)
    log.info(f"Applying rook resource from {template_name}")
    occli = OCP()
    occli.apply(cfg_file)


def run_must_gather(log_dir_path, image, command=None, cluster_config=None):
    """
    Runs the must-gather tool against the cluster

    Args:
        log_dir_path (str): directory for dumped must-gather logs
        image (str): must-gather image registry path
        command (str): optional command to execute within the must-gather image
        cluster_config (MultiClusterConfig): Holds specifc cluster config object in case of multicluster

    Returns:
        mg_output (str): must-gather cli output

    """
    # Must-gather has many changes on 4.6 which add more time to the collection.
    # https://github.com/red-hat-storage/ocs-ci/issues/3240
    if not cluster_config:
        cluster_config = ocsci_config
    mg_output = ""
    ocs_version = version.get_semantic_ocs_version_from_config()
    if ocs_version >= version.VERSION_4_10:
        timeout = 2100
    elif ocs_version >= version.VERSION_4_6:
        timeout = 1500
    else:
        timeout = 600

    must_gather_timeout = cluster_config.REPORTING.get("must_gather_timeout", timeout)

    log.info(f"Must gather image: {image} will be used.")
    create_directory_path(log_dir_path)
    cmd = f"adm must-gather --image={image} --dest-dir={log_dir_path}"
    if command:
        cmd += f" -- {command}"

    log.info(f"OCS logs will be placed in location {log_dir_path}")
    occli = OCP()
    try:
        mg_output = occli.exec_oc_cmd(
            cmd,
            out_yaml_format=False,
            timeout=must_gather_timeout,
            cluster_config=cluster_config,
        )
    except CommandFailed as ex:
        log.error(
            f"Failed during must gather logs! Error: {ex}"
            f"Must-Gather Output: {mg_output}"
        )
        export_mg_pods_logs(log_dir_path=log_dir_path)

    except TimeoutExpired as ex:
        log.error(
            f"Failed during must gather logs! Error: {ex}"
            f"Must-Gather Output: {mg_output}"
        )
        export_mg_pods_logs(log_dir_path=log_dir_path)
    return mg_output


def export_mg_pods_logs(log_dir_path):
    """
    Export must gather pods logs

    Args:
        log_dir_path (str): the path of copying the logs

    """
    get_logs_ocp_mg_pods(log_dir_path)
    get_helper_pods_output(log_dir_path)


def get_logs_ocp_mg_pods(log_dir_path):
    """
    Get logs from OCP Must Gather pods

    Args:
        log_dir_path (str): the path of copying the logs

    """
    from ocs_ci.ocs.resources.pod import get_all_pods, get_pod_logs

    namespaces = get_namespce_name_by_pattern(pattern="openshift-must-gather")
    try:
        for namespace in namespaces:
            pods_mg_ns = get_all_pods(namespace=namespace)
            for pod_mg_ns in pods_mg_ns:
                log.info(
                    f"*** ocp_mg_pod_name: {pod_mg_ns.name} ocp_mg_pod_namespace: {namespace} ***"
                )

                file_path_describe = os.path.join(
                    log_dir_path, f"describe_ocp_mg_{pod_mg_ns.name}.log"
                )
                pod_mg_describe = pod_mg_ns.describe()
                with open(file_path_describe, "w") as df:
                    df.write(f"ocp mg pod describe:\n{pod_mg_describe}")
                log.debug(f"ocp mg pod describe:\n{pod_mg_describe}")

                ocp_mg_pod_logs = get_pod_logs(
                    pod_name=pod_mg_ns.name, namespace=namespace, all_containers=True
                )
                file_path_describe = os.path.join(
                    log_dir_path, f"log_ocp_mg_{pod_mg_ns.name}.log"
                )
                with open(file_path_describe, "w") as df:
                    df.write(ocp_mg_pod_logs)
                log.debug(f"ocp mg pod logs:\n{ocp_mg_pod_logs}")
    except Exception as e:
        log.error(e)


def get_helper_pods_output(log_dir_path):
    """
    Get the output of "oc describe mg-helper pods"

    Args:
        log_dir_path (str): the path of copying the logs

    """
    from ocs_ci.ocs.resources.pod import get_pod_obj, get_pod_logs

    helper_pods = get_pod_name_by_pattern(pattern="helper")
    for helper_pod in helper_pods:
        try:
            helper_pod_obj = get_pod_obj(
                name=helper_pod, namespace=ocsci_config.ENV_DATA["cluster_namespace"]
            )

            describe_helper_pod = helper_pod_obj.describe()
            file_path_describe = os.path.join(
                log_dir_path, f"describe_ocs_mg_helper_pod_{helper_pod}.log"
            )
            with open(file_path_describe, "w") as df:
                df.write(describe_helper_pod)
            log.debug(
                f"****helper pod {helper_pod} describe****\n{describe_helper_pod}\n"
            )

            log_helper_pod = get_pod_logs(pod_name=helper_pod)
            file_path_describe = os.path.join(
                log_dir_path, f"log_ocs_mg_helper_pod_{helper_pod}.log"
            )
            with open(file_path_describe, "w") as df:
                df.write(log_helper_pod)
            log.debug(f"****helper pod {helper_pod} logs***\n{log_helper_pod}")
        except Exception as e:
            log.error(e)


def collect_noobaa_db_dump(log_dir_path, cluster_config=None):
    """
    Collect the Noobaa DB dump

    Args:
        log_dir_path (str): directory for dumped Noobaa DB
        cluster_config (MultiClusterConfig): If multicluster scenario then this object will have
            specific cluster config

    """
    from ocs_ci.ocs.resources.pod import (
        get_pods_having_label,
        download_file_from_pod,
        Pod,
    )

    ocs_version = version.get_semantic_ocs_version_from_config(
        cluster_config=cluster_config
    )
    nb_db_label = (
        constants.NOOBAA_DB_LABEL_46_AND_UNDER
        if ocs_version < version.VERSION_4_7
        else constants.NOOBAA_DB_LABEL_47_AND_ABOVE
    )
    try:
        nb_db_pod = Pod(
            **get_pods_having_label(
                label=nb_db_label,
                namespace=ocsci_config.ENV_DATA["cluster_namespace"],
                cluster_config=cluster_config,
            )[0]
        )
    except IndexError:
        log.warning(
            "Unable to find pod using label `%s` in namespace `%s`",
            nb_db_label,
            ocsci_config.ENV_DATA["cluster_namespace"],
        )
        return
    ocs_log_dir_path = os.path.join(log_dir_path, "noobaa_db_dump")
    create_directory_path(ocs_log_dir_path)
    ocs_log_dir_path = os.path.join(ocs_log_dir_path, "nbcore.gz")
    if ocs_version < version.VERSION_4_7:
        cmd = "mongodump --archive=nbcore.gz --gzip --db=nbcore"
        remote_path = "/opt/app-root/src/nbcore.gz"
    else:
        cmd = 'bash -c "pg_dump nbcore | gzip > /tmp/nbcore.gz"'
        remote_path = "/tmp/nbcore.gz"

    nb_db_pod.exec_cmd_on_pod(cmd, cluster_config=cluster_config)
    download_file_from_pod(
        pod_name=nb_db_pod.name,
        remotepath=remote_path,
        localpath=ocs_log_dir_path,
        namespace=ocsci_config.ENV_DATA["cluster_namespace"],
    )


def _collect_ocs_logs(
    cluster_config,
    dir_name,
    ocp=True,
    ocs=True,
    mcg=False,
    status_failure=True,
    ocs_flags=None,
):
    """
    This function runs in thread

    """
    log.info(
        (
            f"RUNNING IN CTX: {cluster_config.ENV_DATA['cluster_name']} RUNID: = {cluster_config.RUN['run_id']}"
        )
    )
    if not (
        cluster_config.RUN.get("kubeconfig", False)
        or os.path.exists(os.path.expanduser("~/.kube/config"))
    ):
        log.warning(
            "Cannot find $KUBECONFIG or ~/.kube/config; " "skipping log collection"
        )
        return
    if status_failure:
        log_dir_path = os.path.join(
            os.path.expanduser(cluster_config.RUN["log_dir"]),
            f"failed_testcase_ocs_logs_{cluster_config.RUN['run_id']}",
            f"{dir_name}_ocs_logs",
            f"{cluster_config.ENV_DATA['cluster_name']}",
        )
    else:
        log_dir_path = os.path.join(
            os.path.expanduser(cluster_config.RUN["log_dir"]),
            f"{dir_name}_{cluster_config.RUN['run_id']}",
            f"{cluster_config.ENV_DATA['cluster_name']}",
        )

    if ocs:
        latest_tag = cluster_config.REPORTING.get(
            "ocs_must_gather_latest_tag",
            cluster_config.REPORTING.get(
                "default_ocs_must_gather_latest_tag",
                cluster_config.DEPLOYMENT["default_latest_tag"],
            ),
        )
        ocs_log_dir_path = os.path.join(log_dir_path, "ocs_must_gather")
        ocs_must_gather_image = cluster_config.REPORTING.get(
            "ocs_must_gather_image",
            cluster_config.REPORTING["default_ocs_must_gather_image"],
        )
        ocs_must_gather_image_and_tag = f"{ocs_must_gather_image}:{latest_tag}"
        if cluster_config.DEPLOYMENT.get("disconnected"):
            ocs_must_gather_image_and_tag = mirror_image(
                ocs_must_gather_image_and_tag, cluster_config
            )
        mg_output = run_must_gather(
            ocs_log_dir_path,
            ocs_must_gather_image_and_tag,
            cluster_config=cluster_config,
            command=ocs_flags,
        )
        if (
            ocsci_config.DEPLOYMENT.get("disconnected")
            and "cannot stat 'jq'" in mg_output
        ):
            raise ValueError(
                f"must-gather fails in an disconnected environment bz-1974959\n{mg_output}"
            )
    if ocp:
        ocp_log_dir_path = os.path.join(log_dir_path, "ocp_must_gather")
        ocp_must_gather_image = cluster_config.REPORTING["ocp_must_gather_image"]
        if cluster_config.DEPLOYMENT.get("disconnected"):
            ocp_must_gather_image = mirror_image(ocp_must_gather_image)
        run_must_gather(
            ocp_log_dir_path, ocp_must_gather_image, cluster_config=cluster_config
        )
        run_must_gather(
            ocp_log_dir_path,
            ocp_must_gather_image,
            "/usr/bin/gather_service_logs worker",
            cluster_config=cluster_config,
        )
    if mcg:
        counter = 0
        while counter < 5:
            counter += 1
            try:
                if (
                    ocsci_config.multicluster
                    and ocsci_config.get_active_acm_index()
                    == cluster_config.MULTICLUSTER["multicluster_index"]
                ):
                    break
                collect_noobaa_db_dump(log_dir_path, cluster_config)
                break
            except CommandFailed as ex:
                log.error(f"Failed to dump noobaa DB! Error: {ex}")
                sleep(30)
    # Collect ACM logs only from ACM
    if cluster_config.MULTICLUSTER.get("multicluster_mode", None) == "regional-dr":
        if cluster_config.MULTICLUSTER.get("acm_cluster", False):
            log.info("Collecting ACM logs")
            image_prefix = '"acm_must_gather"'
            acm_mustgather_path = os.path.join(log_dir_path, "acmlogs")
            csv_cmd = (
                f"oc --kubeconfig {cluster_config.RUN['kubeconfig']} "
                f"get csv -l {constants.ACM_CSV_LABEL} -n open-cluster-management -o json"
            )
            jq_cmd = f"jq -r '.items[0].spec.relatedImages[]|select(.name=={image_prefix}).image'"
            json_out = run_cmd(csv_cmd)
            out = subprocess.run(
                shlex.split(jq_cmd), input=json_out.encode(), stdout=subprocess.PIPE
            )
            acm_mustgather_image = out.stdout.decode()
            run_must_gather(
                acm_mustgather_path, acm_mustgather_image, cluster_config=cluster_config
            )

        submariner_log_path = os.path.join(
            log_dir_path,
            "submariner",
        )
        run_cmd(f"mkdir -p {submariner_log_path}")
        cwd = os.getcwd()
        run_cmd(f"chmod -R 777 {submariner_log_path}")
        os.chdir(submariner_log_path)
        submariner_log_collect = (
            f"subctl gather --kubeconfig {cluster_config.RUN['kubeconfig']}"
        )
        log.info("Collecting submariner logs")
        out = run_cmd(submariner_log_collect)
        run_cmd(f"chmod -R 777 {submariner_log_path}")
        os.chdir(cwd)
        log.info(out)


def collect_ocs_logs(
    dir_name, ocp=True, ocs=True, mcg=False, status_failure=True, ocs_flags=None
):
    """
    Collects OCS logs

    Args:
        dir_name (str): directory name to store OCS logs. Logs will be stored
            in dir_name suffix with _ocs_logs.
        ocp (bool): Whether to gather OCP logs
        ocs (bool): Whether to gather OCS logs
        mcg (bool): True for collecting MCG logs (noobaa db dump)
        status_failure (bool): Whether the collection is after success or failure,
            allows better naming for folders under logs directory
        ocs_flags (str): flags to ocs must gather command for example ["-- /usr/bin/gather -cs"]

    """
    results = None
    with ThreadPoolExecutor() as executor:
        results = [
            executor.submit(
                _collect_ocs_logs,
                cluster,
                dir_name=dir_name,
                ocp=ocp,
                ocs=ocs,
                mcg=mcg,
                status_failure=status_failure,
                ocs_flags=ocs_flags,
            )
            for cluster in ocsci_config.clusters
        ]

    for f in as_completed(results):
        try:
            log.info(f.result())
        except Exception as e:
            log.error("Must-gather collection failed")
            log.error(e)
            raise


def collect_prometheus_metrics(
    metrics,
    dir_name,
    start,
    stop,
    step=1.0,
    threading_lock=None,
):
    """
    Collects metrics from Prometheus and saves them in file in json format.
    Metrics can be found in OCP Console in Monitoring -> Metrics.

    Args:
        metrics (list): list of metrics to get from Prometheus
            (E.g. ceph_cluster_total_used_bytes, cluster:cpu_usage_cores:sum,
            cluster:memory_usage_bytes:sum)
        dir_name (str): directory name to store metrics. Metrics will be stored
            in dir_name suffix with _ocs_metrics.
        start (str): start timestamp of required datapoints
        stop (str): stop timestamp of required datapoints
        step (float): step of required datapoints
        threading_lock: (threading.RLock): Lock to use for thread safety (default: None)
    """
    api = PrometheusAPI(threading_lock=threading_lock)
    log_dir_path = os.path.join(
        os.path.expanduser(ocsci_config.RUN["log_dir"]),
        f"failed_testcase_ocs_logs_{ocsci_config.RUN['run_id']}",
        f"{dir_name}_ocs_metrics",
    )
    if not os.path.exists(log_dir_path):
        log.info(f"Creating directory {log_dir_path}")
        os.makedirs(log_dir_path)

    for metric in metrics:
        datapoints = api.get(
            "query_range", {"query": metric, "start": start, "end": stop, "step": step}
        )
        file_name = os.path.join(log_dir_path, f"{metric}.json")
        log.info(f"Saving {metric} data into {file_name}")
        with open(file_name, "w") as outfile:
            json.dump(datapoints.json(), outfile)


def oc_get_all_obc_names():
    """
    Returns:
        set: A set of all OBC names

    """
    all_obcs_in_namespace = (
        OCP(namespace=ocsci_config.ENV_DATA["cluster_namespace"], kind="obc")
        .get()
        .get("items")
    )
    return {obc.get("metadata").get("name") for obc in all_obcs_in_namespace}


def get_external_mode_rhcs():
    """
    Get external cluster info from config and obtain
    external cluster object


    Returns:
        external_ceph.Ceph object

    """
    external_rhcs_info = ocsci_config.EXTERNAL_MODE.get(
        "external_cluster_node_roles", ""
    )
    if not external_rhcs_info:
        log.error("No external RHCS cluster info found")
        raise ExternalClusterDetailsException()

    return get_cluster_object(external_rhcs_info)


def get_cluster_object(external_rhcs_info):
    """
    Build a external_ceph.ceph object with all node and role
    info

    Args:
        external_rhcs_info (dict):

    Returns:
        external_ceph.ceph object

    """
    # List of CephNode objects
    node_list = []
    for node, node_info in external_rhcs_info.items():
        node_info["username"] = ocsci_config.EXTERNAL_MODE["login"]["username"]
        node_info["password"] = ocsci_config.EXTERNAL_MODE["login"]["password"]
        node_info["no_of_volumes"] = ""

        log.info(node_info)
        node_list.append(CephNode(**node_info))

    return Ceph(node_list=node_list)


def kill_osd_external(ceph_cluster, osd_id, sig_type="SIGTERM"):
    """
    Kill an osd with given signal

    Args:
        ceph_cluster (external_cluster.Ceph): Cluster object
        osd_id (int): id of osd
        sig_type (str): type of signal to be sent

    Raises:
        CommandFailed exception

    """
    log.info(f"OSDID={osd_id}")
    kill_cmd = f"systemctl kill -s {sig_type} ceph-osd@{osd_id}"
    osd_obj = ceph_cluster.get_nodes(role=f"osd.{osd_id}")
    for osd in osd_obj:
        log.info(f"OSD node = {osd.vmname}")
        try:
            osd.exec_command(cmd=kill_cmd)
        except CommandFailed:
            log.error("Failed to kill osd")
            raise


def revive_osd_external(ceph_cluster, osd_id):
    """
    Start an already stopped osd

    Args:
        ceph_cluster (external_cluster.Ceph): cluster object
        osd_id (int): id of osd

    Raises:
        CommandFailed exception in case of failure

    """
    log.info(f"Reviving osd ={osd_id}")
    revive_cmd = f"systemctl start ceph-osd@{osd_id}"
    osd_obj = ceph_cluster.get_nodes(role=f"osd.{osd_id}")
    for osd in osd_obj:
        try:
            osd.exec_command(cmd=revive_cmd)
        except CommandFailed:
            log.error("Failed to revive osd")
            raise


def reboot_node(ceph_node, timeout=300):
    """
    Reboot a node with given ceph_node object

    Args:
        ceph_node (CephNode): Ceph node object representing the node.
        timeout (int): Wait time in seconds for the node to comeback.

    Raises:
        SSHException: if not able to connect through ssh
    """
    ceph_node.exec_command(
        cmd="reboot",
        check_ec=False,
        long_running=True,
    )

    try:
        ceph_node.connect(timeout)
    except SSHException:
        log.exception(f"Failed to connect to node {ceph_node.hostname}")
        raise


def enable_console_plugin(value="[odf-console]"):
    """
    Enables console plugin for ODF

    Arg:
        value(str): the odf console to enable

    """
    ocs_version = version.get_semantic_ocs_version_from_config()
    if (
        ocs_version >= version.VERSION_4_9
        and ocsci_config.ENV_DATA["enable_console_plugin"]
    ):
        log.info("Enabling console plugin")
        path = "/spec/plugins"
        params = f"""[{{"op": "add", "path": "{path}", "value": {value}}}]"""
        ocp_obj = OCP(kind=constants.CONSOLE_CONFIG)
        ocp_obj.patch(params=params, format_type="json"), (
            "Failed to run patch command to update odf-console"
        )


def get_non_acm_cluster_config():
    """
    Get a list of non-acm cluster's config objects

    Returns:
        list: of cluster config objects

    """
    non_acm_list = []
    for i in range(len(ocsci_config.clusters)):
        if i in get_all_acm_indexes():
            continue
        else:
            non_acm_list.append(ocsci_config.clusters[i])

    return non_acm_list


def get_all_acm_indexes():
    """
    Get indexes fro all ACM clusters
    This is more relevant in case of MDR scenario

    Returns:
        list: A list of ACM indexes

    """
    acm_indexes = []
    for cluster in ocsci_config.clusters:
        if cluster.MULTICLUSTER["acm_cluster"]:
            acm_indexes.append(cluster.MULTICLUSTER["multicluster_index"])
    return acm_indexes


def enable_mco_console_plugin():
    """
    Enables console plugin for MCO
    """
    if (
        "odf-multicluster-console"
        in OCP(kind="console.operator", resource_name="cluster").get()["spec"][
            "plugins"
        ]
    ):
        log.info("MCO console plugin is enabled")
    else:
        patch = '\'[{"op": "add", "path": "/spec/plugins/-", "value": "odf-multicluster-console"}]\''
        patch_cmd = (
            f"patch console.operator cluster -n openshift-console"
            f" --type json -p {patch}"
        )
        log.info("Enabling MCO console plugin")
        ocp_obj = OCP()
        ocp_obj.exec_oc_cmd(command=patch_cmd)


def get_active_acm_index():
    """
    Get index of active acm cluster
    """
    for cluster in ocsci_config.clusters:
        if cluster.MULTICLUSTER["active_acm_cluster"]:
            return cluster.MULTICLUSTER["multicluster_index"]


def get_passive_acm_index():
    """
    Get index of passive acm cluster
    """
    for cluster in ocsci_config.clusters:
        if (
            cluster.MULTICLUSTER["acm_cluster"]
            and not cluster.MULTICLUSTER["active_acm_cluster"]
        ):
            return cluster.MULTICLUSTER["multicluster_index"]


def get_primary_cluster_config():
    """
    Get the primary cluster config object in a DR scenario

    Return:
        framework.config: primary cluster config obhect from config.clusters

    """
    for cluster in ocsci_config.clusters:
        if cluster.MULTICLUSTER["primary_cluster"]:
            return cluster


def thread_init_class(class_init_operations, shutdown):
    if len(class_init_operations) > 0:
        executor = ThreadPoolExecutor(max_workers=len(class_init_operations))
        futures = []
        i = 0
        for operation in class_init_operations:
            i += 1
            future = executor.map(operation)
            futures.append(future)
            if i == shutdown:
                future.add_done_callback(executor.shutdown(wait=False))
                return
        if shutdown == 0:
            executor.shutdown(wait=True)
            return


def label_pod_security_admission(namespace=None, upgrade_version=None):
    """
    Label PodSecurity admission

    Args:
        namespace (str): Namespace name
        upgrade_version (semantic_version.Version): ODF semantic version for upgrade
            if it's an upgrade run, otherwise None.
    """
    namespace = namespace or ocsci_config.ENV_DATA["cluster_namespace"]
    log.info(f"Labelling namespace {namespace} for PodSecurity admission")
    if version.get_semantic_ocp_running_version() >= version.VERSION_4_12 or (
        upgrade_version and upgrade_version >= version.VERSION_4_12
    ):
        ocp_obj = OCP(kind="namespace")
        label = (
            "security.openshift.io/scc.podSecurityLabelSync=false "
            f"pod-security.kubernetes.io/enforce={constants.PSA_PRIVILEGED} "
            f"pod-security.kubernetes.io/warn={constants.PSA_BASELINE} "
            f"pod-security.kubernetes.io/audit={constants.PSA_BASELINE} --overwrite"
        )
        ocp_obj.add_label(resource_name=namespace, label=label)


def collect_pod_container_rpm_package(dir_name):
    """
    Collect information about rpm packages from all containers + go version

    Args:
        dir_name(str): directory to store container rpm package info

    """
    # Import pod here to avoid circular dependency issue
    from ocs_ci.ocs.resources import pod

    timestamp = time.time()
    cluster_namespace = ocsci_config.ENV_DATA["cluster_namespace"]

    log_dir_path = os.path.join(
        os.path.expanduser(ocsci_config.RUN["log_dir"]),
        f"{dir_name}_{ocsci_config.RUN['run_id']}",
    )
    package_log_dir_path = os.path.join(
        log_dir_path, "rpm_package", f"rpm_list_{timestamp}"
    )
    create_directory_path(package_log_dir_path)
    log.info(f"Directory path for rpm logs is {package_log_dir_path}")
    pods = pod.get_all_pods(namespace=cluster_namespace)
    ocp_obj = OCP(namespace=cluster_namespace)
    for pod_obj in pods:
        pod_object = pod_obj.get()
        pod_containers = pod_object.get("spec").get("containers")
        ocp_pod_obj = OCP(kind=constants.POD, namespace=cluster_namespace)
        pod_status = ocp_pod_obj.get_resource_status(pod_obj.name)
        if pod_status == constants.STATUS_RUNNING:
            for container in pod_containers:
                container_output = ""
                go_output = ""
                container_name = container["name"]
                command = f"exec -i {pod_obj.name} -c {container_name} -- rpm -qa"
                go_command = (
                    f"exec -i {pod_obj.name} -c {container_name} --"
                    " /bin/bash -c '[ -f /go.version ] && cat /go.version || exit 0'"
                )
                try:
                    container_output = ocp_obj.exec_oc_cmd(command)
                    go_output = ocp_obj.exec_oc_cmd(go_command)
                except Exception as e:
                    log.warning(
                        f"Following exception {e} was raised for pod {pod_obj.name} and container {container_name}"
                    )
                if container_output:
                    log_file_name = f"{package_log_dir_path}/{pod_obj.name}-{container_name}-rpm.log"
                    with open(log_file_name, "w") as f:
                        f.write(container_output)
                if go_output:
                    go_log_file_name = f"{package_log_dir_path}/{pod_obj.name}-{container_name}-go-version.log"
                    with open(go_log_file_name, "w") as f:
                        f.write(go_output)


def is_dr_scenario():
    """
    Check if it is RDR or MDR setup

    Returns:
        bool: return True if it is rdr or mdr setup otherwise False

    """
    return ocsci_config.MULTICLUSTER.get("multicluster_mode") in (
        "metro-dr",
        "regional-dr",
    )


def get_dr_operator_versions():
    """
    Get all DR operator versions on hub and primary clusters

    Returns:
        dict: return operator name as key and version as value

    """
    versions_dic = dict()
    if is_dr_scenario():
        with ocsci_config.RunWithAcmConfigContext():
            acm_operator_version = get_acm_version()
            if acm_operator_version:
                versions_dic["acm_version"] = acm_operator_version
            ocp_dr_hub_operator_version = get_dr_hub_operator_version()
            if ocp_dr_hub_operator_version:
                versions_dic["dr_hub_version"] = ocp_dr_hub_operator_version
            odf_multicluster_orchestrator_version = (
                get_odf_multicluster_orchestrator_version()
            )
            if odf_multicluster_orchestrator_version:
                versions_dic[
                    "odf_multicluster_orchestrator_version"
                ] = odf_multicluster_orchestrator_version
        with ocsci_config.RunWithPrimaryConfigContext():
            oadp_operator_version = get_oadp_version()
            if oadp_operator_version:
                versions_dic["oadp_version"] = oadp_operator_version
            ocp_dr_cluster_operator_version = get_dr_cluster_operator_version()
            if ocp_dr_cluster_operator_version:
                versions_dic["dr_cluster_version"] = ocp_dr_cluster_operator_version
            gitops_operator_version = get_ocp_gitops_operator_version()
            if gitops_operator_version:
                versions_dic["gitops_version"] = gitops_operator_version
            volsync_operator_version = get_volsync_operator_version()
            if volsync_operator_version:
                versions_dic["volsync_version"] = volsync_operator_version
            submariner_operator_version = get_submariner_operator_version()
            if submariner_operator_version:
                versions_dic["submariner_version"] = submariner_operator_version
    return versions_dic
