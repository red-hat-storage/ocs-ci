import getpass
import json
import logging
import os
import platform
import random
import shlex
import smtplib
import subprocess
import time
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from reportportal_client import ReportPortalServiceAsync

from ocs import defaults
from ocs.exceptions import CommandFailed, UnsupportedOSType
from ocsci.enums import TestStatus
from .aws import AWS

log = logging.getLogger(__name__)

# variables
mounting_dir = '/mnt/cephfs/'
clients = []
md5sum_list1 = []
md5sum_list2 = []
fuse_clients = []
kernel_clients = []
mon_node = ''
mon_node_ip = ''
mds_nodes = []
md5sum_file_lock = []
active_mdss = []
RC = []
failure = {}
output = []
unique_test_names = []


# function for getting the clients
def get_client_info(ceph_nodes, clients):
    log.info("Getting Clients")
    for node in ceph_nodes:
        if node.role == 'client':
            clients.append(node)
    # Identifying MON node
    for node in ceph_nodes:
        if node.role == 'mon':
            mon_node = node
            out, err = mon_node.exec_command(cmd='sudo hostname -I')
            mon_node_ip = out.read().decode().rstrip('\n')
            break
    for node in ceph_nodes:
        if node.role == 'mds':
            mds_nodes.append(node)
    for node in clients:
        node.exec_command(cmd='sudo yum install -y attr')

    fuse_clients = clients[0:2]  # seperating clients for fuse and kernel
    kernel_clients = clients[2:4]
    return fuse_clients, kernel_clients, mon_node, mounting_dir, mds_nodes, md5sum_file_lock, mon_node_ip


# function for providing authorization to the clients from MON ndoe
def auth_list(clients, mon_node):
    for node in clients:
        log.info("Giving required permissions for clients from MON node:")
        mon_node.exec_command(
            cmd="sudo ceph auth get-or-create client.%s mon 'allow *' mds 'allow *, allow rw path=/' "
                "osd 'allow rw pool=cephfs_data' -o /etc/ceph/ceph.client.%s.keyring" % (node.hostname, node.hostname))
        out, err = mon_node.exec_command(
            sudo=True, cmd='cat /etc/ceph/ceph.client.%s.keyring' % (node.hostname))
        keyring = out.read().decode()
        key_file = node.write_file(
            sudo=True,
            file_name='/etc/ceph/ceph.client.%s.keyring' % (node.hostname),
            file_mode='w')
        key_file.write(keyring)

        key_file.flush()

        node.exec_command(cmd="sudo chmod 644 /etc/ceph/ceph.client.%s.keyring" % (node.hostname))
        # creating mounting directory
        node.exec_command(cmd='sudo mkdir %s' % (mounting_dir))


# MOunting single FS with ceph-fuse
def fuse_mount(fuse_clients, mounting_dir):
    try:
        for client in fuse_clients:
            log.info("Creating mounting dir:")
            log.info("Mounting fs with ceph-fuse on client %s:" % (client.hostname))
            client.exec_command(cmd="sudo ceph-fuse -n client.%s %s" % (client.hostname, mounting_dir))
            out, err = client.exec_command(cmd='mount')
            mount_output = out.read().decode()
            mount_output.split()
            log.info("Checking if fuse mount is is passed of failed:")
            if 'fuse' in mount_output:
                log.info("ceph-fuse mounting passed")
            else:
                log.error("ceph-fuse mounting failed")
        return md5sum_list1
    except Exception as e:
        log.error(e)


def kernel_mount(mounting_dir, mon_node_ip, kernel_clients):
    try:
        for client in kernel_clients:
            out, err = client.exec_command(cmd='sudo ceph auth get-key client.%s' % (client.hostname))
            secret_key = out.read().decode().rstrip('\n')
            mon_node_ip = mon_node_ip.replace(" ", "")
            client.exec_command(
                cmd='sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s' % (
                    mon_node_ip, mounting_dir, client.hostname, secret_key))
            out, err = client.exec_command(cmd='mount')
            mount_output = out.read().decode()
            mount_output.split()
            log.info("Checking if kernel mount is is passed of failed:")
            if '%s:6789:/' % (mon_node_ip) in mount_output:
                log.info("kernel mount passed")
            else:
                log.error("kernel mount failed")
        return md5sum_list2
    except Exception as e:
        log.error(e)


def fuse_client_io(client, mounting_dir):
    try:
        rand_count = random.randint(1, 5)
        rand_bs = random.randint(100, 300)
        log.info("Performing IOs on fuse-clients")
        client.exec_command(
            cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d" %
                (mounting_dir, client.hostname, rand_bs, rand_count),
            long_running=True)
    except Exception as e:
        log.error(e)


def kernel_client_io(client, mounting_dir):
    try:
        rand_count = random.randint(1, 6)
        rand_bs = random.randint(100, 500)
        log.info("Performing IOs on kernel-clients")
        client.exec_command(
            cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d" %
                (mounting_dir, client.hostname, rand_bs, rand_count),
            long_running=True)
    except Exception as e:
        log.error(e)


def fuse_client_md5(fuse_clients, md5sum_list1):
    try:
        log.info("Calculating MD5 sums of files in fuse-clients:")
        for client in fuse_clients:
            md5sum_list1.append(
                client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir), long_running=True))

    except Exception as e:
        log.error(e)


def kernel_client_md5(kernel_clients, md5sum_list2):
    try:
        log.info("Calculating MD5 sums of files in kernel-clients:")
        for client in kernel_clients:
            md5sum_list2.append(
                client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir), long_running=True))
    except Exception as e:
        log.error(e)


# checking file locking mechanism
def file_locking(client):
    try:
        to_lock_file = """
import fcntl
import subprocess
import time
try:
    f = open('/mnt/cephfs/to_test_file_lock', 'w+')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print "locking file:--------------------------------"
    subprocess.check_output(["sudo","dd","if=/dev/zero","of=/mnt/cephfs/to_test_file_lock","bs=1M","count=2"])
except IOError as e:
    print e
finally:
    print "Unlocking file:------------------------------"
    fcntl.lockf(f,fcntl.LOCK_UN)
            """
        to_lock_code = client.write_file(
            sudo=True,
            file_name='/home/cephuser/file_lock.py',
            file_mode='w')
        to_lock_code.write(to_lock_file)
        to_lock_code.flush()
        out, err = client.exec_command(cmd="sudo python /home/cephuser/file_lock.py")
        output = out.read().decode()
        output.split()
        if 'Errno 11' in output:
            log.info("File locking achieved, data is not corrupted")
        elif 'locking' in output:
            log.info("File locking achieved, data is not corrupted")
        else:
            log.error("Data is corrupted")

        out, err = client.exec_command(cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" % (mounting_dir))

        md5sum_file_lock.append(out.read().decode())

    except Exception as e:
        log.error(e)


def activate_multiple_mdss(mds_nodes):
    try:
        log.info("Activating Multiple MDSs")
        for node in mds_nodes:
            out1, err = node.exec_command(cmd="sudo ceph fs set cephfs allow_multimds true --yes-i-really-mean-it")
            out2, err = node.exec_command(cmd="sudo ceph fs set cephfs max_mds 2")
            break

    except Exception as e:
        log.error(e)


def mkdir_pinning(clients, range1, range2, dir_name, pin_val):
    try:
        log.info("Creating Directories and Pinning to MDS %s" % (pin_val))
        for client in clients:
            for num in range(range1, range2):
                out, err = client.exec_command(cmd='sudo mkdir %s%s_%d' % (mounting_dir, dir_name, num))
                if pin_val != '':
                    client.exec_command(
                        cmd='sudo setfattr -n ceph.dir.pin -v %s %s%s_%d' % (pin_val, mounting_dir, dir_name, num))
                else:
                    print("Pin val not given")
                print(out.read().decode())
                print(time.time())
            break
    except Exception as e:
        log.error(e)


def allow_dir_fragmentation(mds_nodes):
    try:
        log.info("Allowing directorty fragmenation for splitting")
        for node in mds_nodes:
            node.exec_command(cmd='sudo ceph fs set cephfs allow_dirfrags 1')
            break
    except Exception as e:
        log.error(e)


def mds_fail_over(mds_nodes):
    try:
        rand = random.randint(0, 1)
        for node in mds_nodes:
            log.info("Failing MDS %d" % (rand))
            node.exec_command(cmd='sudo ceph mds fail %d' % (rand))
            break

    except Exception as e:
        log.error(e)


def pinned_dir_io(clients, mds_fail_over, num_of_files, range1, range2):
    try:
        log.info("Performing IOs and MDSfailovers on clients")
        for client in clients:
            client.exec_command(cmd='sudo pip install crefi')
            for num in range(range1, range2):
                if mds_fail_over != '':
                    mds_fail_over(mds_nodes)
                out, err = client.exec_command(cmd='sudo crefi -n %d %sdir_%d' % (num_of_files, mounting_dir, num))
                rc = out.channel.recv_exit_status()
                print(out.read().decode())
                RC.append(rc)
                print(time.time())
                if rc == 0:
                    log.info("Client IO is going on,success")
                else:
                    log.error("Client IO got interrupted")
                    failure.update({client: out})
                    break
            break

    except Exception as e:
        log.error(e)


def rc_verify(tc, RC):
    return_codes_set = set(RC)

    if len(return_codes_set) == 1:

        out = "Test case %s Passed" % (tc)

        return out
    else:
        out = "Test case %s Failed" % (tc)

        return out


# colors for pass and fail status
# class Bcolors:
#     HEADER = '\033[95m'
#     OKGREEN = '\033[92m'
#     FAIL = '\033[91m'
#     ENDC = '\033[0m'
#     BOLD = '\033[1m'


def configure_logger(test_name, run_dir, level=logging.DEBUG):
    """
    Configures a new FileHandler for the root logger.

    Args:
        test_name: name of the test being executed. used for naming the logfile
        run_dir: directory where logs are being placed
        level: logging level

    Returns:
        URL where the log file can be viewed or None if the run_dir does not exist
    """
    if not os.path.isdir(run_dir):
        log.error("Run directory '{run_dir}' does not exist, logs will not output to file.".format(run_dir=run_dir))
        return None
    _root = logging.getLogger()

    full_log_name = "{test_name}.log".format(test_name=test_name)
    test_logfile = os.path.join(run_dir, full_log_name)
    log.info("Test logfile: {}".format(test_logfile))
    close_and_remove_filehandlers()
    _handler = logging.FileHandler(test_logfile)
    _handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    _handler.setFormatter(formatter)
    _root.addHandler(_handler)

    url_base = "http://magna002.ceph.redhat.com/cephci-jenkins"
    run_dir_name = run_dir.split('/')[-1]
    log_url = "{url_base}/{run_dir}/{log_name}".format(url_base=url_base, run_dir=run_dir_name, log_name=full_log_name)

    log.info("Completed log configuration")
    return log_url


def create_run_dir(run_id):
    """
    Create the directory where test logs will be placed.

    Args:
        run_id: id of the test run. used to name the directory

    Returns:
        Full path of the created directory
    """
    dir_name = "cephci-run-{run_id}".format(run_id=run_id)
    base_dir = "/ceph/cephci-jenkins"
    if not os.path.isdir(base_dir):
        base_dir = "/tmp"
    run_dir = os.path.join(base_dir, dir_name)
    try:
        os.makedirs(run_dir)
    except OSError:
        if "jenkins" in getpass.getuser():
            raise

    return run_dir


def close_and_remove_filehandlers(logger=logging.getLogger()):
    """
    Close FileHandlers and then remove them from the loggers handlers list.

    Args:
        logger: the logger in which to remove the handlers from, defaults to root logger

    Returns:
        None
    """
    handlers = logger.handlers[:]
    for h in handlers:
        if isinstance(h, logging.FileHandler):
            h.close()
            logger.removeHandler(h)


def create_report_portal_session():
    """
    Configures and creates a session to the Report Portal instance.

    Returns:
        The session object
    """
    cfg = get_ocsci_config()['report-portal']

    return ReportPortalServiceAsync(
        endpoint=cfg['endpoint'], project=cfg['project'], token=cfg['token'], error_handler=error_handler)


def timestamp():
    """
    The current epoch timestamp in milliseconds as a string.

    Returns:
        The timestamp
    """
    return str(int(time.time() * 1000))


def error_handler(exc_info):
    """
    Error handler for the Report Portal session.

    Returns:
        None
    """
    print("Error occurred: {}".format(exc_info[1]))
    traceback.print_exception(*exc_info)


def create_unique_test_name(test_name):
    """
    Creates a unique test name using the actual test name and an increasing integer for each duplicate test name.

    Args:
        test_name: name of the test

    Returns:
        unique name for the test case
    """
    global unique_test_names
    base = "_".join(test_name.split())
    num = 0
    while "{base}_{num}".format(base=base, num=num) in unique_test_names:
        num += 1
    name = "{base}_{num}".format(base=base, num=num)
    unique_test_names.append(name)
    return name


def get_latest_container_image_tag(version):
    """
    Retrieves the container image tag of the latest compose for the given version

    Args:
        version: version to get the latest image tag for (2.x, 3.0, or 3.x)

    Returns:
        str: Image tag of the latest compose for the given version

    """
    image_tag = get_latest_container(version).get('docker_tag')
    log.info("Found image tag: {image_tag}".format(image_tag=image_tag))
    return str(image_tag)


def get_latest_container(version):
    """
    Retrieves latest nightly-build container details from magna002.ceph.redhat.com

    Args:
        version: version to get the latest image tag, should match ceph-container-latest-{version} filename at magna002
                 storage

    Returns:
        Container details dictionary with given format:
        {'docker_registry': docker_registry, 'docker_image': docker_image, 'docker_tag': docker_tag}
    """
    url = 'http://magna002.ceph.redhat.com/latest-ceph-container-builds/latest-RHCEPH-{version}.json'.format(
        version=version)
    data = requests.get(url)
    docker_registry, docker_image_tag = data.json()['repository'].split('/')
    docker_image, docker_tag = docker_image_tag.split(':')
    return {'docker_registry': docker_registry, 'docker_image': docker_image, 'docker_tag': docker_tag}


def custom_ceph_config(suite_config, custom_config, custom_config_file):
    """
    Combines and returns custom configuration overrides for ceph.
    Hierarchy is as follows:
        custom_config > custom_config_file > suite_config

    Args:
        suite_config: ceph_conf_overrides that currently exist in the test suite
        custom_config: custom config args provided by the cli (these all go to the global scope)
        custom_config_file: path to custom config yaml file provided by the cli

    Returns
        New value to be used for ceph_conf_overrides in test config
    """
    log.debug("Suite config: {}".format(suite_config))
    log.debug("Custom config: {}".format(custom_config))
    log.debug("Custom config file: {}".format(custom_config_file))

    full_custom_config = suite_config or {}
    cli_config_dict = {}
    custom_config_dict = {}

    # retrieve custom config from file
    if custom_config_file:
        with open(custom_config_file) as f:
            custom_config_dict = yaml.load(f)
            log.info("File contents: {}".format(custom_config_dict))

    # format cli configs into dict
    if custom_config:
        cli_config_dict = dict(item.split('=') for item in custom_config)

    # combine file and cli configs
    if cli_config_dict:
        if not custom_config_dict.get('global'):
            custom_config_dict['global'] = {}
        for key, value in cli_config_dict.items():
            custom_config_dict['global'][key] = value

    # combine file and suite configs
    for key, value in custom_config_dict.items():
        subsection = {}
        if full_custom_config.get(key):
            subsection.update(full_custom_config[key])
        subsection.update(value)
        full_custom_config[key] = subsection

    log.info("Full custom config: {}".format(full_custom_config))
    return full_custom_config


def email_results(results_list, run_id, send_to_cephci=False):
    """
    Email results of test run to QE

    Args:
        results_list (list): test case results info
        run_id (str): id of the test run
        send_to_cephci (bool): send to cephci@redhat.com as well as user email

    Returns: None

    """
    cfg = get_ocsci_config().get('email')
    sender = "ocs-ci@redhat.com"
    recipients = []
    if cfg and cfg.get('address'):
        recipients = [cfg['address']]
    else:
        log.warning("No email address configured in ~/.ocs-ci.yaml. "
                    "Please configure if you would like to receive run result emails.")

    if send_to_cephci:
        pass  # TODO: determine email address to use for ocs-ci results and append to recipients
        # recipients.append(sender)

    if recipients:
        run_name = "cephci-run-{id}".format(id=run_id)
        log_link = "http://magna002.ceph.redhat.com/cephci-jenkins/{run}/".format(run=run_name)

        msg = MIMEMultipart('alternative')
        msg['Subject'] = "cephci results for {run}".format(run=run_name)
        msg['From'] = sender
        msg['To'] = ", ".join(recipients)

        project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        template_dir = os.path.join(project_dir, 'templates')

        env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        template = env.get_template('result-email-template.html')

        html = template.render(run_name=run_name,
                               log_link=log_link,
                               test_results=results_list)

        part1 = MIMEText(html, 'html')
        msg.attach(part1)

        try:
            s = smtplib.SMTP('localhost')
            s.sendmail(sender, recipients, msg.as_string())
            s.quit()
            log.info("Results have been emailed to {recipients}".format(recipients=recipients))

        except Exception as e:
            print("\n")
            log.exception(e)


def get_ocsci_config():
    """
    Receives the data from ~/.ocs-ci.yaml.

    Returns:
        (dict) configuration from ~/.ocs-ci.yaml

    """
    home_dir = os.path.expanduser("~")
    cfg_file = os.path.join(home_dir, ".ocs-ci.yaml")
    try:
        with open(cfg_file, "r") as yml:
            cfg = yaml.safe_load(yml)
    except IOError:
        log.error(
            "Please create ~/.ocs-ci.yaml from the ocs-ci.yaml.template. "
            "See README for more information."
        )
        raise
    return cfg


def run_cmd(cmd, **kwargs):
    """
    Run an arbitrary command locally

    Args:
        cmd (str): command to run

    Raises:
        CommandFailed: In case the command execution fails

    Returns:
        (str) Decoded stdout of command

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
        url (str): URL of the file to download
        filename (str): Name of the file to write the download to

    """
    with open(filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    assert r.ok


def destroy_cluster(cluster_path):
    """
    Destroy existing cluster resources in AWS.

    Args:
        cluster_path (str): filepath to cluster directory to be destroyed

    Returns:
        TestStatus: enum for status of cluster deletion

    """
    # Download installer
    installer = get_openshift_installer()

    destroy_cmd = (
        f"{installer} destroy cluster "
        f"--dir {cluster_path} "
        f"--log-level debug"
    )

    try:
        cluster_path = os.path.normpath(cluster_path)

        # Retrieve cluster name and aws region from metadata
        metadata_file = os.path.join(cluster_path, "metadata.json")
        with open(metadata_file) as f:
            metadata = json.loads(f.read())
        cluster_name = metadata.get("clusterName")
        region_name = metadata.get("aws").get("region")

        # Execute destroy cluster using OpenShift installer
        log.info(f"Destroying cluster defined in {cluster_path}")
        run_cmd(destroy_cmd)

        # Find and delete volumes
        aws = AWS(region_name)
        volume_pattern = f"{cluster_name}*"
        log.debug(f"Finding volumes with pattern: {volume_pattern}")
        volumes = aws.get_volumes_by_name_pattern(volume_pattern)
        log.debug(f"Found volumes: \n {volumes}")
        for volume in volumes:
            aws.detach_and_delete_volume(volume)

        # Remove installer
        os.remove(installer)
        return TestStatus.PASSED

    except Exception:
        log.error(traceback.format_exc())
        return TestStatus.FAILED


def get_openshift_installer(version=defaults.INSTALLER_VERSION):
    """
    Get path of the openshift installer binary, download it if not available.

    Args:
        version (str): version of the installer to download

    Returns:
        str: path of the installer binary

    """
    installer_filename = "openshift-install"
    installer_binary_path = os.path.join(defaults.BIN_DIR, installer_filename)
    if os.path.isfile(installer_binary_path):
        log.info("Installer exists, skipping download")
        # TODO: check installer version
    else:
        log.info("Downloading openshift installer")
        if platform.system() == "Darwin":
            os_type = "mac"
        elif platform.system() == "Linux":
            os_type = "linux"
        else:
            raise UnsupportedOSType
        url = (
            f"https://mirror.openshift.com/pub/openshift-v4/clients/ocp/"
            f"{version}/openshift-install-{os_type}-{version}.tar.gz"
        )
        # Prepare BIN_DIR
        try:
            os.mkdir(defaults.BIN_DIR)
        except FileExistsError:
            pass
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(defaults.BIN_DIR)
        tarball = f"{installer_filename}.tar.gz"
        download_file(url, tarball)
        run_cmd(f"tar xzvf {tarball}")
        os.remove(tarball)
        # return to the previous working directory
        os.chdir(previous_dir)

    return installer_binary_path

def get_openshift_client(version=defaults.CLIENT_VERSION):
    """
    Get path of the openshift client binary, download it if not available.

    Args:
        version (str): version of the client to download

    Returns:
        str: path of the client binary

    """
    client_binary_path = os.path.join(defaults.BIN_DIR, 'oc')
    if os.path.isfile(client_binary_path):
        log.info("Client exists, skipping download")
        # TODO: check client version
    else:
        log.info("Downloading openshift client")
        if platform.system() == "Darwin":
            os_type = "mac"
        elif platform.system() == "Linux":
            os_type = "linux"
        else:
            raise UnsupportedOSType
        url = (
            f"https://mirror.openshift.com/pub/openshift-v4/clients/ocp/"
            f"{version}/openshift-client-{os_type}-{version}.tar.gz"
        )
        # Prepare BIN_DIR
        try:
            os.mkdir(defaults.BIN_DIR)
        except FileExistsError:
            pass
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(defaults.BIN_DIR)
        tarball = "openshift-client.tar.gz"
        download_file(url, tarball)
        run_cmd(f"tar xzvf {tarball}")
        os.remove(tarball)
        # return to the previous working directory
        os.chdir(previous_dir)

    return client_binary_path
