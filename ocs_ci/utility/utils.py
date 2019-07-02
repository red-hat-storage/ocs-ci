import json
import logging
import os
import platform
import random
import shlex
import string
import subprocess
import time
import traceback
import requests
import yaml
import re
import smtplib

from ocs_ci.ocs.exceptions import (
    CommandFailed, UnsupportedOSType, TimeoutExpiredError,
)
from ocs_ci.framework import config
from ocs_ci.utility.aws import AWS
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from ocs_ci.ocs import constants
from bs4 import BeautifulSoup

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
            f"Error during execution of command: {cmd}."
            f"\nError is {r.stderr.decode()}"
        )
    return r.stdout.decode()


def download_file(url, filename):
    """
    Download a file from a specified url

    Args:
        url (str): URL of the file to download
        filename (str): Name of the file to write the download to

    """
    log.debug(f"Download '{url}' to '{filename}'.")
    with open(filename, "wb") as f:
        r = requests.get(url)
        f.write(r.content)
    assert r.ok


def destroy_cluster(cluster_path):
    """
    Destroy existing cluster resources in AWS.

    Args:
        cluster_path (str): filepath to cluster directory to be destroyed

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
        delete_file(installer)

    except Exception:
        log.error(traceback.format_exc())


def get_openshift_installer(
    version=None,
    bin_dir=None,
    force_download=False,
):
    """
    Download the OpenShift installer binary, if not already present.
    Update env. PATH and get path of the openshift installer binary.

    Args:
        version (str): Version of the installer to download
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
        force_download (bool): Force installer download even if already present

    Returns:
        str: Path to the installer binary

    """
    version = version or config.DEPLOYMENT['installer_version']
    bin_dir = os.path.expanduser(bin_dir or config.RUN['bin_dir'])
    installer_filename = "openshift-install"
    installer_binary_path = os.path.join(bin_dir, installer_filename)
    if os.path.isfile(installer_binary_path) and force_download:
        delete_file(installer_binary_path)
    if os.path.isfile(installer_binary_path):
        log.debug(f"Installer exists ({installer_binary_path}), skipping download.")
        # TODO: check installer version
    else:
        log.info(f"Downloading openshift installer ({version}).")
        prepare_bin_dir()
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(bin_dir)
        tarball = f"{installer_filename}.tar.gz"
        url = get_openshift_mirror_url(installer_filename, version)
        download_file(url, tarball)
        run_cmd(f"tar xzvf {tarball} {installer_filename}")
        delete_file(tarball)
        # return to the previous working directory
        os.chdir(previous_dir)

    installer_version = run_cmd(f"{installer_binary_path} version")
    log.info(f"OpenShift Installer version: {installer_version}")

    return installer_binary_path


def get_openshift_client(
    version=None,
    bin_dir=None,
    force_download=False,
):
    """
    Download the OpenShift client binary, if not already present.
    Update env. PATH and get path of the oc binary.

    Args:
        version (str): Version of the client to download
            (default: config.RUN['client_version'])
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
        force_download (bool): Force client download even if already present

    Returns:
        str: Path to the client binary

    """
    version = version or config.RUN['client_version']
    bin_dir = os.path.expanduser(bin_dir or config.RUN['bin_dir'])
    client_binary_path = os.path.join(bin_dir, 'oc')
    if os.path.isfile(client_binary_path) and force_download:
        delete_file(client_binary_path)
    if os.path.isfile(client_binary_path):
        log.debug(f"Client exists ({client_binary_path}), skipping download.")
        # TODO: check client version
    else:
        log.info(f"Downloading openshift client ({version}).")
        prepare_bin_dir()
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(bin_dir)
        url = get_openshift_mirror_url('openshift-client', version)
        tarball = "openshift-client.tar.gz"
        download_file(url, tarball)
        run_cmd(f"tar xzvf {tarball} oc kubectl")
        delete_file(tarball)
        # return to the previous working directory
        os.chdir(previous_dir)

    client_version = run_cmd(f"{client_binary_path} version")
    log.info(f"OpenShift Client version: {client_version}")

    return client_binary_path


def get_openshift_mirror_url(file_name, version):
    """
    Format url to OpenShift mirror (for client and installer download).

    Args:
        file_name (str): Name of file
        version (str): Version of the installer or client to download

    Returns:
        str: Url of the desired file (installer or client)

    """
    if platform.system() == "Darwin":
        os_type = "mac"
    elif platform.system() == "Linux":
        os_type = "linux"
    else:
        raise UnsupportedOSType
    url = (
        f"https://mirror.openshift.com/pub/openshift-v4/clients/ocp/"
        f"{version}/{file_name}-{os_type}-{version}.tar.gz"
    )
    return url


def prepare_bin_dir(bin_dir=None):
    """
    Prepare bin directory for OpenShift client and installer

    Args:
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
    """
    bin_dir = os.path.expanduser(bin_dir or config.RUN['bin_dir'])
    try:
        os.mkdir(bin_dir)
        log.info(f"Directory '{bin_dir}' successfully created.")
    except FileExistsError:
        log.debug(f"Directory '{bin_dir}' already exists.")


def add_path_to_env_path(path):
    """
    Add path to the PATH environment variable (if not already there).

    Args:
        path (str): Path which should be added to the PATH env. variable

    """
    env_path = os.environ['PATH'].split(os.pathsep)
    if path not in env_path:
        os.environ['PATH'] = os.pathsep.join([path] + env_path)
        log.info(f"Path '{path}' added to the PATH environment variable.")
    log.debug(f"PATH: {os.environ['PATH']}")


def delete_file(file_name):
    """
    Delete file_name

    Args:
        file_name (str): Path to the file you want to delete
    """
    os.remove(file_name)


class TimeoutSampler(object):
    """
    Samples the function output.

    This is a generator object that at first yields the output of function
    `func`. After the yield, it either raises instance of `timeout_exc_cls` or
    sleeps `sleep` seconds.

    Yielding the output allows you to handle every value as you wish.

    Feel free to set the instance variables.
    """

    def __init__(self, timeout, sleep, func, *func_args, **func_kwargs):
        self.timeout = timeout
        ''' Timeout in seconds. '''
        self.sleep = sleep
        ''' Sleep interval seconds. '''

        self.func = func
        ''' A function to sample. '''
        self.func_args = func_args
        ''' Args for func. '''
        self.func_kwargs = func_kwargs
        ''' Kwargs for func. '''

        self.start_time = None
        ''' Time of starting the sampling. '''
        self.last_sample_time = None
        ''' Time of last sample. '''

        self.timeout_exc_cls = TimeoutExpiredError
        ''' Class of exception to be raised.  '''
        self.timeout_exc_args = (self.timeout,)
        ''' An args for __init__ of the timeout exception. '''

    def __iter__(self):
        if self.start_time is None:
            self.start_time = time.time()
        while True:
            self.last_sample_time = time.time()
            try:
                yield self.func(*self.func_args, **self.func_kwargs)
            except Exception:
                pass

            if self.timeout < (time.time() - self.start_time):
                raise self.timeout_exc_cls(*self.timeout_exc_args)
            time.sleep(self.sleep)

    def wait_for_func_status(self, result):
        """
        Get function and run it for given time until success or timeout.
        (using __iter__ function)

        Args:
            result (bool): Expected result from func.

        Examples:
            sample = TimeoutSampler(
                timeout=60, sleep=1, func=some_func, func_arg1="1",
                func_arg2="2"
            )
            if not sample.waitForFuncStatus(result=True):
                raise Exception
        """
        try:
            for res in self:
                if result == res:
                    return True

        except self.timeout_exc_cls:
            log.error(
                f"({self.func.__name__}) return incorrect status after timeout"
            )
            return False


def get_random_str(size=13):
    """
    generates the random string of given size

    Args:
        size (int): number of random characters to generate

    Returns:
         str : string of random characters of given size

    """
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(size))


def run_async(command):
    """
    Run command locally and return without waiting for completion

    Args:
        command (str): The command to run.

    Returns:
        An open descriptor to be used by the calling function.

    Example:
        command = 'oc delete pvc pvc1'
        proc = run_async(command)
        ret, out, err = proc.async_communicate()
    """
    log.info(f"Executing command: {command}")
    popen_obj = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True,
        encoding='utf-8'
    )

    def async_communicate():
        """
        Wait for command to complete and fetch the result

        Returns:
            retcode, stdout, stderr of the command
        """
        stdout, stderr = popen_obj.communicate()
        retcode = popen_obj.returncode
        return retcode, stdout, stderr

    popen_obj.async_communicate = async_communicate
    return popen_obj


def is_cluster_running(cluster_path):
    from ocs_ci.ocs.openshift_ops import OCP
    return config.RUN['cli_params'].get('cluster_path') and OCP.set_kubeconfig(
        os.path.join(cluster_path, config.RUN.get('kubeconfig_location'))
    )


def decompose_html_attributes(soup, attributes):
    """
    Decomposes the given html attributes

    Args:
        soup (obj): BeautifulSoup object
        attributes (list): attributes to decompose

    Returns: None

    """
    for attribute in attributes:
        tg = soup.find_all(attrs={"class": attribute})
        for each in tg:
            each.decompose()


def parse_html_for_email(soup):
    """
    Parses the html and filters out the unnecessary data/tags/attributes
    for email reporting

    Args:
        soup (obj): BeautifulSoup object

    """
    decompose_html_attributes(soup, ["extra", "col-links"])
    soup.find(id="not-found-message").decompose()

    for tr in soup.find_all('tr'):
        for th in tr.find_all('th'):
            if "Links" in th.text:
                th.decompose()

    for p in soup.find_all('p'):
        if "(Un)check the boxes to filter the results." in p.text:
            p.decompose()
        if "pytest-html" in p.text:
            data = p.text.split("by")[0]
            p.string = data

    for ip in soup.find_all('input'):
        if not ip.has_attr('disabled'):
            ip['disabled'] = 'true'

    for td in soup.find_all('td'):
        if "pytest" in td.text or "html" in td.text:
            data = td.text.replace('&apos', '')
            td.string = data

    main_header = soup.find('h1')
    main_header.string.replace_with('OCS-CI RESULTS')


def email_reports():
    """
    Email results of test run

    """
    mailids = config.RUN['cli_params']['email']
    recipients = []
    [recipients.append(mailid) for mailid in mailids.split(",")]
    sender = "ocs-ci@redhat.com"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"ocs-ci results for RUN ID: {config.RUN['run_id']}"
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    html = config.RUN['cli_params']['--html']
    html_data = open(os.path.expanduser(html)).read()
    soup = BeautifulSoup(html_data, "html.parser")

    parse_html_for_email(soup)
    part1 = MIMEText(soup, 'html')
    msg.attach(part1)
    try:
        s = smtplib.SMTP('localhost')
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
        log.info(f"Results have been emailed to {recipients}")
    except Exception as e:
        log.exception(e)


def get_cluster_version_info():
    """
    Gets the complete cluster version information

    Returns:
        dict: cluster version information

    """
    # importing here to avoid circular imports
    from ocs_ci.ocs.ocp import OCP
    ocp = OCP(kind="clusterversion")
    cluster_version_info = ocp.get("version")
    return cluster_version_info


def get_cluster_version():
    """
    Gets the cluster version

    Returns:
         str: cluster version

    """
    return get_cluster_version_info()["status"]["desired"]["version"]


def get_cluster_image():
    """
    Gets the cluster image

    Returns:
         str: cluster image

    """
    return get_cluster_version_info()["status"]["desired"]["image"]


def get_ceph_version():
    """
    Gets the ceph version

    Returns:
         str: ceph version

    """
    # importing here to avoid circular imports
    from ocs_ci.ocs.resources import pod
    ct_pod = pod.get_ceph_tools_pod()
    ceph_version = ct_pod.exec_ceph_cmd("ceph version")
    return re.split(r'ceph version ', ceph_version['version'])[1]


def get_rook_version():
    """
    Gets the rook version

    Returns:
        str: rook version

    """
    # importing here to avoid circular imports
    from ocs_ci.ocs.resources import pod
    ct_pod = pod.get_ceph_tools_pod()
    rook_versions = ct_pod.exec_ceph_cmd("rook version", format='')
    return rook_versions['rook']


def get_csi_versions():
    """
    Gets the CSI related version information

    Returns:
        dict: CSI related version information

    """
    csi_versions = {}
    # importing here to avoid circular imports
    from ocs_ci.ocs.ocp import OCP
    ocp_pod_obj = OCP(
        kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
    )
    csi_provisioners = [
        'csi-cephfsplugin-provisioner',
        'csi-rbdplugin-provisioner'
    ]
    for provisioner in csi_provisioners:
        csi_provisioner_pod = run_cmd(
            f"oc -n {config.ENV_DATA['cluster_namespace']} get pod -l "
            f"'app={provisioner}' -o jsonpath='{{.items[0].metadata.name}}'"
        )
        desc = ocp_pod_obj.get(csi_provisioner_pod)
        for container in desc['spec']['containers']:
            name = container['image'].split("/")[-1].split(":")[0]
            version = container['image'].split("/")[-1].split(":")[1]
            csi_versions[name] = version
    return csi_versions
