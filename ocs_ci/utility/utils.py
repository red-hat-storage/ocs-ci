import hcl
import json
import logging
import os
import platform
import random
import shlex
import string
import subprocess
import time
from copy import deepcopy
from shutil import which

import requests
import yaml
import re
import smtplib

from ocs_ci.ocs.exceptions import (
    CephHealthException,
    CommandFailed,
    TagNotFoundException,
    TimeoutException,
    TimeoutExpiredError,
    UnavailableBuildException,
    UnsupportedOSType,
)
from ocs_ci.framework import config
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from ocs_ci.ocs import constants
from ocs_ci.utility.retry import retry
from bs4 import BeautifulSoup
from paramiko import SSHClient, AutoAddPolicy

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
    Hierarchy is as follows::

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
            custom_config_dict = yaml.safe_load(f)
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


def mask_secrets(plaintext, secrets):
    """
    Replace secrets in plaintext with asterisks

    Args:
        plaintext (str): The plaintext to remove the secrets from
        secrets (list): List of secret strings to replace in the plaintext

    Returns:
        str: The censored version of plaintext

    """
    if secrets:
        for secret in secrets:
            plaintext = plaintext.replace(secret, '*' * 5)
    return plaintext


def run_cmd(cmd, secrets=None, timeout=600, ignore_error=False, **kwargs):
    """
    Run an arbitrary command locally

    Args:
        cmd (str): command to run
        secrets (list): A list of secrets to be masked with asterisks
            This kwarg is popped in order to not interfere with
            subprocess.run(``**kwargs``)
        timeout (int): Timeout for the command, defaults to 600 seconds.
        ignore_error (bool): True if ignore non zero return code and do not
            raise the exception.

    Raises:
        CommandFailed: In case the command execution fails

    Returns:
        (str) Decoded stdout of command

    """
    masked_cmd = mask_secrets(cmd, secrets)
    log.info(f"Executing command: {masked_cmd}")
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    r = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        timeout=timeout,
        **kwargs
    )
    log.debug(f"Command output: {r.stdout.decode()}")
    if r.stderr and not r.returncode:
        log.warning(f"Command warning: {mask_secrets(r.stderr.decode(), secrets)}")
    if r.returncode and not ignore_error:
        raise CommandFailed(
            f"Error during execution of command: {masked_cmd}."
            f"\nError is {mask_secrets(r.stderr.decode(), secrets)}"
        )
    return mask_secrets(r.stdout.decode(), secrets)


def run_mcg_cmd(cmd, namespace=None):
    """
    Invokes `run_cmd` with a noobaa prefix

    Args:
        cmd: The MCG command to be run
        namespace: The namespace to use for the command

    Returns:
        str: Stdout of the command

    """
    namespace = namespace if namespace else config.ENV_DATA['cluster_namespace']
    return run_cmd(f'noobaa -n {namespace} ' + cmd)


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
        assert r.ok, (
            f"The URL {url} is not available! Status: {r.status_code}."
        )
        f.write(r.content)


def get_url_content(url):
    """
    Return URL content

    Args:
        url (str): URL address to return
    Returns:
        str: Content of URL

    Raises:
        AssertionError: When couldn't load URL

    """
    log.debug(f"Download '{url}' content.")
    r = requests.get(url)
    assert r.ok, f"Couldn't load URL: {url} content! Status: {r.status_code}."
    return r.content


def expose_nightly_ocp_version(version):
    """
    This helper function exposes latest nightly version of OCP. When the
    version string ends with .nightly (e.g. 4.2.0-0.nightly) it will expose
    the version to latest accepted OCP build
    (e.g. 4.2.0-0.nightly-2019-08-08-103722)

    Args:
        version (str): Verison of OCP

    Returns:
        str: Version of OCP exposed to full version if latest nighly passed

    """
    if not version.endswith(".nightly"):
        return version
    else:
        latest_nightly_url = (
            f"https://openshift-release.svc.ci.openshift.org/api/v1/"
            f"releasestream/{version}/latest"
        )
        version_url_content = get_url_content(latest_nightly_url)
        version_json = json.loads(version_url_content)
        return version_json['name']


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
    version = expose_nightly_ocp_version(version)
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
    version = expose_nightly_ocp_version(version)
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

    client_version = run_cmd(f"{client_binary_path} version --client")
    log.info(f"OpenShift Client version: {client_version}")

    return client_binary_path


def ensure_nightly_build_availability(build_url):
    base_build_url = build_url.rsplit('/', 1)[0]
    r = requests.get(base_build_url)
    extracting_condition = b"Extracting" in r.content
    if extracting_condition:
        log.info("Build is extracting now, may take up to a minute.")
    return r.ok and not extracting_condition


def get_openshift_mirror_url(file_name, version):
    """
    Format url to OpenShift mirror (for client and installer download).

    Args:
        file_name (str): Name of file
        version (str): Version of the installer or client to download

    Returns:
        str: Url of the desired file (installer or client)

    Raises:
        UnsupportedOSType: In case the OS type is not supported
        UnavailableBuildException: In case the build url is not reachable
    """
    if platform.system() == "Darwin":
        os_type = "mac"
    elif platform.system() == "Linux":
        os_type = "linux"
    else:
        raise UnsupportedOSType
    url_template = config.DEPLOYMENT.get(
        'ocp_url_template',
        "https://openshift-release-artifacts.svc.ci.openshift.org/"
        "{version}/{file_name}-{os_type}-{version}.tar.gz"
    )
    url = url_template.format(
        version=version,
        file_name=file_name,
        os_type=os_type,
    )
    sample = TimeoutSampler(
        timeout=540, sleep=5, func=ensure_nightly_build_availability,
        build_url=url,
    )
    if not sample.wait_for_func_status(result=True):
        raise UnavailableBuildException(
            f"The build url {url} is not reachable"
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

        Examples::

            sample = TimeoutSampler(
                timeout=60, sleep=1, func=some_func, func_arg1="1",
                func_arg2="2"
            )
            if not sample.wait_for_func_status(result=True):
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
    build_id = get_ocs_build_number()
    build_str = f"BUILD ID: {build_id} " if build_id else ""
    mailids = config.RUN['cli_params']['email']
    recipients = []
    [recipients.append(mailid) for mailid in mailids.split(",")]
    sender = "ocs-ci@redhat.com"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = (
        f"ocs-ci results for {get_testrun_name()} "
        f"({build_str}"
        f"RUN ID: {config.RUN['run_id']})"
    )
    msg['From'] = sender
    msg['To'] = ", ".join(recipients)

    html = config.RUN['cli_params']['--html']
    html_data = open(os.path.expanduser(html)).read()
    soup = BeautifulSoup(html_data, "html.parser")

    parse_html_for_email(soup)
    part1 = MIMEText(soup, 'html')
    msg.attach(part1)
    try:
        s = smtplib.SMTP(config.REPORTING['email']['smtp_server'])
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


def get_ocs_build_number():
    """
    Gets the build number for ocs operator

    Return:
        str: build number for ocs operator version

    """
    from ocs_ci.ocs.resources.catalog_source import CatalogSource

    build_num = ""
    ocs_catalog = CatalogSource(
        resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
        namespace="openshift-marketplace"
    )
    if config.REPORTING['us_ds'] == 'DS':
        build_info = ocs_catalog.get_image_name()
        try:
            return build_info.split("-")[1].split(".")[0]
        except (IndexError, AttributeError):
            logging.warning("No version info found for OCS operator")
    return build_num


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
            name = container['name']
            version = container['image'].split("/")[-1].split(":")[1]
            csi_versions[name] = version
    return csi_versions


def parse_pgsql_logs(data):
    """
    Parse the pgsql benchmark data from ripsaw and return
    the data in list format

    Args:
        data (str): log data from pgsql bench run

    Returns:
        list_data (list): data digestable by scripts with below format
            e.g. ( with only one item in the list)::

                [
                    {'num_clients': '2', 'num_threads': '7', 'latency_avg': '7',
                    'lat_stddev': '0', 'tps_incl': '234', 'tps_excl': '243'}
                ]

    """

    match = re.findall(
        r'\[\{\'number_.*?\'number_of_transactions_per_client\':\s+\w+}\]',
        data
    )

    list_data = []
    for log in match:
        pgsql_data = dict()
        clients = re.search(r"number_of_clients\':\s+(\d+),", log)
        if clients and clients.group(1):
            pgsql_data['num_clients'] = clients.group(1)
        threads = re.search(r"number of threads\':\s+(\d+)", log)
        if threads and threads.group(1):
            pgsql_data['num_threads'] = threads.group(1)
        lat_avg = re.search(r"latency_average_ms\':\s+(\d+)", log)
        if lat_avg and lat_avg.group(1):
            pgsql_data['latency_avg'] = lat_avg.group(1)
        lat_stddev = re.search(r"latency_stddev_ms\':\s+(\d+)", log)
        if lat_stddev and lat_stddev.group(1):
            pgsql_data['lat_stddev'] = lat_stddev.group(1)
        tps_incl = re.search(r"tps_incl_con_est\':\s+(\w+)", log)
        if tps_incl and tps_incl.group(1):
            pgsql_data['tps_incl'] = tps_incl.group(1)
        tps_excl = re.search(r"tps_excl_con_est\':\s+(\w+)", log)
        if tps_excl and tps_excl.group(1):
            pgsql_data['tps_excl'] = tps_excl.group(1)
        list_data.append(pgsql_data)

    return list_data


def create_directory_path(path):
    """
    Creates directory if path doesn't exists
    """
    path = os.path.expanduser(path)
    if not os.path.exists(path):
        os.makedirs(path)
    else:
        log.debug(f"{path} already exists")


def ocsci_log_path():
    """
    Construct the full path for the log directory.

    Returns:
        str: full path for ocs-ci log directory

    """
    return os.path.expanduser(
        os.path.join(
            config.RUN['log_dir'],
            f"ocs-ci-logs-{config.RUN['run_id']}"
        )
    )


def get_testrun_name():
    """
    Prepare testrun ID for Polarion (and other reports).

    Returns:
        str: String containing testrun name

    """
    markers = config.RUN['cli_params'].get('-m', '').replace(" ", "-")
    us_ds = config.REPORTING.get("us_ds")
    if us_ds.upper() == "US":
        us_ds = "Upstream"
    elif us_ds.upper() == "DS":
        us_ds = "Downstream"
    ocp_version = ".".join(
        config.DEPLOYMENT.get('installer_version').split('.')[:-2]
    )
    ocp_version_string = f"OCP{ocp_version}" if ocp_version else ''
    ocs_version = config.ENV_DATA.get('ocs_version')
    ocs_version_string = f"OCS{ocs_version}" if ocs_version else ''
    worker_os = 'RHEL' if config.ENV_DATA.get('rhel_workers') else 'RHCOS'
    build_user = None

    if config.REPORTING.get('display_name'):
        testrun_name = config.REPORTING.get('display_name')
    else:
        build_user = config.REPORTING.get('build_user')
        testrun_name = (
            f"{config.ENV_DATA.get('platform', '').upper()} "
            f"{config.ENV_DATA.get('deployment_type', '').upper()} "
            f"{get_az_count()}AZ "
            f"{worker_os} "
            f"{config.ENV_DATA.get('master_replicas')}M "
            f"{config.ENV_DATA.get('worker_replicas')}W "
            f"{markers}"
        )
    testrun_name = (
        f"{ocs_version_string} {us_ds} {ocp_version_string} "
        f"{testrun_name}"
    )
    if build_user:
        testrun_name = f"{build_user} {testrun_name}"
    # replace invalid character(s) by '-'
    testrun_name = testrun_name.translate(
        str.maketrans(
            {key: '-' for key in ''' \\/.:*"<>|~!@#$?%^&'*(){}+`,=\t'''}
        )
    )
    log.info("testrun_name: %s", testrun_name)
    return testrun_name


def get_az_count():
    """
    Using a number of different configuration attributes, determine how many
    availability zones the cluster is configured for.

    Returns:
        int: number of availability zones

    """
    if config.ENV_DATA.get('availability_zone_count'):
        return int(config.ENV_DATA.get('availability_zone_count'))
    elif config.ENV_DATA.get('worker_availability_zones'):
        return len(config.ENV_DATA.get('worker_availability_zones'))
    elif config.ENV_DATA.get('platform') == 'vsphere':
        return 1
    else:
        return 3


@retry((CephHealthException, CommandFailed), tries=20, delay=30, backoff=1)
def ceph_health_check(namespace=None):
    """
    Args:
        namespace (str): Namespace of OCS
            (default: config.ENV_DATA['cluster_namespace'])

    Returns: ceph_health_check_base with default retries of 20, delay of 30 seconds
    """
    return ceph_health_check_base(namespace)


def ceph_health_check_base(namespace=None):
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


def get_rook_repo(branch='master', to_checkout=None):
    """
    Clone and checkout the rook repository to specific branch/commit.

    Args:
        branch (str): Branch name to checkout
        to_checkout (str): Commit id or tag to checkout

    """
    cwd = constants.ROOK_REPO_DIR
    if not os.path.isdir(cwd):
        log.info(f"Cloning rook repository into {cwd}.")
        run_cmd(f"git clone {constants.ROOK_REPOSITORY} {cwd}")
    else:
        log.info(
            f"The rook directory {cwd} already exists, ocs-ci will skip the "
            f"clone of rook repository."
        )
        log.info("Fetching latest changes from rook repository.")
        run_cmd("git fetch --all", cwd=cwd)
    log.info(f"Checkout rook repository to specific branch: {branch}")
    run_cmd(f"git checkout {branch}", cwd=cwd)
    log.info(f"Reset branch: {branch} with latest changes")
    run_cmd(f"git reset --hard origin/{branch}", cwd=cwd)
    if to_checkout:
        run_cmd(f"git checkout {to_checkout}", cwd=cwd)


def clone_repo(url, location, branch='master', to_checkout=None):
    """
    Clone a repository or checkout latest changes if it already exists at
        specified location.

    Args:
        url (str): location of the repository to clone
        location (str): path where the repository will be cloned to
        branch (str): branch name to checkout
        to_checkout (str): commit id or tag to checkout
    """
    if not os.path.isdir(location):
        log.info("Cloning repository into %s", location)
        run_cmd(f"git clone {url} {location}")
    else:
        log.info("Repository already cloned at %s, skipping clone", location)
        log.info("Fetching latest changes from repository")
        run_cmd('git fetch --all', cwd=location)
    log.info("Checking out repository to specific branch: %s", branch)
    run_cmd(f"git checkout {branch}", cwd=location)
    log.info("Reset branch: %s with latest changes", branch)
    run_cmd(f"git reset --hard origin/{branch}", cwd=location)
    if to_checkout:
        run_cmd(f"git checkout {to_checkout}", cwd=location)


def get_latest_ds_olm_tag(upgrade=False, latest_tag=None):
    """
    This function returns latest tag of OCS downstream registry or one before
    latest if upgrade parameter is True

    Args:
        upgrade (str): If True then it returns one version of the build before
            the latest.
        latest_tag (str): Tag of the latest build. If not specified
            config.DEPLOYMENT['default_latest_tag'] or 'latest' will be used.

    Returns:
        str: latest tag for downstream image from quay registry

    Raises:
        TagNotFoundException: In case no tag found

    """
    latest_tag = latest_tag or config.DEPLOYMENT.get(
        'default_latest_tag', 'latest'
    )
    _req = requests.get(
        constants.OPERATOR_CS_QUAY_API_QUERY.format(tag_limit=20)
    )
    latest_image = None
    tags = _req.json()['tags']
    for tag in tags:
        if tag['name'] == latest_tag:
            latest_image = tag['image_id']
            break
    if not latest_image:
        raise TagNotFoundException(f"Couldn't find latest tag!")
    latest_tag_found = False
    for tag in tags:
        if not upgrade:
            if (
                tag['name'] not in constants.LATEST_TAGS
                and tag['image_id'] == latest_image
            ):
                return tag['name']
        if upgrade:
            if not latest_tag_found and tag['name'] == latest_tag:
                latest_tag_found = True
                continue
            if not latest_tag_found:
                continue
            if (
                tag['name'] not in constants.LATEST_TAGS
                and tag['image_id'] != latest_image and "rc" in tag['name']
            ):
                return tag['name']
    raise TagNotFoundException(f"Couldn't find any desired tag!")


def get_next_version_available_for_upgrade(current_tag):
    """
    This function returns the tag built after the current_version

    Args:
        current_tag (str): Current build tag from which to search the next one
            build tag.

    Returns:
        str: tag for downstream image from quay registry built after
            the current_tag.

    Raises:
        TagNotFoundException: In case no tag suitable for upgrade found

    """
    req = requests.get(
        constants.OPERATOR_CS_QUAY_API_QUERY.format(tag_limit=100)
    )
    if current_tag in constants.LATEST_TAGS:
        return current_tag
    tags = req.json()['tags']
    current_tag_index = None
    for index, tag in enumerate(tags):
        if tag['name'] == current_tag:
            if index < 2:
                raise TagNotFoundException(f"Couldn't find tag for upgrade!")
            current_tag_index = index
            break
    sliced_reversed_tags = tags[:current_tag_index]
    sliced_reversed_tags.reverse()
    for tag in sliced_reversed_tags:
        if tag['name'] not in constants.LATEST_TAGS and "rc" in tag['name']:
            return tag['name']
    raise TagNotFoundException(f"Couldn't find any tag!")


def check_if_executable_in_path(exec_name):
    """
    Checks whether an executable can be found in the $PATH

    Args:
        exec_name: Name of executable to look for

    Returns:
        Boolean: Whether the executable was found

    """
    return which(exec_name) is not None


def upload_file(server, localpath, remotepath, user=None, password=None):
    """
    Upload a file to remote server

    Args:
        server (str): Name of the server to upload
        localpath (str): Local file to upload
        remotepath (str): Target path on the remote server. filename should be included
        user (str): User to use for the remote connection

    """
    if not user:
        user = 'root'

    ssh = SSHClient()
    ssh.set_missing_host_key_policy(
        AutoAddPolicy())
    ssh.connect(hostname=server, username=user, password=password)
    sftp = ssh.open_sftp()
    log.info(f"uploading {localpath} to {user}@{server}:{remotepath}")
    sftp.put(localpath, remotepath)
    sftp.close()
    ssh.close()


def read_file_as_str(filepath):
    """
    Reads the file content

    Args:
        filepath (str): File to read

    Returns:
        str : File contents in string

    """
    with open(rf"{filepath}") as fd:
        content = fd.read()
    return content


def replace_content_in_file(file, old, new):
    """
    Replaces contents in file, if old value is not found, it adds
    new value to the file

    Args:
        file (str): Name of the file in which contents will be replaced
        old (str): Data to search for
        new (str): Data to replace the old value

    """
    # Read the file
    with open(rf"{file}", 'r') as fd:
        file_data = fd.read()

    # Replace/add the new data
    if old in file_data:
        file_data = file_data.replace(old, new)
    else:
        file_data = new + file_data

    # Write the file out again
    with open(rf"{file}", 'w') as fd:
        fd.write(file_data)


@retry((CommandFailed), tries=100, delay=10, backoff=1)
def wait_for_co(operator):
    """
    Waits for ClusterOperator to created

    Args:
        operator (str): Name of the ClusterOperator

    """
    from ocs_ci.ocs.ocp import OCP
    ocp = OCP(kind='ClusterOperator')
    ocp.get(operator)


def censor_values(data_to_censor):
    """
    This function censor string and numeric values in dictionary based on
    keys that match pattern defined in config_keys_patterns_to_censor in
    constants. It is performed recursively for nested dictionaries.

    Args:
        data_to_censor (dict): Data to censor.

    Returns:
        dict: filtered data

    """
    for key in data_to_censor:
        if isinstance(data_to_censor[key], dict):
            censor_values(data_to_censor[key])
        elif isinstance(data_to_censor[key], (str, int, float)):
            for pattern in constants.config_keys_patterns_to_censor:
                if pattern in key.lower():
                    data_to_censor[key] = '*' * 5
    return data_to_censor


def dump_config_to_file(file_path):
    """
    Dump the config to the yaml file with censored secret values.

    Args:
        file_path (str): Path to file where to write the configuration.

    """
    config_copy = deepcopy(config.to_dict())
    censor_values(config_copy)
    with open(file_path, "w+") as fs:
        yaml.safe_dump(config_copy, fs)


def create_rhelpod(namespace, pod_name, timeout=300):
    """
    Creates the RHEL pod

    Args:
        namespace (str): Namespace to create RHEL pod
        pod_name (str): Pod name
        timeout (int): wait time for RHEL pod to be in Running state

    Returns:
        pod: Pod instance for RHEL

    """
    # importing here to avoid dependencies
    from tests import helpers
    rhelpod_obj = helpers.create_pod(
        namespace=namespace,
        pod_name=pod_name,
        pod_dict_path=constants.RHEL_7_7_POD_YAML
    )
    helpers.wait_for_resource_state(rhelpod_obj, constants.STATUS_RUNNING, timeout)
    return rhelpod_obj


def check_timeout_reached(start_time, timeout, err_msg=None):
    """
    Check if timeout reached and if so raise the exception.

    Args:
        start_time (time): Star time of the operation.
        timeout (int): Timeout in seconds.
        err_msg (str): Error message for the exception.

    Raises:
        TimeoutException: In case the timeout reached.

    """
    msg = f"Timeout {timeout} reached!"
    if err_msg:
        msg += " Error: {err_msg}"

    if timeout < (time.time() - start_time):
        raise TimeoutException(msg)


def convert_yaml2tfvars(yaml):
    """
    Converts yaml file to tfvars. It creates the tfvars with the
    same filename in the required format which is used for deployment.

    Args:
        yaml (str): File path to yaml

    Returns:
        str: File path to tfvars

    """
    # importing here to avoid dependencies
    from ocs_ci.utility.templating import load_yaml
    data = load_yaml(yaml)
    tfvars_file = os.path.splitext(yaml)[0]
    with open(tfvars_file, "w+") as fd:
        for key, val in data.items():
            if key == "control_plane_ignition":
                fd.write("control_plane_ignition = <<END_OF_MASTER_IGNITION\n")
                fd.write(f"{val}\n")
                fd.write("END_OF_MASTER_IGNITION\n")
                continue

            if key == "compute_ignition":
                fd.write("compute_ignition = <<END_OF_WORKER_IGNITION\n")
                fd.write(f"{val}\n")
                fd.write("END_OF_WORKER_IGNITION\n")
                continue

            fd.write(key)
            fd.write(" = ")
            fd.write("\"")
            fd.write(f"{val}")
            fd.write("\"\n")

    return tfvars_file


def remove_keys_from_tf_variable_file(tf_file, keys):
    """
    Removes the keys from the tf files and convert to json format

    Args:
        tf_file (str): path to tf file
        keys (list): list of keys to remove

    """
    # importing here to avoid dependencies
    from ocs_ci.utility.templating import dump_data_to_json
    with open(tf_file, 'r') as fd:
        obj = hcl.load(fd)
    for key in keys:
        obj['variable'].pop(key)

    dump_data_to_json(obj, f"{tf_file}.json")
    os.rename(tf_file, f"{tf_file}.backup")


def get_kubeadmin_password():
    filename = os.path.join(
        config.ENV_DATA['cluster_path'],
        config.RUN['password_location']
    )
    with open(filename) as f:
        return f.read()
