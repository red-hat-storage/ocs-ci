from functools import reduce
import base64
import io
import json
import logging
import os
import platform
import random
import re
import shlex
import smtplib
import socket
import string
import subprocess
import time
import traceback
from typing import Match
import stat
import shutil
from copy import deepcopy
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd
from scipy.stats import tmean, scoreatpercentile
from shutil import which, move, rmtree
import pexpect
import pytest
import unicodedata

import hcl2
import requests
import yaml
import git
from bs4 import BeautifulSoup
from paramiko import SSHClient, AutoAddPolicy
from paramiko.auth_handler import AuthenticationException, SSHException
from semantic_version import Version
from tempfile import NamedTemporaryFile, mkdtemp, TemporaryDirectory
from jinja2 import FileSystemLoader, Environment
from ocs_ci.framework import config
from ocs_ci.framework import GlobalVariables as GV
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.exceptions import (
    CephHealthException,
    ClientDownloadError,
    CommandFailed,
    ConfigurationError,
    TagNotFoundException,
    TimeoutException,
    TimeoutExpiredError,
    UnavailableBuildException,
    UnexpectedImage,
    UnknownCloneTypeException,
    UnsupportedOSType,
    InteractivePromptException,
    NotFoundError,
    CephToolBoxNotFoundException,
    NoRunningCephToolBoxException,
    ClusterNotInSTSModeException,
)
from ocs_ci.utility import version as version_module
from ocs_ci.utility.flexy import load_cluster_info
from ocs_ci.utility.retry import retry
from psutil._common import bytes2human


log = logging.getLogger(__name__)

# variables
mounting_dir = "/mnt/cephfs/"
clients = []
md5sum_list1 = []
md5sum_list2 = []
fuse_clients = []
kernel_clients = []
mon_node = ""
mon_node_ip = ""
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
        if node.role == "client":
            clients.append(node)
    # Identifying MON node
    for node in ceph_nodes:
        if node.role == "mon":
            mon_node = node
            out, err = mon_node.exec_command(cmd="sudo hostname -I")
            mon_node_ip = out.read().decode().rstrip("\n")
            break
    for node in ceph_nodes:
        if node.role == "mds":
            mds_nodes.append(node)
    for node in clients:
        node.exec_command(cmd="sudo yum install -y attr")

    fuse_clients = clients[0:2]  # seperating clients for fuse and kernel
    kernel_clients = clients[2:4]
    return (
        fuse_clients,
        kernel_clients,
        mon_node,
        mounting_dir,
        mds_nodes,
        md5sum_file_lock,
        mon_node_ip,
    )


# function for providing authorization to the clients from MON ndoe
def auth_list(clients, mon_node):
    for node in clients:
        log.info("Giving required permissions for clients from MON node:")
        mon_node.exec_command(
            cmd="sudo ceph auth get-or-create client.%s mon 'allow *' mds 'allow *, allow rw path=/' "
            "osd 'allow rw pool=cephfs_data' -o /etc/ceph/ceph.client.%s.keyring"
            % (node.hostname, node.hostname)
        )
        out, err = mon_node.exec_command(
            sudo=True, cmd="cat /etc/ceph/ceph.client.%s.keyring" % (node.hostname)
        )
        keyring = out.read().decode()
        key_file = node.write_file(
            sudo=True,
            file_name="/etc/ceph/ceph.client.%s.keyring" % (node.hostname),
            file_mode="w",
        )
        key_file.write(keyring)

        key_file.flush()

        node.exec_command(
            cmd="sudo chmod 644 /etc/ceph/ceph.client.%s.keyring" % (node.hostname)
        )
        # creating mounting directory
        node.exec_command(cmd="sudo mkdir %s" % (mounting_dir))


# MOunting single FS with ceph-fuse
def fuse_mount(fuse_clients, mounting_dir):
    try:
        for client in fuse_clients:
            log.info("Creating mounting dir:")
            log.info("Mounting fs with ceph-fuse on client %s:" % (client.hostname))
            client.exec_command(
                cmd="sudo ceph-fuse -n client.%s %s" % (client.hostname, mounting_dir)
            )
            out, err = client.exec_command(cmd="mount")
            mount_output = out.read().decode()
            mount_output.split()
            log.info("Checking if fuse mount is is passed of failed:")
            if "fuse" in mount_output:
                log.info("ceph-fuse mounting passed")
            else:
                log.error("ceph-fuse mounting failed")
        return md5sum_list1
    except Exception as e:
        log.error(e)


def kernel_mount(mounting_dir, mon_node_ip, kernel_clients):
    try:
        for client in kernel_clients:
            out, err = client.exec_command(
                cmd="sudo ceph auth get-key client.%s" % (client.hostname)
            )
            secret_key = out.read().decode().rstrip("\n")
            mon_node_ip = mon_node_ip.replace(" ", "")
            client.exec_command(
                cmd="sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s"
                % (mon_node_ip, mounting_dir, client.hostname, secret_key)
            )
            out, err = client.exec_command(cmd="mount")
            mount_output = out.read().decode()
            mount_output.split()
            log.info("Checking if kernel mount is is passed of failed:")
            if "%s:6789:/" % (mon_node_ip) in mount_output:
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
            cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d"
            % (mounting_dir, client.hostname, rand_bs, rand_count),
            long_running=True,
        )
    except Exception as e:
        log.error(e)


def kernel_client_io(client, mounting_dir):
    try:
        rand_count = random.randint(1, 6)
        rand_bs = random.randint(100, 500)
        log.info("Performing IOs on kernel-clients")
        client.exec_command(
            cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d"
            % (mounting_dir, client.hostname, rand_bs, rand_count),
            long_running=True,
        )
    except Exception as e:
        log.error(e)


def fuse_client_md5(fuse_clients, md5sum_list1):
    try:
        log.info("Calculating MD5 sums of files in fuse-clients:")
        for client in fuse_clients:
            md5sum_list1.append(
                client.exec_command(
                    cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir),
                    long_running=True,
                )
            )

    except Exception as e:
        log.error(e)


def kernel_client_md5(kernel_clients, md5sum_list2):
    try:
        log.info("Calculating MD5 sums of files in kernel-clients:")
        for client in kernel_clients:
            md5sum_list2.append(
                client.exec_command(
                    cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir),
                    long_running=True,
                )
            )
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
            sudo=True, file_name="/home/cephuser/file_lock.py", file_mode="w"
        )
        to_lock_code.write(to_lock_file)
        to_lock_code.flush()
        out, err = client.exec_command(cmd="sudo python /home/cephuser/file_lock.py")
        output = out.read().decode()
        output.split()
        if "Errno 11" in output:
            log.info("File locking achieved, data is not corrupted")
        elif "locking" in output:
            log.info("File locking achieved, data is not corrupted")
        else:
            log.error("Data is corrupted")

        out, err = client.exec_command(
            cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" % (mounting_dir)
        )

        md5sum_file_lock.append(out.read().decode())

    except Exception as e:
        log.error(e)


def activate_multiple_mdss(mds_nodes):
    try:
        log.info("Activating Multiple MDSs")
        for node in mds_nodes:
            out1, err = node.exec_command(
                cmd="sudo ceph fs set cephfs allow_multimds true --yes-i-really-mean-it"
            )
            out2, err = node.exec_command(cmd="sudo ceph fs set cephfs max_mds 2")
            break

    except Exception as e:
        log.error(e)


def mkdir_pinning(clients, range1, range2, dir_name, pin_val):
    try:
        log.info("Creating Directories and Pinning to MDS %s" % (pin_val))
        for client in clients:
            for num in range(range1, range2):
                out, err = client.exec_command(
                    cmd="sudo mkdir %s%s_%d" % (mounting_dir, dir_name, num)
                )
                if pin_val != "":
                    client.exec_command(
                        cmd="sudo setfattr -n ceph.dir.pin -v %s %s%s_%d"
                        % (pin_val, mounting_dir, dir_name, num)
                    )
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
            node.exec_command(cmd="sudo ceph fs set cephfs allow_dirfrags 1")
            break
    except Exception as e:
        log.error(e)


def mds_fail_over(mds_nodes):
    try:
        rand = random.randint(0, 1)
        for node in mds_nodes:
            log.info("Failing MDS %d" % (rand))
            node.exec_command(cmd="sudo ceph mds fail %d" % (rand))
            break

    except Exception as e:
        log.error(e)


def pinned_dir_io(clients, mds_fail_over, num_of_files, range1, range2):
    try:
        log.info("Performing IOs and MDSfailovers on clients")
        for client in clients:
            client.exec_command(cmd="sudo pip install crefi")
            for num in range(range1, range2):
                if mds_fail_over != "":
                    mds_fail_over(mds_nodes)
                out, err = client.exec_command(
                    cmd="sudo crefi -n %d %sdir_%d" % (num_of_files, mounting_dir, num)
                )
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
        cli_config_dict = dict(item.split("=") for item in custom_config)

    # combine file and cli configs
    if cli_config_dict:
        if not custom_config_dict.get("global"):
            custom_config_dict["global"] = {}
        for key, value in cli_config_dict.items():
            custom_config_dict["global"][key] = value

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
        plaintext (str or list): The plaintext to remove the secrets from or
            list of strings to remove secrets from
        secrets (list): List of secret strings to replace in the plaintext

    Returns:
        str: The censored version of plaintext

    """
    if secrets:
        for secret in secrets:
            if isinstance(plaintext, list):
                plaintext = [string.replace(secret, "*" * 5) for string in plaintext]
            else:
                plaintext = plaintext.replace(secret, "*" * 5)
    return plaintext


def run_cmd(
    cmd,
    secrets=None,
    timeout=600,
    ignore_error=False,
    threading_lock=None,
    silent=False,
    cluster_config=None,
    **kwargs,
):
    """
    *The deprecated form of exec_cmd.*
    Run an arbitrary command locally

    Args:
        cmd (str): command to run
        secrets (list): A list of secrets to be masked with asterisks
            This kwarg is popped in order to not interfere with
            subprocess.run(``**kwargs``)
        timeout (int): Timeout for the command, defaults to 600 seconds.
        ignore_error (bool): True if ignore non zero return code and do not
            raise the exception.
        threading_lock (threading.RLock): threading.RLock object that is used
            for handling concurrent oc commands
        silent (bool): If True will silent errors from the server, default false

    Raises:
        CommandFailed: In case the command execution fails

    Returns:
        (str) Decoded stdout of command
    """
    completed_process = exec_cmd(
        cmd,
        secrets,
        timeout,
        ignore_error,
        threading_lock,
        silent=silent,
        cluster_config=cluster_config,
        **kwargs,
    )
    return mask_secrets(completed_process.stdout.decode(), secrets)


def run_cmd_interactive(
    cmd, prompts_answers, timeout=300, string_answer=False, raise_exception=True
):
    """
    Handle interactive prompts with answers during subctl command

    Args:
        cmd(str): Command to be executed
        prompts_answers(dict): Prompts as keys and answers as values
        timeout(int): Timeout in seconds, for pexpect to wait for prompt
        string_answer (bool): string answer
        raise_exception (bool): raise excption
    Raises:
        InteractivePromptException: in case something goes wrong

    """
    child = pexpect.spawn(cmd)
    for prompt, answer in prompts_answers.items():
        if child.expect(prompt, timeout=timeout):
            if raise_exception:
                raise InteractivePromptException("Unexpected Prompt")
        if string_answer:
            send_line = answer
        else:
            send_line = "".join([answer, constants.ENTER_KEY])
        if not child.sendline(send_line):
            raise InteractivePromptException("Failed to provide answer to the prompt")


def run_cmd_multicluster(
    cmd, secrets=None, timeout=600, ignore_error=False, skip_index=None, **kwargs
):
    """
    Run command on multiple clusters. Useful in multicluster scenarios
    This is wrapper around exec_cmd

    Args:
        cmd (str): command to be run
        secrets (list): A list of secrets to be masked with asterisks
            This kwarg is popped in order to not interfere with
            subprocess.run(``**kwargs``)
        timeout (int): Timeout for the command, defaults to 600 seconds.
        ignore_error (bool): True if ignore non zero return code and do not
            raise the exception.
        skip_index (list of int): List of indexes that needs to be skipped from executing the command

    Raises:
        CommandFailed: In case the command execution fails

    Returns:
        list : of CompletedProcess objects as per cluster's index in config.clusters
            i.e. [cluster1_completedprocess, None, cluster2_completedprocess]
            if command execution skipped on a particular cluster then corresponding entry will have None

    """
    # Skip indexed cluster while running commands
    # Useful to skip operations on ACM cluster
    restore_ctx_index = config.cur_index
    completed_process = [None] * len(config.clusters)
    # this need's to be done to skip none value as skip_index accepts type none
    if not isinstance(skip_index, list):
        skip_index = [skip_index]
    for cluster in config.clusters:
        if cluster.MULTICLUSTER["multicluster_index"] in skip_index:
            log.warning(f"skipping index = {skip_index}")
            continue
        else:
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            log.info(
                f"Switched the context to cluster:{cluster.ENV_DATA['cluster_name']}"
            )
            try:
                completed_process[
                    cluster.MULTICLUSTER["multicluster_index"]
                ] = exec_cmd(
                    cmd,
                    secrets=secrets,
                    timeout=timeout,
                    ignore_error=ignore_error,
                    **kwargs,
                )
            except CommandFailed:
                # In case of failure, restore the cluster context to where we started
                config.switch_ctx(restore_ctx_index)
                log.error(
                    f"Command {cmd} execution failed on cluster {cluster.ENV_DATA['cluster_name']} "
                )
                raise
    config.switch_ctx(restore_ctx_index)
    return completed_process


def exec_cmd(
    cmd,
    secrets=None,
    timeout=600,
    ignore_error=False,
    threading_lock=None,
    silent=False,
    use_shell=False,
    cluster_config=None,
    **kwargs,
):
    """
    Run an arbitrary command locally

    If the command is grep and matching pattern is not found, then this function
    returns "command terminated with exit code 1" in stderr.

    Args:
        cmd (str): command to run
        secrets (list): A list of secrets to be masked with asterisks
            This kwarg is popped in order to not interfere with
            subprocess.run(``**kwargs``)
        timeout (int): Timeout for the command, defaults to 600 seconds.
        ignore_error (bool): True if ignore non zero return code and do not
            raise the exception.
        threading_lock (threading.RLock): threading.RLock object that is used
            for handling concurrent oc commands
        silent (bool): If True will silent errors from the server, default false
        use_shell (bool): If True will pass the cmd without splitting
        cluster_config (MultiClusterConfig): In case of multicluster environment this object
                will be non-null

    Raises:
        CommandFailed: In case the command execution fails

    Returns:
        (CompletedProcess) A CompletedProcess object of the command that was executed
        CompletedProcess attributes:
        args: The list or str args passed to run().
        returncode (str): The exit code of the process, negative for signals.
        stdout     (str): The standard output (None if not captured).
        stderr     (str): The standard error (None if not captured).

    """
    masked_cmd = mask_secrets(cmd, secrets)
    log.info(f"Executing command: {masked_cmd}")
    if isinstance(cmd, str) and not kwargs.get("shell"):
        cmd = shlex.split(cmd)
    if config.RUN.get("custom_kubeconfig_location") and cmd[0] == "oc":
        if "--kubeconfig" in cmd:
            cmd.pop(2)
            cmd.pop(1)
        cmd = list_insert_at_position(cmd, 1, ["--kubeconfig"])
        cmd = list_insert_at_position(
            cmd, 2, [config.RUN["custom_kubeconfig_location"]]
        )
    if cluster_config and cmd[0] == "oc" and "--kubeconfig" not in cmd:
        kubepath = cluster_config.RUN["kubeconfig"]
        kube_index = 1
        # check if we have an oc plugin in the command
        plugin_list = "oc plugin list"
        cp = subprocess.run(
            shlex.split(plugin_list),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        subcmd = cmd[1].split("-")
        if len(subcmd) > 1:
            subcmd = "_".join(subcmd)
        if not isinstance(subcmd, str) and isinstance(subcmd, list):
            subcmd = str(subcmd[0])

        for l in cp.stdout.decode().splitlines():
            if subcmd in l:
                # If oc cmdline has plugin name then we need to push the
                # --kubeconfig to next index
                kube_index = 2
                log.info(f"Found oc plugin {subcmd}")
        cmd = list_insert_at_position(cmd, kube_index, ["--kubeconfig"])
        cmd = list_insert_at_position(cmd, kube_index + 1, [kubepath])
    if threading_lock and cmd[0] == "oc":
        threading_lock.acquire()
    completed_process = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,
        timeout=timeout,
        **kwargs,
    )
    if threading_lock and cmd[0] == "oc":
        threading_lock.release()
    masked_stdout = mask_secrets(completed_process.stdout.decode(), secrets)
    if len(completed_process.stdout) > 0:
        log.debug(f"Command stdout: {masked_stdout}")
    else:
        log.debug("Command stdout is empty")

    masked_stderr = mask_secrets(completed_process.stderr.decode(), secrets)
    if len(completed_process.stderr) > 0:
        if not silent:
            log.warning(f"Command stderr: {masked_stderr}")
    else:
        log.debug("Command stderr is empty")
    log.debug(f"Command return code: {completed_process.returncode}")
    if completed_process.returncode and not ignore_error:
        masked_stderr = bin_xml_escape(filter_out_emojis(masked_stderr))
        if (
            "grep" in masked_cmd
            and b"command terminated with exit code 1" in completed_process.stderr
        ):
            log.info(f"No results found for grep command: {masked_cmd}")
        else:
            raise CommandFailed(
                f"Error during execution of command: {masked_cmd}."
                f"\nError is {masked_stderr}"
            )
    return completed_process


def bin_xml_escape(arg):
    """
    Visually escape invalid XML characters.

    For example, transforms 'hello\aworld\b' into 'hello#x07world#x08'

    Args:
        arg (object) Object on top of which the invalid XML characters will be escaped

    Returns:
        str: string with escaped invalid characters

    """

    def repl(matchobj: Match[str]) -> str:
        i = ord(matchobj.group())
        if i <= 0xFF:
            return "#x%02X" % i
        else:
            return "#x%04X" % i

    # The spec range of valid chars is:
    # Char ::= #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
    # For an unknown(?) reason, we disallow #x7F (DEL) as well.
    illegal_xml_re = (
        "[^\u0009\u000A\u000D\u0020-\u007E\u0080-\uD7FF\uE000-\uFFFD\u10000-\u10FFFF]"
    )
    return re.sub(illegal_xml_re, repl, str(arg))


def download_file(url, filename, **kwargs):
    """
    Download a file from a specified url

    Args:
        url (str): URL of the file to download
        filename (str): Name of the file to write the download to
        kwargs (dict): additional keyword arguments passed to requests.get(...)

    """
    log.debug(f"Download '{url}' to '{filename}'.")
    with open(filename, "wb") as f:
        r = requests.get(url, **kwargs)
        assert r.ok, f"The URL {url} is not available! Status: {r.status_code}."
        f.write(r.content)


def get_url_content(url, **kwargs):
    """
    Return URL content

    Args:
        url (str): URL address to return
        kwargs (dict): additional keyword arguments passed to requests.get(...)
    Returns:
        str: Content of URL

    Raises:
        AssertionError: When couldn't load URL

    """
    log.debug(f"Download '{url}' content.")
    r = requests.get(url, **kwargs)
    assert r.ok, f"Couldn't load URL: {url} content! Status: {r.status_code}."
    return r.content


def expose_ocp_version(version):
    """
    This helper function exposes latest nightly version or GA version of OCP.
    When the version string ends with .nightly (e.g. 4.2.0-0.nightly) it will
    expose the version to latest accepted OCP build
    (e.g. 4.2.0-0.nightly-2019-08-08-103722)
    If the version ends with -ga than it will find the latest GA OCP version
    and will expose 4.2-ga to for example 4.2.22.

    Args:
        version (str): Verison of OCP

    Returns:
        str: Version of OCP exposed to full version if latest nighly passed

    """
    if version.endswith(".nightly"):
        latest_nightly_url = (
            f"https://amd64.ocp.releases.ci.openshift.org/api/v1/"
            f"releasestream/{version}/latest"
        )
        version_url_content = get_url_content(latest_nightly_url)
        version_json = json.loads(version_url_content)
        return version_json["name"]
    if version.endswith("-ga"):
        channel = config.DEPLOYMENT.get("ocp_channel", "stable")
        ocp_version = version.rstrip("-ga")
        index = config.DEPLOYMENT.get("ocp_version_index", -1)
        return get_latest_ocp_version(f"{channel}-{ocp_version}", index)
    else:
        return version


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
    version = version or config.DEPLOYMENT["installer_version"]
    bin_dir_rel_path = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
    bin_dir = os.path.abspath(bin_dir_rel_path)
    if (
        config.ENV_DATA.get("fips")
        and version_module.get_semantic_ocp_version_from_config()
        >= version_module.VERSION_4_16
    ):
        installer_filename = "openshift-install-fips"
        os.environ["OPENSHIFT_INSTALL_SKIP_HOSTCRYPT_VALIDATION"] = "True"
    else:
        installer_filename = "openshift-install"
    installer_binary_path = os.path.join(bin_dir, installer_filename)
    client_binary_path = os.path.join(bin_dir, "oc")
    client_exist = os.path.isfile(client_binary_path)
    custom_ocp_image = config.DEPLOYMENT.get("custom_ocp_image")
    if not client_exist:
        get_openshift_client()
        config.RUN["custom_client_downloaded_from_installer"] = True
    if custom_ocp_image:
        extract_ocp_binary_from_image("openshift-install", custom_ocp_image, bin_dir)
        return installer_binary_path
    if os.path.isfile(installer_binary_path) and force_download:
        delete_file(installer_binary_path)
    if os.path.isfile(installer_binary_path):
        log.debug(f"Installer exists ({installer_binary_path}), skipping download.")
        # TODO: check installer version
    else:
        version = expose_ocp_version(version)
        log.info(f"Downloading openshift installer ({version}).")
        prepare_bin_dir()
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(bin_dir)
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        cmd = (
            f"oc adm release extract --registry-config {pull_secret_path} --command={installer_filename} "
            f"--to ./ registry.ci.openshift.org/ocp/release:{version}"
        )
        exec_cmd(cmd)
        # return to the previous working directory
        os.chdir(previous_dir)

    installer_version = run_cmd(f"{installer_binary_path} version")
    config.ENV_DATA["installer_path"] = installer_binary_path
    log.info(f"OpenShift Installer version: {installer_version}")
    return installer_binary_path


def get_ocm_cli(
    version=None,
    bin_dir=None,
    force_download=False,
):
    """
    Download the OCM binary, if not already present.
    Update env. PATH and get path of the OCM binary.

    Args:
        version (str): Version of the OCM to download
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
        force_download (bool): Force OCM download even if already present

    Returns:
        str: Path to the OCM binary

    """
    bin_dir = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
    ocm_filename = "ocm"
    ocm_binary_path = os.path.join(bin_dir, ocm_filename)
    if os.path.isfile(ocm_binary_path) and force_download:
        delete_file(ocm_binary_path)
    if os.path.isfile(ocm_binary_path):
        log.debug(f"ocm exists ({ocm_binary_path}), skipping download.")
    else:
        log.info(f"Downloading ocm cli ({version}).")
        prepare_bin_dir()
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(bin_dir)
        url = f"https://github.com/openshift-online/ocm-cli/releases/download/v{version}/ocm-linux-amd64"
        download_file(url, ocm_filename)
        # return to the previous working directory
        os.chdir(previous_dir)

    current_file_permissions = os.stat(ocm_binary_path)
    os.chmod(
        ocm_binary_path,
        current_file_permissions.st_mode | stat.S_IEXEC,
    )
    ocm_version = run_cmd(f"{ocm_binary_path} version")
    log.info(f"OCM version: {ocm_version}")

    return ocm_binary_path


def get_rosa_cli(
    version=None,
    bin_dir=None,
    force_download=False,
):
    """
    Download the ROSA binary, if not already present.
    Update env. PATH and get path of the ROSA binary.

    Args:
        version (str): Version of the ROSA to download
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
        force_download (bool): Force ROSA download even if already present

    Returns:
        str: Path to the rosa binary

    """
    bin_dir = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
    rosa_filename = "rosa"
    rosa_binary_path = os.path.join(bin_dir, rosa_filename)
    if os.path.isfile(rosa_binary_path) and force_download:
        delete_file(rosa_binary_path)
    if os.path.isfile(rosa_binary_path):
        log.debug(f"rosa exists ({rosa_binary_path}), skipping download.")
    else:
        log.info(f"Downloading rosa cli ({version}).")
        prepare_bin_dir()
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(bin_dir)
        url = f"https://github.com/openshift/rosa/releases/download/v{version}/rosa-linux-amd64"
        download_file(url, rosa_filename)
        # return to the previous working directory
        os.chdir(previous_dir)

    current_file_permissions = os.stat(rosa_binary_path)
    os.chmod(
        rosa_binary_path,
        current_file_permissions.st_mode | stat.S_IEXEC,
    )
    rosa_version = run_cmd(
        f"{rosa_binary_path} version", ignore_error=True, timeout=1800
    )
    log.info(f"rosa version: {rosa_version}")

    return rosa_binary_path


def extract_ocp_binary_from_image(binary, image, bin_dir):
    """
    Extract binary (oc client or openshift installer) from custom OCP image

    Args:
        binary (str): type of binary (oc or openshift-install)
        image (str): image URL
        bin_dir (str): path to bin folder where to extract the binary

    """
    binary_path = os.path.join(bin_dir, binary)
    binary_path_exists = os.path.isfile(binary_path)
    with TemporaryDirectory() as temp_dir:
        pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
        cmd = f'oc adm release extract -a {pull_secret_path} --to {temp_dir} --command={binary} "{image}"'
        exec_cmd(cmd)
        temp_binary = os.path.join(temp_dir, binary)
        if binary_path_exists:
            backup_file = f"{binary_path}.bak"
            os.rename(binary_path, backup_file)
            try:
                shutil.move(temp_binary, binary_path)
                delete_file(backup_file)
                log.info("Deleted backup binaries.")
            except FileNotFoundError as ex:
                log.error(
                    f"Something went wrong with copying binary, reverting backup file. Exception: {ex}"
                )
                shutil.move(backup_file, binary_path)
        else:
            shutil.move(temp_binary, binary_path)


def get_openshift_client(
    version=None, bin_dir=None, force_download=False, skip_comparison=False
):
    """
    Download the OpenShift client binary, if not already present.
    Update env. PATH and get path of the oc binary.

    Args:
        version (str): Version of the client to download
            (default: config.RUN['client_version'])
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
        force_download (bool): Force client download even if already present
        skip_comparison (bool): Skip the comparison between the existing OCP client
            version and the configured one.

    Returns:
        str: Path to the client binary

    """
    version = version or config.RUN["client_version"]
    bin_dir_rel_path = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
    bin_dir = os.path.abspath(bin_dir_rel_path)
    client_binary_path = os.path.join(bin_dir, "oc")
    kubectl_binary_path = os.path.join(bin_dir, "kubectl")
    download_client = True
    client_version = None
    try:
        version = expose_ocp_version(version)
    except Exception:
        log.exception("Unable to expose OCP version, skipping client download.")
        skip_comparison = True
        download_client = False
        force_download = False

    client_exist = os.path.isfile(client_binary_path)
    custom_ocp_image = config.DEPLOYMENT.get("custom_ocp_image")
    skip_if_client_downloaded_from_installer = config.RUN.get(
        "custom_client_downloaded_from_installer"
    )
    if (
        client_exist
        and custom_ocp_image
        and not skip_if_client_downloaded_from_installer
    ):
        extract_ocp_binary_from_image("oc", custom_ocp_image, bin_dir)
        return
    if force_download:
        log.info("Forcing client download.")
    elif client_exist and not skip_comparison:
        current_client_version = get_client_version(client_binary_path)
        if current_client_version != version:
            log.info(
                f"Existing client version ({current_client_version}) does not match "
                f"configured version ({version})."
            )
        else:
            log.debug(
                f"Client exists ({client_binary_path}) and matches configured version, "
                f"skipping download."
            )
            download_client = False

    if download_client:
        # Move existing client binaries to backup location
        client_binary_backup = f"{client_binary_path}.bak"
        kubectl_binary_backup = f"{kubectl_binary_path}.bak"

        try:
            os.rename(client_binary_path, client_binary_backup)
            os.rename(kubectl_binary_path, kubectl_binary_backup)
        except FileNotFoundError:
            pass

        # Download the client
        log.info(f"Downloading openshift client ({version}).")
        prepare_bin_dir()
        # record current working directory and switch to BIN_DIR
        previous_dir = os.getcwd()
        os.chdir(bin_dir)

        tarball = "openshift-client.tar.gz"
        try:
            url = get_openshift_mirror_url("openshift-client", version)
            download_file(url, tarball)
            run_cmd(f"tar xzvf {tarball} oc kubectl")
            delete_file(tarball)
        except Exception as e:
            log.error(f"Failed to download the openshift client. Exception '{e}'")
            # check given version is GA'ed or not
            if "nightly" in version:
                get_nightly_oc_via_ga(version, tarball)

        if custom_ocp_image and not skip_if_client_downloaded_from_installer:
            extract_ocp_binary_from_image("oc", custom_ocp_image, bin_dir)
        try:
            client_version = run_cmd(f"{client_binary_path} version --client")
        except CommandFailed:
            log.error("Unable to get version from downloaded client.")
        if client_version:
            try:
                delete_file(client_binary_backup)
                delete_file(kubectl_binary_backup)
                log.info("Deleted backup binaries.")
            except FileNotFoundError:
                pass
        else:
            try:
                os.rename(client_binary_backup, client_binary_path)
                os.rename(kubectl_binary_backup, kubectl_binary_path)
                log.info("Restored backup binaries to their original location.")
            except FileNotFoundError:
                raise ClientDownloadError(
                    "No backups exist and new binary was unable to be verified."
                )

        # return to the previous working directory
        os.chdir(previous_dir)

    log.info(f"OpenShift Client version: {client_version}")
    return client_binary_path


def is_ocp_version_gaed(version):
    """
    Checks whether given OCP version is GA'ed or not

    Args:
        version (str): OCP version ( eg: 4.16, 4.15 )

    Returns:
        bool: True if OCP is GA'ed otherwise False

    """
    channel = f"stable-{version}"
    total_versions_count = len(get_available_ocp_versions(channel))
    if total_versions_count != 0:
        return True


def get_nightly_oc_via_ga(version, tarball="openshift-client.tar.gz"):
    """
    Downloads the nightly OC via GA'ed version

    Args:
        version (str): nightly OC version to download
        tarball (str): target name of the tarfile

    """
    version_major_minor = str(
        version_module.get_semantic_version(version, only_major_minor=True)
    )

    # For GA'ed version, check for N, N-1 and N-2 versions
    for current_version_count in range(3):
        previous_version = version_module.get_previous_version(
            version_major_minor, current_version_count
        )
        log.debug(
            f"previous version with count {current_version_count} is {previous_version}"
        )
        if is_ocp_version_gaed(previous_version):
            # Download GA'ed version
            pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
            log.info(
                f"version {previous_version} is GA'ed, use the same version to download oc"
            )
            config.DEPLOYMENT["ocp_url_template"] = (
                "https://mirror.openshift.com/pub/openshift-v4/clients/"
                "ocp/{version}/{file_name}-{os_type}-{version}.tar.gz"
            )
            ga_version = expose_ocp_version(f"{previous_version}-ga")
            url = get_openshift_mirror_url("openshift-client", ga_version)
            download_file(url, tarball)

            # extract to tmp location, since we need to download the nightly version again
            tmp_oc_path = "/tmp"
            run_cmd(f"tar xzvf {tarball} -C {tmp_oc_path}")

            # use appropriate oc based on glibc version
            glibc_version = get_glibc_version()
            if version_module.get_semantic_version(
                glibc_version
            ) < version_module.get_semantic_version("2.34"):
                oc_type = "oc.rhel8"
            else:
                oc_type = "oc"

            # extract oc
            cmd = (
                f"{tmp_oc_path}/oc adm release extract -a {pull_secret_path} --command={oc_type} "
                f"registry.ci.openshift.org/ocp/release:{version} --to ."
            )
            exec_cmd(cmd)
            delete_file(tarball)
            break
        else:
            log.debug(f"version {previous_version} is not GA'ed")


def get_vault_cli(bind_dir=None, force_download=False):
    """
    Download vault based on platform
    basically for CLI purpose. Binary will be directly
    put into ocs_ci/bin/ directory

    Args:
        bind_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
        force_download (bool): Force vault cli download even if already present

    """
    res = requests.get(constants.VAULT_VERSION_INFO_URL)
    version = res.url.split("/")[-1].lstrip("v")
    bin_dir = os.path.expanduser(bind_dir or config.RUN["bin_dir"])
    system = platform.system()
    if "Darwin" not in system and "Linux" not in system:
        raise UnsupportedOSType("Not a supported platform for vault")

    system = system.lower()
    zip_file = f"vault_{version}_{system}_amd64.zip"
    vault_cli_filename = "vault"
    vault_binary_path = os.path.join(bin_dir, vault_cli_filename)
    if os.path.isfile(vault_binary_path):
        vault_ver = re.search(
            r"Vault\sv*([\d.]+)", run_cmd(f"{vault_binary_path} version")
        ).group(1)
        if (Version.coerce(version) > Version.coerce(vault_ver)) or force_download:
            delete_file(vault_binary_path)

    if os.path.isfile(vault_binary_path):
        log.debug(
            f"Vault CLI binary already exists {vault_binary_path}, skipping download."
        )
    else:
        log.info(f"Downloading vault cli {version}")
        prepare_bin_dir()
        previous_dir = os.getcwd()
        os.chdir(bin_dir)
        url = f"{constants.VAULT_DOWNLOAD_BASE_URL}/{version}/{zip_file}"
        download_file(url, zip_file)
        run_cmd(f"unzip {zip_file}")
        delete_file(zip_file)
        os.chdir(previous_dir)
    vault_ver = run_cmd(f"{vault_binary_path} version")
    log.info(f"Vault cli version:{vault_ver}")


def ensure_nightly_build_availability(build_url):
    base_build_url = build_url.rsplit("/", 1)[0] + "/"
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
    target_arch = ""
    rhel_version = ""
    arch = get_architecture_host()
    log.debug(f"Host architecture: {arch}")
    if platform.system() == "Darwin":
        os_type = "mac"
    elif platform.system() == "Linux":
        os_type = "linux"
        # form the target architecture and rhel version to download oc
        if "openshift-client" in file_name:
            # form the target architecture to download oc
            if "x86_64" in arch:
                target_arch = "-amd64"
            elif "arm" in arch or "aarch" in arch:
                target_arch = "-arm64"
            elif "ppc" in arch:
                target_arch = "-ppc64le"

            glibc_version = get_glibc_version()
            if version_module.get_semantic_version(
                glibc_version
            ) < version_module.get_semantic_version("2.34"):
                rhel_version = "-rhel8"
            else:
                rhel_version = "-rhel9"
    else:
        raise UnsupportedOSType
    url_template = config.DEPLOYMENT.get(
        "ocp_url_template",
        "https://openshift-release-artifacts.apps.ci.l2s4.p1.openshiftapps.com/"
        f"{version}/{file_name}-{os_type}{target_arch}{rhel_version}-{version}.tar.gz",
    )
    url = url_template.format(
        version=version,
        file_name=file_name,
        os_type=os_type,
    )
    sample = TimeoutSampler(
        timeout=540,
        sleep=5,
        func=ensure_nightly_build_availability,
        build_url=url,
    )
    if not sample.wait_for_func_status(result=True):
        raise UnavailableBuildException(f"The build url {url} is not reachable")
    return url


def prepare_bin_dir(bin_dir=None):
    """
    Prepare bin directory for OpenShift client and installer

    Args:
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])
    """
    bin_dir = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
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
    env_path = os.environ["PATH"].split(os.pathsep)
    if path not in env_path:
        os.environ["PATH"] = os.pathsep.join([path] + env_path)
        log.info(f"Path '{path}' added to the PATH environment variable.")
    log.debug(f"PATH: {os.environ['PATH']}")


def delete_file(file_name):
    """
    Delete file_name

    Args:
        file_name (str): Path to the file you want to delete
    """
    os.remove(file_name)


def delete_dir(dir_name):
    """
    Deletes the directory

    Args:
        dir_name (str): Directory path to delete

    """
    try:
        rmtree(dir_name)
    except OSError as e:
        log.error(f"Failed to delete the directory {dir_name}. Error: {e.strerror}")


class TimeoutSampler(object):
    """
    Samples the function output.

    This is a generator object that at first yields the output of function
    `func`. After the yield, it either raises instance of `timeout_exc_cls` or
    sleeps `sleep` seconds.

    Yielding the output allows you to handle every value as you wish.

    Feel free to set the instance variables.


    Args:
        timeout (int): Timeout in seconds
        sleep (int): Sleep interval in seconds
        func (function): The function to sample
        func_args: Arguments for the function
        func_kwargs: Keyword arguments for the function
    """

    def __init__(self, timeout, sleep, func, *func_args, **func_kwargs):
        self.timeout = timeout
        self.sleep = sleep
        # check that given timeout and sleep values makes sense
        if self.timeout < self.sleep:
            raise ValueError("timeout should be larger than sleep time")

        self.func = func
        self.func_args = func_args
        self.func_kwargs = func_kwargs

        # Timestamps of the first and most recent samples
        self.start_time = None
        self.last_sample_time = None
        # The exception to raise
        self.timeout_exc_cls = TimeoutExpiredError
        # Arguments that will be passed to the exception
        self.timeout_exc_args = [self.timeout]
        try:
            self.timeout_exc_args.append(
                f"Timed out after {timeout}s running {self._build_call_string()}"
            )
        except Exception:
            log.exception(
                "Failed to assemble call string. Not necessarily a test failure."
            )

    def _build_call_string(self):
        def stringify(value):
            if isinstance(value, str):
                return f'"{value}"'
            return str(value)

        args = list(map(stringify, self.func_args))
        kwargs = [f"{stringify(k)}={stringify(v)}" for k, v in self.func_kwargs.items()]
        all_args_string = ", ".join(args + kwargs)
        return f"{self.func.__name__}({all_args_string})"

    def __iter__(self):
        if self.start_time is None:
            self.start_time = time.time()
        while True:
            self.last_sample_time = time.time()
            if self.timeout <= (self.last_sample_time - self.start_time):
                raise self.timeout_exc_cls(*self.timeout_exc_args)
            try:
                yield self.func(*self.func_args, **self.func_kwargs)
            except Exception as ex:
                msg = f"Exception raised during iteration: {ex}"
                log.exception(msg)
            if self.timeout <= (time.time() - self.start_time):
                raise self.timeout_exc_cls(*self.timeout_exc_args)
            log.info("Going to sleep for %d seconds before next iteration", self.sleep)
            time.sleep(self.sleep)

    def wait_for_func_value(self, value):
        """
        Implements common usecase of TimeoutSampler: waiting until func (given
        function) returns a given value.

        Args:
            value: Expected return value of func we are waiting for.
        """
        try:
            for i_value in self:
                if i_value == value:
                    break
        except self.timeout_exc_cls:
            log.error(
                "function %s failed to return expected value %s "
                "after multiple retries during %d second timeout",
                self.func.__name__,
                value,
                self.timeout,
            )
            raise

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
            self.wait_for_func_value(result)
            return True
        except self.timeout_exc_cls:
            return False


class TimeoutIterator(TimeoutSampler):
    """
    Wrapper of TimeoutSampler which separates parameters of the class itself
    and func arguments in __init__ method. Such way of passing function with
    parameters is used in python standard library.

    This allows more explicit usage, which improves readability, eg.::

        t1 = TimeoutIterator(timeout=60, sleep=5, func=foo, func_args=[bar])
        t2 = TimeoutIterator(3600, sleep=10, func=foo, func_args=[bar])
    """

    def __init__(self, timeout, sleep, func, func_args=None, func_kwargs=None):
        if func_args is None:
            func_args = []
        if func_kwargs is None:
            func_kwargs = {}
        super().__init__(timeout, sleep, func, *func_args, **func_kwargs)


def get_random_str(size=13):
    """
    generates the random string of given size

    Args:
        size (int): number of random characters to generate

    Returns:
         str : string of random characters of given size

    """
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choice(chars) for _ in range(size))


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
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        shell=True,
        encoding="utf-8",
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

    def _multicluster_is_cluster_running(cluster_path):
        return config.RUN["cli_params"].get(
            f"cluster_path{config.cluster_ctx.MULTICLUSTER['multicluster_index'] + 1}"
        ) and OCP.set_kubeconfig(
            os.path.join(cluster_path, config.RUN.get("kubeconfig_location"))
        )

    if config.multicluster:
        return _multicluster_is_cluster_running(cluster_path)
    return config.RUN["cli_params"].get("cluster_path") and OCP.set_kubeconfig(
        os.path.join(cluster_path, config.RUN.get("kubeconfig_location"))
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
    attributes_to_decompose = ["extra"]
    if not config.RUN.get("logs_url"):
        attributes_to_decompose.append("col-links")
    decompose_html_attributes(soup, attributes_to_decompose)
    soup.find(id="not-found-message").decompose()

    if not config.RUN.get("logs_url"):
        for tr in soup.find_all("tr"):
            for th in tr.find_all("th"):
                if "Links" in th.text:
                    th.decompose()

    for p in soup.find_all("p"):
        if "(Un)check the boxes to filter the results." in p.text:
            p.decompose()
        if "pytest-html" in p.text:
            data = p.text.split("by")[0]
            p.string = data

    for ip in soup.find_all("input"):
        if not ip.has_attr("disabled"):
            ip["disabled"] = "true"

    for td in soup.find_all("td"):
        if "pytest" in td.text or "html" in td.text:
            data = td.text.replace("&apos", "")
            td.string = data

    skips_ceph_health_ratio = config.RUN.get("skipped_on_ceph_health_ratio")
    if skips_ceph_health_ratio > 0:
        skipped = soup.body.find_all(attrs={"class": "skipped"})
        skipped_number = skipped[0].string.split(" ")[0]
        skipped[0].string.replace_with(
            f"{skipped_number} skipped ({skips_ceph_health_ratio * 100}% on Ceph health)"
        )

    main_header = soup.find("h1")
    main_header.string.replace_with("OCS-CI RESULTS")


def add_squad_analysis_to_email(session, soup):
    """
    Add squad analysis to the html test results used in email reporting

    Args:
        session (obj): Pytest session object
        soup (obj): BeautifulSoup object of HTML Report data

    """
    failed = {}
    skipped = {}
    # sort out failed and skipped test cases to failed and skipped dicts
    for result in session.results.values():
        if result.failed or result.skipped:
            squad_marks = [
                key[:-6].capitalize() for key in result.keywords if "_squad" in key
            ]
            if squad_marks:
                for squad in squad_marks:
                    if result.failed:
                        if squad not in failed:
                            failed[squad] = []
                        failed[squad].append(result.nodeid)

                    if result.skipped:
                        if squad not in skipped:
                            skipped[squad] = []
                        try:
                            skipped_message = result.longrepr[2][8:]
                        except TypeError:
                            skipped_message = "--unknown--"
                        skipped[squad].append((result.nodeid, skipped_message))

            else:
                # unassigned
                if result.failed:
                    if "UNASSIGNED" not in failed:
                        failed["UNASSIGNED"] = []
                    failed["UNASSIGNED"].append(result.nodeid)
                if result.skipped:
                    if "UNASSIGNED" not in skipped:
                        skipped["UNASSIGNED"] = []
                    try:
                        skipped_message = result.longrepr[2][8:]
                    except TypeError:
                        skipped_message = "--unknown--"
                    skipped["UNASSIGNED"].append((result.nodeid, skipped_message))

    # no failed or skipped tests - exit the function
    if not failed and not skipped:
        return

    # add CSS for the Squad Analysis report
    style = soup.find("style")
    # use colors for squad names from squad names
    style.string += "\n".join(
        [
            f"h4.squad-{color.lower()} {{\n    color: {color.lower()};\n}}"
            for color in constants.SQUADS
        ]
    )
    # few additional styles
    style.string += """
    .squad-analysis {
        color: black;
        font-family: monospace;
        background-color: #eee;
        padding: 5px;
        margin-top: 10px;
    }
    .squad-analysis h2 {
        margin: 0px;
    }
    .squad-analysis h3 {
        margin: 0px;
        margin-top: 10px;
    }
    .squad-analysis h4 {
        margin: 0px;
    }
    .squad-analysis ul {
        margin: 0px;
    }
    .squad-analysis ul li em {
        margin-left: 1em;
    }
    .squad-unassigned {
        background-color: #FFBA88;
    }
    h4.squad-yellow {
        color: black;
        background-color: yellow;
        display: inline;
    }
    """
    # prepare place for the Squad Analysis in the email
    squad_analysis_div = soup.new_tag("div")
    squad_analysis_div["class"] = "squad-analysis"
    main_header = soup.find("h1")
    main_header.insert_after(squad_analysis_div)
    failed_h2_tag = soup.new_tag("h2")
    failed_h2_tag.string = "Squad Analysis - please analyze:"
    squad_analysis_div.append(failed_h2_tag)
    if failed:
        # print failed testcases peer squad
        failed_div_tag = soup.new_tag("div")
        squad_analysis_div.append(failed_div_tag)
        failed_h3_tag = soup.new_tag("h3")
        failed_h3_tag.string = "Failures:"
        failed_div_tag.append(failed_h3_tag)
        for squad in failed:
            failed_h4_tag = soup.new_tag("h4")
            failed_h4_tag.string = f"{squad} squad"
            failed_h4_tag["class"] = f"squad-{squad.lower()}"
            failed_div_tag.append(failed_h4_tag)
            failed_ul_tag = soup.new_tag("ul")
            failed_ul_tag["class"] = f"squad-{squad.lower()}"
            failed_div_tag.append(failed_ul_tag)
            for test in failed[squad]:
                failed_li_tag = soup.new_tag("li")
                failed_li_tag.string = test
                failed_ul_tag.append(failed_li_tag)
    if skipped:
        # print skipped testcases with reason peer squad
        skips_div_tag = soup.new_tag("div")
        squad_analysis_div.append(skips_div_tag)
        skips_h3_tag = soup.new_tag("h3")
        skips_h3_tag.string = "Skips:"
        skips_div_tag.append(skips_h3_tag)
        if config.RUN.get("display_skipped_msg_in_email"):
            skip_reason_h4_tag = soup.new_tag("h4")
            skip_reason_h4_tag.string = config.RUN.get("display_skipped_msg_in_email")
            skips_div_tag.append(skip_reason_h4_tag)
        for squad in skipped:
            skips_h4_tag = soup.new_tag("h4")
            skips_h4_tag.string = f"{squad} squad"
            skips_h4_tag["class"] = f"squad-{squad.lower()}"
            skips_div_tag.append(skips_h4_tag)
            skips_ul_tag = soup.new_tag("ul")
            skips_ul_tag["class"] = f"squad-{squad.lower()}"
            skips_div_tag.append(skips_ul_tag)
            for test in skipped[squad]:
                skips_li_tag = soup.new_tag("li")
                skips_test_span_tag = soup.new_tag("span")
                skips_test_span_tag.string = test[0]
                skips_li_tag.append(skips_test_span_tag)
                skips_li_tag.append(soup.new_tag("br"))
                skips_reason_em_tag = soup.new_tag("em")
                skips_reason_em_tag.string = f"Reason: {test[1]}"
                skips_li_tag.append(skips_reason_em_tag)
                skips_ul_tag.append(skips_li_tag)


def move_summary_to_top(soup):
    """
    Move summary to the top of the eamil report

    """
    summary = []
    summary.append(soup.find("h2", string="Summary"))
    for tag in summary[0].next_siblings:
        if tag.name == "h2":
            break
        else:
            summary.append(tag)
    for tag in summary:
        tag.extract()
    main_header = soup.find("h1")
    # because we are inserting the tags just after the header one by one, we
    # have to insert them in reverse order
    summary.reverse()
    for tag in summary:
        main_header.insert_after(tag)


def add_mem_stats(soup):
    """
    Add performance summary to the soup to print the table:
    columns = ['TC name', 'Peak total RAM consumed', 'Peak total VMS consumed', 'RAM leak']
    """
    if "memory" in config.RUN and isinstance(config.RUN["memory"], pd.DataFrame):
        mem_table = config.RUN["memory"]
        mem_table["Peak RAM consumed"] = mem_table["Peak total RAM consumed"].apply(
            bytes2human
        )
        mem_table["Peak VMS consumed"] = mem_table["Peak total VMS consumed"].apply(
            bytes2human
        )
        mem_div = soup.new_tag("div")
        mem_h2_tag = soup.new_tag("h2")
        mem_h2_tag.string = "Memory Test Performance:"
        mem_div.append(mem_h2_tag)
        mem_div.append(
            pd.DataFrame(config.RUN["memory"]).to_markdown(
                headers="keys", index=False, tablefmt="grid"
            )
        )
    else:
        log.debug(
            "No memory records was found, skip Memory Test Performance email reporting"
        )


def email_reports(session):
    """
    Email results of test run

    """
    # calculate percentage pass
    # reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    # passed = len(reporter.stats.get("passed", []))
    # failed = len(reporter.stats.get("failed", []))
    # error = len(reporter.stats.get("error", []))
    # total = passed + failed + error
    # percentage_passed = (passed / total) * 100

    try:
        build_id = get_ocs_build_number()
    except Exception:
        build_id = ""
        log.exception("Getting OCS operator build number failed!")
    build_str = f"BUILD ID: {build_id} " if build_id else ""
    mailids = config.RUN["cli_params"]["email"]
    recipients = []
    [recipients.append(mailid) for mailid in mailids.split(",")]
    sender = "ocs-ci@redhat.com"
    msg = MIMEMultipart("alternative")
    aborted_message = ""
    if config.RUN.get("aborted"):
        aborted_message = "[JOB ABORTED] "
    msg["Subject"] = (
        f"{aborted_message}"
        f"ocs-ci results for {get_testrun_name()} "
        f"({build_str}"
        f"RUN ID: {config.RUN['run_id']}) "
        # f"Passed: {percentage_passed:.0f}%"
    )
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    html = config.RUN["cli_params"]["--html"]
    with open(os.path.expanduser(html)) as fd:
        html_data = fd.read()
    soup = BeautifulSoup(html_data, "html.parser")

    parse_html_for_email(soup)
    if config.RUN["cli_params"].get("squad_analysis"):
        add_squad_analysis_to_email(session, soup)
    move_summary_to_top(soup)
    add_time_report_to_email(session, soup)
    part1 = MIMEText(soup, "html")
    add_mem_stats(soup)
    msg.attach(part1)
    try:
        s = smtplib.SMTP(config.REPORTING["email"]["smtp_server"])
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
        log.info(f"Results have been emailed to {recipients}")
    except Exception:
        log.exception("Sending email with results failed!")


def save_reports():
    """
    Save reports of test run to logs directory

    """
    try:
        if (
            "memory" in config.RUN
            and isinstance(config.RUN["memory"], pd.DataFrame)
            and not config.RUN["memory"].empty
        ):
            stats_dir = create_stats_dir()
            mem_report_file = os.path.join(stats_dir, "session_mem_report_file")
            config.RUN["memory"].to_csv(mem_report_file, index=False)
            log.info(f"Memory performance report saved to '{mem_report_file}'")
        else:
            log.info("Memory performance report not saved - no data")
    except Exception:
        log.exception("Failed save reports to logs directory")


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
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix
    from ocs_ci.ocs.resources.catalog_source import CatalogSource
    from ocs_ci.ocs.resources.packagemanifest import get_selector_for_ocs_operator

    build_num = ""
    if (
        version_module.get_semantic_ocs_version_from_config()
        >= version_module.VERSION_4_9
    ):
        operator_name = defaults.ODF_OPERATOR_NAME
        if (
            config.ENV_DATA.get("platform") in constants.HCI_PROVIDER_CLIENT_PLATFORMS
            and config.ENV_DATA.get("cluster_type") == constants.HCI_CLIENT
        ):
            operator_name = defaults.HCI_CLIENT_ODF_OPERATOR_NAME
    else:
        operator_name = defaults.OCS_OPERATOR_NAME
    ocs_csvs = get_csvs_start_with_prefix(
        operator_name,
        config.ENV_DATA["cluster_namespace"],
    )
    try:
        ocs_csv = ocs_csvs[0]
        csv_labels = ocs_csv["metadata"]["labels"]
        if "full_version" in csv_labels:
            return csv_labels["full_version"]
        build_num = ocs_csv["spec"]["version"]
        operator_selector = get_selector_for_ocs_operator()
        # This is a temporary solution how to get the build id from the registry image.
        # Because we are now missing build ID in the CSV. If catalog source with our
        # internal label exists, we will be getting build id from the tag of the image
        # in catalog source. Boris is working on better way how to populate the internal
        # build version in the CSV.
        if operator_selector:
            catalog_source = CatalogSource(
                resource_name=constants.OPERATOR_CATALOG_SOURCE_NAME,
                namespace=constants.MARKETPLACE_NAMESPACE,
                selector=operator_selector,
            )
            cs_data = catalog_source.get()["items"][0]
            cs_image = cs_data["spec"]["image"]
            image_tag = cs_image.split(":")[1]
            if "-" in image_tag:
                build_id = image_tag.split("-")[1]
                build_num += f"-{build_id}"

    except (IndexError, AttributeError, CommandFailed, KeyError):
        log.exception("No version info found for OCS operator")
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
    return re.split(r"ceph version ", ceph_version["version"])[1]


def get_rook_version():
    """
    Gets the rook version

    Returns:
        str: rook version

    """
    # importing here to avoid circular imports
    from ocs_ci.ocs.resources import pod

    ct_pod = pod.get_ceph_tools_pod()
    rook_versions = ct_pod.exec_ceph_cmd("rook version", format="")
    return rook_versions["rook"]


def get_csi_versions():
    """
    Gets the CSI related version information

    Returns:
        dict: CSI related version information

    """
    csi_versions = {}
    # importing here to avoid circular imports
    from ocs_ci.ocs.ocp import OCP

    for provisioner in [
        constants.CSI_CEPHFSPLUGIN_PROVISIONER_LABEL,
        constants.CSI_RBDPLUGIN_PROVISIONER_LABEL,
    ]:
        ocp_pod_obj = OCP(
            kind=constants.POD,
            namespace=config.ENV_DATA["cluster_namespace"],
            selector=provisioner,
        )
        for container in ocp_pod_obj.data["items"][0]["spec"]["containers"]:
            try:
                name = container["name"]
                version = container["image"].split("/")[-1].split(":")[1]
                csi_versions[name] = version
            except ValueError:
                raise NotFoundError(
                    f"items | spec | containers " f"not found:\n {str(container)}"
                )
    return csi_versions


def get_ocp_version(seperator=None):
    """
    *The deprecated form of 'get current ocp version'*
    Use ocs_ci/utility/version.py:get_semantic_ocp_version_from_config()

    Get current ocp version

    Args:
        seperator (str): String that would seperate major and
            minor version nubers

    Returns:
        string : If seperator is 'None', version string will be returned as is
            eg: '4.2', '4.3'.
            If seperator is provided then '.' in the version string would be
            replaced by seperator and resulting string will be returned.
            eg: If seperator is '_' then string returned would be '4_2'

    """
    char = seperator if seperator else "."
    raw_version = config.DEPLOYMENT["installer_version"]
    if config.ENV_DATA.get("skip_ocp_deployment"):
        try:
            raw_version = json.loads(run_cmd("oc version -o json"))["openshiftVersion"]
        except KeyError:
            if (
                config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
                and config.ENV_DATA["deployment_type"] == "managed"
            ):
                # In IBM ROKS, there is some issue that openshiftVersion is not available
                # after fresh deployment. As W/A we are taking the version from config only if not found.
                log.warning(
                    "openshiftVersion key not found! Taking OCP version from config."
                )
            else:
                raise
    version = Version.coerce(raw_version)
    return char.join([str(version.major), str(version.minor)])


def get_running_ocp_version(separator=None):
    """
    Get current running ocp version

    Args:
        separator (str): String that would separate major and
            minor version numbers

    Returns:
        string : If separator is 'None', version string will be returned as is
            eg: '4.2', '4.3'.
            If separator is provided then '.' in the version string would be
            replaced by separator and resulting string will be returned.
            eg: If separator is '_' then string returned would be '4_2'

    """
    char = separator if separator else "."
    namespace = config.ENV_DATA["cluster_namespace"]
    try:
        # if the cluster exist, this part will be run
        results = run_cmd(f"oc get clusterversion -n {namespace} -o yaml")
        build = yaml.safe_load(results)["items"][0]["status"]["desired"]["version"]
        return char.join(build.split(".")[0:2])
    except Exception:
        # this part will return version from the config file in case
        # cluster is not exists.
        return get_ocp_version(seperator=char)


def get_ocp_repo(rhel_major_version=None):
    """
    Get ocp repo file, name will be generated dynamically based on
    ocp version.

    Args:
        rhel_major_version (int): Major version of RHEL. If not specified it will
            take major version from config.ENV_DATA["rhel_version"]

    Returns:
        string : Path to ocp repo file

    """
    rhel_version = (
        rhel_major_version or Version.coerce(config.ENV_DATA["rhel_version"]).major
    )
    repo_path = os.path.join(
        constants.REPO_DIR, f"ocp_{get_ocp_version('_')}_rhel{rhel_version}.repo"
    )
    path = os.path.expanduser(repo_path)
    assert os.path.exists(path), f"OCP repo file {path} doesn't exists!"
    return path


def get_running_acm_version():
    """
    Get the currently deployed ACM version

    Returns:
        string: ACM version

    """
    occmd = "oc get mch multiclusterhub -n open-cluster-management -o json"
    jq_cmd = "jq -r .status.currentVersion"
    json_out = subprocess.Popen(shlex.split(occmd), stdout=subprocess.PIPE)
    acm_version = subprocess.Popen(
        shlex.split(jq_cmd), stdin=json_out.stdout, stdout=subprocess.PIPE
    )
    json_out.stdout.close()
    return acm_version.communicate()[0].decode()


def parse_pgsql_logs(data):
    """
    Parse the pgsql benchmark data from ripsaw and return
    the data in list format

    Args:
        data (str): log data from pgsql bench run

    Returns:
        list_data (list): data digestable by scripts with below format
            e.g.:

                [
                {1: {'num_clients': '2','num_threads': '7','latency_avg': '7',
                'lat_stddev': '0', 'tps_incl': '234', 'tps_excl': '243'},
                {2: {'num_clients': '2','num_threads': '7','latency_avg': '7',
                'lat_stddev': '0', 'tps_incl': '234', 'tps_excl': '243'},
                {3: {'num_clients': '2','num_threads': '7','latency_avg': '7',
                'lat_stddev': '0', 'tps_incl': '234', 'tps_excl': '243'},
                ]
                where keys{1,2,3} are run-IDs

    """
    match = data.split("PGBench Results")
    list_data = []
    for i in range(1, len(match)):
        log = "".join(match[i].split("\n"))
        pgsql_data = dict()
        pgsql_data[i] = {}
        clients = re.search(r"scaling_factor\':\s+(\d+),", log)
        if clients and clients.group(1):
            pgsql_data[i]["scaling_factor"] = clients.group(1)
        clients = re.search(r"number_of_clients\':\s+(\d+),", log)
        if clients and clients.group(1):
            pgsql_data[i]["num_clients"] = clients.group(1)
        threads = re.search(r"number_of_threads\':\s+(\d+)", log)
        if threads and threads.group(1):
            pgsql_data[i]["num_threads"] = threads.group(1)
        clients = re.search(r"number_of_transactions_per_client\':\s+(\d+),", log)
        if clients and clients.group(1):
            pgsql_data[i]["number_of_transactions_per_client"] = clients.group(1)
        clients = re.search(
            r"number_of_transactions_actually_processed\':\s+(\d+),", log
        )
        if clients and clients.group(1):
            pgsql_data[i]["number_of_transactions_actually_processed"] = clients.group(
                1
            )
        lat_avg = re.search(r"latency_average_ms\':\s+(\d+)", log)
        if lat_avg and lat_avg.group(1):
            pgsql_data[i]["latency_avg"] = lat_avg.group(1)
        lat_stddev = re.search(r"latency_stddev_ms\':\s+(\d+)", log)
        if lat_stddev and lat_stddev.group(1):
            pgsql_data[i]["lat_stddev"] = lat_stddev.group(1)
        tps_incl = re.search(r"tps_incl_con_est\':\s+(\w+)", log)
        if tps_incl and tps_incl.group(1):
            pgsql_data[i]["tps_incl"] = tps_incl.group(1)
        tps_excl = re.search(r"tps_excl_con_est\':\s+(\w+)", log)
        if tps_excl and tps_excl.group(1):
            pgsql_data[i]["tps_excl"] = tps_excl.group(1)
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


def create_stats_dir():
    """
    create directory for Test run performance stats
    """
    stats_log_dir = os.path.join(
        os.path.expanduser(config.RUN["log_dir"]),
        f"stats_log_dir_{config.RUN['run_id']}",
    )
    create_directory_path(stats_log_dir)
    return stats_log_dir


def ocsci_log_path():
    """
    Construct the full path for the log directory.

    Returns:
        str: full path for ocs-ci log directory

    """
    return os.path.expanduser(
        os.path.join(config.RUN["log_dir"], f"ocs-ci-logs-{config.RUN['run_id']}")
    )


def get_testrun_name():
    """
    Prepare testrun ID for Polarion (and other reports).

    Returns:
        str: String containing testrun name

    """
    markers = config.RUN["cli_params"].get("-m", "").replace(" ", "-")
    us_ds = config.REPORTING.get("us_ds")
    if us_ds.upper() == "US":
        us_ds = "Upstream"
    elif us_ds.upper() == "DS":
        us_ds = "Downstream"
    ocp_version = version_module.get_semantic_version(
        config.DEPLOYMENT.get("installer_version"), only_major_minor=True
    )
    ocp_version_string = f"OCP{ocp_version}" if ocp_version else ""
    ocs_version = config.ENV_DATA.get("ocs_version")
    ocs_version_string = f"OCS{ocs_version}" if ocs_version else ""
    worker_os = "RHEL" if config.ENV_DATA.get("rhel_workers") else "RHCOS"
    build_user = None
    baremetal_config = None
    if config.ENV_DATA.get("mon_type"):
        baremetal_config = (
            f"MON {config.ENV_DATA.get('mon_type').upper()} "
            f"OSD {config.ENV_DATA.get('osd_type').upper()}"
        )

    lso_deployment = ""
    if not baremetal_config and config.DEPLOYMENT.get("local_storage"):
        lso_deployment = "LSO "

    if config.REPORTING.get("display_name"):
        testrun_name = config.REPORTING.get("display_name")
    else:
        build_user = config.REPORTING.get("build_user")
        testrun_name = (
            f"{config.ENV_DATA.get('platform', '').upper()} "
            f"{config.ENV_DATA.get('deployment_type', '').upper()} "
        )
        if baremetal_config:
            testrun_name = f"LSO {baremetal_config} {testrun_name}"
        if config.ENV_DATA.get("sno"):
            testrun_name = f"{testrun_name} SNO"
        if config.ENV_DATA.get("lvmo"):
            testrun_name = f"{testrun_name} LVMO"
        post_upgrade = config.REPORTING.get("post_upgrade", "")
        if post_upgrade:
            post_upgrade = "post-upgrade"
        testrun_name = (
            f"{testrun_name}"
            f"{get_az_count()}AZ "
            f"{worker_os} "
            f"{lso_deployment}"
            f"{config.ENV_DATA.get('master_replicas')}M "
            f"{config.ENV_DATA.get('worker_replicas')}W "
            f"{markers} {post_upgrade}".rstrip()
        )
    testrun_name = (
        f"{ocs_version_string} {us_ds} {ocp_version_string} " f"{testrun_name}"
    )
    if build_user:
        testrun_name = f"{build_user} {testrun_name}"
    # replace invalid character(s) by '-'
    testrun_name = testrun_name.translate(
        str.maketrans({key: "-" for key in """ \\/.:*"<>|~!@#$?%^&'*(){}+`,=\t"""})
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
    if config.ENV_DATA.get("availability_zone_count"):
        return int(config.ENV_DATA.get("availability_zone_count"))
    elif config.ENV_DATA.get("worker_availability_zones"):
        return len(config.ENV_DATA.get("worker_availability_zones"))
    elif config.ENV_DATA.get("platform") == "vsphere":
        return 1
    else:
        return 1


def wait_for_ceph_health_not_ok(timeout=300, sleep=10):
    """
    Wait until the ceph health is NOT OK

    """

    def check_ceph_health_not_ok():
        """
        Check if ceph health is NOT OK

        """

        status = run_ceph_health_cmd(constants.OPENSHIFT_STORAGE_NAMESPACE)
        return str(status).strip() != "HEALTH_OK"

    sampler = TimeoutSampler(
        timeout=timeout, sleep=sleep, func=check_ceph_health_not_ok
    )
    sampler.wait_for_func_status(True)


def ceph_health_check(namespace=None, tries=20, delay=30):
    """
    Args:
        namespace (str): Namespace of OCS
            (default: config.ENV_DATA['cluster_namespace'])
        tries (int): Number of retries
        delay (int): Delay in seconds between retries

    Returns:
        bool: ceph_health_check_base return value with default retries of 20,
            delay of 30 seconds if default values are not changed via args.

    """
    if config.ENV_DATA["platform"].lower() == constants.IBM_POWER_PLATFORM:
        delay = 60
    return retry(
        (
            CephHealthException,
            CommandFailed,
            subprocess.TimeoutExpired,
            NoRunningCephToolBoxException,
        ),
        tries=tries,
        delay=delay,
        backoff=1,
    )(ceph_health_check_base)(namespace)


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
    namespace = namespace or config.ENV_DATA["cluster_namespace"]
    health = run_ceph_health_cmd(namespace)

    if health.strip() == "HEALTH_OK":
        log.info("Ceph cluster health is HEALTH_OK.")
        return True
    else:
        raise CephHealthException(f"Ceph cluster health is not OK. Health: {health}")


def create_ceph_health_cmd(namespace):
    """
    Forms the ceph health command

    Args:
        namespace (str): Namespace of OCS

    Returns:
        str: ceph health command

    """
    tools_pod = run_cmd(
        f"oc -n {namespace} get pod -l '{constants.TOOL_APP_LABEL}' "
        f"-o jsonpath='{{.items[0].metadata.name}}'",
        timeout=60,
    )
    ceph_health_cmd = f"oc -n {namespace} exec {tools_pod} -- ceph health"
    return ceph_health_cmd


def run_ceph_health_cmd(namespace):
    """
    Run the ceph health command

    Args:
        namespace: Namespace of OCS

    Raises:
        CommandFailed: In case the rook-ceph-tools pod failed to reach the Ready state.

    Returns:
        str: The output of the ceph health command

    """
    # Import here to avoid circular loop

    from ocs_ci.ocs.resources.pod import get_ceph_tools_pod

    try:
        ct_pod = get_ceph_tools_pod(namespace=namespace)
    except (AssertionError, CephToolBoxNotFoundException) as ex:
        raise CommandFailed(ex)

    return ct_pod.exec_ceph_cmd(
        ceph_cmd="ceph health", format=None, out_yaml_format=False, timeout=120
    )


def ceph_health_multi_storagecluster_external_base():
    """
    Check ceph health for multi-storagecluster external implementation.

    Returns:
        bool: True if cluster health is ok.

    Raises:
        CephHealthException: Incase ceph health is not ok.

    """
    # Import here to avoid circular loop
    from ocs_ci.utility.connection import Connection
    from ocs_ci.deployment.helpers.external_cluster_helpers import (
        get_external_cluster_client,
    )

    host, user, password, ssh_key = get_external_cluster_client()
    connection_to_cephcluster = Connection(
        host=host, user=user, password=password, private_key=ssh_key
    )
    ceph_health_tuple = connection_to_cephcluster.exec_cmd("ceph health")
    health = ceph_health_tuple[1]
    if health.strip() == "HEALTH_OK":
        log.info("Ceph external multi-storagecluster health is HEALTH_OK.")
        return True
    else:
        raise CephHealthException(
            f"Ceph cluster health for external multi-storagecluster is not OK. Health: {health}"
        )


def ceph_health_check_multi_storagecluster_external(tries=20, delay=30):
    """
    Check ceph health for multi-storagecluster external.

    """
    return retry(
        (
            CephHealthException,
            CommandFailed,
        ),
        tries=tries,
        delay=delay,
        backoff=1,
    )(ceph_health_multi_storagecluster_external_base)()


def get_rook_repo(branch="master", to_checkout=None):
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


def clone_repo(
    url,
    location,
    tmp_repo=False,
    branch="master",
    to_checkout=None,
    clone_type="shallow",
    force_checkout=False,
):
    """
    Clone a repository or checkout latest changes if it already exists at
        specified location.

    Args:
        url (str): location of the repository to clone
        location (str): path where the repository will be cloned to
        tmp_repo (bool): temporary repo, means it will be copied to temp path, to 'location'
        branch (str): branch name to checkout
        to_checkout (str): commit id or tag to checkout
        clone_type (str): type of clone (shallow, blobless, treeless and normal)
            By default, shallow clone will be used. For normal clone use
            clone_type as "normal".
        force_checkout (bool): True for force checkout to branch.
            force checkout will ignore the unmerged entries.

    Raises:
        UnknownCloneTypeException: In case of incorrect clone_type is used

    """
    if clone_type == "shallow":
        if branch != "master":
            git_params = "--no-single-branch --depth=1"
        else:
            git_params = "--depth=1"
    elif clone_type == "blobless":
        git_params = "--filter=blob:none"
    elif clone_type == "treeless":
        git_params = "--filter=tree:0"
    elif clone_type == "normal":
        git_params = ""
    else:
        raise UnknownCloneTypeException
    """
    Workaround as a temp solution since sno installer git is different from ocp installer if directory already exist
    it checks if the repo already exist from SNO but the git is OCP it delete the installer directory and
    the other way around
    """
    installer_path_exist = os.path.isdir(location)
    if ("installer" in location) and installer_path_exist:
        if "coreos" not in location:
            installer_dir = os.path.join(constants.EXTERNAL_DIR, "installer")
            remote_output = run_cmd(f"git -C {installer_dir} remote -v")
            if (("srozen" in remote_output) and ("openshift" in url)) or (
                ("openshift" in remote_output) and ("srozen" in url)
            ):
                shutil.rmtree(installer_dir)
                log.info(
                    f"Waiting for 5 seconds to get all files and folder deleted from {installer_dir}"
                )
                time.sleep(5)

    if not os.path.isdir(location) or (tmp_repo and os.path.isdir(location)):
        log.info("Cloning repository into %s", location)
        run_cmd(f"git clone {git_params} {url} {location}")
    else:
        log.info("Repository already cloned at %s, skipping clone", location)
        log.info("Fetching latest changes from repository")
        run_cmd("git fetch --all", cwd=location)
    log.info("Checking out repository to specific branch: %s", branch)
    if force_checkout:
        run_cmd(f"git checkout --force {branch}", cwd=location)
    else:
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
    latest_tag = latest_tag or config.DEPLOYMENT.get("default_latest_tag", "latest")
    tags = get_ocs_olm_operator_tags()
    latest_image = None
    ocs_version = config.ENV_DATA["ocs_version"]
    upgrade_ocs_version = config.UPGRADE.get("upgrade_ocs_version")
    use_rc_build = config.UPGRADE.get("use_rc_build")
    previous_rc_build = config.UPGRADE.get("previous_rc_build")
    upgrade_version_change = upgrade_ocs_version and ocs_version != upgrade_ocs_version
    if upgrade and use_rc_build and previous_rc_build and not upgrade_version_change:
        latest_tag = previous_rc_build
    if upgrade_version_change:
        upgrade = False
    for tag in tags:
        if tag["name"] == latest_tag:
            latest_image = tag["manifest_digest"]
            break
    if not latest_image:
        raise TagNotFoundException("Couldn't find latest tag!")
    latest_tag_found = False
    for tag in tags:
        if not upgrade:
            if (
                not any(t in tag["name"] for t in constants.LATEST_TAGS)
                and tag["manifest_digest"] == latest_image
            ):
                return tag["name"]
        if upgrade:
            if not latest_tag_found and tag["name"] == latest_tag:
                latest_tag_found = True
                continue
            if not latest_tag_found:
                continue
            if (
                not any(t in tag["name"] for t in constants.LATEST_TAGS)
                and tag["manifest_digest"] != latest_image
                and ocs_version in tag["name"]
            ):
                if config.UPGRADE.get("use_rc_build") and "rc" not in tag["name"]:
                    continue
                return tag["name"]
    raise TagNotFoundException("Couldn't find any desired tag!")


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
    tags = get_ocs_olm_operator_tags()
    if any(t in current_tag for t in constants.LATEST_TAGS):
        return current_tag
    current_tag_index = None
    for index, tag in enumerate(tags):
        if tag["name"] == current_tag:
            if index < 2:
                raise TagNotFoundException("Couldn't find tag for upgrade!")
            current_tag_index = index
            break
    sliced_reversed_tags = tags[:current_tag_index]
    sliced_reversed_tags.reverse()
    ocs_version = config.ENV_DATA["ocs_version"]
    for tag in sliced_reversed_tags:
        if (
            not any(t in tag["name"] for t in constants.LATEST_TAGS)
            and ocs_version in tag["name"]
        ):
            if config.UPGRADE.get("use_rc_build") and "rc" not in tag["name"]:
                continue
            return tag["name"]
    raise TagNotFoundException("Couldn't find any tag!")


def load_auth_config():
    """
    Load the authentication config YAML from /data/auth.yaml

    Raises:
        FileNotFoundError: if the auth config is not found

    Returns:
        dict: A dictionary reprensenting the YAML file

    """
    log.info("Retrieving the authentication config dictionary")
    auth_file = os.path.join(constants.DATA_DIR, constants.AUTHYAML)
    try:
        with open(auth_file) as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        log.warning(
            f"Unable to find the authentication configuration at {auth_file}, "
            f"please refer to the getting started guide ({constants.AUTH_CONFIG_DOCS})"
        )
        return {}


def get_ocs_olm_operator_tags(limit=100):
    """
    Query the OCS OLM Operator repo and retrieve a list of tags. Since we are limited
    to 100 tags per page, we end up making several API calls and combining the results
    into a single list of tags.

    Args:
        limit: the number of tags to limit the request to

    Raises:
        KeyError: if the auth config isn't setup properly
        requests.RequestException: if the response return code is not ok

    Returns:
        list: OCS OLM Operator tags

    """
    try:
        quay_access_token = load_auth_config()["quay"]["access_token"]
    except (KeyError, TypeError):
        log.error(
            "Unable to retrieve the access token for quay, please refer to "
            f"the getting started guide ({constants.AUTH_CONFIG_DOCS}) "
            "to properly setup your authentication configuration"
        )
        raise
    headers = {"Authorization": f"Bearer {quay_access_token}"}
    image = "ocs-registry"
    try:
        ocs_version = version_module.get_semantic_ocs_version_from_config()
        if ocs_version < version_module.VERSION_4_5:
            image = "ocs-olm-operator"
    except (ValueError, TypeError):
        log.warning("Invalid ocs_version given, defaulting to ocs-registry image")
        pass
    all_tags = []
    page = 1
    while True:
        log.info(f"Retrieving OCS OLM Operator tags (limit {limit}, page {page})")
        resp = requests.get(
            constants.OPERATOR_CS_QUAY_API_QUERY.format(
                tag_limit=limit,
                image=image,
                page=page,
            ),
            headers=headers,
        )
        if not resp.ok:
            raise requests.RequestException(resp.json())
        tags = resp.json()["tags"]
        if len(tags) == 0:
            log.info("No more tags to retrieve")
            break
        log.debug(tags)
        all_tags.extend(tags)
        page += 1
    return all_tags


def check_if_executable_in_path(exec_name):
    """
    Checks whether an executable can be found in the $PATH

    Args:
        exec_name: Name of executable to look for

    Returns:
        Boolean: Whether the executable was found

    """
    return which(exec_name) is not None


def upload_file(server, localpath, remotepath, user=None, password=None, key_file=None):
    """
    Upload a file to remote server

    Args:
        server (str): Name of the server to upload
        localpath (str): Local file to upload
        remotepath (str): Target path on the remote server. filename should be included
        user (str): User to use for the remote connection

    """
    if not user:
        user = "root"
    try:
        ssh = SSHClient()
        ssh.set_missing_host_key_policy(AutoAddPolicy())
        if password:
            ssh.connect(hostname=server, username=user, password=password)
        else:
            log.info(key_file)
            ssh.connect(hostname=server, username=user, key_filename=key_file)
        sftp = ssh.open_sftp()
        log.info(f"uploading {localpath} to {user}@{server}:{remotepath}")
        sftp.put(localpath, remotepath)
        sftp.close()
        ssh.close()
    except AuthenticationException as authException:
        log.error(f"Authentication failed: {authException}")
        raise authException
    except SSHException as sshException:
        log.error(f"SSH connection failed: {sshException}")
        raise sshException


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


def replace_content_in_file(file, old, new, match_and_replace_line=False):
    """
    Replaces contents in file, if old value is not found, it adds
    new value to the file

    Args:
        file (str): Name of the file in which contents will be replaced
        old (str): Data to search for
        new (str): Data to replace the old value
        match_and_replace_line (bool): If True, it will match a line if
            `old` pattern is found in the line. The whole line will be replaced
            with `new` content.
            Otherwise it will replace only `old` string with `new` string but
            the rest of the line will be intact. This is the default option.

    """
    # Read the file
    with open(rf"{file}", "r") as fd:
        file_data = [line.rstrip("\n") for line in fd.readlines()]

    if match_and_replace_line:
        # Replace the whole line with `new` string if the line contains `old`
        # string pattern.
        file_data = [new if old in line else line for line in file_data]
    else:
        # Replace the old string by new
        file_data = [
            line.replace(old, new) if old in line else line for line in file_data
        ]
    updated_data = [line for line in file_data if new in line]
    # In case the old pattern wasn't found it will be added as first line
    if not updated_data:
        file_data.insert(0, new)
    file_data = [f"{line}\n" for line in file_data]
    # Write the file out again
    with open(rf"{file}", "w") as fd:
        fd.writelines(file_data)


@retry((CommandFailed), tries=100, delay=10, backoff=1)
def wait_for_co(operator):
    """
    Waits for ClusterOperator to created

    Args:
        operator (str): Name of the ClusterOperator

    """
    from ocs_ci.ocs.ocp import OCP

    ocp = OCP(kind="ClusterOperator")
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
                    data_to_censor[key] = "*" * 5
            for expression in constants.config_keys_expressions_to_censor:
                if key == expression:
                    data_to_censor[key] = "*" * 5

    return data_to_censor


def filter_unrepresentable_values(data_to_filter):
    """
    This function filter values in dictionary or list which are not possible convert
    to yaml (e.g. objects), to prevent following error raised from yaml.safe_dump
    yaml.representer.RepresenterError("cannot represent an object",...)
    It is performed recursively for nested dictionaries or lists.

    Args:
        data_to_filter (dict|list|tuple): Data to censor.

    Returns:
        dict: filtered data

    """
    if isinstance(data_to_filter, tuple):
        data_to_filter = list(data_to_filter)
    if isinstance(data_to_filter, dict):
        for key in data_to_filter:
            if data_to_filter[key] is None:
                continue
            if isinstance(data_to_filter[key], tuple):
                data_to_filter[key] = list(data_to_filter[key])
            if isinstance(data_to_filter[key], (dict, list)):
                filter_unrepresentable_values(data_to_filter[key])
            elif not isinstance(
                data_to_filter[key], (dict, list, tuple, str, int, float)
            ):
                data_to_filter[key] = str(data_to_filter[key])
    if isinstance(data_to_filter, (list, tuple)):
        for i in range(len(data_to_filter)):
            if data_to_filter[i] is None:
                continue
            if isinstance(data_to_filter[i], tuple):
                data_to_filter[i] = list(data_to_filter[i])
            if isinstance(data_to_filter[i], (dict, list)):
                data_to_filter[i] = filter_unrepresentable_values(data_to_filter[i])
            elif not isinstance(
                data_to_filter[i], (dict, list, tuple, str, int, float)
            ):
                data_to_filter[i] = str(data_to_filter[i])
    return data_to_filter


def dump_config_to_file(file_path):
    """
    Dump the config to the yaml file with censored secret values.

    Args:
        file_path (str): Path to file where to write the configuration.

    """
    config_copy = deepcopy(config.to_dict())
    censor_values(config_copy)
    filter_unrepresentable_values(config_copy)
    with open(file_path, "w+") as fs:
        yaml.safe_dump(config_copy, fs)


def create_rhelpod(namespace, pod_name, rhel_version=8, timeout=300):
    """
    Creates the RHEL pod

    Args:
        namespace (str): Namespace to create RHEL pod
        pod_name (str): Pod name
        rhel_version (int): RHEL version to be used for ansible
        timeout (int): wait time for RHEL pod to be in Running state

    Returns:
        pod: Pod instance for RHEL

    """
    # importing here to avoid dependencies
    from ocs_ci.helpers import helpers
    from ocs_ci.ocs.utils import label_pod_security_admission

    label_pod_security_admission(namespace=namespace)

    if rhel_version >= 8:
        rhel_pod_yaml = constants.RHEL_8_7_POD_YAML
    else:
        rhel_pod_yaml = constants.RHEL_7_7_POD_YAML

    rhelpod_obj = helpers.create_pod(
        namespace=namespace,
        pod_name=pod_name,
        pod_dict_path=rhel_pod_yaml,
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
        msg += f" Error: {err_msg}"

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
    log.debug(f"Converting {yaml} to {tfvars_file}")
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

            if key == "vm_dns_addresses":
                fd.write(f'vm_dns_addresses = ["{val}"]\n')
                continue

            fd.write(key)
            fd.write(" = ")
            fd.write('"')
            fd.write(f"{val}")
            fd.write('"\n')

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

    with open(tf_file, "r") as fd:
        obj = hcl2.load(fd)
    for key in keys:
        obj["variable"].pop(key)

    dump_data_to_json(obj, f"{tf_file}.json")
    os.rename(tf_file, f"{tf_file}.backup")


def get_kubeadmin_password():
    filename = os.path.join(
        config.ENV_DATA["cluster_path"], config.RUN["password_location"]
    )
    with open(filename) as f:
        return f.read()


def get_infra_id(cluster_path):
    """
    Get infraID from metadata.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: metadata.json['infraID']

    """
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata["infraID"]


def get_infra_id_from_openshift_install_state(cluster_path):
    """
    Get infraID from openshift_install_state.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: cluster infraID

    """
    metadata_file = os.path.join(cluster_path, ".openshift_install_state.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata["*installconfig.ClusterID"]["InfraID"]


def get_cluster_name(cluster_path):
    """
    Get clusterName from metadata.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: metadata.json['clusterName']

    """
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata["clusterName"]


def skipif_ocp_version(expressions):
    """
    This function evaluates the condition for test skip
    based on expression

    Args:
        expressions (str OR list): condition for which we need to check,
        eg: A single expression string '>=4.2' OR
            A list of expressions like ['<4.3', '>4.2'], ['<=4.3', '>=4.2']

    Return:
        'True' if test needs to be skipped else 'False'

    """
    ocp_version = get_running_ocp_version()
    expr_list = [expressions] if isinstance(expressions, str) else expressions
    return any(
        version_module.compare_versions(ocp_version + expr) for expr in expr_list
    )


def skipif_ocs_version(expressions):
    """
    This function evaluates the condition for test skip
    based on expression

    Args:
        expressions (str OR list): condition for which we need to check,
        eg: A single expression string '>=4.2' OR
            A list of expressions like ['<4.3', '>4.2'], ['<=4.3', '>=4.2']

    Return:
        'True' if test needs to be skipped else 'False'
    """
    expr_list = [expressions] if isinstance(expressions, str) else expressions
    return any(
        version_module.compare_versions(config.ENV_DATA["ocs_version"] + expr)
        for expr in expr_list
    )


def skipif_ui_not_support(ui_test):
    """
    This function evaluates the condition for ui test skip
    based on ui_test expression

    Args:
        ui_test (str): condition for which we need to check,

    Return:
        'True' if test needs to be skipped else 'False'

    """
    from ocs_ci.ocs.ui.views import locators

    ocp_version = get_running_ocp_version()
    if (
        (
            # Skip for IMB Cloud managed AKA ROKS
            config.ENV_DATA["platform"].lower() == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"].lower() == "managed"
        )
        or config.ENV_DATA["platform"].lower() == constants.OPENSHIFT_DEDICATED_PLATFORM
        or config.ENV_DATA["platform"].lower() == constants.ROSA_PLATFORM
    ):
        return True
    try:
        locators[ocp_version][ui_test]
    except KeyError:
        return True
    return False


def get_ocs_version_from_image(image):
    """
    Parse major.minor version from OCS image tag.

    Args:
        image (str): image in format url:tag

    Returns
        str: Version in x.y format

    Raises:
        ValueError: In case of the tag which we cannot parse to version.

    """
    try:
        version = (
            image.rsplit(":", 1)[1]
            .lstrip("latest-")
            .lstrip("stable-")
            .lstrip("rc-")
            .lstrip("upgrade-")
        )
        version = Version.coerce(version)
        return "{major}.{minor}".format(major=version.major, minor=version.minor)
    except ValueError:
        log.error(f"The version: {version} couldn't be parsed!")
        raise


def get_available_ocp_versions(channel):
    """
    Find all available OCP versions for specific channel.

    Args:
        channel (str): Channel of OCP (e.g. stable-4.2 or fast-4.2)

    Returns
        list: Sorted list with OCP versions for specified channel.

    """
    headers = {"Accept": "application/json"}
    req = requests.get(
        constants.OPENSHIFT_UPGRADE_INFO_API.format(channel=channel), headers=headers
    )
    data = req.json()
    versions = [Version(node["version"]) for node in data["nodes"]]
    versions.sort()
    return versions


def get_latest_ocp_version(channel, index=-1):
    """
    Find latest OCP version for specific channel.

    Args:
        channel (str): Channel of OCP (e.g. stable-4.2 or fast-4.2)
        index (int): Index to get from all available versions list
            e.g. default -1 is latest version (version[-1]). If you want to get
            previous version pass index -2 and so on.

    Returns
        str: Latest OCP version for specified channel.

    """
    versions = get_available_ocp_versions(channel)
    return str(versions[index])


def load_config_file(config_file):
    """
    Loads config file to the ocs-ci config

    Args:
        config_file (str): Path to yaml config file.

    Raises:
        FileNotFoundError: In the case the config file not found.

    """
    config_file = os.path.expanduser(config_file)
    assert os.path.exists(config_file), f"Config file {config_file} doesn't exist!"
    with open(os.path.abspath(os.path.expanduser(config_file)), "r") as file_stream:
        custom_config_data = yaml.safe_load(file_stream)
        config.update(custom_config_data)


def destroy_cluster(installer, cluster_path, log_level="DEBUG"):
    """
    Destroy OCP cluster specific


    Args:
        installer (str): The path to the installer binary
        cluster_path (str): The path of the cluster
        log_level (str): log level openshift-installer (default: DEBUG)

    """
    destroy_cmd = (
        f"{installer} destroy cluster "
        f"--dir {cluster_path} "
        f"--log-level {log_level}"
    )

    try:
        # Execute destroy cluster using OpenShift installer
        log.info(f"Destroying cluster defined in {cluster_path}")
        run_cmd(destroy_cmd, timeout=1200)
    except CommandFailed:
        log.error(traceback.format_exc())
        raise
    except Exception:
        log.error(traceback.format_exc())


def config_to_string(config):
    """
    Convert ConfigParser object to string in INI format.

    Args:
        config (obj): ConfigParser object

    Returns:
        str: Config in one string

    """
    strio = io.StringIO()
    config.write(strio, space_around_delimiters=False)
    return strio.getvalue()


class AZInfo(object):
    """
    A class for getting different az numbers across calls
    """

    zone_number = 0

    def get_zone_number(self):
        """
        Increment current zone_number and perform modulus op
        to roll-on to next available number

        Returns:
           int: zone number index
        """
        prev = AZInfo.zone_number
        AZInfo.zone_number += 1
        AZInfo.zone_number %= get_az_count()
        return prev


def convert_device_size(unformatted_size, units_to_covert_to, convert_size=1000):
    """
    Convert a string representing a size to an int according to the given units
    to convert to

    Args:
        unformatted_size (str): The size to convert (i.e, '1Gi'/'100Mi')
            acceptable units are: "Ti", "Gi", "Mi", "Ki", "Bi"
        units_to_covert_to (str): The units to convert the size to (i.e, TB/GB/MB)
            acceptable units are: "TB", "GB", "MB", "KB", "BY"
        convert_size (int): set convert by 1000 or 1024

    Returns:
        int: The converted size

    """
    units = unformatted_size[-2:]
    abso = int(unformatted_size[:-2])
    conversion_1000 = {
        "TB": {
            "Ti": abso,
            "Gi": abso / 1000,
            "Mi": abso / 1e6,
            "Ki": abso / 1e9,
            "Bi": abso / 1e12,
        },
        "GB": {
            "Ti": abso * 1000,
            "Gi": abso,
            "Mi": abso / 1000,
            "Ki": abso / 1e6,
            "Bi": abso / 1000,
        },
        "MB": {
            "Ti": abso * 1e6,
            "Gi": abso * 1000,
            "Mi": abso,
            "Ki": abso / 1000,
            "Bi": abso / 1e6,
        },
        "KB": {
            "Ti": abso * 1e9,
            "Gi": abso * 1e6,
            "Mi": abso * 1000,
            "Ki": abso,
            "Bi": abso / 1000,
        },
        "BY": {
            "Ti": abso * 1e12,
            "Gi": abso * 1e9,
            "Mi": abso * 1e6,
            "Ki": abso * 1000,
            "Bi": abso,
        },
    }
    conversion_1024 = {
        "TB": {
            "Ti": abso,
            "Gi": abso / 1024,
            "Mi": abso / 1024**2,
            "Ki": abso / 1024**3,
            "Bi": abso / 1024**4,
        },
        "GB": {
            "Ti": abso * 1024,
            "Gi": abso,
            "Mi": abso / 1024,
            "Ki": abso / 1024**2,
            "Bi": abso / 1024**3,
        },
        "MB": {
            "Ti": abso * 1024**2,
            "Gi": abso * 1024,
            "Mi": abso,
            "Ki": abso / 1024,
            "Bi": abso / 1024**2,
        },
        "KB": {
            "Ti": abso * 1024**3,
            "Gi": abso * 1024**2,
            "Mi": abso * 1024,
            "Ki": abso,
            "Bi": abso / 1024,
        },
        "BY": {
            "Ti": abso * 1024**4,
            "Gi": abso * 1024**3,
            "Mi": abso * 1024**2,
            "Ki": abso * 1024,
            "Bi": abso,
        },
    }
    if convert_size == 1000:
        return conversion_1000[units_to_covert_to][units]
    elif convert_size == 1024:
        return conversion_1024[units_to_covert_to][units]


def convert_bytes_to_unit(bytes_to_convert):
    """
    Convert bytes to bigger units like Kb, Mb, Gb or Tb.

    Args:
        bytes_to_convert (str): The bytes to convert.

    Returns:
        str: The converted bytes as biggest unit possible

    """
    if not isinstance(bytes_to_convert, str):
        log.error("Unable to convert, expected string")
    if float(bytes_to_convert) < constants.BYTES_IN_KB:
        return f"{bytes_to_convert}B"
    if constants.BYTES_IN_KB <= float(bytes_to_convert) < constants.BYTES_IN_MB:
        size = float(bytes_to_convert) / constants.BYTES_IN_KB
        return f"{size:.2f}KB"
    if constants.BYTES_IN_MB <= float(bytes_to_convert) < constants.BYTES_IN_GB:
        size = float(bytes_to_convert) / constants.BYTES_IN_MB
        return f"{size:.2f}MB"
    if constants.BYTES_IN_GB <= float(bytes_to_convert) < constants.BYTES_IN_TB:
        size = float(bytes_to_convert) / constants.BYTES_IN_GB
        return f"{size:.2f}GB"
    if constants.BYTES_IN_TB <= float(bytes_to_convert):
        size = float(bytes_to_convert) / constants.BYTES_IN_TB
        return f"{size:.2f}TB"


def human_to_bytes_ui(size_str):
    """
    Convert human readable size to bytes.
    Use this function when working with UI pages or when format "MiB", "KiB" with space separation,  is used.

    Args:
        size_str (str): The size to convert (i.e, "1 GiB" is 1048576 bytes)
            acceptable units are: "EiB"/"Ei", "PiB"/"Pi" "TiB"/"Ti", "GiB"/"Gi", "MiB"/"Mi", "KiB"/"Ki", "B"/"Bytes"

    Returns:
        int: The converted size in bytes

    """
    units = {
        "E": 2**60,
        "P": 2**50,
        "T": 2**40,
        "G": 2**30,
        "M": 2**20,
        "K": 2**10,
        "B": 1,
    }
    size, unit = size_str.split()
    unit = unit[0]
    size = float(size)
    return int(size * units[unit])


def prepare_customized_pull_secret(images=None):
    """
    Prepare customized pull-secret containing auth section related to given
    image(s). If image(s) not defined or no related section is found, it will
    use whole content of pull-secret.

    Args:
        images (str, list): image (or images) to match with auth section

    Returns:
        NamedTemporaryFile: prepared pull-secret

    """
    log.debug(f"Prepare customized pull-secret for images: {images}")
    if isinstance(images, str):
        images = [images]
    # load pull-secret file to pull_secret dict
    pull_secret_path = os.path.join(constants.DATA_DIR, "pull-secret")
    with open(pull_secret_path) as pull_secret_fo:
        pull_secret = json.load(pull_secret_fo)

    authfile_content = {"auths": {}}
    # if images defined, try to find auth section related to specified images
    if images:
        for image in images:
            # find all auths which might be related to the specified image
            tmp_auths = [auth for auth in pull_secret["auths"] if auth in image]
            # get the most specific auth for particular image
            tmp_auths = sorted(tmp_auths, key=len, reverse=True)
            if tmp_auths:
                # if there is match to particular auth, prepare authfile just with the
                # matching auth
                auth = tmp_auths[0]
                # as key use only server name, without namespace
                authfile_content["auths"][auth.split("/", 1)[0]] = pull_secret["auths"][
                    auth
                ]

    if not authfile_content["auths"]:
        authfile_content = pull_secret

    # create temporary auth file
    authfile_fo = NamedTemporaryFile(mode="w", prefix="authfile_")
    json.dump(authfile_content, authfile_fo)
    # ensure the content will be saved into the file
    authfile_fo.flush()
    return authfile_fo


def inspect_image(image, authfile_fo, cluster_config=None):
    """
    Inspect image

    Args:
        image (str): image to inspect
        authfile_fo (NamedTemporaryFile): pull-secret required for pulling the given image
        cluster_config (MultiClusterConfig): Holds the context of a cluster

    Returns:
        dict: json object of the inspected image

    """
    # pull original image (to be able to inspect it)
    exec_cmd(
        f"podman image pull {image} --authfile {authfile_fo.name}",
        cluster_config=cluster_config,
    )
    # inspect the image
    cmd_result = exec_cmd(
        f"podman image inspect {image}", cluster_config=cluster_config
    )
    image_inspect = json.loads(cmd_result.stdout)
    return image_inspect


def get_image_with_digest(image):
    """
    Return image with sha256 digest for usage in disconnected environment

    Args:
        image (str): image

    Raises:
        UnexpectedImage: In case the image information is unexpected

    Returns:
        str: image with sha256 digest specification

    """
    if "@sha256:" in image:
        return image
    with prepare_customized_pull_secret(image) as authfile_fo:
        image_inspect = inspect_image(image, authfile_fo)

    # we expect, that 'Digest' will match one of the images in 'RepoDigests',
    # if not, raise UnexpectedImage
    for image in image_inspect[0]["RepoDigests"]:
        if image_inspect[0]["Digest"] in image:
            return image
    else:
        raise UnexpectedImage(
            f"Image digest ({image_inspect[0]['Digest']}) doesn't match with "
            f"any image from RepoDigests ({image_inspect[0]['RepoDigests']})."
        )


def login_to_mirror_registry(authfile, cluster_config=None):
    """
    Login to mirror registry

    Args:
        authfile (str): authfile (pull-secret) path

    """
    if not cluster_config:
        cluster_config = config
    # load cluster info
    load_cluster_info(cluster_config)

    mirror_registry = cluster_config.DEPLOYMENT["mirror_registry"]
    mirror_registry_user = cluster_config.DEPLOYMENT["mirror_registry_user"]
    mirror_registry_password = cluster_config.DEPLOYMENT["mirror_registry_password"]
    login_cmd = (
        f"podman login --authfile {authfile} "
        f"{mirror_registry} -u {mirror_registry_user} "
        f"-p {mirror_registry_password} --tls-verify=false"
    )
    exec_cmd(
        login_cmd,
        (mirror_registry_user, mirror_registry_password),
        cluster_config=cluster_config,
    )


def mirror_image(image, cluster_config=None):
    """
    Mirror image to mirror image registry.

    Args:
        image (str): image to be mirrored, can be defined just with name or
            with full url, with or without tag or digest
        cluster_config (MultiClusterConfig): Config object if single cluster, if its multicluster scenario
            then we will have MultiClusterConfig object

    Returns:
        str: the mirrored image link

    """
    if not cluster_config:
        cluster_config = config
    mirror_registry = cluster_config.DEPLOYMENT.get("mirror_registry")
    if not mirror_registry:
        raise ConfigurationError(
            'DEPLOYMENT["mirror_registry"] parameter not configured!\n'
            "This might be caused by previous failure in OCP deployment or wrong configuration."
        )
    if image.startswith(mirror_registry):
        log.debug(f"Skipping mirror of image {image}, it is already mirrored.")
        return image
    with prepare_customized_pull_secret(image) as authfile_fo:
        # login to mirror registry
        login_to_mirror_registry(authfile_fo.name, cluster_config)

        # if there is any tag specified, use it in the full image url,
        # otherwise use url with digest
        image_inspect = inspect_image(image, authfile_fo, cluster_config)
        if image_inspect[0].get("RepoTags"):
            orig_image_full = image_inspect[0]["RepoTags"][0]
        else:
            orig_image_full = image_inspect[0]["RepoDigests"][0]
        # prepare mirrored image url
        mirrored_image = mirror_registry + re.sub(r"^[^/]*", "", orig_image_full)
        # mirror the image
        log.info(
            f"Mirroring image '{image}' ('{orig_image_full}') to '{mirrored_image}'"
        )
        exec_cmd(
            f"oc image mirror --insecure --registry-config"
            f" {authfile_fo.name} {orig_image_full} {mirrored_image}",
            cluster_config=cluster_config,
        )
    return mirrored_image


def update_container_with_mirrored_image(job_pod_dict):
    """
    Update Job or Pod configuration dict with mirrored image (required for
    disconnected installation).

    Args:
        job_pod_dict (dict): dictionary with Job or Pod configuration

    Returns:
        dict: for disconnected installation, returns updated Job or Pod dict,
            for normal installation return unchanged job_pod_dict

    """
    if config.DEPLOYMENT.get("disconnected"):
        if "containers" in job_pod_dict["spec"]:
            container = job_pod_dict["spec"]["containers"][0]
        else:
            container = job_pod_dict["spec"]["template"]["spec"]["containers"][0]
        container["image"] = mirror_image(container["image"])
    return job_pod_dict


def get_trim_mean(values, percentage=20):
    """
    Get the trimmed mean of a list of values.
    Explanation: This function finds the arithmetic mean of given values,
    ignoring values outside the given limits.

    Args:
        values (list): The list of values
        percentage (int): The percentage to be trimmed

    Returns:
        float: Trimmed mean. In case trimmed mean calculation fails,
            the regular mean average is returned

    """
    lower_limit = scoreatpercentile(values, percentage)
    upper_limit = scoreatpercentile(values, 100 - percentage)
    try:
        return tmean(values, limits=(lower_limit, upper_limit))
    except ValueError:
        log.warning(
            f"Failed to calculate the trimmed mean of {values}. The "
            f"Regular mean average will be calculated instead"
        )
    return sum(values) / len(values)


def set_selinux_permissions(workers=None):
    """
    Workaround for #1777384 - enable container_use_cephfs on RHEL workers
    Ticket: RHSTOR-787, see more details in the issue: #1151

    Args:
        workers (list): List of worker nodes to set selinux permissions

    """
    log.info("Running WA for ticket: RHSTOR-787")
    from ocs_ci.ocs import ocp

    ocp_obj = ocp.OCP()
    cmd = ["/usr/sbin/setsebool -P container_use_cephfs on"]
    cmd_list = cmd.copy()
    if not workers:
        from ocs_ci.ocs.node import get_typed_worker_nodes

        if "rhel7" not in config.ENV_DATA.get("rhel_template", "None"):
            log.debug(
                f"selinux permission are not needed for {config.ENV_DATA.get('rhel_template')}"
            )
            return
        worker_nodes = get_typed_worker_nodes(os_id="rhel")
    else:
        worker_nodes = workers

    for worker in worker_nodes:
        node = worker.get().get("metadata").get("name") if not workers else worker
        log.info(f"{node} is a RHEL based worker - applying '{cmd_list}'")
        if config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM:
            retry(CommandFailed, tries=10, delay=3, backoff=2)(
                ocp_obj.exec_oc_debug_cmd
            )(node=node, cmd_list=cmd_list)
        else:
            retry(CommandFailed)(ocp_obj.exec_oc_debug_cmd)(
                node=node, cmd_list=cmd_list
            )


def set_registry_to_managed_state():
    """
    In order to be able to deploy from stage we need to change
    image registry config to Managed state.
    More described in BZs:
    https://bugzilla.redhat.com/show_bug.cgi?id=1806593
    https://bugzilla.redhat.com/show_bug.cgi?id=1807471#c3
    We need to change to managed state as described here:
    https://github.com/red-hat-storage/ocs-ci/issues/1436
    So this is not suppose to be deleted as WA case we really need to do
    this operation for OCS deployment as was originally done here:
    https://github.com/red-hat-storage/ocs-ci/pull/1437
    Currently it has to be moved here to enable CA certificate to be
    properly propagated for the stage deployment as mentioned in BZ.
    """
    # In RHV platform config is already set to Managed and storage pre-configured
    on_prem_platform_to_exclude = [constants.RHV_PLATFORM]
    platform_list_to_exclude = constants.CLOUD_PLATFORMS + on_prem_platform_to_exclude
    if config.ENV_DATA["platform"] not in platform_list_to_exclude:
        cluster_config = yaml.safe_load(
            exec_cmd(f"oc get {constants.IMAGE_REGISTRY_CONFIG} -o yaml").stdout
        )
        if "emptyDir" not in cluster_config["spec"].get("storage", {}).keys():
            run_cmd(
                f"oc patch {constants.IMAGE_REGISTRY_CONFIG} --type merge -p "
                f'\'{{"spec":{{"storage": {{"emptyDir":{{}}}}}}}}\''
            )
        if cluster_config["spec"].get("managementState") != "Managed":
            run_cmd(
                f"oc patch {constants.IMAGE_REGISTRY_CONFIG} --type merge -p "
                f'\'{{"spec":{{"managementState": "Managed"}}}}\''
            )


def add_stage_cert():
    """
    Deploy stage certificate to the cluster.
    """
    log.info("Create configmap stage-registry-config with stage CA.")
    run_cmd(
        f"oc -n openshift-config create configmap stage-registry-config"
        f" --from-file=registry.stage.redhat.io={constants.STAGE_CA_FILE}"
    )

    log.info("Add stage-registry-config to additionalTrustedCA.")
    additional_trusted_ca_patch = (
        '{"spec":{"additionalTrustedCA":{"name":"stage-registry-config"}}}'
    )
    run_cmd(
        f"oc patch image.config.openshift.io cluster --type=merge"
        f" -p '{additional_trusted_ca_patch}'"
    )


def get_terraform(version=None, bin_dir=None):
    """
    Downloads the terraform binary

    Args:
        version (str): Version of the terraform to download
        bin_dir (str): Path to bin directory (default: config.RUN['bin_dir'])

    Returns:
        str: Path to the terraform binary

    """
    if platform.system() == "Darwin":
        os_type = "darwin"
    elif platform.system() == "Linux":
        os_type = "linux"
    else:
        raise UnsupportedOSType

    version = version or config.DEPLOYMENT["terraform_version"]
    bin_dir = os.path.expanduser(bin_dir or config.RUN["bin_dir"])
    terraform_zip_file = f"terraform_{version}_{os_type}_amd64.zip"
    terraform_filename = "terraform"
    terraform_binary_path = os.path.join(bin_dir, terraform_filename)
    log.info(f"Downloading terraform version {version}")
    previous_dir = os.getcwd()
    os.chdir(bin_dir)
    url = f"https://releases.hashicorp.com/terraform/{version}/" f"{terraform_zip_file}"
    download_file(url, terraform_zip_file)
    run_cmd(f"unzip -o {terraform_zip_file}")
    delete_file(terraform_zip_file)
    # return to the previous working directory
    os.chdir(previous_dir)

    return terraform_binary_path


def get_terraform_ignition_provider(terraform_dir, version=None):
    """
    Downloads the terraform ignition provider

    Args:
        terraform_dir (str): Path to terraform working directory
        version (str): Version of the terraform ignition provider to download

    """
    version = version or constants.TERRAFORM_IGNITION_PROVIDER_VERSION
    terraform_ignition_provider_zip_file = (
        f"terraform-provider-ignition_{version[1:]}_linux_amd64.zip"
    )
    terraform_plugins_path = ".terraform/plugins/linux_amd64/"
    terraform_ignition_provider = os.path.join(
        terraform_plugins_path, "terraform-provider-ignition"
    )
    log.info(f"Downloading terraform ignition provider version {version}")
    previous_dir = os.getcwd()
    os.chdir(terraform_dir)
    url = (
        "https://github.com/community-terraform-providers/"
        f"terraform-provider-ignition/releases/download/{version}/"
        f"{terraform_ignition_provider_zip_file}"
    )

    # Download and untar
    download_file(url, terraform_ignition_provider_zip_file)
    run_cmd(f"unzip -o {terraform_ignition_provider_zip_file}")

    # move the ignition provider binary to plugins path
    # If the cluster is upgraded from OCP 4.10, target_terraform_ignition_provider should
    # be terraform_ignition_provider
    create_directory_path(terraform_plugins_path)
    if (
        version_module.get_semantic_ocp_version_from_config()
        >= version_module.VERSION_4_11
        and config.ENV_DATA.get("original_installed_ocp_version_major_minor_obj")
        != version_module.VERSION_4_10
    ):
        target_terraform_ignition_provider = terraform_plugins_path
    else:
        target_terraform_ignition_provider = terraform_ignition_provider
    move(
        f"terraform-provider-ignition_{version}",
        target_terraform_ignition_provider,
    )

    # delete the downloaded files
    delete_file(terraform_ignition_provider_zip_file)

    # return to the previous working directory
    os.chdir(previous_dir)


def get_module_ip(terraform_state_file, module):
    """
    Gets the node IP from terraform.tfstate file

    Args:
        terraform_state_file (str): Path to terraform state file
        module (str): Module name in terraform.tfstate file
            e.g: constants.LOAD_BALANCER_MODULE

    Returns:
        list: IP of the node

    """
    ips = []
    with open(terraform_state_file) as fd:
        obj = json.loads(fd.read())

        if config.ENV_DATA.get("folder_structure"):
            resources = obj["resources"]
            log.debug(f"Extracting module information for {module}")
            log.debug(f"Resource in {terraform_state_file}: {resources}")
            for resource in resources:
                if resource.get("module") == module and resource.get("mode") == "data":
                    for each_resource in resource["instances"]:
                        resource_body = each_resource["attributes"]["body"]
                        ips.append(resource_body.split('"')[3])
        else:
            modules = obj["modules"]
            target_module = module.split("_")[1]
            log.debug(f"Extracting module information for {module}")
            log.debug(f"Modules in {terraform_state_file}: {modules}")
            for each_module in modules:
                if target_module in each_module["path"]:
                    return each_module["outputs"]["ip_addresses"]["value"]

        return ips


def set_aws_region(region=None):
    """
    Exports environment variable AWS_REGION

    Args:
        region (str): AWS region to export

    """
    log.debug("Exporting environment variable AWS_REGION")
    region = region or config.ENV_DATA["region"]
    os.environ["AWS_REGION"] = region


def get_system_architecture():
    """
    Get output from 'uname -m' command run on first worker node.

    Returns:
        str: Architecture of system

    """
    from ocs_ci.ocs.node import get_nodes

    log.info("Checking architecture of system")
    node = get_nodes(node_type=constants.WORKER_MACHINE)[0]
    return node.ocp.exec_oc_debug_cmd(node.data["metadata"]["name"], ["uname -m"])


def wait_for_machineconfigpool_status(node_type, timeout=1900, skip_tls_verify=False):
    """
    Check for Machineconfigpool status

    Args:
        node_type (str): The node type to check machineconfigpool
            status is updated.
            e.g: worker, master and all if we want to check for all nodes
        timeout (int): Time in seconds to wait
        skip_tls_verify (bool): True if allow skipping TLS verification

    """
    log.info("Sleeping for 60 sec to start update machineconfigpool status")
    time.sleep(60)
    # importing here to avoid dependencies
    from ocs_ci.ocs import ocp

    node_types = [node_type]
    if node_type == "all":
        node_types = [f"{constants.WORKER_MACHINE}", f"{constants.MASTER_MACHINE}"]

    for role in node_types:
        log.info(f"Checking machineconfigpool status for {role} nodes")
        ocp_obj = ocp.OCP(
            kind=constants.MACHINECONFIGPOOL,
            resource_name=role,
            skip_tls_verify=skip_tls_verify,
        )
        machine_count = ocp_obj.get()["status"]["machineCount"]

        assert ocp_obj.wait_for_resource(
            condition=str(machine_count),
            column="READYMACHINECOUNT",
            timeout=timeout,
            sleep=5,
        )


def configure_chrony_and_wait_for_machineconfig_status(
    node_type=constants.WORKER_MACHINE, timeout=900
):
    """
    Configure chrony on the nodes

    Args:
        node_type (str): The node type to configure chrony
            e.g: worker, master and all if we want to configure on all nodes
        timeout (int): Time in seconds to wait

    """
    # importing here to avoid dependencies
    from ocs_ci.utility.templating import load_yaml
    from ocs_ci.ocs.resources.ocs import OCS

    chrony_data = load_yaml(constants.NTP_CHRONY_CONF)

    node_types = [node_type]
    if node_type == "all":
        node_types = [f"{constants.WORKER_MACHINE}", f"{constants.MASTER_MACHINE}"]

    for role in node_types:
        log.info(f"Creating chrony for {role} nodes")
        chrony_data["metadata"]["labels"][
            "machineconfiguration.openshift.io/role"
        ] = role
        chrony_data["metadata"]["name"] = f"{role}-chrony-configuration"
        chrony_obj = OCS(**chrony_data)
        chrony_obj.create()

        wait_for_machineconfigpool_status(role, timeout=timeout)


def modify_csv(csv, replace_from, replace_to):
    """
    Modify the CSV

    Args:
        csv (str): The CSV name
        replace_from (str): The pattern to replace from in the CSV
        replace_to (str): The pattern to replace to in the CSV

    """
    data = (
        f"oc -n openshift-storage get csv {csv} -o yaml | sed"
        f" 's,{replace_from},{replace_to},g' | oc replace -f -"
    )
    log.info(
        f"CSV {csv} will be modified: {replace_from} will be replaced "
        f"with {replace_to}.\nThe command that will be used for that is:\n{data}"
    )

    temp_file = NamedTemporaryFile(mode="w+", prefix="csv_modification", suffix=".sh")

    with open(temp_file.name, "w") as t_file:
        t_file.writelines(data)

    run_cmd(f"chmod 777 {temp_file.name}")
    run_cmd(f"sh {temp_file.name}")


def check_for_rhcos_images(url):
    """
    Check for rhcos images are present in given location

    Args:
        url (str): rhcos_images url
    Returns:
        (bool): True if images present if not false

    """
    r = requests.head(url)
    return r.status_code == requests.codes.ok


def download_file_from_git_repo(git_repo_url, path_to_file_in_git, filename):
    """
    Download a file from a specified git repository

    Args:
        git_repo_url (str): The git repository url
        path_to_file_in_git (str): Path to the file to download
            in git repository
        filename (str): Name of the file to write the download to

    """
    log.debug(
        f"Download file '{path_to_file_in_git}' from "
        f"git repository {git_repo_url} to local file '{filename}'."
    )
    temp_dir = mkdtemp()
    git.Repo.clone_from(git_repo_url, temp_dir, branch="master", depth=1)
    move(os.path.join(temp_dir, path_to_file_in_git), filename)
    rmtree(temp_dir)


def skipif_upgraded_from(version_list):
    """
    This function evaluates the condition to skip a test if the cluster
    is upgraded from a particular OCS version

    Args:
        version_list (list): List of versions to check

    Return:
        (bool): True if test needs to be skipped else False

    """
    try:
        from ocs_ci.ocs.resources.ocs import get_ocs_csv

        skip_this = False
        version_list = [version_list] if isinstance(version_list, str) else version_list
        ocs_csv = get_ocs_csv()
        csv_info = ocs_csv.get()
        prev_version = csv_info.get("spec").get("replaces", "")
        for version in version_list:
            if f".v{version}" in prev_version:
                skip_this = True
                break
        return skip_this
    except Exception as err:
        log.error(str(err))
        return False


def get_cluster_id(cluster_path):
    """
    Get ClusterID from metadata.json in given cluster_path

    Args:
        cluster_path: path to cluster install directory

    Returns:
        str: metadata.json['clusterID']

    """
    metadata_file = os.path.join(cluster_path, "metadata.json")
    with open(metadata_file) as f:
        metadata = json.load(f)
    return metadata["clusterID"]


def get_running_cluster_id():
    """
    Get cluster UUID
    Not relying on metadata.json as user sometimes want to run
    only with kubeconfig for some tests. For this function to work
    cluster has to be in running state

    Returns:
        str: cluster UUID

    """
    cluster_id = run_cmd(
        "oc get clusterversion version -o jsonpath='{.spec.clusterID}'"
    )
    return cluster_id


def get_ocp_upgrade_history():
    """
    Gets the OCP upgrade history for the cluster

    Returns:
        list: List of OCP upgrade paths. Latest version in the
            beginning of the list

    """
    # importing here to avoid circular imports
    from ocs_ci.ocs.ocp import OCP

    ocp = OCP(kind="clusterversion")
    cluster_version_info = ocp.get("version")
    upgrade_history_info = cluster_version_info["status"]["history"]
    upgrade_history = [each_upgrade["version"] for each_upgrade in upgrade_history_info]
    return upgrade_history


def get_attr_chain(obj, attr_chain):
    """
    Attempt to retrieve object attributes when uncertain about the existence of the attribute
    or a different attribute in a given attribute chain. If the retrieval fails, None is returned.
    The function can be used to retrieve a direct attribute, or a chain of attributes.
    i.e. - obj.attr_a, obj_attr_a.sub_attr

    Another example - trying to access "sub_attr_b" in object.attr.sub_attr_a.sub_attr_b -
    get_attr_chain(object, "attr.sub_attr_a.sub_attr_b")

    The function can be used to try and retrieve "sub_attribute_b" without an exception,
    even in cases where "attr" or "sub_attr_a" might not exist.
    In those cases, the function will return None.

    Args:
        obj: An object
        attr_chain (str): A string containing one attribute or several sub-attributes
            separated by dots (i.e. - "attr.sub_attr_a.sub_attr_b")

    Returns:
        The requested attribute if found, otherwise None
    """
    return reduce(
        lambda _obj, _attr: getattr(_obj, _attr, None), attr_chain.split("."), obj
    )


def get_default_if_keyval_empty(dictionary, key, default_val):
    """
    if Key has an empty value OR key doesn't exist
    then return default value

    Args:
        dictionary (dict): Dictionary where we have to lookup
        key (str): key to lookup
        default_val (str): If key doesn't have value then return
            this default_val

    Returns:
        dictionary[key] if value is present else default_val

    """
    if not dictionary.get(key):
        return default_val
    return dictionary.get(key)


def get_client_version(client_binary_path):
    """
    Get version reported by `oc version`.

    Args:
        client_binary_path (str): path to `oc` binary

    Returns:
        str: version reported by `oc version`.
            None if the client does not exist at the provided path.

    """
    if os.path.isfile(client_binary_path):
        cmd = f"{client_binary_path} version --client -o json"
        resp = exec_cmd(cmd)
        stdout = json.loads(resp.stdout.decode())
        return stdout["releaseClientVersion"]


def clone_notify():
    """
    Repository contains the source code of notify tool,
    which is a python3 based tool wrapped by a container
    used to configure Ceph Bucket Notifications

    Returns:
        notify_path (str): Path location of the notify code

    """
    notify_dir = mkdtemp(prefix="notify_")
    log.info(f"cloning repo notify in {notify_dir}")
    git_clone_cmd = f"git clone {constants.RGW_KAFKA_NOTIFY}"
    subprocess.run(git_clone_cmd, shell=True, cwd=notify_dir, check=True)
    notify_path = f"{notify_dir}/notify/notify.py"
    return notify_path


def add_chrony_to_ocp_deployment():
    """
    Create and Add necessary chrony resources

    """
    for role in ["master", "worker"]:
        log.info(f"Creating and Adding Chrony file for {role}")
        with open(constants.CHRONY_TEMPLATE) as file_stream:
            chrony_template_obj = yaml.safe_load(file_stream)
        chrony_template_obj["metadata"]["labels"][
            "machineconfiguration.openshift.io/role"
        ] = role
        chrony_template_obj["metadata"]["name"] = f"99-{role}-chrony-configuration"
        ignition_version = config.DEPLOYMENT["ignition_version"]
        chrony_template_obj["spec"]["config"]["ignition"]["version"] = ignition_version

        if Version.coerce(ignition_version) < Version.coerce("3.0"):
            chrony_template_obj["spec"]["config"]["storage"]["files"][0][
                "filesystem"
            ] = "root"

        chrony_template_str = yaml.safe_dump(chrony_template_obj)
        chrony_file = os.path.join(
            config.ENV_DATA["cluster_path"],
            "openshift",
            f"99-{role}-chrony-configuration.yaml",
        )
        with open(chrony_file, "w") as f:
            f.write(chrony_template_str)


def enable_huge_pages():
    """
    Applies huge pages

    """
    log.info("Enabling huge pages.")
    exec_cmd(f"oc apply -f {constants.HUGE_PAGES_TEMPLATE}")
    time.sleep(10)
    log.info("Waiting for machine config will be applied with huge pages")
    # Wait for Master nodes ready state when Compact mode 3M 0W config
    from ocs_ci.ocs.node import get_nodes

    if not len(get_nodes(node_type=constants.WORKER_MACHINE)):
        wait_for_machineconfigpool_status(
            node_type=constants.MASTER_MACHINE, timeout=1200
        )
    else:
        wait_for_machineconfigpool_status(
            node_type=constants.WORKER_MACHINE, timeout=1200
        )


def disable_huge_pages():
    """
    Removes huge pages

    """
    log.info("Disabling huge pages.")
    exec_cmd(f"oc delete -f {constants.HUGE_PAGES_TEMPLATE}")
    time.sleep(10)
    log.info("Waiting for machine config to be ready")
    wait_for_machineconfigpool_status(node_type=constants.WORKER_MACHINE, timeout=1200)


def encode(message):
    """
    Encodes the message in base64

    Args:
        message (str/list): message to encode

    Returns:
        str: encoded message in base64

    """
    message_bytes = message.encode("ascii")
    encoded_base64_bytes = base64.b64encode(message_bytes)
    encoded_message = encoded_base64_bytes.decode("ascii")
    return encoded_message


def decode(encoded_message):
    """
    Decodes the message in base64

    Args:
        encoded_message (str): encoded message

    Returns:
        str: decoded message

    """
    encoded_message_bytes = encoded_message.encode("ascii")
    decoded_base64_bytes = base64.b64decode(encoded_message_bytes)
    decoded_message = decoded_base64_bytes.decode("ascii")
    return decoded_message


def get_root_disk(node):
    """
    Fetches the root (boot) disk for node

    Args:
        node (str): Node name

    Returns:
        str: Root disk

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs import ocp

    ocp_obj = ocp.OCP()

    # get the root disk
    cmd = 'lsblk -n -o "KNAME,PKNAME,MOUNTPOINT" --json'
    out = ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=[cmd])
    disk_info_json = json.loads(out)
    for blockdevice in disk_info_json["blockdevices"]:
        if blockdevice["mountpoint"] == "/boot":
            root_disk = blockdevice["pkname"]
            break
    log.info(f"root disk for {node}: {root_disk}")
    return root_disk


def wipe_partition(node, disk_path):
    """
    Wipes out partition for disk using sgdisk

    Args:
        node (str): Name of the node (OCP Node)
        disk_path (str): Disk to wipe partition

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs import ocp

    ocp_obj = ocp.OCP()

    log.info(f"wiping partition for disk {disk_path} on {node}")
    cmd = f"sgdisk --zap-all {disk_path}"
    out = ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=[cmd])
    log.info(out)


def wipe_all_disk_partitions_for_node(node):
    """
    Wipes out partition for all disks which has "nvme" prefix

    Args:
        node (str): Name of the node (OCP Node)

    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs import ocp

    ocp_obj = ocp.OCP()

    # get the root disk
    root_disk = get_root_disk(node)

    cmd = "lsblk -nd -o NAME --json"
    out = ocp_obj.exec_oc_debug_cmd(node=node, cmd_list=[cmd])
    lsblk_json = json.loads(out)
    for blockdevice in lsblk_json["blockdevices"]:
        if "nvme" in blockdevice["name"]:
            disk_to_wipe = blockdevice["name"]
            # double check if disk to wipe is not root disk
            if disk_to_wipe != root_disk:
                disk_path = f"/dev/{disk_to_wipe}"
                wipe_partition(node, disk_path)


def convert_hostnames_to_ips(hostnames):
    """
    Gets the IP's from hostname with FQDN

    Args:
        hostnames (list): List of host names with FQDN

    Returns:
        list: Host IP's

    """
    return [socket.gethostbyname(host) for host in hostnames]


def string_chunkify(cstring, csize):
    """
    Create string chunks of size csize from cstring and
    yield chunk by chunk

    Args:
        cstring (str): Original string which need to be chunkified
        csize (int): size of each chunk

    """
    i = 0
    while len(cstring[i:]) > csize:
        yield cstring[i : i + csize]
        i += csize
    yield cstring[i:]


def get_pytest_fixture_value(request, fixture_name):
    """
    Get the value of a fixture name from the request

    Args:
        request (_pytest.fixtures.SubRequest'): The pytest request fixture
        fixture_name: Fixture for which this request is being performed

    Returns:
        Any: The fixture value

    """
    if fixture_name not in request.fixturenames:
        return None

    return request.getfixturevalue(fixture_name)


def switch_to_correct_cluster_at_setup(request):
    """
    Switch to the correct cluster index at setup, according to the 'cluster_type' fixture parameter
    provided in the test.

    Args:
        request (_pytest.fixtures.SubRequest'): The pytest request fixture

    """
    from ocs_ci.ocs.cluster import is_managed_service_cluster, is_hci_cluster

    cluster_type = get_pytest_fixture_value(request, "cluster_type")
    if not cluster_type:
        log.info(
            "The cluster type is not provided in the request params. "
            "Continue the test with the current cluster"
        )
        return

    if not (is_managed_service_cluster() or is_hci_cluster()):
        if cluster_type == constants.NON_MS_CLUSTER_TYPE:
            log.info(
                "The cluster is a non-MS cluster. Continue the test with the current cluster"
            )
            return
        else:
            pytest.skip(
                f"The test will not run on a non-MS cluster with the cluster type '{cluster_type}'"
            )

    # If the cluster is an MS cluster
    if not config.is_cluster_type_exist(cluster_type):
        pytest.skip(f"The cluster type '{cluster_type}' does not exist in the run")

    # Switch to the correct cluster type
    log.info(f"Switching to the cluster with the cluster type '{cluster_type}'")
    config.switch_to_cluster_by_cluster_type(cluster_type)


def list_insert_at_position(lst, index, element):
    """
    Insert an element into the list at a specific index
    while shifting all the element one setp right to the index

    """
    return lst[:index] + element + lst[index:]


def get_latest_acm_tag_unreleased(version):
    """
    Get Latest tag for acm unreleased image

     Args:
        version (str): version of acm for getting latest tag

    Returns:
        str: image tag for the specified version

    Raises:
        TagNotFoundException: When the given version is not found


    """
    params = {
        "onlyActiveTags": "true",
        "limit": "100",
    }
    response = requests.get(
        "https://quay.io/api/v1/repository/acm-d/acm-custom-registry/tag/",
        params=params,
    )
    responce_data = response.json()
    for data in responce_data["tags"]:
        if version in data["name"] and "v" not in data["name"]:
            log.info(f"Found Image Tag {data['name']}")
            return data["name"]

    raise TagNotFoundException("Couldn't find given ACM tag!")


def is_emoji(char):
    # Check if a character belongs to the "So" (Symbol, Other) Unicode category
    return unicodedata.category(char) == "So"


def filter_out_emojis(plaintext):
    """
    Filter out emojis from a string

    Args:
        string (str): String to filter out emojis from

    Returns:
        str: Filtered string

    """

    # Create a list of characters that are not emojis
    filtered_chars = [char for char in plaintext if not is_emoji(char)]
    # Join the characters back together to form the filtered string
    filtered_string = "".join(filtered_chars)
    return filtered_string


def remove_ceph_crashes(toolbox_pod):
    """
    Deletes the Ceph crashes

    Args:
        toolbox_pod (obj): Ceph toolbox pod object

    """
    ceph_crash_ids = get_ceph_crashes(toolbox_pod)
    archive_ceph_crashes(toolbox_pod)
    log.info(f"Removing all ceph crashes {ceph_crash_ids}")
    for each_ceph_crash in ceph_crash_ids:
        toolbox_pod.exec_ceph_cmd(f"ceph crash rm {each_ceph_crash}")


def get_ceph_crashes(toolbox_pod):
    """
    Gets all Ceph crashes

    Args:
        toolbox_pod (obj): Ceph toolbox pod object

    Returns:
        list: List of ceph crash ID's

    """
    ceph_crashes = toolbox_pod.exec_ceph_cmd("ceph crash ls")
    return [each_crash["crash_id"] for each_crash in ceph_crashes]


def archive_ceph_crashes(toolbox_pod):
    """
    Archive all Ceph crashes

    Args:
        toolbox_pod (obj): Ceph toolbox pod object

    """
    log.info("Archiving all ceph crashes")
    toolbox_pod.exec_ceph_cmd("ceph crash archive-all")


def ceph_crash_info_display(toolbox_pod):
    """
    Displays ceph crash information

    Args:
        toolbox_pod (obj): Ceph toolbox pod object

    """
    ceph_crashes = get_ceph_crashes(toolbox_pod)
    for each_crash in ceph_crashes:
        log.error(f"ceph crash: {each_crash}")
        crash_info = toolbox_pod.exec_ceph_cmd(
            f"ceph crash info {each_crash}", out_yaml_format=False
        )
        log.error(crash_info)


def add_time_report_to_email(session, soup):
    """
    Takes the time report dictionary and converts it into HTML table
    """
    data = GV.TIMEREPORT_DICT
    sorted_data = dict(
        sorted(data.items(), key=lambda item: item[1].get("total", 0), reverse=True)
    )

    file_loader = FileSystemLoader(constants.HTML_REPORT_TEMPLATE_DIR)
    env = Environment(loader=file_loader)
    table_html_template = env.get_template("test_time_table.html.j2")
    data = list(sorted_data.items())
    table_html = table_html_template.render(sorted_data=data[:5])
    summary_tag = soup.find("h2", string="Summary")
    time_div = soup.new_tag("div")
    table = BeautifulSoup(table_html, "html.parser")
    time_div.append(table)
    summary_tag.insert_after(time_div)


def get_oadp_version(namespace=constants.OADP_NAMESPACE):
    """
    Returns:
        str: returns version string
    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix("oadp-operator", namespace=namespace)
    for csv in csv_list:
        if "oadp-operator" in csv["metadata"]["name"]:
            # extract version string
            return csv["spec"]["version"]


def get_acm_version(namespace=constants.ACM_HUB_NAMESPACE):
    """
    Get ACM version from CSV

    Returns:
        str: returns version string
    """
    # Importing here to avoid circular dependency
    from ocs_ci.ocs.resources.csv import get_csvs_start_with_prefix

    csv_list = get_csvs_start_with_prefix(
        "advanced-cluster-management", namespace=namespace
    )
    for csv in csv_list:
        if "advanced-cluster-management" in csv["metadata"]["name"]:
            # extract version string
            return csv["spec"]["version"]


def is_cluster_y_version_upgraded():
    """
    Checks whether cluster is upgraded or not

    Returns:
        bool: True if cluster is upgraded from previous versions

    """
    # importing here to avoid circular import
    from ocs_ci.ocs.resources.ocs import get_ocs_csv

    is_upgraded = False
    ocs_csv = get_ocs_csv()
    csv_info = ocs_csv.get()
    prev_version = csv_info.get("spec").get("replaces", "")
    csv_version = csv_info.get("spec").get("version", "")
    log.debug(f"Replaces version: {prev_version}")
    log.debug(f"csv_version: {csv_version}")
    if not prev_version:
        log.info("No previous version detected in cluster")
        return is_upgraded
    prev_version_num = prev_version.split("ocs-operator.")[1].lstrip("v")
    if version_module.get_semantic_version(
        csv_version, only_major_minor=True
    ) > version_module.get_semantic_version(prev_version_num, only_major_minor=True):
        is_upgraded = True
    return is_upgraded


def exec_nb_db_query(query):
    """
    Send a psql query to the Noobaa DB

    Example usage:
        exec_nb_db_query("SELECT data ->> 'key' FROM objectmds;")

    Args:
        query (str): The query to send

    Returns:
        list of str: The query result rows

    """
    # importing here to avoid circular imports
    from ocs_ci.ocs.resources import pod

    nb_db_pod = pod.Pod(
        **pod.get_pods_having_label(
            label=constants.NOOBAA_DB_LABEL_47_AND_ABOVE,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )[0]
    )

    response = nb_db_pod.exec_cmd_on_pod(
        command=f'psql -U postgres -d nbcore -c "{query}"',
        out_yaml_format=False,
    )

    output = response.strip().split("\n")

    if len(output) >= 3:
        # Remove the two header rows and the summary row
        output = output[2:-1]

    return output


def get_role_arn_from_sub():
    """
    Get the RoleARN from the OCS subscription

    Returns:
        role_arn (str): Role ARN used for ODF deployment

    Raises:
        ClusterNotInSTSModeException (Exception) if cluster
        not in STS mode

    """
    from ocs_ci.ocs.ocp import OCP

    if config.DEPLOYMENT.get("sts_enabled"):
        role_arn = None
        odf_sub = OCP(
            kind=constants.SUBSCRIPTION,
            resource_name=constants.ODF_SUBSCRIPTION,
            namespace=constants.OPENSHIFT_STORAGE_NAMESPACE,
        )
        for item in odf_sub.get()["spec"]["config"]["env"]:
            if item["name"] == "ROLEARN":
                role_arn = item["value"]
                break
        return role_arn
    else:
        raise ClusterNotInSTSModeException


def get_glibc_version():
    """
    Gets the GLIBC version.

    Returns:
        str: GLIBC version

    """
    cmd = "ldd --version ldd"
    res = exec_cmd(cmd)
    out = res.stdout.decode("utf-8")
    version_match = re.search(r"ldd \((?:Ubuntu GLIBC|GNU libc)\D*(\d+\.\d+)", out)
    if version_match:
        return version_match.group(1)
    else:
        log.warning("GLIBC version number not found")


def get_architecture_host():
    """
    Gets the architecture of host

    Returns:
        str: Host architecture

    """
    return os.uname().machine


def get_latest_release_version():
    """
    Fetch the latest supported release version of OpenShift from its official mirror site.

    Returns:
        str: The latest release version. Example: As of 22 May 2024 the function returns string "4.15.14"

    """
    cmd = (
        "curl -sL https://mirror.openshift.com/pub/openshift-v4/clients/ocp/latest/release.txt | "
        "awk '/^Name:/ {print $2}'"
    )
    try:
        return exec_cmd(cmd, shell=True).stdout.decode("utf-8").strip()
    except CommandFailed:
        return


def sum_of_two_storage_sizes(storage_size1, storage_size2, convert_size=1024):
    """
    Calculate the sum of two storage sizes given as strings.
    Valid units: "Mi", "Gi", "Ti", "MB", "GB", "TB".

    Args:
        storage_size1 (str): The first storage size, e.g., "800Mi", "100Gi", "2Ti".
        storage_size2 (str): The second storage size, e.g., "700Mi", "500Gi", "300Gi".
        convert_size (int): Set convert by 1000 or 1024. The default value is 1024.

    Returns:
        str: The sum of the two storage sizes as a string, e.g., "1500Mi", "600Gi", "2300Gi".

    Raises:
        ValueError: If the units of the storage sizes are not match the Valid units

    """
    valid_units = {"Mi", "Gi", "Ti", "MB", "GB", "TB"}
    unit1 = storage_size1[-2:]
    unit2 = storage_size2[-2:]
    if unit1 not in valid_units or unit2 not in valid_units:
        raise ValueError(f"Storage sizes must have valid units: {valid_units}")

    storage_size1 = storage_size1.replace("B", "i")
    storage_size2 = storage_size2.replace("B", "i")

    if "Mi" in f"{storage_size1}{storage_size2}":
        unit, units_to_convert = "Mi", "MB"
    elif "Gi" in f"{storage_size1}{storage_size2}":
        unit, units_to_convert = "Gi", "GB"
    else:
        unit, units_to_convert = "Ti", "TB"

    size1 = convert_device_size(storage_size1, units_to_convert, convert_size)
    size2 = convert_device_size(storage_size2, units_to_convert, convert_size)
    size = size1 + size2
    new_storage_size = f"{size}{unit}"
    return new_storage_size
