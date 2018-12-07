import getpass
import logging
import os
import random
import smtplib
import time
import traceback
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape
from reportportal_client import ReportPortalServiceAsync

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
            mon_node_ip = out.read().rstrip('\n')
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
        keyring = out.read()
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
            mount_output = out.read()
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
            secret_key = out.read().rstrip('\n')
            mon_node_ip = mon_node_ip.replace(" ", "")
            client.exec_command(
                cmd='sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s' % (
                    mon_node_ip, mounting_dir, client.hostname, secret_key))
            out, err = client.exec_command(cmd='mount')
            mount_output = out.read()
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
        output = out.read()
        output.split()
        if 'Errno 11' in output:
            log.info("File locking achieved, data is not corrupted")
        elif 'locking' in output:
            log.info("File locking achieved, data is not corrupted")
        else:
            log.error("Data is corrupted")

        out, err = client.exec_command(cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" % (mounting_dir))

        md5sum_file_lock.append(out.read())

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
                    print "PIn val not given"
                print out.read()
                print time.time()
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
                print out.read()
                RC.append(rc)
                print time.time()
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


def configure_logger(test_name, run_dir, level=logging.INFO):
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
    cfg = get_cephci_config()['report-portal']

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


def create_unique_test_name(test_name, name_list):
    """
    Creates a unique test name using the actual test name and an increasing integer for each duplicate test name.

    Args:
        test_name: name of the test
        name_list: list of names to compare test name against

    Returns:
        unique name for the test case
    """
    base = "_".join(test_name.split())
    num = 0
    while "{base}_{num}".format(base=base, num=num) in name_list:
        num += 1
    return "{base}_{num}".format(base=base, num=num)


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
        for key, value in cli_config_dict.iteritems():
            custom_config_dict['global'][key] = value

    # combine file and suite configs
    for key, value in custom_config_dict.iteritems():
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
    cfg = get_cephci_config().get('email')
    sender = "cephci@redhat.com"
    recipients = []
    if cfg and cfg.get('address'):
        recipients = [cfg['address']]
    else:
        log.warn("No email address configured in ~/.cephci.yaml. "
                 "Please configure if you would like to receive run result emails.")

    if send_to_cephci:
        recipients.append(sender)

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

        s = smtplib.SMTP('localhost')
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()

        log.info("Results have been emailed to {recipients}".format(recipients=recipients))


def get_cephci_config():
    """
    Receives the data from ~/.cephci.yaml.

    Returns:
        (dict) configuration from ~/.cephci.yaml

    """
    home_dir = os.path.expanduser("~")
    cfg_file = os.path.join(home_dir, ".cephci.yaml")
    try:
        with open(cfg_file, "r") as yml:
            cfg = yaml.load(yml)
    except IOError:
        log.error("Please create ~/.cephci.yaml from the cephci.yaml.template. "
                  "See README for more information.")
        raise
    return cfg
