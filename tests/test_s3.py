import json
import logging
import time

import os

from ceph.ceph import CommandFailed
from ceph.utils import open_firewall_port

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running s3-tests")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    test_data = kw.get('test_data')
    client_node = None
    rgw_node = None
    for node in ceph_nodes:
        if node.role == 'client':
            client_node = node
            break

    for node in ceph_nodes:
        if node.role == 'rgw':
            rgw_node = node
            break

    if client_node:
        if test_data['install_version'].startswith('2'):
            client_node.exec_command(sudo=True, cmd='yum install -y ceph-radosgw')
        setup_s3_tests(client_node, rgw_node, config)
        exit_status = execute_s3_tests(client_node)
        cleanup(client_node)

        log.info("Returning status code of {}".format(exit_status))
        return exit_status

    else:
        log.warn("No client node in cluster, skipping s3 tests.")
        return 0


def setup_s3_tests(client_node, rgw_node, config):
    """
    Performs initial setup and configuration for s3 tests on client node.

    Args:
        client_node: node to setup for s3 tests
        rgw_node: node running rados gateway
        config: test configuration

    Returns:
        None

    """
    log.info("Removing existing s3-tests directory if it exists")
    client_node.exec_command(cmd="if test -d s3-tests; then rm -r s3-tests; fi")

    log.info("Cloning s3-tests repository")
    branch = config.get('branch', 'ceph-luminous')
    repo_url = "https://github.com/ceph/s3-tests.git"
    client_node.exec_command(cmd="git clone -b {branch} {repo_url}".format(branch=branch, repo_url=repo_url))

    log.info("Running bootstrap")
    client_node.exec_command(cmd="cd s3-tests; ./bootstrap")

    main_info = create_s3_user(client_node, 'main-user')
    alt_info = create_s3_user(client_node, 'alt-user', email=True)
    tenant_info = create_s3_user(client_node, 'tenant', email=True)

    log.info("Creating configuration file")
    port = 8080
    s3_config = '''
[DEFAULT]
host = {host}
port = {port}
is_secure = no

[fixtures]
bucket prefix = cephuser-{random}-

[s3 main]
user_id = {main_id}
display_name = {main_name}
access_key = {main_access_key}
secret_key = {main_secret_key}

[s3 alt]
user_id = {alt_id}
display_name = {alt_name}
email = {alt_email}
access_key = {alt_access_key}
secret_key = {alt_secret_key}

[s3 tenant]
user_id = {tenant_id}
display_name = {tenant_name}
email = {tenant_email}
access_key = {tenant_access_key}
secret_key = {tenant_secret_key}
    '''.format(host=rgw_node.shortname,
               port=port,
               random='{random}',
               main_id=main_info['user_id'],
               main_name=main_info['display_name'],
               main_access_key=main_info['keys'][0]['access_key'],
               main_secret_key=main_info['keys'][0]['secret_key'],
               alt_id=alt_info['user_id'],
               alt_name=alt_info['display_name'],
               alt_email=alt_info['email'],
               alt_access_key=alt_info['keys'][0]['access_key'],
               alt_secret_key=alt_info['keys'][0]['secret_key'],
               tenant_id=tenant_info['user_id'],
               tenant_name=tenant_info['display_name'],
               tenant_email=tenant_info['email'],
               tenant_access_key=tenant_info['keys'][0]['access_key'],
               tenant_secret_key=tenant_info['keys'][0]['secret_key'])

    log.info("s3-tests configuration: {s3_config}".format(s3_config=s3_config))
    config_file = client_node.write_file(file_name='s3-tests/config.yaml', file_mode='w')
    config_file.write(s3_config)
    config_file.flush()

    log.info("Opening port on rgw node")
    open_firewall_port(rgw_node, port=port, protocol='tcp')


def create_s3_user(client_node, display_name, email=False):
    """
    Create an s3 user with the given display_name. The uid will be generated.

    Args:
        client_node: node in the cluster to create the user on
        display_name: display name for the new user
        email: (optional) generate fake email address for user

    Returns:
        user_info dict

    """
    uid = os.urandom(32).encode('hex')
    log.info("Creating user: {display_name}".format(display_name=display_name))
    cmd = "radosgw-admin user create --uid={uid} --display_name={display_name}".format(
        uid=uid, display_name=display_name)
    if email:
        cmd += " --email={email}@foo.bar".format(email=uid)

    out, err = client_node.exec_command(sudo=True, cmd=cmd)
    user_info = json.loads(out.read())

    return user_info


def execute_s3_tests(client_node):
    """
    Execute the s3-tests

    Args:
        client_node: node to execute tests from

    Returns:
        0 - Success
        1 - Failure
    """
    log.info("Executing s3-tests")
    try:
        out, err = client_node.exec_command(
            cmd="cd s3-tests; S3TEST_CONF=config.yaml ./virtualenv/bin/nosetests -v -a '!fails_on_rgw,!lifecycle'",
            timeout=3600)
        log.info(out.read())
        log.info(err.read())
        return 0
    except CommandFailed as e:
        log.warn("Received CommandFailed")
        log.warn(e.message)
        time.sleep(30)
        return 1


def cleanup(client_node):
    """
    Cleans up after running s3-tests
    Args:
        client_node: node to cleanup

    Returns:
        None
    """
    log.info("Removing s3-tests directory")
    client_node.exec_command(cmd="rm -r s3-tests")
