import logging

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    log.info("Running rgw tests v2")
    rgw_client_nodes = []
    for node in ceph_nodes:
        if node.role == 'rgw':
            rgw_client_nodes.append(node)
    git_url = 'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git'
    branch = ' -b master'
    git_clone = 'sudo git clone ' + git_url + branch
    rgw_node = rgw_client_nodes[0]
    # cleanup any existing stale test dir
    log.info('flushing iptables')
    rgw_node.exec_command(cmd='sudo iptables -F')
    test_folder = 'rgw-tests'
    rgw_node.exec_command(cmd='sudo rm -rf ' + test_folder)
    rgw_node.exec_command(cmd='sudo mkdir ' + test_folder)
    rgw_node.exec_command(cmd='cd ' + test_folder + ' ; ' + git_clone)
    rgw_node.exec_command(cmd='sudo pip install boto3 names PyYaml ConfigParser python-swiftclient swiftly')
    config = kw.get('config')
    script_name = config.get('script-name')
    config_file_name = config.get('config-file-name')
    timeout = config.get('timeout', 300)
    out, err = rgw_node.exec_command(
        cmd='sudo python ~/' + test_folder + '/ceph-qe-scripts/rgw/v2/tests/s3_swift/' + script_name + ' -c ' +
            test_folder + '/ceph-qe-scripts/rgw/v2/tests/s3_swift/configs/' + config_file_name,
        timeout=timeout)
    log.info(out.read())
    log.info(err.read())
    return 0
