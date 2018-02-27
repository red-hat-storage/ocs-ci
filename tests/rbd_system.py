import logging

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running rbd tests")
    ceph_nodes = kw.get('ceph_nodes')
    rgw_client_nodes = []
    for node in ceph_nodes:
        if node.role == 'client':
            rgw_client_nodes.append(node)
    git_url = 'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git'
    git_clone = 'git clone ' + git_url
    client_node = rgw_client_nodes[0]
    # cleanup any existing stale test dir
    test_folder = 'rbd-tests'
    client_node.exec_command(cmd='rm -rf ' + test_folder)
    client_node.exec_command(cmd='mkdir ' + test_folder)
    client_node.exec_command(cmd='cd ' + test_folder + ' ; ' + git_clone)
    client_node.exec_command(cmd='sudo pip install boto names PyYaml ConfigParser')
    config = kw.get('config')
    script_name = config.get('test_name')
    timeout = config.get('timeout', 1800)
    command = 'sudo python ~/' + test_folder + '/ceph-qe-scripts/rbd/system/' + script_name
    stdout, stderr = client_node.exec_command(cmd=command, timeout=timeout, check_ec=False)
    output = stdout.read()
    if output:
        log.info(output)
    output = stderr.read()
    if output:
        log.error(output)
    ec = client_node.exit_status
    if ec == 0:
        log.info("{command} completed successfully".format(command=command))
    else:
        log.error("{command} has failed".format(command=command))
    return ec
