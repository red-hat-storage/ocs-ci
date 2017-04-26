import logging

log = logging.getLogger(__name__)

def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    log.info("Running rgw tests")
    rgw_client_nodes = []
    for node in ceph_nodes:
        if node.role == 'rgw':
            rgw_client_nodes.append(node)
    git_url = 'http://gitlab.osas.lab.eng.rdu2.redhat.com/ceph/ceph-qe-scripts.git'
    git_clone = 'git clone -b wip-defaults ' + git_url
    rgw_node = rgw_client_nodes[0]
    # cleanup any existing stale test dir
    test_folder = 'rgw-tests'
    rgw_node.exec_command(cmd='rm -rf ' + test_folder)
    rgw_node.exec_command(cmd='mkdir ' + test_folder)
    rgw_node.exec_command(cmd='cd ' + test_folder + ' ; ' + git_clone)
    rgw_node.exec_command(cmd='sudo pip install boto names PyYaml ConfigParser')
    config = kw.get('config')
    script_name = config.get('script-name')
    timeout = config.get('timeout', 300)
    out, err = rgw_node.exec_command(cmd='sudo python ~/' + test_folder + '/ceph-qe-scripts/rgw/tests/s3/' + script_name,
                                     timeout=timeout)
    log.info(out.read())
    return 0
