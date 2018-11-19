import logging

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """

    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster
    """
    log.info("Running test")
    log.info("Running rgw tests v2")
    rgw_ceph_object = ceph_cluster.get_ceph_object('rgw')
    git_url = 'http://gitlab.cee.redhat.com/ceph/ceph-qe-scripts.git'
    branch = ' -b master'
    git_clone = 'sudo git clone ' + git_url + branch
    rgw_node = rgw_ceph_object.node
    # cleanup any existing stale test dir
    log.info('flushing iptables')
    rgw_node.exec_command(cmd='sudo iptables -F')
    test_folder = 'rgw-tests'
    test_folder_path = '~/{test_folder}'.format(test_folder=test_folder)
    rgw_node.exec_command(cmd='sudo rm -rf ' + test_folder)
    rgw_node.exec_command(cmd='sudo mkdir ' + test_folder)
    rgw_node.exec_command(cmd='cd ' + test_folder + ' ; ' + git_clone)
    if ceph_cluster.containerized:
        test_folder_path = '/{test_folder}'.format(test_folder=test_folder)
        rgw_ceph_object.exec_command(cmd='sudo rm -rf ' + test_folder)
        rgw_ceph_object.exec_command(cmd='sudo mkdir ' + test_folder)
        rgw_node.exec_command(
            cmd='sudo docker cp {test_folder}/* {container}:/{test_folder}/'.format(
                container=rgw_ceph_object.container_name,
                test_folder=test_folder))
        rgw_node.exec_command(cmd='curl https://bootstrap.pypa.io/get-pip.py -o ~/get-pip.py')
        rgw_node.exec_command(
            cmd='sudo docker cp ~/get-pip.py {container}:/get-pip.py'.format(container=rgw_ceph_object.container_name))
        rgw_ceph_object.exec_command('python /get-pip.py')
    rgw_ceph_object.exec_command(cmd='sudo pip install boto3 names PyYaml ConfigParser python-swiftclient swiftly')
    config = kw.get('config')
    script_name = config.get('script-name')
    config_file_name = config.get('config-file-name')
    timeout = config.get('timeout', 300)
    out, err = rgw_ceph_object.exec_command(
        cmd='sudo python ' + test_folder_path + '/ceph-qe-scripts/rgw/v2/tests/s3_swift/' + script_name + ' -c ' +
            test_folder + '/ceph-qe-scripts/rgw/v2/tests/s3_swift/configs/' + config_file_name,
        timeout=timeout)
    log.info(out.read())
    log.error(err.read())
    return 0
