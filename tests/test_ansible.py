import datetime
import logging

from ceph.utils import get_public_network

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    """
    Runs ceph-ansible deployment
    Args:
        ceph_cluster (ceph.ceph.Ceph): Ceph cluster object
    """
    log.info("Running test")
    log.info("Running ceph ansible test")
    config = kw.get('config')
    filestore = config.get('filestore', False)
    k_and_m = config.get('ec-pool-k-m')
    hotfix_repo = config.get('hotfix_repo')
    test_data = kw.get('test_data')

    ubuntu_repo = config.get('ubuntu_repo', None)
    base_url = config.get('base_url', None)
    installer_url = config.get('installer_url', None)
    mixed_lvm_configs = config.get('is_mixed_lvm_configs', None)
    device_to_add = config.get('device', None)
    config['ansi_config']['public_network'] = get_public_network()

    ceph_cluster.ansible_config = config['ansi_config']
    ceph_cluster.custom_config = test_data.get('custom-config')
    ceph_cluster.custom_config_file = test_data.get('custom-config-file')

    ceph_cluster.use_cdn = config.get('use_cdn')
    build = config.get('build', config.get('rhbuild'))
    ceph_cluster.rhcs_version = build

    if config.get('skip_setup') is True:
        log.info("Skipping setup of ceph cluster")
        return 0

    test_data['install_version'] = build

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    ansible_dir = ceph_installer.ansible_dir

    ceph_cluster.setup_ceph_firewall()

    ceph_cluster.setup_ssh_keys()

    ceph_cluster.setup_packages(base_url, hotfix_repo, installer_url, ubuntu_repo)

    ceph_installer.install_ceph_ansible(build)
    hosts_file = ceph_cluster.generate_ansible_inventory(device_to_add, mixed_lvm_configs, filestore)
    ceph_installer.write_inventory_file(hosts_file)

    if config.get('docker-insecure-registry'):
        ceph_cluster.setup_insecure_registry()

    # use the provided sample file as main site.yml
    ceph_installer.setup_ansible_site_yml(ceph_cluster.containerized)

    ceph_cluster.distribute_all_yml()

    # add iscsi setting if it is necessary
    if test_data.get("luns_setting", None) and test_data.get("initiator_setting", None):
        ceph_installer.add_iscsi_settings(test_data)

    log.info("Ceph ansible version " + ceph_installer.get_installed_ceph_versions())

    out, rc = ceph_installer.exec_command(
        cmd='cd {} ; ANSIBLE_STDOUT_CALLBACK=debug; ansible-playbook -vv -i hosts site.yml'.format(ansible_dir),
        long_running=True)

    # manually handle client creation in a containerized deployment (temporary)
    if ceph_cluster.containerized:
        for node in ceph_cluster.get_ceph_objects('client'):
            log.info("Manually installing client node")
            node.exec_command(sudo=True, cmd="yum install -y ceph-common")

    if rc != 0:
        log.error("Failed during deployment")
        return rc

    # check if all osd's are up and in
    timeout = 300
    if config.get('timeout'):
        timeout = datetime.timedelta(seconds=config.get('timeout'))
    # add test_data for later use by upgrade test etc
    num_osds = ceph_cluster.ceph_demon_stat['osd']
    num_mons = ceph_cluster.ceph_demon_stat['mon']
    test_data['ceph-ansible'] = {'num-osds': num_osds, 'num-mons': num_mons, 'rhbuild': build}

    # create rbd pool used by tests/workunits
    ceph_cluster.create_rbd_pool(k_and_m)

    if ceph_cluster.check_health(timeout=timeout) != 0:
        return 1
    return rc
