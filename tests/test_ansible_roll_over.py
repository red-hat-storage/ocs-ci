import datetime
import logging
import re

from ceph.ceph import NodeVolume

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    """
    Rolls updates over existing ceph-ansible deployment
    Args:
        ceph_cluster (ceph.ceph.Ceph): ceph cluster object
        **kw(dict):
            config sample:
                config:
                  ansi_config:
                      ceph_test: True
                      ceph_origin: distro
                      ceph_stable_release: luminous
                      ceph_repository: rhcs
                      osd_scenario: collocated
                      osd_auto_discovery: False
                      journal_size: 1024
                      ceph_stable: True
                      ceph_stable_rh_storage: True
                      public_network: 172.16.0.0/12
                      fetch_directory: ~/fetch
                      copy_admin_key: true
                      ceph_conf_overrides:
                          global:
                            osd_pool_default_pg_num: 64
                            osd_default_pool_size: 2
                            osd_pool_default_pgp_num: 64
                            mon_max_pg_per_osd: 1024
                          mon:
                            mon_allow_pool_delete: true
                      cephfs_pools:
                        - name: "cephfs_data"
                          pgs: "8"
                        - name: "cephfs_metadata"
                          pgs: "8"
                  add:
                      - node:
                          node-name: .*node15.*
                          demon:
                              - mon
    Returns:
        int: non-zero on failure, zero on pass
    """
    log.info("Running test")
    log.info("Running ceph ansible test")
    config = kw.get('config')
    bluestore = config.get('bluestore')
    k_and_m = config.get('ec-pool-k-m')
    hotfix_repo = config.get('hotfix_repo')
    test_data = kw.get('test_data')

    ubuntu_repo = config.get('ubuntu_repo', None)
    base_url = config.get('base_url', None)
    installer_url = config.get('installer_url', None)
    ceph_cluster.ansible_config = config['ansi_config']

    ceph_cluster.use_cdn = config.get('use_cdn')
    build = config.get('build', config.get('rhbuild'))

    if config.get('add'):
        for added_node in config.get('add'):
            added_node = added_node.get('node')
            node_name = added_node.get('node-name')
            demon_list = added_node.get('demon')
            osds_required = [demon for demon in demon_list if demon == 'osd']
            short_name_list = [ceph_node.shortname for ceph_node in ceph_cluster.get_nodes()]
            matcher = re.compile(node_name)
            matched_short_names = filter(matcher.match, short_name_list)
            if len(matched_short_names) > 1:
                raise RuntimeError('Multiple nodes are matching node-name {node_name}: \n{matched_short_names}'.format(
                    node_name=node_name, matched_short_names=matched_short_names))
            if len(matched_short_names) == 0:
                raise RuntimeError('No match for {node_name}'.format(node_name=node_name))
            for ceph_node in ceph_cluster:
                if ceph_node.shortname == matched_short_names[0]:
                    matched_ceph_node = ceph_node
                    break
            free_volumes = matched_ceph_node.get_free_volumes()
            if len(osds_required) > len(free_volumes):
                raise RuntimeError(
                    'Insufficient volumes on the {node_name} node. Rquired: {required} - Found: {found}'.format(
                        node_name=matched_ceph_node.shotrtname, required=len(osds_required),
                        found=len(free_volumes)))
            log.debug('osds_required: {}'.format(osds_required))
            log.debug('matched_ceph_node.shortname: {}'.format(matched_ceph_node.shortname))
            for osd in osds_required:
                free_volumes.pop().status = NodeVolume.ALLOCATED
            for demon in demon_list:
                if len(matched_ceph_node.get_ceph_objects(demon)) == 0:
                    matched_ceph_node.create_ceph_object(demon)

    test_data['install_version'] = build

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    ansible_dir = ceph_installer.ansible_dir

    ceph_cluster.setup_ceph_firewall()

    ceph_cluster.setup_packages(base_url, hotfix_repo, installer_url, ubuntu_repo)

    ceph_installer.install_ceph_ansible(build)

    hosts_file = ceph_cluster.generate_ansible_inventory(bluestore)
    ceph_installer.write_inventory_file(hosts_file)

    if config.get('docker-insecure-registry'):
        ceph_cluster.setup_insecure_registry()

    # use the provided sample file as main site.yml
    ceph_installer.setup_ansible_site_yml(ceph_cluster.containerized)

    ceph_cluster.distribute_all_yml()

    # add iscsi setting if it is necessary
    if test_data.get("luns_setting", None) and test_data.get("initiator_setting", None):
        ceph_installer.add_iscsi_settings(test_data)

    log.info("Ceph versions " + ceph_installer.get_installed_ceph_versions())

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
