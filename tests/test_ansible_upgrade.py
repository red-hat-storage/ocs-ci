import logging

import yaml

from ceph.utils import setup_deb_repos, setup_cdn_repos, setup_deb_cdn_repo, write_docker_daemon_json
from ceph.utils import setup_repos, check_ceph_healthly, get_ceph_versions
from utils.utils import get_latest_container_image_tag

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    log.info("Running ceph ansible test")
    config = kw.get('config')
    test_data = kw.get('test_data')
    prev_install_version = test_data['install_version']
    upgrade_to_version = config.get('build')

    ubuntu_repo = None
    ceph_installer = None
    ceph_mon = None
    base_url = None

    if config.get('ubuntu_repo'):
        ubuntu_repo = config.get('ubuntu_repo')
    if config.get('base_url'):
        base_url = config.get('base_url')
    installer_url = None
    if config.get('installer_url'):
        installer_url = config.get('installer_url')
    if config.get('skip_setup') is True:
        log.info("Skipping setup of ceph cluster")
        return 0
    for node in ceph_nodes:
        if node.role == 'installer':
            ceph_installer = node
        elif node.role == 'mon':
            ceph_mon = node

    for ceph in ceph_nodes:
        if config.get('use_cdn'):
            log.info("Using the cdn repo for the test")
            if ceph.pkg_type == 'deb':
                if ceph.role == 'installer':
                    setup_deb_cdn_repo(ceph, config.get('build'))
            else:
                setup_cdn_repos(ceph_nodes, build=config.get('build'))
        else:
            log.info("Using nightly repos for the test")
            if ceph.pkg_type == 'deb':
                setup_deb_repos(ceph, ubuntu_repo)
            else:
                setup_repos(ceph, base_url, installer_url)

        log.info("Updating metadata")
        if ceph.pkg_type == 'rpm':
            ceph.exec_command(sudo=True, cmd='yum update metadata')

    # Backup existing hosts file and ansible config
    ansible_dir = '/usr/share/ceph-ansible'
    ceph_installer.exec_command(cmd='cp {}/hosts /tmp/hosts'.format(ansible_dir))
    ceph_installer.exec_command(cmd='cp {}/group_vars/all.yml /tmp/all.yml'.format(ansible_dir))

    # Update ceph-ansible
    if ceph_installer.pkg_type == 'deb':
        ceph_installer.exec_command(sudo=True, cmd='apt-get install -y ceph-ansible')
    else:
        ceph_installer.exec_command(sudo=True, cmd='yum update -y ceph-ansible')

    # Restore hosts file
    ceph_installer.exec_command(sudo=True, cmd='cp /tmp/hosts {}/hosts'.format(ansible_dir))

    # If upgrading from version 2 update hosts file with mgrs
    if prev_install_version.startswith('2') and upgrade_to_version.startswith('3'):
        log.info("Adding mons as mgrs in hosts file")
        mon_nodes = [node for node in ceph_nodes if node.role == 'mon']
        mgr_block = '[mgrs]\n'
        for node in mon_nodes:
            mgr_block += node.shortname + ' monitor_interface=' + node.eth_interface + '\n'

        host_file = ceph_installer.write_file(sudo=True, file_name='{}/hosts'.format(ansible_dir), file_mode='a')
        host_file.write(mgr_block)
        host_file.flush()

        log.info(mgr_block)

    if config.get('ansi_config').get('fetch_directory') is None:
        config['ansi_config']['fetch_directory'] = '~/fetch/'

    containerized = config.get('ansi_config').get('containerized_deployment')

    if containerized and config.get('ansi_config').get('docker-insecure-registry'):
        # set the docker image tag
        config['ansi_config']['ceph_docker_image_tag'] = get_latest_container_image_tag(config['build'])
    log.info("gvar: {}".format(config.get('ansi_config')))
    gvar = yaml.dump(config.get('ansi_config'), default_flow_style=False)

    host_file = ceph_installer.write_file(sudo=True, file_name='{}/hosts'.format(ansible_dir), file_mode='a')
    log.info("Hosts file {}".format(host_file))

    # Create all.yml
    log.info("global vars {}".format(gvar))
    gvars_file = ceph_installer.write_file(
        sudo=True, file_name='{}/group_vars/all.yml'.format(ansible_dir), file_mode='w')
    gvars_file.write(gvar)
    gvars_file.flush()

    pre_upgrade_versions = get_ceph_versions(ceph_nodes, containerized)

    # retrieve container count if containerized
    if containerized:
        pre_upgrade_container_counts = {}
        for node in ceph_nodes:
            if node.role != 'installer':
                out, rc = node.exec_command(sudo=True, cmd='docker ps | grep $(hostname) | wc -l')
                count = out.read().rstrip()
                log.info("{} has {} containers running".format(node.shortname, count))
                pre_upgrade_container_counts.update({node.shortname: count})

    if containerized and config.get('docker-insecure-registry') and \
            config.get('ansi_config').get('ceph_docker_registry'):
        insecure_registry = '{{"insecure-registries" : ["{registry}"]}}'.format(
            registry=config.get('ansi_config').get('ceph_docker_registry'))
        log.warn('Adding insecure registry:\n{registry}'.format(registry=insecure_registry))
        for node in ceph_nodes:
            if node.role != 'installer':
                write_docker_daemon_json(insecure_registry, node)
                log.info("Restarting docker on {node}".format(node=node.shortname))
                node.exec_command(sudo=True, cmd='systemctl restart docker')

    jewel_minor_update = upgrade_to_version.startswith('2')

    # copy rolling update from infrastructure playbook
    ceph_installer.exec_command(
        sudo=True, cmd='cd {} ; cp infrastructure-playbooks/rolling_update.yml .'.format(ansible_dir))
    cmd = 'cd {};' \
          'ANSIBLE_STDOUT_CALLBACK=debug;' \
          'ansible-playbook -e ireallymeanit=yes -vv -i hosts rolling_update.yml'.format(
              ansible_dir)
    if jewel_minor_update:
        cmd += " -e jewel_minor_update=true"
        log.info("Upgrade is jewel_minor_update, cmd: {cmd}".format(cmd=cmd))
    out, rc = ceph_installer.exec_command(cmd=cmd, long_running=True)

    if rc != 0:
        log.error("Failed during upgrade (rc = {})".format(rc))
        return rc

    # set build to new version
    if config.get('build'):
        log.info("Setting install_version to {build}".format(build=config['build']))
        test_data['install_version'] = config['build']

    # check if all mon's and osd's are in correct state
    num_osds = test_data['ceph-ansible']['num-osds']
    num_mons = test_data['ceph-ansible']['num-mons']

    post_upgrade_versions = get_ceph_versions(ceph_nodes, containerized)
    for name, version in post_upgrade_versions.iteritems():
        if 'installer' not in name and pre_upgrade_versions[name] == version:
            log.error("Pre upgrade version matches post upgrade version")
            log.error("{}: {} matches".format(name, version))
            return 1

    # retrieve container count if containerized
    if containerized:
        post_upgrade_container_counts = {}
        for node in ceph_nodes:
            if node.role != 'installer':
                out, rc = node.exec_command(sudo=True, cmd='docker ps | grep $(hostname) | wc -l')
                count = out.read().rstrip()
                log.info("{} has {} container(s) running".format(node.shortname, count))
                post_upgrade_container_counts.update({node.shortname: count})

        log.info("Post upgrade container counts: {}".format(post_upgrade_container_counts))
        # compare container count to pre-upgrade container count
        for node, count in post_upgrade_container_counts.iteritems():
            if pre_upgrade_container_counts[node] != count:
                log.error("Mismatched container count post upgrade")
                return 1

    mon_container = None
    if config.get('ansi_config').get('containerized_deployment') is True:
        mon_container = 'ceph-mon-{host}'.format(host=ceph_mon.hostname)

    return check_ceph_healthly(ceph_mon, num_osds, num_mons, mon_container)
