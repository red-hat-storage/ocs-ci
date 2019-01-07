import logging

import yaml

from ceph.ceph import RolesContainer
from ceph.utils import get_ceph_versions
from ceph.utils import write_docker_daemon_json
from utility.utils import get_latest_container_image_tag

log = logging.getLogger(__name__)


def run(ceph_cluster, **kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    log.info("Running ceph ansible test")
    config = kw.get('config')
    test_data = kw.get('test_data')
    prev_install_version = test_data['install_version']
    skip_version_compare = config.get('skip_version_compare')
    containerized = config.get('ansi_config').get('containerized_deployment')
    build = config.get('build', config.get('rhbuild'))
    log.info("Build for upgrade: {build}".format(build=build))

    ubuntu_repo = config.get('ubuntu_repo')
    hotfix_repo = config.get('hotfix_repo')
    base_url = config.get('base_url')
    installer_url = config.get('installer_url')
    ceph_cluster.ansible_config = config['ansi_config']
    ceph_cluster.custom_config = test_data.get('custom-config')
    ceph_cluster.custom_config_file = test_data.get('custom-config-file')
    ceph_cluster.use_cdn = config.get('use_cdn')

    ceph_installer = ceph_cluster.get_ceph_object('installer')
    ansible_dir = '/usr/share/ceph-ansible'

    if config.get('skip_setup') is True:
        log.info("Skipping setup of ceph cluster")
        return 0

    # set pre-upgrade install version
    test_data['install_version'] = build
    log.info("Previous install version: {}".format(prev_install_version))

    # retrieve pre-upgrade versions and initialize container counts
    pre_upgrade_versions = get_ceph_versions(ceph_cluster.get_nodes(), containerized)
    pre_upgrade_container_counts = {}

    # setup packages based on build
    ceph_cluster.setup_packages(base_url, hotfix_repo, installer_url, ubuntu_repo, build)

    # backup existing hosts file and ansible config
    ceph_installer.exec_command(cmd='cp {}/hosts /tmp/hosts'.format(ansible_dir))
    ceph_installer.exec_command(cmd='cp {}/group_vars/all.yml /tmp/all.yml'.format(ansible_dir))

    # update ceph-ansible
    ceph_installer.install_ceph_ansible(build, upgrade=True)

    # restore hosts file
    ceph_installer.exec_command(sudo=True, cmd='cp /tmp/hosts {}/hosts'.format(ansible_dir))

    # If upgrading from version 2 update hosts file with mgrs
    if prev_install_version.startswith('2') and build.startswith('3'):
        collocate_mons_with_mgrs(ceph_cluster, ansible_dir)

    # configure fetch directory path
    if config.get('ansi_config').get('fetch_directory') is None:
        config['ansi_config']['fetch_directory'] = '~/fetch/'

    # set the docker image tag if necessary
    if containerized and config.get('ansi_config').get('docker-insecure-registry'):
        config['ansi_config']['ceph_docker_image_tag'] = get_latest_container_image_tag(build)
    log.info("gvar: {}".format(config.get('ansi_config')))
    gvar = yaml.dump(config.get('ansi_config'), default_flow_style=False)

    # create all.yml
    log.info("global vars {}".format(gvar))
    gvars_file = ceph_installer.write_file(
        sudo=True, file_name='{}/group_vars/all.yml'.format(ansible_dir), file_mode='w')
    gvars_file.write(gvar)
    gvars_file.flush()

    # retrieve container count if containerized
    if containerized:
        pre_upgrade_container_counts = get_container_counts(ceph_cluster)

    # configure insecure registry if necessary
    if containerized and config.get('docker-insecure-registry') and \
            config.get('ansi_config').get('ceph_docker_registry'):
        registry = config.get('ansi_config').get('ceph_docker_registry')
        configure_insecure_registry(ceph_cluster, registry)

    # copy rolling update from infrastructure playbook
    jewel_minor_update = build.startswith('2')
    ceph_installer.exec_command(
        sudo=True, cmd='cd {} ; cp infrastructure-playbooks/rolling_update.yml .'.format(ansible_dir))
    cmd = 'cd {};' \
          'ANSIBLE_STDOUT_CALLBACK=debug;' \
          'ansible-playbook -e ireallymeanit=yes -vv -i hosts rolling_update.yml'.format(ansible_dir)
    if jewel_minor_update:
        cmd += " -e jewel_minor_update=true"
        log.info("Upgrade is jewel_minor_update, cmd: {cmd}".format(cmd=cmd))
    out, rc = ceph_installer.exec_command(cmd=cmd, long_running=True)

    if rc != 0:
        log.error("Failed during upgrade (rc = {})".format(rc))
        return rc

    # set build to new version
    log.info("Setting install_version to {build}".format(build=build))
    test_data['install_version'] = build
    ceph_cluster.rhcs_version = build

    # check if all mon's and osd's are in correct state
    num_osds = ceph_cluster.ceph_demon_stat['osd']
    num_mons = ceph_cluster.ceph_demon_stat['mon']
    test_data['ceph-ansible'] = {'num-osds': num_osds, 'num-mons': num_mons, 'rhbuild': build}

    # compare pre and post upgrade versions
    if skip_version_compare:
        log.warn("Skipping version comparison.")
    else:
        if not jewel_minor_update:
            post_upgrade_versions = get_ceph_versions(ceph_nodes, containerized)
            version_compare_fail = compare_ceph_versions(pre_upgrade_versions, post_upgrade_versions)
            if version_compare_fail:
                return version_compare_fail

    # compare pre and post upgrade container counts
    if containerized:
        post_upgrade_container_counts = get_container_counts(ceph_cluster)
        container_count_fail = compare_container_counts(pre_upgrade_container_counts,
                                                        post_upgrade_container_counts,
                                                        prev_install_version)
        if container_count_fail:
            return container_count_fail

    return ceph_cluster.check_health(timeout=config.get('timeout', 300))


def compare_ceph_versions(pre_upgrade_versions, post_upgrade_versions):
    """
    Compare pre-upgrade and post-upgrade ceph versions on all non-installer nodes.

    Args:
        pre_upgrade_versions(dict): pre-upgrade ceph versions.
        post_upgrade_versions(dict): post-upgrade ceph versions.

    Returns: 1 if any non-installer version is the same post-upgrade, 0 if versions change.

    """
    for name, version in pre_upgrade_versions.items():
        if 'installer' not in name and post_upgrade_versions[name] == version:
            log.error("Pre upgrade version matches post upgrade version")
            log.error("{}: {} matches".format(name, version))
            return 1
    return 0


def get_container_counts(ceph_cluster):
    """
    Get container counts on all non-installer nodes in the cluster.

    Args:
        ceph_cluster(ceph.ceph.Ceph): ceph cluster to check container counts on.

    Returns:
        dict: container counts for the cluster.

    """
    container_counts = {}
    for node in ceph_cluster.get_nodes(ignore="installer"):
        out, rc = node.exec_command(sudo=True, cmd='docker ps | grep $(hostname) | wc -l')
        count = int(out.read().rstrip())
        log.info("{} has {} containers running".format(node.shortname, count))
        container_counts.update({node.shortname: count})
    return container_counts


def compare_container_counts(pre_upgrade_counts, post_upgrade_counts, prev_install_version):
    """
    Compare pre-upgrade and post-upgrade container counts.

    Args:
        pre_upgrade_counts: pre-upgrade container counts.
        post_upgrade_counts: post-upgrade container counts.
        prev_install_version: ceph version pre-upgrade containers were running.
            Skip comparison if this is a jewel version.

    Returns: 1 if a container count mismatch exists, 0 if counts are correct.

    """
    log.info("Pre upgrade container counts: {}".format(pre_upgrade_counts))
    log.info("Post upgrade container counts: {}".format(post_upgrade_counts))

    for node, count in post_upgrade_counts.items():
        if prev_install_version.startswith('2'):
            # subtract 1 since mgr containers are now collocated on mons
            if '-mon' in node:
                count -= 1
        if pre_upgrade_counts[node] != count:
            log.error("Mismatched container count post upgrade")
            return 1
    return 0


def configure_insecure_registry(ceph_cluster, registry):
    """
    Configure the insecure registry for nightly docker images.

    Args:
        ceph_cluster(ceph.ceph.Ceph): cluster to configure insecure registry on.
        registry(str): registry to configure.

    Returns: None

    """
    insecure_registry = '{{"insecure-registries" : ["{registry}"]}}'.format(registry=registry)
    log.warn('Adding insecure registry:\n{registry}'.format(registry=insecure_registry))
    role_list = ["installer"]
    if ceph_cluster.rhcs_version < '3':
        role_list.append('mgr')
    ignored_roles = RolesContainer(role_list)
    log.info("Roles ignored for insecure registry configuration: {roles}".format(roles=ignored_roles))
    for node in ceph_cluster.get_nodes(ignore=ignored_roles):
        write_docker_daemon_json(insecure_registry, node)
        log.info("Restarting docker on {node}".format(node=node.shortname))
        node.exec_command(sudo=True, cmd='systemctl restart docker')


def collocate_mons_with_mgrs(ceph_cluster, ansible_dir):
    """
    Configure the hosts file to reflect that mon nodes will be collocated with mgr daemons.

    Args:
        ceph_cluster: cluster to configure mon nodes on.
        ansible_dir: directory of ceph-ansible installation.

    Returns: None

    """
    log.info("Adding mons as mgrs in hosts file")
    mon_nodes = [node for node in ceph_cluster.get_nodes(role="mon")]
    ceph_installer = ceph_cluster.get_nodes(role="installer")[0]
    mgr_block = '\n[mgrs]\n'
    for node in mon_nodes:
        mgr_block += node.shortname + ' monitor_interface=' + node.eth_interface + '\n'

    host_file = ceph_installer.write_file(sudo=True, file_name='{}/hosts'.format(ansible_dir), file_mode='a')
    host_file.write(mgr_block)
    host_file.flush()

    host_file = ceph_installer.write_file(sudo=True, file_name='{}/hosts'.format(ansible_dir), file_mode='r')
    host_contents = ""
    with host_file:
        for line in host_file:
            host_contents += line
    host_file.flush()
    log.info("Hosts file: \n{}".format(host_contents))
