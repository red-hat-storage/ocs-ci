import datetime
import json
import logging
from time import sleep
import yaml

from ceph.utils import setup_deb_repos, get_iso_file_url, setup_cdn_repos, write_docker_daemon_json, \
    search_ethernet_interface
from ceph.utils import setup_repos, check_ceph_healthly

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    log.info("Running ceph ansible test")
    config = kw.get('config')
    test_data = kw.get('test_data')
    ubuntu_repo = None
    ansible_dir = '/usr/share/ceph-ansible'

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

    # remove mgr nodes from list if build is 2.x
    build = config.get('build', '3')
    if build.startswith('2'):
        ceph_nodes = [node for node in ceph_nodes if node.role != 'mgr']

    ceph_installer = None
    ceph_mon = None
    for ceph in ceph_nodes:
        if ceph.role == 'mon':
            ceph.exec_command(sudo=True, cmd="systemctl enable firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl start firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl status firewalld")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6789/tcp")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6789/tcp --permanent")
    for ceph in ceph_nodes:
        if ceph.role == 'osd':
            ceph.exec_command(sudo=True, cmd="systemctl enable firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl start firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl status firewalld")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6800-7300/tcp")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6800-7300/tcp --permanent")
    for ceph in ceph_nodes:
        if ceph.role == 'mgr':
            ceph.exec_command(sudo=True, cmd="systemctl enable firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl start firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl status firewalld")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6800-7300/tcp")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6800-7300/tcp --permanent")
    for ceph in ceph_nodes:
        if ceph.role == 'mds':
            ceph.exec_command(sudo=True, cmd="systemctl enable firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl start firewalld")
            ceph.exec_command(sudo=True, cmd="systemctl status firewalld")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6800/tcp")
            ceph.exec_command(sudo=True, cmd="firewall-cmd --zone=public --add-port=6800/tcp --permanent")
    for node in ceph_nodes:
        if node.role == 'installer':
            log.info("Setting installer node")
            ceph_installer = node
            break
    keys = ''
    hosts = ''
    hostkeycheck = 'Host *\n\tStrictHostKeyChecking no\n\tServerAliveInterval 2400\n'

    for ceph in ceph_nodes:
        ceph.generate_id_rsa()
        keys = keys + ceph.id_rsa_pub
        hosts = hosts + ceph.ip_address + "\t" + ceph.hostname \
            + "\t" + ceph.shortname + "\n"

    # check to see for any additional repo (test mode)
    if config.get('add-repo'):
        repo = config['add-repo']
        for ceph in ceph_nodes:
            if ceph.pkg_type == 'rpm':
                log.info(
                    "Additing addition repo {repo} to {sn}".format(
                        repo=repo, sn=ceph.shortname))
                ceph.exec_command(
                    sudo=True, cmd='wget -O /etc/yum.repos.d/rh_add_repo.repo {repo}'.format(repo=repo))
                ceph.exec_command(sudo=True, cmd='yum update metadata')

    for ceph in ceph_nodes:
        keys_file = ceph.write_file(
            file_name='.ssh/authorized_keys', file_mode='a')
        hosts_file = ceph.write_file(
            sudo=True, file_name='/etc/hosts', file_mode='a')
        ceph.exec_command(
            cmd='[ -f ~/.ssh/config ] && chmod 700 ~/.ssh/config',
            check_ec=False)
        ssh_config = ceph.write_file(file_name='.ssh/config', file_mode='a')
        keys_file.write(keys)
        hosts_file.write(hosts)
        ssh_config.write(hostkeycheck)
        keys_file.flush()
        hosts_file.flush()
        ssh_config.flush()
        ceph.exec_command(cmd='chmod 600 ~/.ssh/authorized_keys')
        ceph.exec_command(cmd='chmod 400 ~/.ssh/config')

    for ceph in ceph_nodes:
        if not config.get('use_cdn', False):
            if config['ansi_config'].get('ceph_repository_type') != 'iso' or \
                    config['ansi_config'].get('ceph_repository_type') == 'iso' and \
                    (ceph.role == 'installer'):
                if ceph.pkg_type == 'deb':
                    setup_deb_repos(ceph, ubuntu_repo)
                    sleep(15)
                    # install python2 on xenial
                    ceph.exec_command(sudo=True, cmd='sudo apt-get install -y python')
                    ceph.exec_command(sudo=True, cmd='apt-get install -y python-pip')
                    ceph.exec_command(sudo=True, cmd='apt-get install -y ntp')
                    ceph.exec_command(sudo=True, cmd='apt-get install -y chrony')
                    ceph.exec_command(sudo=True, cmd='pip install nose')
                else:
                    setup_repos(ceph, base_url, installer_url)
            if config['ansi_config'].get('ceph_repository_type') == 'iso' and ceph.role == 'installer':
                iso_file_url = get_iso_file_url(base_url)
                ceph.exec_command(sudo=True, cmd='mkdir -p {}/iso'.format(ansible_dir))
                ceph.exec_command(sudo=True, cmd='wget -O {}/iso/ceph.iso {}'.format(ansible_dir, iso_file_url))
        else:
            log.info("Using the cdn repo for the test")
            setup_cdn_repos(ceph_nodes, build=config.get('build'))
        log.info("Updating metadata")
        sleep(15)
    if ceph_installer.pkg_type == 'deb':
        ceph_installer.exec_command(
            sudo=True, cmd='apt-get install -y ceph-ansible')
    else:
        ceph_installer.exec_command(
            sudo=True, cmd='yum install -y ceph-ansible')
    sleep(4)
    sleep(2)
    mon_hosts = []
    osd_hosts = []
    rgw_hosts = []
    mds_hosts = []
    mgr_hosts = []
    client_hosts = []
    num_osds = 0
    num_mons = 0
    num_mgrs = 0
    for node in ceph_nodes:
        eth_interface = search_ethernet_interface(node, ceph_nodes)
        if eth_interface is None:
            log.error('No suitable interface is found on {node}'.format(node=node.ip_address))
            return 1
        node.set_eth_interface(eth_interface)
        mon_interface = ' monitor_interface=' + node.eth_interface + ' '
        if node.role == 'mon':
            mon_host = node.shortname + ' monitor_interface=' + node.eth_interface
            mon_hosts.append(mon_host)
            num_mons += 1
        if node.role == 'mgr':
            mgr_host = node.shortname + ' monitor_interface=' + node.eth_interface
            mgr_hosts.append(mgr_host)
            num_mgrs += 1
        if node.role == 'osd':
            devices = node.no_of_volumes
            devchar = 98
            devs = []
            for vol in range(0, devices):
                dev = '/dev/vd' + chr(devchar)
                devs.append(dev)
                devchar += 1
            reserved_devs = []
            if config['ansi_config'].get('osd_scenario') == 'non-collocated':
                reserved_devs = \
                    [raw_journal_device for raw_journal_device in set(config['ansi_config'].get('dedicated_devices'))]
            devs = [_dev for _dev in devs if _dev not in reserved_devs]
            num_osds = num_osds + len(devs)
            auto_discovey = config['ansi_config'].get('osd_auto_discovery', False)
            osd_host = node.shortname + mon_interface + \
                (" devices='" + json.dumps(devs) + "'" if not auto_discovey else '')
            osd_hosts.append(osd_host)
        if node.role == 'mds':
            mds_host = node.shortname + ' monitor_interface=' + node.eth_interface
            mds_hosts.append(mds_host)
        if node.role == 'rgw':
            rgw_host = node.shortname + ' radosgw_interface=' + node.eth_interface
            rgw_hosts.append(rgw_host)
        if node.role == 'client':
            client_host = node.shortname + ' client_interface=' + node.eth_interface
            client_hosts.append(client_host)

    hosts_file = ''
    if mon_hosts:
        mon = '[mons]\n' + '\n'.join(mon_hosts)
        hosts_file += mon + '\n'
    if mgr_hosts:
        mgr = '[mgrs]\n' + '\n'.join(mgr_hosts)
        hosts_file += mgr + '\n'
    if osd_hosts:
        osd = '[osds]\n' + '\n'.join(osd_hosts)
        hosts_file += osd + '\n'
    if mds_hosts:
        mds = '[mdss]\n' + '\n'.join(mds_hosts)
        hosts_file += mds + '\n'
    if rgw_hosts:
        rgw = '[rgws]\n' + '\n'.join(rgw_hosts)
        hosts_file += rgw + '\n'
    if client_hosts:
        client = '[clients]\n' + '\n'.join(client_hosts)
        hosts_file += client + '\n'

    log.info('Generated hosts file: \n{file}'.format(file=hosts_file))
    host_file = ceph_installer.write_file(
        sudo=True, file_name='{}/hosts'.format(ansible_dir), file_mode='w')
    host_file.write(hosts_file)
    host_file.flush()
    if config.get('ansi_config').get('containerized_deployment') and config.get('docker-insecure-registry') and \
            config.get('ansi_config').get('ceph_docker_registry'):
        insecure_registry = '{{"insecure-registries" : ["{registry}"]}}'.format(
            registry=config.get('ansi_config').get('ceph_docker_registry'))
        log.warn('Adding insecure registry:\n{registry}'.format(registry=insecure_registry))
        for node in ceph_nodes:
            write_docker_daemon_json(insecure_registry, node)

    # use the provided sample file as main site.yml
    if config.get('ansi_config').get('containerized_deployment') is True:
        ceph_installer.exec_command(
            sudo=True,
            cmd='cp -R {ansible_dir}/site-docker.yml.sample {ansible_dir}/site.yml'.format(ansible_dir=ansible_dir))
    else:
        ceph_installer.exec_command(
            sudo=True, cmd='cp -R {ansible_dir}/site.yml.sample {ansible_dir}/site.yml'.format(ansible_dir=ansible_dir))

    gvar = yaml.dump(config.get('ansi_config'), default_flow_style=False)
    log.info("global vars " + gvar)
    gvars_file = ceph_installer.write_file(
        sudo=True, file_name='{}/group_vars/all.yml'.format(ansible_dir), file_mode='w')
    gvars_file.write(gvar)
    gvars_file.flush()

    if ceph_installer.pkg_type == 'rpm':
        out, rc = ceph_installer.exec_command(cmd='rpm -qa | grep ceph')
    else:
        out, rc = ceph_installer.exec_command(cmd='apt-cache search ceph')
    log.info("Ceph versions " + out.read())
    out, rc = ceph_installer.exec_command(
        cmd='cd {} ; ansible-playbook -vv -i hosts site.yml'.format(ansible_dir), long_running=True)

    if rc != 0:
        log.error("Failed during deployment")
        return rc

    # Add all clients
    for node in ceph_nodes:
        if node.role == 'mon':
            ceph_mon = node
            break
    mon_container = None
    if config.get('ansi_config').get('containerized_deployment') is True:
        mon_container = 'ceph-mon-{host}'.format(host=ceph_mon.hostname)
    # check if all osd's are up and in
    timeout = 300
    if config.get('timeout'):
        timeout = datetime.timedelta(seconds=config.get('timeout'))
    if check_ceph_healthly(ceph_mon, num_osds, num_mons, mon_container, timeout) != 0:
        return 1
    # add test_data for later use by upgrade test etc
    test_data['ceph-ansible'] = {'num-osds': num_osds, 'num-mons': num_mons, 'rhbuild': build}

    # create rbd pool used by tests/workunits
    if not build.startswith('2'):
        if config.get('ansi_config').get('containerized_deployment') is True:
            ceph_mon.exec_command(
                sudo=True, cmd='docker exec {container} ceph osd pool create rbd 64 64'.format(container=mon_container))
            ceph_mon.exec_command(
                sudo=True, cmd='docker exec {container} ceph osd pool application enable rbd rbd --yes-i-really-mean-it'
                    .format(container=mon_container))
        else:
            ceph_mon.exec_command(sudo=True, cmd='ceph osd pool create rbd 64 64')
            ceph_mon.exec_command(sudo=True, cmd='ceph osd pool application enable rbd rbd --yes-i-really-mean-it')
    return rc
