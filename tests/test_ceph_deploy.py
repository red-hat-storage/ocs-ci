import logging
import time

from ceph.utils import keep_alive, setup_deb_repos
from ceph.utils import setup_repos, create_ceph_conf

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
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
    ceph1 = ceph_nodes[0]
    out, _ = ceph1.exec_command(cmd='uuidgen')
    uuid = out.read().strip()
    ceph_mon_nodes = []
    mon_names = ''
    all_nodes = ''
    for ceph in ceph_nodes:
        if ceph.role == 'mon':
            ceph_mon_nodes.append(ceph)
            mon_names = mon_names + ceph.shortname + ' '
        all_nodes = all_nodes + ceph.shortname + ' '
    ceph_conf = create_ceph_conf(fsid=uuid, mon_hosts=ceph_mon_nodes)
    keys = ''
    hosts = ''
    hostkeycheck = 'Host *\n\tStrictHostKeyChecking no\n\tServerAliveInterval 2400\n'

    for ceph in ceph_nodes:
        ceph.generate_id_rsa()
        keys = keys + ceph.id_rsa_pub
        hosts = hosts + ceph.ip_address + "\t" + ceph.hostname \
            + "\t" + ceph.shortname + "\n"

    for ceph in ceph_nodes:
        keys_file = ceph.write_file(
            file_name='.ssh/authorized_keys', file_mode='w')
        hosts_file = ceph.write_file(
            sudo=True, file_name='/etc/hosts', file_mode='a')
        ceph.exec_command(
            cmd='[ -f ~/.ssh/config ] && chmod 700 ~/.ssh/config',
            check_ec=False)
        ssh_config = ceph.write_file(file_name='.ssh/config', file_mode='w')
        keys_file.write(keys)
        hosts_file.write(hosts)
        ssh_config.write(hostkeycheck)
        keys_file.flush()
        hosts_file.flush()
        ssh_config.flush()
        ceph.exec_command(cmd='chmod 600 ~/.ssh/authorized_keys')
        ceph.exec_command(cmd='chmod 400 ~/.ssh/config')

    for ceph in ceph_nodes:
        if config.get('use_cdn') is False:
            if ceph.pkg_type == 'deb':
                setup_deb_repos(ceph, ubuntu_repo)
                # install python2 on xenial
                ceph.exec_command(cmd='sudo apt-get install -y python')
            else:
                setup_repos(ceph, base_url, installer_url)
        else:
            log.info("Using the cdn repo for the test")
        log.info("Updating metadata")
        if ceph.pkg_type == 'rpm':
            ceph.exec_command(sudo=True, cmd='yum update metadata')

    ceph1.exec_command(cmd='mkdir cd')
    ceph1.exec_command(sudo=True, cmd='cd cd; yum install -y ceph-deploy')
    ceph1.exec_command(
        cmd='cd cd; ceph-deploy new {mons}'.format(mons=mon_names))
    cc = ceph1.write_file(file_name='cd/ceph.conf', file_mode='w')
    cc.write(ceph_conf)
    cc.flush()
    out, err = ceph1.exec_command(
        cmd='cd cd; ceph-deploy install {all_n}'.format(
            all_n=all_nodes), timeout=600, check_ec=False)
    running = True
    while running:
        keep_alive(ceph_nodes)
        log.info("Wait for 120 seconds before next check")
        time.sleep(120)
        if out.channel.exit_status_ready():
            log.info(
                "Command completed on remote node %d",
                out.channel.recv_exit_status())
            running = False
            log.info(out.read())
            log.info(err.read())
        else:
            log.info("Command still running")
    out, err = ceph1.exec_command(
        cmd='cd cd; ceph-deploy mon create-initial', timeout=300)
    if ceph1.exit_status != 0:
        log.error("Failed during mon create-initial")
        return ceph1.exit_status

    for cnode in ceph_nodes:
        if cnode.role == 'osd':
            hostname = cnode.shortname
            devices = cnode.no_of_volumes
            dev = 98  # start with b
            for vol in range(0, devices):
                device = hostname + ':' + '/dev/vd' + chr(dev)
                # --dmcrypt {device}
                ceph1.exec_command(
                    cmd='cd cd; ceph-deploy osd prepare {device}'.format(device=device), timeout=300)
                device = hostname + ':' + '/dev/vd' + chr(dev) + '1'
                ceph1.exec_command(
                    cmd='cd cd; ceph-deploy osd activate {device}'.format(device=device), timeout=60)
                time.sleep(2)
                dev = dev + 1
                if ceph1.exit_status != 0:
                    log.error("Failed during osd activate")
                    return ceph1.exit_status
        elif cnode.role == 'client':
            ceph1.exec_command(
                cmd='cd cd; ceph-deploy admin ' + cnode.shortname
            )

    return 0
