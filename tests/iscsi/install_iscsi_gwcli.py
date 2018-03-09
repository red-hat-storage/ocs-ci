import logging
import random
from time import sleep

from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info('Running iscsi configuration')
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    ceph_osd = []
    iscsi_util = IscsiUtils(ceph_nodes)
    global no_of_luns
    no_of_luns = 0
    ceph_iscsi_initiatorname = []
    trusted_ip_list = ''
    if config.get('no_of_gateways'):
        no_of_gateways = int(config.get('no_of_gateways'))
    else:
        no_of_gateways = 2
    if config.get('no_of_luns'):
        no_of_luns = int(config.get('no_of_luns'))
    else:
        no_of_luns = 10

    iscsi_util.copy_keyring()  # copy keyring to gateway nodes

    for ceph in ceph_nodes:
        if ceph.role == 'osd':
            ceph_osd_nodes = ceph
            break
    check = 1
    iscsi_util.do_iptables_flush()
    for ceph in ceph_nodes:
        if ceph.role == 'osd' and check <= no_of_gateways:
            ceph_osd.append(ceph.shortname)
            (out, rc) = ceph.exec_command(cmd='hostname -I')
            trusted_ip_list = trusted_ip_list + out.read().rstrip('\n ')
            trusted_ip_list = trusted_ip_list + ','.rstrip(' ')
            check = check + 1
    if no_of_gateways % 2 == 0:
        if no_of_gateways <= len(ceph_osd):
            trusted_ip_list = trusted_ip_list.rstrip(' ,')
            iscsi_gateway_cfg = \
                """
[config]
cluster_name = ceph
gateway_keyring = ceph.client.admin.keyring
api_secure = false
api_ssl_verify = false
trusted_ip_list = {0}""".format(trusted_ip_list)
            check = 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    conf_file = ceph.write_file(
                        sudo=True, file_name='/etc/ceph/iscsi-gateway.cfg',
                        file_mode='w')
                    conf_file.write(iscsi_gateway_cfg)
                    conf_file.flush()
                    check = check + 1
            check = 1
            count = 0
            iscsi_util.do_iptables_flush()
            count = count + 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    ceph.exec_command(
                        sudo=True, cmd='systemctl reset-failed rbd-target-gw')
                    sleep(2)
                    check = check + 1
            check = 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    ceph.exec_command(sudo=True,
                                      cmd='systemctl start rbd-target-gw')
                    sleep(2)
                    check = check + 1
            check = 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    ceph.exec_command(
                        sudo=True, cmd='systemctl reset-failed rbd-target-api')
                    sleep(2)
                    check = check + 1
            check = 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    ceph.exec_command(sudo=True,
                                      cmd='systemctl start rbd-target-api')
                    sleep(2)
                    check = check + 1
            log.info('Services enabled and started')
            log.info('Starting to create software iscsi')
            count = 0
            sleep(5)
            ceph_osd_nodes.exec_command(
                cmd='sudo gwcli /iscsi-target create '
                    'iqn.2003-01.com.redhat.iscsi-gw:ceph-igw')
            sleep(5)
            log.info('created software iscsi')
            check = 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    (out, rc) = ceph.exec_command(cmd='hostname -I')
                    ip = out.read()
                    ceph_osd_nodes.exec_command(
                        cmd='sudo gwcli /iscsi-target'
                            '/iqn.2003-01.com.redhat.iscsi-'
                            'gw:ceph-igw/gateways create ' +
                            ceph.hostname +
                            ' ' +
                            ip.rstrip('\n '),
                        long_running=True)
                    sleep(20)
                    log.info(ceph.shortname + ' gateway added')
                    check = check + 1
            image_name = 'test_image' + str(random.randint(10, 999))

            for ceph in ceph_nodes:
                if ceph.role == 'iscsi-clients':
                    iscsi_initiators = ceph
            (out, err) = iscsi_initiators.exec_command(
                sudo=True, cmd='sudo cat /etc/iscsi/initiatorname.iscsi')
            output = out.read()
            temp = output.split('=')
            ceph_iscsi_initiatorname.append(temp[1])
            ini_name = temp[1]
            ini_name = ini_name.split(':')
            ini_name = ini_name[1]
            log.info('Adding iscsi-clients')

            count = 0
            for ceph in range(len(ceph_iscsi_initiatorname)):
                ceph_osd_nodes.exec_command(
                    cmd='sudo gwcli /iscsi-target/'
                        'iqn.2003-01.com.redhat.iscsi-gw:ceph-'
                        'igw/hosts create iqn.1994-05.com.redhat:' +
                        iscsi_util.get_initiatorname())
                ceph_osd_nodes.exec_command(
                    cmd='sudo gwcli /iscsi-target/iqn.2003-01.com.redhat.'
                        'iscsi-gw:ceph-igw/hosts/iqn.1994-05.com.redhat:' +
                        iscsi_util.get_initiatorname() +
                        ' auth ' +
                        ini_name.rstrip('\n') +
                        '/redhat@123456 "|" nochap')
                log.info('Client Added ' +
                         ceph_iscsi_initiatorname[ceph])
            iscsi_util.create_luns(
                no_of_luns,
                ceph_osd_nodes,
                image_name,
                iosize="2g",
                map_to_client=True)
            iscsi_util.write_multipath(iscsi_initiators)

            return 0
        else:
            log.error('No_of_gateways excited ' + no_of_gateways +
                      'gateway node found')
    else:
        log.error('gateway nodes must be multiple of 2')
