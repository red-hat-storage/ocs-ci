import datetime
import yaml
import logging
import random
import json
import re
from ceph.utils import setup_deb_repos, get_iso_file_url
from ceph.utils import setup_repos, create_ceph_conf
from time import sleep
logger = logging.getLogger(__name__)
log = logger
def run(**kw):
    log.info('Running iscsi configuration')
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    ceph_osd = []
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
    for ceph in ceph_nodes:
        if ceph.role == 'mon':
            ceph_mon = ceph
            break
    count = 0
    for ceph in ceph_nodes:
        ceph.exec_command(sudo=True, cmd='iptables -F')
    for ceph in ceph_nodes:
        if ceph.role == 'osd':
            (out, err) = ceph_mon.exec_command(sudo=True,
                    cmd='cat /etc/ceph/ceph.client.admin.keyring')
            ceph_keyring = out.read()
            key_file = ceph.write_file(sudo=True,
                    file_name='/etc/ceph/ceph.client.admin.keyring',
                    file_mode='w')
            key_file.write(ceph_keyring)
            key_file.flush()
            ceph.exec_command(cmd='sudo chmod u+rw /etc/ceph/ceph.client.admin.keyring'
                              )
    for ceph in ceph_nodes:
        if ceph.role == 'osd':
            ceph_osd_nodes = ceph
            break
    check = 1
    for ceph in ceph_nodes:
        ceph.exec_command(sudo=True, cmd='iptables -F')
    for ceph in ceph_nodes:
        if ceph.role == 'osd' and check <= no_of_gateways:
            ceph_osd.append(ceph.shortname)
            (out, rc) = ceph.exec_command(cmd='hostname -I')
            trusted_ip_list = trusted_ip_list + out.read().rstrip('\n '
                    ) + ','.rstrip(' ')
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
                    conf_file = ceph.write_file(sudo=True,
                            file_name='/etc/ceph/iscsi-gateway.cfg',
                            file_mode='w')
                    conf_file.write(iscsi_gateway_cfg)
                    conf_file.flush()
                    check = check + 1
            check = 1
            count = 0
            for ceph in ceph_nodes:
                ceph.exec_command(sudo=True, cmd='iptables -F')
            count = count + 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    ceph.exec_command(sudo=True,
                            cmd='targetcli clearconfig confirm=true')
                    sleep(5)
                    ceph.exec_command(sudo=True,
                            cmd='systemctl reset-failed rbd-target-gw')
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
                    ceph.exec_command(sudo=True,
                            cmd='systemctl reset-failed rbd-target-api')
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
            for ceph in ceph_nodes:
                ceph.exec_command(sudo=True, cmd='iptables -F')
            sleep(5)
            ceph_osd_nodes.exec_command(cmd='sudo gwcli /iscsi-target create iqn.2003-01.com.redhat.iscsi-gw:ceph-igw'
                    )
            sleep(5)
            log.info('created software iscsi')
            check = 1
            for ceph in ceph_nodes:
                if ceph.role == 'osd' and check <= no_of_gateways:
                    (out, rc) = ceph.exec_command(cmd='hostname -I')
                    ip = out.read()
                    ceph_osd_nodes.exec_command(cmd='sudo gwcli /iscsi-target/iqn.2003-01.com.redhat.iscsi-gw:ceph-igw/gateways create '
                             + ceph.hostname + ' ' + ip.rstrip('\n ')
                            + ' skipchecks=true', long_running=True)
                    sleep(20)
                    log.info(ceph.shortname + ' gateway added')
                    check = check + 1
            image_name = 'test_image' + str(random.randint(10, 999))
            for i in range(0, no_of_luns):
                ceph_osd_nodes.exec_command(sudo=True,
                        cmd='gwcli /disks create rbd image='
                        + image_name + str(i) + ' size=2g')
            for ceph in ceph_nodes:
                if ceph.role == 'iscsi-clients':
                    iscsi_initiators = ceph
            (out, err) = iscsi_initiators.exec_command(sudo=True,
                    cmd='sudo cat /etc/iscsi/initiatorname.iscsi')
            output = out.read()
            temp = output.split('=')
            ceph_iscsi_initiatorname.append(temp[1])
            ini_name = temp[1]
            ini_name = ini_name.split(':')
            ini_name = ini_name[1]
            log.info('Adding iscsi-clients')
            count = 0
            for ceph in range(len(ceph_iscsi_initiatorname)):
                ceph_osd_nodes.exec_command(cmd='sudo gwcli /iscsi-target/iqn.2003-01.com.redhat.iscsi-gw:ceph-igw/hosts create '
                         + ceph_iscsi_initiatorname[ceph].rstrip())
                ceph_osd_nodes.exec_command(cmd='sudo gwcli /iscsi-target/iqn.2003-01.com.redhat.iscsi-gw:ceph-igw/hosts/'
                         + ceph_iscsi_initiatorname[ceph].rstrip()
                        + ' auth ' + ini_name.rstrip('\n')
                        + '/redhat@123456 "|" nochap')
                log.info('Client Added '
                         + ceph_iscsi_initiatorname[ceph])
            multipath = \
                """devices {
                            device {
                                    vendor                 "LIO-ORG"
                                    hardware_handler       "1 alua"
                                    path_grouping_policy   "failover"
                                    path_selector          "queue-length 0"
                                    failback               60
                                    path_checker           tur
                                    prio                   alua
                                    prio_args              exclusive_pref_bit
                                    fast_io_fail_tmo       25
                                    no_path_retry          queue
                            }
                    }"""
            log.info('Configuring Multipath IO: ')
            iscsi_initiators.exec_command(sudo=True,
                    cmd='mpathconf --enable --with_multipathd y')
            multipath_file = iscsi_initiators.write_file(sudo=True,
                    file_name='/etc/multipath.conf', file_mode='a')
            multipath_file.write(multipath)
            multipath_file.flush()
            iscsi_initiators.exec_command(sudo=True,
                    cmd='systemctl reload multipathd',
                    long_running=True)
            for i in range(0, no_of_luns):
                ceph_osd_nodes.exec_command(sudo=True,
                        cmd='gwcli /iscsi-target/iqn.2003-01.com.redhat.iscsi-gw:ceph-igw/hosts/'
                         + ceph_iscsi_initiatorname[0].rstrip('\n')
                        + ' disk add rbd.' + image_name + str(i))
            return 0
        else:
            log.error('No_of_gateways excited ' + no_of_gateways
                      + 'gateway node found')
    else:
        log.error('gateway nodes must be multiple of 2')