import logging
import random

from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info('Running iscsi configuration')
    ceph_nodes = kw.get('ceph_nodes')
    clients = kw.get('clients')
    win_client = clients[0]
    config = kw.get('config')
    no_of_gateways = config.get('no_of_gateways', 2)
    no_of_luns = config.get('no_of_luns', 10)
    image_name = 'test_image' + str(random.randint(10, 999))

    log.info('Preparing ceph cluster')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_util.restart_ceph_mon()
    iscsi_util.install_prereq_gw()
    iscsi_util.do_iptables_flush()
    gw_list = iscsi_util.get_gw_list(no_of_gateways)
    gwcli_node = iscsi_util.setup_gw(gw_list)
    iscsi_util.run_gw(gwcli_node, gw_list)

    log.info('Preparing windows cient')
    win_client.start_iscsi_initiator()
    win_client.create_new_target(gwcli_node.private_ip)
    initiator_name = win_client.get_iscsi_initiator_name()
    login = initiator_name.split(":")[1]

    log.info('Creating iscsi host')
    iscsi_util.create_host(gwcli_node, initiator_name)
    iscsi_util.create_luns(
        no_of_luns,
        gwcli_node,
        initiator_name,
        image_name,
        iosize="2g",
        map_to_client=True)

    log.info('Connecting disks')
    win_client.connect_to_target(gwcli_node.private_ip, login, "redhat@123456")
    win_client.create_disk()
    return 0
