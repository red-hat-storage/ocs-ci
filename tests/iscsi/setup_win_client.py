import logging

from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    ceph_nodes = kw.get('ceph_nodes')
    clients = kw.get('clients')
    test_data = kw.get('test_data')
    win_client = clients[0]

    log.info('Preparing windows cient')
    win_client.start_iscsi_initiator()
    win_client.create_new_target(test_data['gwcli_node'].private_ip)
    test_data['initiator_name'] = win_client.get_iscsi_initiator_name()

    log.info('Creating iscsi host')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_util.create_host(test_data['gwcli_node'], test_data['initiator_name'])

    return 0
