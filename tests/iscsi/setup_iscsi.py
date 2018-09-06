import logging

from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info('Running iscsi configuration')
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    test_data = kw.get('test_data')
    no_of_gateways = config.get('no_of_gateways', 2)

    log.info('Preparing ceph cluster')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_util.restart_ceph_mon()
    iscsi_util.install_prereq_gw()
    iscsi_util.open_ports()
    test_data['gw_list'] = iscsi_util.get_gw_list(no_of_gateways)
    iscsi_util.check_installed_rpm(test_data['gw_list'])
    test_data['gwcli_node'] = iscsi_util.setup_gw(test_data['gw_list'])
    iscsi_util.create_and_check_target(test_data['gwcli_node'])
    iscsi_util.create_and_check_gateways(test_data['gwcli_node'], test_data['gw_list'])
    return 0
