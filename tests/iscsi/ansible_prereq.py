import logging

from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info("Running setup")
    log.info('Running iscsi configuration')
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    test_data = kw.get('test_data')
    no_of_luns = config.get('no_of_luns')
    no_of_gw = config.get('no_of_gateways')

    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_util.install_prereq_rhel_client()
    initiator_name = iscsi_util.get_initiatorname(full=True)
    host_name = iscsi_util.get_initiatorname()
    luns_setting, luns_list = iscsi_util.generate_luns(no_of_luns)
    client_setting = iscsi_util.generate_clients(initiator_name, luns_list, host_name)
    test_data["gw_ip_list"] = iscsi_util.generate_gw_ips(no_of_gw)
    test_data["luns_setting"] = luns_setting
    test_data["initiator_setting"] = client_setting
    iscsi_util.install_config_pkg()
    return 0
