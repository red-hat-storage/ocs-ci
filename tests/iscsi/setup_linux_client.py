import logging
import random

from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    config = kw.get('config')
    test_data = kw.get('test_data')
    test_data['no_of_luns'] = config.get('no_of_luns', 10)
    ceph_nodes = kw.get('ceph_nodes')
    image_name = 'test_image' + str(random.randint(10, 999))

    log.info('Creating iscsi host')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_util.install_prereq_rhel_client()
    initiator_name = iscsi_util.get_initiatorname(full=True)
    iscsi_util.create_host(test_data['gwcli_node'], initiator_name)
    iscsi_util.create_luns(
        test_data['no_of_luns'],
        test_data['gwcli_node'],
        initiator_name,
        image_name,
        iosize="2g",
        map_to_client=True)

    return 0
