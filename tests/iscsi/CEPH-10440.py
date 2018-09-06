import logging

from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    test_data = kw.get('test_data')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_initiator = iscsi_util.get_iscsi_initiator_linux()
    initiatorname = iscsi_util.get_initiatorname()
    iscsi_util.write_multipath(iscsi_initiator)  # to be chnage later
    iscsi_util.write_chap(initiatorname, iscsi_initiator)
    no_of_luns = test_data['no_of_luns']
    device_list = iscsi_util.get_devicelist_luns(no_of_luns)
    iscsi_util.create_directory_with_io(
        device_list, iscsi_initiator, io_size="2G")
    rc = iscsi_util.do_ios(iscsi_initiator, device_list)
    if iscsi_util.check_mnted_disks(iscsi_initiator, device_list) == 1:
        return 1
    iscsi_util.umount_directory(device_list, iscsi_initiator)
    iscsi_util.dissconect_linux_initiator(iscsi_initiator)
    if rc != 0:
        log.error("fio test failed")
        return 1
    else:
        return 0
