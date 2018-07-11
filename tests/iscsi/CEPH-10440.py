import logging

import install_iscsi_gwcli
from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_initiators = iscsi_util.get_iscsi_initiator_linux()
    initiatorname = iscsi_util.get_initiatorname()
    iscsi_util.write_multipath(iscsi_initiators)  # to be chnage later
    iscsi_util.write_chap(initiatorname, iscsi_initiators)
    no_of_luns = install_iscsi_gwcli.no_of_luns
    device_list = iscsi_util.get_devicelist_luns(no_of_luns)
    if isinstance(device_list, list):
        pass
    else:
        return 1
    rc = iscsi_util.create_directory_with_io(
        device_list, iscsi_initiators, io_size="2G", do_io=1)
    if rc == 1:
        iscsi_util.umount_directory(device_list, iscsi_initiators)
        iscsi_initiators.exec_command(
            sudo=True,
            cmd="iscsiadm -m node -T iqn.2003-01.com.redhat.iscsi-"
                "gw:ceph-igw -u",
            long_running=True)
        iscsi_initiators.exec_command(
            sudo=True,
            cmd="systemctl stop multipathd",
            long_running=True)
        return 1
    if len(device_list) == no_of_luns:
        iscsi_util.umount_directory(device_list, iscsi_initiators)
        iscsi_initiators.exec_command(
            sudo=True,
            cmd="iscsiadm -m node -T iqn.2003-01.com.redhat.iscsi-"
                "gw:ceph-igw -u",
            long_running=True)
        iscsi_initiators.exec_command(
            sudo=True,
            cmd="systemctl stop multipathd",
            long_running=True)
        return 0
    else:
        log.info("Not all luns are been mapped")
        return 1
