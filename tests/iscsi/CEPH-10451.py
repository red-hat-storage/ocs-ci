import datetime
import logging
from time import sleep

import install_iscsi_gwcli
from ceph.parallel import parallel
from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_initiators = iscsi_util.get_iscsi_initiators()
    initiatorname = iscsi_util.get_initiatorname()
    iscsi_util.write_chap(initiatorname, iscsi_initiators)
    no_of_luns = install_iscsi_gwcli.no_of_luns
    rc = []
    directory_name = ''

    device_list = iscsi_util.get_devicelist_luns(no_of_luns)
    if isinstance(device_list, list):
        pass
    else:
        return 1
    out = iscsi_util.create_directory_with_io(
        device_list, iscsi_initiators, io_size="2G", do_io=0)
    if isinstance(out, int):
        rc.append(out)
    else:
        directory_name = out
    with parallel() as p:
        p.spawn(do_ios, iscsi_initiators, directory_name)
        p.spawn(do_failover, iscsi_initiators, device_list, ceph_nodes)
        for op in p:
            rc.append(op)

    print "return   " + str(rc)
    rc = set(rc)
    if len(rc) == 1:
        print rc
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
        print rc

        return 1


def do_ios(iscsi_initiators, directory_name):
    out, err = iscsi_initiators.exec_command(
        sudo=True, cmd="cd ~/" + directory_name + " ; fio fio.fio "
        "--verify=md5", long_running=True)
    err_fio = err

    if err_fio == 0:
        return 0
    else:
        return 1


def do_failover(iscsi_initiators, device_list, ceph_nodes):
    sleep(10)
    out, err = iscsi_initiators.exec_command(
        sudo=True, cmd="multipath -ll |grep -A 9 mpa"
                       "" + device_list[0] + " |grep -A 1 status=enabled "
                                             "|awk -F "
        '" "'
        " '{print $(NF - 4)}'")

    active_device = out.read()
    active_device = active_device.rstrip("\n")
    active_device = active_device.split()
    out, err = iscsi_initiators.exec_command(
        sudo=True, cmd="ls -l /dev/disk/by-path | grep "
                       "" + active_device[1] + " |awk -F "
        '" "'
        " '{print $(NF - 2)}' |cut -d: -f1 | uniq", long_running=True)
    ip_to_restart = out

    ip_to_restart = ip_to_restart.rstrip("\n")
    ip_to_restart = ip_to_restart.split("-")
    for node in ceph_nodes:
        if node.role == "osd":
            out, err = node.exec_command(cmd="hostname -I")
            output = out.read()
            output = output.rstrip()
            if output == ip_to_restart[1]:
                node.exec_command(sudo=True, cmd="reboot", check_ec=False)
                sleep(5)
                break
    sleep(60)
    t1 = datetime.datetime.now()
    time_plus_5 = t1 + datetime.timedelta(minutes=15)
    log.info("wating to get failed device active")
    while (1):
        t2 = datetime.datetime.now()
        if (t2 <= time_plus_5):
            sleep(40)
            out, err = iscsi_initiators.exec_command(
                sudo=True, cmd="multipath -ll |grep -A 9 mpa"
                "" + device_list[0] + " |grep -B 1 " + active_device[1] + "  "
                "|awk -F "
                '" "'
                " '{print $(NF - 2)}'")
            active_device_status = out.read()
            active_device_status = active_device_status.rstrip("\n")
            active_device_status = active_device_status.split()
            print active_device_status
            if (active_device_status[1] == "active"):
                rc = "active"
                break
            else:
                for node in ceph_nodes:
                    if node.role == "osd":
                        out, err = node.exec_command(cmd="hostname -I")
                        output = out.read()
                        output = output.rstrip()
                        if output == ip_to_restart[1]:
                            node.exec_command(sudo=True, cmd="iptables -F")
                            sleep(5)
        else:
            log.info("failed device didn't came up to active")
            rc = "not"
    print active_device_status
    print active_device
    if rc == "active":

        return 0
    else:
        return 1
