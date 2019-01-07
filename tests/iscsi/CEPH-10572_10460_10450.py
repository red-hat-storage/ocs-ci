import datetime
import logging
from time import sleep

from ceph.parallel import parallel
from tests.iscsi.iscsi_utils import IscsiUtils

log = logging


def run(**kw):
    log.info("Running test")
    ceph_nodes = kw.get('ceph_nodes')
    test_data = kw.get('test_data')
    iscsi_util = IscsiUtils(ceph_nodes)
    iscsi_initiators = iscsi_util.get_iscsi_initiator_linux()
    initiatorname = iscsi_util.get_initiatorname()
    iscsi_util.write_multipath(iscsi_initiators)
    iscsi_util.write_chap(initiatorname, iscsi_initiators)
    no_of_luns = test_data['no_of_luns']
    rc = []

    device_list = iscsi_util.get_devicelist_luns(no_of_luns)
    iscsi_util.create_directory_with_io(
        device_list, iscsi_initiators, io_size="1G")
    with parallel() as p:
        p.spawn(iscsi_util.do_ios, iscsi_initiators, device_list)
        p.spawn(do_failover, iscsi_initiators, device_list, ceph_nodes)
        for op in p:
            rc.append(op)

    uuid = []
    iscsi_initiators.exec_command(
        sudo=True, cmd="cp /etc/fstab /etc/fstab.backup")
    out, err = iscsi_initiators.exec_command(sudo=True, cmd="cat /etc/fstab")
    output = out.read()
    fstab = output.rstrip("\n")
    for device in device_list:
        out, err = iscsi_initiators.exec_command(
            sudo=True, cmd="blkid /dev/mapper/mpa" + device + ""
            " -s UUID -o value", long_running=True)
        output = out.rstrip("\n")
        uuid.append(output)
    for i in range(no_of_luns):
        temp = "\nUUID=" + uuid[i] + "\t/mnt/" + \
               device_list[i] + "/\text4\t_netdev\t0 0"
        fstab += temp
    fstab_file = iscsi_initiators.write_file(
        sudo=True, file_name='/etc/fstab', file_mode='w')
    fstab_file.write(fstab)
    fstab_file.flush()
    mnted_disks = list_mnted_disks(iscsi_initiators)
    iscsi_initiators.exec_command(sudo=True, cmd="reboot", check_ec=False)
    sleep(200)
    iscsi_initiators.reconnect()
    iscsi_util.do_iptables_flush()
    mnted_disks_after_reboot = list_mnted_disks(iscsi_initiators)
    log.info("i/o exit code: {}, failover exit code{}".format(rc[0], rc[1]))
    log.info("disks before reboot:\n" + str(mnted_disks))
    log.info("disks after reboot:\n" + str(mnted_disks_after_reboot))
    log.info("number number of disks before reboot:" + str(len(mnted_disks)))
    log.info("number number of disks after reboot:" + str(len(mnted_disks_after_reboot)))
    if sum(rc) == 0 and mnted_disks_after_reboot == mnted_disks:
        iscsi_util.umount_directory(device_list, iscsi_initiators)
        iscsi_util.dissconect_linux_initiator(iscsi_initiators)
        iscsi_initiators.exec_command(
            sudo=True, cmd="mv /etc/fstab.backup /etc/fstab")
        return 0
    else:
        return 1


def list_mnted_disks(iscsi_initiator):
    out, err = iscsi_initiator.exec_command(
        sudo=True, cmd="df -h | grep '/dev/mapper/mpa'| awk '{print $1}'")
    disks = out.read()
    disks = disks.rstrip()
    disks = sorted(disks.split())
    return disks


def do_failover(iscsi_initiators, device_list, ceph_nodes):
    sleep(10)
    out, err = iscsi_initiators.exec_command(
        sudo=True, cmd="multipath -ll |grep -A 9 mpa" + device_list[0] + " "
        "|grep -A 1 status=active |awk -F "
        '" "'" '{print $(NF - 4)}'")

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
    sleep(40)
    out, err = iscsi_initiators.exec_command(
        sudo=True, cmd="multipath -ll |grep -A 9 mpa" + device_list[0] + " "
        "|grep -A 1 status=active |awk -F "
        '" "'" '{print $(NF - 4)}'")

    active_device_after_reboot = out.read()
    active_device_after_reboot = active_device_after_reboot.rstrip("\n")
    active_device_after_reboot = active_device_after_reboot.split()
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
            print(active_device_status)
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
    print(active_device_status)
    print(active_device)
    print(active_device_after_reboot)
    if active_device[1] != active_device_after_reboot[1] and rc == "active":
        return 0
    else:
        return 1
