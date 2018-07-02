import datetime
import logging
import random
import string
import time

log = logging


class IscsiUtils(object):
    def __init__(self, ceph_nodes):
        self.ceph_nodes = ceph_nodes
        self.disk_list = ''

    def restart_ceph_mon(self):
        for ceph in self.ceph_nodes:
            if ceph.role == 'mon':
                ceph_mon = ceph
                break
        ceph_mon.exec_command(
            sudo=True,
            cmd='systemctl restart ceph-mon@{}'.format(ceph_mon.hostname),
            long_running=True)

    def install_prereq_gw(self):
        for ceph in self.ceph_nodes:
            if ceph.role == 'osd':
                ceph.exec_command(
                    sudo=True,
                    cmd='yum install -y ceph-iscsi-cli tcmu-runner',
                    long_running=True)

    def install_prereq_rhel_client(self):
        for ceph in self.ceph_nodes:
            if ceph.role == 'iscsi-clients':
                ceph.exec_command(
                    sudo=True,
                    cmd='yum install -y iscsi-initiator-utils device-mapper-multipath fio',
                    long_running=True)

    def umount_directory(self, device_list, iscsi_initiators):
        for i in range(len(device_list)):
            iscsi_initiators.exec_command(
                sudo=True, cmd="umount -l /mnt/" + device_list[i])
            log.info("Umounting - mpa" + str(device_list[i]))
        iscsi_initiators.exec_command(sudo=True, cmd="multipath -F")

    def get_devicelist_luns(self, no_of_luns):
        for node in self.ceph_nodes:
            if node.role == 'osd':
                out, err = node.exec_command(sudo=True, cmd="hostname -I")
                osd = out.read()
                break
        t1 = datetime.datetime.now()
        time_plus_5 = t1 + datetime.timedelta(minutes=5)
        iscsi_initiators = self.get_iscsi_initiator_linux()
        while (1):
            t2 = datetime.datetime.now()
            if (t2 <= time_plus_5):
                try:
                    iscsi_initiators.exec_command(sudo=True,
                                                  cmd="iscsiadm -m session")
                    temp = 0

                except BaseException:
                    iscsi_initiators.exec_command(
                        sudo=True, cmd="iscsiadm -m discovery -t "
                                       "sendtargets -p " + osd)
                    temp = 1
                if temp == 1:
                    iscsi_initiators.exec_command(
                        sudo=True,
                        cmd="iscsiadm -m node -T iqn.2003-01.com.redhat.iscsi-"
                            "gw:ceph-igw -l",
                        long_running=True)
                else:
                    iscsi_initiators.exec_command(
                        sudo=True, cmd="iscsiadm -m session --rescan")
                log.info("Sleeping 1 min to discover luns")
                time.sleep(60)
                iscsi_initiators.exec_command(sudo=True, cmd="multipath -ll")
                time.sleep(10)
                out, err = iscsi_initiators.exec_command(
                    sudo=True, cmd='ls /dev/mapper/ | grep mpath'
                                   '', long_running=True)
                output = out
                output = output.rstrip("\n")

                device_list = filter(bool, output.split("mpa"))
                time.sleep(10)
                if (len(device_list) == no_of_luns):
                    device_list = map(lambda s: s.strip(), device_list)
                    device_list.sort(key=len)
                    return device_list

                else:
                    iscsi_initiators.exec_command(sudo=True,
                                                  cmd="multpath -F",
                                                  long_running=True)
                    del device_list[:]
                    log.info("less no of luns found retrying it again..")
            else:
                log.info("Total no of Luns found to map-" + str(no_of_luns))
                log.info("Currently found -mpa" + str(len(device_list)))
                log.info("less no of luns found and time excited..")
                return 1

    def do_iptables_flush(self):
        for ceph in self.ceph_nodes:
            ceph.exec_command(sudo=True, cmd='iptables -P INPUT ACCEPT', long_running=True)
            ceph.exec_command(sudo=True, cmd='iptables -P FORWARD ACCEPT', long_running=True)
            ceph.exec_command(sudo=True, cmd='iptables -P OUTPUT ACCEPT', long_running=True)
            ceph.exec_command(sudo=True, cmd='iptables -t nat -F', long_running=True)
            ceph.exec_command(sudo=True, cmd='iptables -t mangle -F', long_running=True)
            ceph.exec_command(sudo=True, cmd='iptables -F', long_running=True)
            ceph.exec_command(sudo=True, cmd='iptables -X', long_running=True)

    def get_gw_list(self, gw_quantity):
        osds = []
        gw_list = []
        for ceph in self.ceph_nodes:
            if ceph.role == 'osd':
                osds.append(ceph)
        if gw_quantity % 2 != 0 or gw_quantity > len(osds):
            raise ValueError("Wrong number of gateways. Check config and suite")
        for ceph in osds:
            gw_list.append(ceph)
            if len(gw_list) == gw_quantity:
                break
        return gw_list

    def setup_gw(self, gw_list):
        log.info('Configuring gateways')
        ip_list = []
        for gw in gw_list:
            ip_list.append(gw.private_ip)
        trusted_ips = ','.join(ip_list)
        iscsi_gateway_cfg = """
[config]
cluster_name = ceph
gateway_keyring = ceph.client.admin.keyring
api_secure = false
api_ssl_verify = false
trusted_ip_list = {0}
        """.format(trusted_ips)
        for gw in gw_list:
            conf_file = gw.write_file(
                sudo=True, file_name='/etc/ceph/iscsi-gateway.cfg',
                file_mode='w')
            conf_file.write(iscsi_gateway_cfg)
            conf_file.flush()
            conf_file.close()
        for gw in gw_list:
            gw.exec_command(
                sudo=True,
                cmd='systemctl enable rbd-target-gw')
            gw.exec_command(
                sudo=True,
                cmd='systemctl enable rbd-target-api')
            gw.exec_command(
                sudo=True,
                cmd='systemctl start rbd-target-gw',
                long_running=True)
            gw.exec_command(
                sudo=True,
                cmd='systemctl start rbd-target-api',
                long_running=True)
        gw_cli = random.choice(gw_list)
        return gw_cli

    def run_gw(self, gwcli_node, gw_list):
        log.info("Running gateways")
        gwcli_node.exec_command(
            sudo=True,
            cmd='gwcli /iscsi-target create '
                'iqn.2003-01.com.redhat.iscsi-gw:ceph-igw',
            long_running=True)
        time.sleep(15)
        for gw in gw_list:
            gwcli_node.exec_command(
                sudo=True,
                cmd='gwcli /iscsi-target/iqn.2003-01.com.redhat.iscsi-'
                    'gw:ceph-igw/gateways create {} {}'.format(gw.hostname, gw.private_ip),
                long_running=True)
            time.sleep(15)

    def get_iscsi_initiator_linux(self):
        for node in self.ceph_nodes:
            if node.role == "iscsi-clients":
                iscsi_initiator = node
                return iscsi_initiator

    def get_initiatorname(self, full=False):
        for node in self.ceph_nodes:
            if node.role == "iscsi-clients":
                out, err = node.exec_command(
                    sudo=True, cmd='cat /etc/iscsi/'
                                   'initiatorname.iscsi', check_ec=False)
                output = out.read()
                out = output.split('=')
                name = out[1].rstrip("\n")
                if full:
                    return name
                else:
                    host_name = name.split(":")[1]
                    return host_name

    def create_host(self, gwcli_node, init_name):
        log.info('Adding iscsi-clients')
        login = init_name.split(":")[1]
        gwcli_node.exec_command(
            sudo=True,
            cmd='gwcli /iscsi-target/'
                'iqn.2003-01.com.redhat.iscsi-gw:ceph-'
                'igw/hosts create {}'.format(init_name))
        time.sleep(5)
        gwcli_node.exec_command(
            sudo=True,
            cmd='gwcli /iscsi-target/iqn.2003-01.com.redhat.'
                'iscsi-gw:ceph-igw/hosts/{0} '
                'auth {1}/redhat@123456 "|" nochap'.format(init_name, login))
        time.sleep(5)
        log.info('Client {} was added '.format(init_name))

    def create_luns(
            self,
            no_of_luns,
            gwcli_node,
            init_name,
            image_name,
            iosize,
            map_to_client):
        for i in range(0, no_of_luns):
            disk_name = image_name + str(i)
            gwcli_node.exec_command(
                sudo=True,
                cmd='gwcli /disks create rbd image={0} '
                    'size={1}'.format(disk_name, iosize))
            time.sleep(5)
            if map_to_client:
                gwcli_node.exec_command(
                    sudo=True,
                    cmd='gwcli /iscsi-target/iqn.2003-01.com.redhat.iscsi-'
                        'gw:ceph-igw/hosts/{0} disk add rbd.{1}'.format(
                            init_name, disk_name
                        ))
                time.sleep(5)

    def create_directory_with_io(
            self,
            device_list,
            iscsi_initiators,
            io_size,
            do_io):
        if io_size is None:
            io_size = "2G"
        for i in range(len(device_list)):
            iscsi_initiators.exec_command(
                sudo=True, cmd="mkdir /mnt/" + device_list[i])
            iscsi_initiators.exec_command(
                sudo=True,
                cmd="mkfs.ext4 /dev/mapper/mpa" +
                    device_list[i] +
                    " -q",
                long_running=True,
                output=False)
            iscsi_initiators.exec_command(
                sudo=True,
                cmd="mount /dev/mapper/mpa" +
                    device_list[i] +
                    " /mnt/" +
                    device_list[i],
                long_running=True)
            self.temp = "/mnt/" + device_list[i] + ","
            self.disk_list += self.temp
        self.fio_dir = "fio_" + \
                       ''.join(random.choice(string.lowercase +
                                             string.digits) for _ in range(10))
        iscsi_initiators.exec_command(sudo=True, cmd="mkdir ~/" + self.fio_dir)
        iscsi_initiators.exec_command(
            sudo=True,
            cmd="cd ~/" +
                self.fio_dir +
                " ; disk_list=" +
                self.disk_list.rstrip(","),
            long_running=True)
        out, err = iscsi_initiators.exec_command(
            sudo=True, cmd="cd ~/" + self.fio_dir + " "
            "; genfio -d " + self.disk_list.rstrip(",") + ""
            " -b 4k -r ""180 -p -m write -z " + io_size)
        output = out.read()
        output = output.split()

        to_lock_file = """
import fileinput
from os.path import expanduser
path = expanduser("~/")+'{expand}'
x = fileinput.input(path+'/{file_name}',inplace=1)
for line in x:
     line = line.replace('filename','directory')
     print line,
x.close()
                        """.format(expand=self.fio_dir, file_name=output[1])
        to_lock_code = iscsi_initiators.write_file(
            sudo=True,
            file_name='replacer.py',
            file_mode='w')
        to_lock_code.write(to_lock_file)
        to_lock_code.flush()
        out, err = iscsi_initiators.exec_command(
            sudo=True, cmd="python replacer.py")
        if err.read() == 0:
            iscsi_initiators.exec_command(sudo=True, cmd="rm -rf replacer.py")
        iscsi_initiators.exec_command(
            sudo=True,
            cmd="cd ~/" +
                self.fio_dir +
                " ; mv " +
                output[1] +
                " fio.fio")
        print output
        out, err = iscsi_initiators.exec_command(
            sudo=True, cmd="cd ~/" + self.fio_dir + " "
            "; cat " + output[1] + " ; cd ~/"
            "" + self.fio_dir, long_running=True)
        output = out
        temp = output.replace("filename", "directory")
        print self.fio_dir
        print '~/' + self.fio_dir

        conf_file = iscsi_initiators.write_file(
            sudo=True, file_name='fio.fio', file_mode='w')
        conf_file.write(temp)
        if do_io == 1:
            out, err = iscsi_initiators.exec_command(
                sudo=True, cmd="cd ~/" + self.fio_dir + "; fio fio.fio "
                "--verify=md5", long_running=True)
            return err
        else:
            return self.fio_dir
        return 0

    def write_multipath(self, iscsi_initiators):
        multipath = \
            """
defaults {
    user_friendly_names yes
    find_multipaths yes
}
blacklist {
}
devices {
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
}
"""
        log.info('Configuring Multipath IO: ')
        iscsi_initiators.exec_command(
            sudo=True, cmd='mpathconf --enable --with_multipathd y')
        multipath_file = iscsi_initiators.write_file(
            sudo=True, file_name='/etc/multipath.conf', file_mode='w')
        multipath_file.write(multipath)
        multipath_file.flush()
        iscsi_initiators.exec_command(sudo=True,
                                      cmd='systemctl reload multipathd',
                                      long_running=True)

    def write_chap(self, iscsi_name, iscsi_initiators):
        iscsid = """#
node.startup = automatic
node.leading_login = No
node.session.auth.username = {username}
node.session.auth.password = redhat@123456
node.session.timeo.replacement_timeout = 120
node.conn[0].timeo.login_timeout = 15
node.conn[0].timeo.logout_timeout = 15
node.conn[0].timeo.noop_out_interval = 5
node.conn[0].timeo.noop_out_timeout = 5
node.session.err_timeo.abort_timeout = 15
node.session.err_timeo.lu_reset_timeout = 30
node.session.err_timeo.tgt_reset_timeout = 30
node.session.initial_login_retry_max = 8
node.session.queue_depth = 32
node.session.xmit_thread_priority = -20
node.session.iscsi.InitialR2T = No
node.session.iscsi.ImmediateData = Yes
node.session.iscsi.FirstBurstLength = 262144
node.session.iscsi.MaxBurstLength = 16776192
node.conn[0].iscsi.MaxRecvDataSegmentLength = 262144
node.conn[0].iscsi.MaxXmitDataSegmentLength = 0
discovery.sendtargets.iscsi.MaxRecvDataSegmentLength = 32768
node.conn[0].iscsi.HeaderDigest = None
node.session.nr_sessions = 1
node.session.scan = auto
    """.format(username=iscsi_name)
        multipath_file = iscsi_initiators.write_file(
            sudo=True, file_name='/etc/iscsi/iscsid.conf', file_mode='w')
        multipath_file.write(iscsid)
        multipath_file.flush()
