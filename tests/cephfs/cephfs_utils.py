import random
import string
import time
import datetime
import logging
import re
from ceph.ceph import CommandFailed
logger = logging.getLogger(__name__)
log = logger


class FsUtils(object):
    def __init__(self, ceph_nodes):
        self.ceph_nodes = ceph_nodes
        self.clients = []
        self.result_vals = {}
        self.osd_nodes = []
        self.mds_nodes = []
        self.return_counts = {}
        self.active_mds_1 = ''
        self.active_mds_2 = ''
        self.active_mds_node_1 = ''
        self.active_mds_node_2 = ''
        self.mounting_dir = ''
        self.dirs = ''
        self.rc_list = []

    def get_clients(self):
        log.info("Getting Clients")
        for node in self.ceph_nodes:
            if node.role == 'client':
                self.clients.append(node)
            if node.role == 'mds':
                self.mds_nodes.append(node)
            if node.role == 'osd':
                self.osd_nodes.append(node)
        for node in self.ceph_nodes:
            if node.role == 'mon':
                self.mon_node = node
                out, rc = self.mon_node.exec_command(cmd='sudo hostname -I')
                self.mon_node_ip = out.read().rstrip('\n')
                break

        for node in self.clients:
            if node.pkg_type == 'rpm':
                out, rc = node.exec_command(cmd='sudo rpm -qa | grep -w attr')
                output = out.read()
                output.split()
                if 'attr' not in output:
                    node.exec_command(cmd='sudo yum install -y attr')

                out, rc = node.exec_command(
                    cmd='sudo pip  list --format=legacy')
                output = out.read()
                output.split()
                if 'crefi' not in output:
                    node.exec_command(cmd='sudo pip install crefi')

                out, rc = node.exec_command(cmd="sudo ls /home/cephuser")
                output = out.read()
                output.split()
                if 'smallfile' not in output:
                    node.exec_command(
                        cmd='git clone https://github.com/bengland2/'
                            'smallfile.git')

                out, rc = node.exec_command(cmd='sudo rpm -qa')
                output = out.read()
                output.split()

                if 'fio' not in output:
                    node.exec_command(
                        cmd='sudo yum install -y fio')
        self.mounting_dir = ''.join(
            random.choice(
                string.lowercase +
                string.digits) for _ in range(10))
        self.mounting_dir = '/mnt/cephfs_' + self.mounting_dir + '/'
        # seperating clients for fuse and kernel
        self.fuse_clients = self.clients[0:2]
        self.kernel_clients = self.clients[2:4]
        self.result_vals.update({'clients': self.clients})
        self.result_vals.update({'fuse_clients': self.fuse_clients})
        self.result_vals.update({'kernel_clients': self.kernel_clients})
        self.result_vals.update({'mon_node_ip': self.mon_node_ip})
        self.result_vals.update({'mon_node': self.mon_node})
        self.result_vals.update({'osd_nodes': self.osd_nodes})
        self.result_vals.update({'mds_nodes': self.mds_nodes})
        self.result_vals.update({'mounting_dir': self.mounting_dir})

        return self.result_vals, 0

    def auth_list(self, clients, mon_node):
        for node in clients:
            log.info("Giving required permissions for clients from MON node:")
            mon_node.exec_command(
                cmd="sudo ceph auth get-or-create client.%s mon 'allow *' mds "
                    "'allow *, allow rw path=/' osd 'allow "
                    "rw pool=cephfs_data'"
                    " -o /etc/ceph/ceph.client.%s.keyring" %
                (node.hostname, node.hostname))
            self.rc_list.append(mon_node.exit_status)
            out, rc = mon_node.exec_command(
                sudo=True, cmd='cat /etc/ceph/ceph.client.%s.keyring' %
                (node.hostname))
            self.rc_list.append(mon_node.exit_status)
            keyring = out.read()
            key_file = node.write_file(
                sudo=True,
                file_name='/etc/ceph/ceph.client.%s.keyring' % (node.hostname),
                file_mode='w')
            key_file.write(keyring)
            key_file.flush()
            self.rc_list.append(node.exit_status)
            node.exec_command(
                cmd="sudo chmod 644 /etc/ceph/ceph.client.%s.keyring" %
                (node.hostname))
            self.rc_list.append(node.exit_status)
            rc_set = set(self.rc_list)
            if len(rc_set) == 1:
                return 0
            else:
                return 1

    def fuse_mount(self, fuse_clients, mounting_dir):
        for client in fuse_clients:
            log.info("Creating mounting dir:")
            client.exec_command(cmd='sudo mkdir %s' % (mounting_dir))
            log.info(
                "Mounting fs with ceph-fuse on client %s:" %
                (client.hostname))
            op, rc = client.exec_command(
                cmd="sudo ceph-fuse -n client.%s %s" %
                (client.hostname, mounting_dir))
            out, rc = client.exec_command(cmd='mount')
            mount_output = out.read()
            mount_output.split()
            log.info("Checking if fuse mount is is passed of failed:")
            if 'fuse' in mount_output:
                return 0
            else:
                return 1

    def kernel_mount(self, kernel_clients, mounting_dir, mon_node_ip):
        for client in kernel_clients:
            log.info("Creating mounting dir:")
            client.exec_command(cmd='sudo mkdir %s' % (mounting_dir))
            out, rc = client.exec_command(
                cmd='sudo ceph auth get-key client.%s' %
                (client.hostname))
            secret_key = out.read().rstrip('\n')
            mon_node_ip = mon_node_ip.replace(" ", "")
            op, rc = client.exec_command(
                cmd='sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s' % (
                    mon_node_ip, mounting_dir, client.hostname, secret_key))
            out, rc = client.exec_command(cmd='mount')
            mount_output = out.read()
            mount_output.split()
            log.info("Checking if kernel mount is is passed of failed:")
            if '%s:6789:/' % (mon_node_ip) in mount_output:
                return 0
            else:
                return 1

    def read_write_IO(self, clients, mounting_dir, *args, **kwargs):
        for client in clients:
            log.info("Performing read and write on clients")
            rand_num = random.randint(1, 5)
            fio_read = "sudo fio --name=global --rw=read --size=%d%s " \
                       "--name=%s_%d_%d_%d --directory=%s%s --runtime=300"
            fio_write = "sudo fio --name=global --rw=write --size=%d%s " \
                        "--name=%s_%d_%d_%d --directory=%s%s --runtime=300 " \
                        "--verify=meta"
            fio_readwrite = "sudo fio --name=global --rw=readwrite " \
                            "--size=%d%s" \
                            " --name=%s_%d_%d_%d --directory=%s%s " \
                            "--runtime=300 " \
                            "--verify=meta"
            if kwargs:
                for i, j in kwargs.items():
                    self.dir_name = j
            else:
                self.dir_name = ''
            if args:
                if 'g' in args:
                    size = 'g'
                elif 'm' in args:
                    size = 'm'
                else:
                    size = 'k'
                for arg in args:
                    if arg == 'read':
                        if size == 'g':
                            rand_size = random.randint(1, 5)
                            client.exec_command(
                                cmd=fio_read %
                                (rand_size,
                                 size,
                                 client.hostname,
                                 rand_size,
                                 rand_size,
                                 rand_num,
                                 mounting_dir,
                                 self.dir_name),
                                long_running=True)
                            self.return_counts = self.io_verify(client)
                        elif size == 'm':
                            for num in range(0, 10):
                                rand_size = random.randint(1, 5)
                                client.exec_command(
                                    cmd=fio_read %
                                    (rand_size,
                                     size,
                                     client.hostname,
                                     rand_size,
                                     rand_size,
                                     num,
                                     mounting_dir,
                                     self.dir_name),
                                    long_running=True)
                                self.return_counts = self.io_verify(client)
                            break

                        else:
                            for num in range(0, 500):
                                rand_size = random.randint(50, 100)
                                client.exec_command(
                                    cmd=fio_read %
                                    (rand_size,
                                     size,
                                     client.hostname,
                                     rand_size,
                                     rand_size,
                                     num,
                                     mounting_dir,
                                     self.dir_name),
                                    long_running=True)
                                self.return_counts = self.io_verify(client)
                            break

                    elif arg == 'write':
                        if size == 'g':
                            rand_size = random.randint(1, 5)
                            client.exec_command(
                                cmd=fio_write %
                                (rand_size,
                                 size,
                                 client.hostname,
                                 rand_size,
                                 rand_size,
                                 rand_num,
                                 mounting_dir,
                                 self.dir_name),
                                long_running=True)
                            self.return_counts = self.io_verify(client)
                            break

                        elif size == 'm':
                            for num in range(0, 10):
                                rand_size = random.randint(1, 5)
                                client.exec_command(
                                    cmd=fio_write %
                                    (rand_size,
                                     size,
                                     client.hostname,
                                     rand_size,
                                     rand_size,
                                     num,
                                     mounting_dir,
                                     self.dir_name),
                                    long_running=True)
                                self.return_counts = self.io_verify(client)
                            break

                        else:
                            for num in range(0, 500):
                                rand_size = random.randint(50, 100)
                                client.exec_command(
                                    cmd=fio_write %
                                    (rand_size,
                                     size,
                                     client.hostname,
                                     rand_size,
                                     rand_size,
                                     num,
                                     mounting_dir,
                                     self.dir_name),
                                    long_running=True)
                                self.return_counts = self.io_verify(client)
                            break

                    elif arg == 'readwrite':
                        if size == 'g':
                            rand_size = random.randint(1, 5)

                            client.exec_command(
                                cmd=fio_readwrite %
                                (rand_size,
                                 size,
                                 client.hostname,
                                 rand_num,
                                 rand_num,
                                 rand_size,
                                 mounting_dir,
                                 self.dir_name),
                                long_running=True)
                            self.return_counts = self.io_verify(client)
                            break

                        elif size == 'm':
                            for num in range(0, 10):
                                rand_size = random.randint(50, 100)
                                client.exec_command(
                                    cmd=fio_readwrite %
                                    (rand_size,
                                     size,
                                     client.hostname,
                                     rand_size,
                                     num,
                                     mounting_dir,
                                     self.dir_name),
                                    long_running=True)
                                self.return_counts = self.io_verify(client)
                            break

                        else:
                            for num in range(0, 500):
                                rand_size = random.randint(50, 100)
                                client.exec_command(
                                    cmd=fio_readwrite %
                                    (rand_size,
                                     size,
                                     client.hostname,
                                     rand_size,
                                     num,
                                     mounting_dir,
                                     self.dir_name))
                                self.return_counts = self.io_verify(client)
                            break

            else:
                size = 'k'
                for num in range(0, 500):
                    rand_size = random.randint(50, 100)
                    client.exec_command(
                        cmd=fio_readwrite %
                        (rand_size,
                         size,
                         client.hostname,
                         rand_size,
                         rand_size,
                         num,
                         mounting_dir,
                         self.dir_name),
                        long_running=True)
                    client.exec_command(
                        cmd="sudo touch %s%s_%d" %
                        (mounting_dir, client.hostname, rand_size))
                    self.return_counts = self.io_verify(client)
            break
        return self.return_counts, 0

    def file_locking(self, client, mounting_dir):

        to_lock_file = """
import fcntl
import subprocess
import time
try:
    f = open('%sto_test_file_lock', 'w+')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print "locking file:--------------------------------"
    subprocess.check_output(["sudo","fio","--name=global","--rw=write","--size=10m","--name=to_test_file_lock","--directory=%s","--runtime=10","--verify=meta"])
except IOError as e:
    print e
finally:
    print "Unlocking file:------------------------------"
    fcntl.lockf(f,fcntl.LOCK_UN)
                    """ % (mounting_dir, mounting_dir)
        to_lock_code = client.write_file(
            sudo=True,
            file_name='/home/cephuser/file_lock.py',
            file_mode='w')
        to_lock_code.write(to_lock_file)
        to_lock_code.flush()
        out, rc = client.exec_command(
            cmd="sudo python /home/cephuser/file_lock.py")
        output = out.read()
        output.split()
        if 'Errno 11' in output:
            log.info("File locking achieved, data is not corrupted")
        elif 'locking' in output:
            log.info("File locking achieved, data is not corrupted")
        else:
            log.error("Data is corrupted")

        out, rc = client.exec_command(
            cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" %
            (mounting_dir))
        md5sum_file_lock = out.read()
        return md5sum_file_lock, 0

    def mkdir(self, clients, range1, range2, mounting_dir, dir_name):
        for client in clients:
            for num in range(range1, range2):
                log.info("Creating Directories")
                out, rc = client.exec_command(
                    cmd='sudo mkdir %s%s_%d' %
                    (mounting_dir, dir_name, num))
                print out.read()
                out, rc = client.exec_command(
                    cmd='sudo ls %s | grep %s' %
                    (mounting_dir, dir_name))
                self.dirs = out.read()
            break
        return self.dirs, 0

    def activate_multiple_mdss(self, mds_nodes):
        for node in mds_nodes:
            log.info("Activating Multiple MDSs:")
            node.exec_command(
                cmd="sudo ceph fs set cephfs allow_multimds true "
                    "--yes-i-really-mean-it")
            log.info("Setting max mdss 2:")
            node.exec_command(cmd="sudo ceph fs set cephfs max_mds 2")
            return 0

    def allow_dir_fragmentation(self, mds_nodes):
        """
        This function will performs dir frangmentation on MDS node
        :param mds_nodes: Taken from get_clients()
        """
        log.info("Allowing directorty fragmenation for splitting and merging")
        for node in mds_nodes:
            node.exec_command(cmd='sudo ceph fs set cephfs allow_dirfrags 1')
            break
        return 0

    def mds_fail_over(self, mds_nodes):
        rand = random.randint(0, 1)
        for node in mds_nodes:
            log.info("Failing MDS %d" % (rand))
            node.exec_command(cmd='sudo ceph mds fail %d' % (rand))
            break

        return 0

    def get_active_mdss(self, mds_nodes):
        for node in mds_nodes:
            out, rc = node.exec_command(
                cmd="sudo ceph mds stat | grep -o -P '(?<=0=)."
                    "*(?==up:active,)'")
            self.active_mds_1 = out.read().rstrip('\n')
            out, rc = node.exec_command(
                cmd="sudo ceph mds stat | grep -o -P '(?<=1=)."
                    "*(?==up:active)'")
            self.active_mds_2 = out.read().rstrip('\n')
            break

        for node in mds_nodes:
            if node.hostname == self.active_mds_1:
                self.active_mds_node_1 = node
            if node.hostname == self.active_mds_2:
                self.active_mds_node_2 = node

        return self.active_mds_node_1, self.active_mds_node_2, 0

    def get_mds_info(self, active_mds_node_1, active_mds_node_2, **kwargs):
        for key, val in kwargs.iteritems():
            if val == 'get subtrees':
                out_1, err_1 = active_mds_node_1.exec_command(
                    cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                        "asok %s | grep path" %
                    (active_mds_node_1.hostname, val))
                out_2, err_2 = active_mds_node_2.exec_command(
                    cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                        "asok %s| grep path" %
                    (active_mds_node_2.hostname, val))
                return out_1.read().rstrip('\n'), out_2.read().rstrip('\n'), 0

            elif val == 'session ls':
                out_1, err_1 = active_mds_node_1.exec_command(
                    cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                        "asok %s" %
                    (active_mds_node_1.hostname, val))
                out_2, err_2 = active_mds_node_2.exec_command(
                    cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s."
                        "asok %s" %
                    (active_mds_node_2.hostname, val))
                return out_1.read().rstrip('\n'), out_2.read().rstrip('\n'), 0

    def stress_io(
            self,
            clients,
            mounting_dir,
            dir_name,
            range1,
            range2,
            **kwargs):
        for client in clients:
            for num in range(range1, range2):
                for key, val in kwargs.iteritems():
                    if val == 'touch':
                        out, rc = client.exec_command(
                            cmd="sudo touch %s%s/%d.txt" %
                            (mounting_dir, dir_name, num))
                        self.return_counts = self.io_verify(client)
                    elif val == 'fio':
                        for i in range(0, 10):
                            rand_num = random.randint(1, 5)
                            out, rc = client.exec_command(
                                cmd="sudo fio --name=global --rw=write "
                                    "--size=%dm --name=%s_%d --directory=%s%s "
                                    "--runtime=10 --verify=meta" %
                                (rand_num, client.hostname, rand_num,
                                 mounting_dir, dir_name), long_running=True)
                            self.return_counts = self.io_verify(client)

                    elif val == 'dd':
                        for i in range(0, 10):
                            rand_bs = random.randint(1, 5)
                            rand_count = random.randint(1, 5)
                            out, rc = client.exec_command(
                                cmd="sudo dd if=/dev/zero of=%s%s/%s%d.txt "
                                    "bs=%dM count=%d" %
                                (mounting_dir, dir_name, client.hostname,
                                 num, rand_bs, rand_count), long_running=True)
                            self.return_counts = self.io_verify(client)

                    elif val == 'crefi':
                        for i in range(0, 6):
                            ops = [
                                'create',
                                'rename',
                                'chmod',
                                'chown',
                                'chgrp',
                                'setxattr']
                            rand_ops = random.choice(ops)
                            ftypes = ['text', 'sparse', 'binary', 'tar']
                            rand_filetype = random.choice(ftypes)
                            rand_count = random.randint(2, 10)
                            if ops == 'create':
                                out, rc = client.exec_command(
                                    cmd='sudo crefi %s%s --fop create -t %s '
                                        '--multi -b 10 -d 10 -n 10 -T 10 '
                                        '--random --min=1K --max=%dK' %
                                    (mounting_dir, dir_name, rand_filetype,
                                     rand_count), long_running=True)

                            else:
                                out, rc = client.exec_command(
                                    cmd='sudo crefi %s%s --fop %s -t %s '
                                        '--multi -b 10 -d 10 -n 10 -T 10 '
                                        '--random --min=1K --max=%dK' %
                                    (mounting_dir, dir_name, rand_ops,
                                     rand_filetype, rand_count),
                                    long_running=True)
                            self.return_counts = self.io_verify(client)

                    elif val == 'smallfile_delete':
                        out, rc = client.exec_command(
                            cmd='sudo python /home/cephuser/smallfile/'
                                'smallfile_cli.py --operation create '
                                '--threads 4 --file-size 1024 --files 10 '
                                '--top %s%s ' %
                            (mounting_dir, dir_name), long_running=True)
                        self.return_counts = self.io_verify(client)

                        client.exec_command(
                            cmd='sudo python /home/cephuser/smallfile/'
                                'smallfile_cli.py --operation delete '
                                '--threads 4 --file-size 1024 --files 10 '
                                '--top %s%s ' %
                            (mounting_dir, dir_name), long_running=True)
                        self.return_counts = self.io_verify(client)

                    else:
                        log.error("IO type not specifiesd")
                        return 1
            break
        return self.return_counts, 0

    def pinned_dir_io_mdsfailover(
            self,
            clients,
            mounting_dir,
            dir_name,
            range1,
            range2,
            num_of_files,
            mds_fail_over,
            mds_nodes):
        log.info("Performing IOs on clients")
        for client in clients:
            for num in range(range1, range2):
                log.info("Performing MDS failover:")
                mds_fail_over(mds_nodes)
                out, rc = client.exec_command(
                    cmd='sudo crefi -n %d %s%s_%d' %
                    (num_of_files, mounting_dir, dir_name, num),
                    long_running=True)
                self.return_counts = self.io_verify(client)
            break
        return self.return_counts, 0

    def filesystem_utilities(
            self,
            clients,
            mounting_dir,
            dir_name,
            range1,
            range2):
        commands = ['ls', 'rm -rf', 'ls -l']
        for client in clients:
            for num in range(range1, range2):
                if random.choice(commands) == 'ls':
                    out, rc = client.exec_command(
                        cmd='sudo ls %s%s_%d' %
                        (mounting_dir, dir_name, num))
                elif random.choice(commands) == 'ls -l':
                    out, rc = client.exec_command(
                        cmd='sudo ls -l %s%s_%d/' %
                        (mounting_dir, dir_name, num))
                else:
                    out, rc = client.exec_command(
                        cmd='sudo rm -rf %s%s_%d/*' %
                        (mounting_dir, dir_name, num))
                print out.read()
                self.return_counts = self.io_verify(client)

            break
        return self.return_counts, 0

    def fstab_entry(self, clients, mounting_dir, **kwargs):
        for client in clients:
            out, rc = client.exec_command(cmd='mount')
            mount_output = out.read()
            mount_output.split()
            if 'fuse' in mount_output:
                out, rc = client.exec_command(cmd='sudo cat /etc/fstab')
                out = out.read()

                fuse_fstab = """
{old_entry}
#DEVICE         PATH                 TYPE           OPTIONS
none           {mounting_dir}       {fuse}          ceph.id={client_hostname}
,ceph.conf=/etc/ceph/ceph.conf,_netdev,defaults  0 0
                        """.format(old_entry=out, fuse='fuse.ceph',
                                   mounting_dir=mounting_dir,
                                   client_hostname=client.hostname)
                fstab = client.write_file(
                    sudo=True,
                    file_name='/etc/fstab',
                    file_mode='w')
                fstab.write(fuse_fstab)
                fstab.flush()
                return 0
            else:
                out, rc = client.exec_command(
                    cmd='sudo ceph auth get-key client.%s' %
                    (client.hostname))
                secret_key = out.read().rstrip('\n')
                out, rc = client.exec_command(cmd='sudo cat /etc/fstab')
                out = out.read()
                if kwargs:
                    for key, val in kwargs.iteritems():
                        mon_node_ip = val

                else:
                    log.error("Mon Ip not specified")
                    return 1
                mon_node_ip = mon_node_ip.replace(" ", "")
                kernel_fstab = '''
{old_entry}
#DEVICE              PATH                TYPE          OPTIONS
{mon_ip}:6789:/      {mounting_dir}      {ceph}        name={client_hostname}
,secret={secret_key},_netdev,noatime 00
                        '''.format(
                    old_entry=out,
                    ceph='ceph',
                    mon_ip=mon_node_ip,
                    mounting_dir=mounting_dir,
                    client_hostname=client.hostname,
                    secret_key=secret_key)
                fstab = client.write_file(
                    sudo=True,
                    file_name='/etc/fstab',
                    file_mode='w')
                fstab.write(kernel_fstab)
                fstab.flush()
                return 0

    def reboot(self, clients, **kwargs):
        timeout = 600
        for client in clients:
            client.exec_command(cmd='sudo reboot', check_ec=False)
            self.return_counts.update({client.hostname: client.exit_status})
            timeout = datetime.timedelta(seconds=timeout)
            starttime = datetime.datetime.now()
            while True:
                try:
                    client.reconnect()
                    break
                except BaseException:
                    if datetime.datetime.now() - starttime > timeout:
                        log.error(
                            'Failed to reconnect to the node {node} after '
                            'reboot '.format(
                                node=client.ip_address))
                        time.sleep(5)
                        raise RuntimeError(
                            'Failed to reconnect to the node {node} after '
                            'reboot '.format(
                                node=client.ip_address))
            if kwargs:
                out, rc = client.exec_command(
                    cmd='sudo crefi %s --fop create --multi -b 10 -d 10 -n 10 '
                        '-T 10 --random --min=1K --max=10K' %
                    (self.mounting_dir))
                print out.read()
                self.return_counts.update(
                    {client.hostname: client.exit_status})
            break
        return self.return_counts, 0

    def io_verify(self, client):
        if client.exit_status == 0:
            self.return_counts.update({client.hostname: client.exit_status})
            log.info("Client IO is going on,success")
        else:
            self.return_counts.update({client.hostname: client.exit_status})
            print self.return_counts
            raise CommandFailed("Client IO got interrupted")
        return self.return_counts

    def rc_verify(self, tc, return_counts):
        return_codes_set = set(return_counts.values())
        if len(return_codes_set) == 1:
            out = "Test case %s Passed" % (tc)
            if tc == '':
                output = "Data validation success"
                return output
            else:
                return out

        else:
            out = "Test case %s Failed" % (tc)
            return out

    def del_cephfs(self, mds_nodes, fs_name):
        for mds in mds_nodes:
            mds.exec_command(
                cmd='sudo systemctl stop ceph-mds@%s.service' %
                (mds.hostname))
            self.return_counts.update({mds.hostname: mds.exit_status})
        for mds in mds_nodes:
            log.info("Deleting fs:")
            mds.exec_command(
                cmd='sudo ceph fs rm %s --yes-i-really-mean-it' %
                (fs_name))
            self.return_counts.update({mds.hostname: mds.exit_status})
            return self.return_counts, 0

    def create_fs(self, mds_nodes, fs_name):
        for mds in mds_nodes:
            mds.exec_command(
                cmd='sudo systemctl start ceph-mds@%s.service' %
                (mds.hostname))
            mds.exec_command(cmd='sudo ceph osd pool create fs_data 64 64')
            mds.exec_command(cmd='sudo ceph osd pool create fs_metadata 64 64')
            mds.exec_command(
                cmd='sudo ceph fs new %s fs_metadata fs_data --force '
                    '--allow-dangerous-metadata-overlay' %
                (fs_name))
            out, rc = mds.exec_command(cmd='sudo ceph fs ls')
            if fs_name in out.read():
                log.info("New cephfs created")
                self.return_counts.update({mds.hostname: mds.exit_status})
                return self.return_counts, 0
            else:
                self.return_counts.update({mds.hostname: mds.exit_status})
                return self.return_counts, 0

    def add_pool(self, mon_node, fs_name, pool_name):
        mon_node.exec_command(
            cmd='sudo ceph osd pool create %s 64 64' %
            (pool_name))
        mon_node.exec_command(
            cmd='sudo ceph fs add_data_pool %s  %s' %
            (fs_name, pool_name))
        out, rc = mon_node.exec_command(cmd='sudo ceph fs ls')
        output = out.read().split()
        if pool_name in output:
            log.info("adding new pool to cephfs successfull")
            self.return_counts.update(
                {mon_node.hostname: mon_node.exit_status})
            return self.return_counts, 0
        else:
            self.return_counts.update(
                {mon_node.hostname: mon_node.exit_status})
            print self.return_counts
            return self.return_counts, 0

    def remove_pool(self, mon_node, fs_name, pool_name):
        mon_node.exec_command(
            cmd='sudo ceph fs rm_data_pool %s %s' %
            (fs_name, pool_name))
        out, rc = mon_node.exec_command(cmd='sudo ceph fs ls')
        output = out.read().split()
        if pool_name not in output:
            log.info("removing pool %s to cephfs successfull" % (pool_name))
            self.return_counts.update(
                {mon_node.hostname: mon_node.exit_status})
            return self.return_counts, 0
        else:
            self.return_counts.update(
                {mon_node.hostname: mon_node.exit_status})
            return self.return_counts, 1

    def set_attr(self, mds_nodes, fs_name):
        max_file_size = '1099511627776'
        for node in mds_nodes:
            attrs = [
                'max_mds',
                'max_file_size',
                'allow_new_snaps',
                'inline_data',
                'cluster_down',
                'allow_multimds',
                'allow_dirfrags',
                'balancer',
                'standby_count_wanted']
            print "-----------------------------------------------------"
            if attrs[0]:
                node.exec_command(
                    cmd='sudo ceph fs set  %s %s 2' %
                    (fs_name, attrs[0]))
                out, rc = node.exec_command(
                    cmd='sudo ceph fs get %s| grep %s' %
                    (fs_name, attrs[0]))
                out = out.read().rstrip().replace('\t', '')
                if "max_mds2" in out:
                    log.info("max mds attr passed")
                    log.info("Reverting:")
                    node.exec_command(
                        cmd='sudo ceph fs set  %s %s 1' %
                        (fs_name, attrs[0]))
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs get %s| grep %s' %
                        (fs_name, attrs[0]))
                    out = out.read().rstrip().replace('\t', '')
                    if "max_mds1" in out:
                        log.info("Setting max mds to 1")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("Setting max mds attr failed")
                        return self.return_counts, 1
            print "-----------------------------------------------------"
            if attrs[1]:
                node.exec_command(
                    cmd='sudo ceph fs set  %s %s 65536' %
                    (fs_name, attrs[1]))
                out, rc = node.exec_command(
                    cmd='sudo ceph fs get %s| grep %s' %
                    (fs_name, attrs[1]))
                out = out.read().rstrip()
                print out
                if 'max_file_size	65536' in out:
                    log.info("max file size attr tested successfully")
                    log.info("Reverting:")
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs set  %s %s %s' %
                        (fs_name, attrs[1], max_file_size))
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs get %s| grep %s' %
                        (fs_name, attrs[1]))
                    if max_file_size in out.read():
                        log.info("max file size attr reverted successfully")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("max file size attr failed")
                        return self.return_counts, 1
                else:
                    self.return_counts.update(
                        {node.hostname: node.exit_status})
                    print self.return_counts
                    return self.return_counts, 1

            print "----------------------------------------------------------"
            if attrs[2]:
                out, rc = node.exec_command(
                    cmd='sudo ceph fs set %s %s 1 --yes-i-really-mean-it' %
                    (fs_name, attrs[2]))
                if 'enabled new snapshots' in rc.read():
                    log.info('allow new snap flag is set successfully')
                    log.info("Reverting:")
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs set %s %s 0 --yes-i-really-mean-it' %
                        (fs_name, attrs[2]))
                    if 'disabled new snapshots' in rc.read():
                        print out.read()
                        log.info("Reverted allow_new_snaps successfully")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error('failed to revert new snap shots attr')
                        return self.return_counts, 1
                else:
                    self.return_counts.update(
                        {node.hostname: node.exit_status})
                    print self.return_counts
                    log.error('failed to enable new snap shots')
                    return self.return_counts, 1

            print "----------------------------------------------------------"
            if attrs[3]:
                out, rc = node.exec_command(
                    cmd='sudo ceph fs set %s %s 1 --yes-i-really-mean-it' %
                    (fs_name, attrs[3]))
                if 'inline data enabled' in rc.read():
                    log.info("inline data set succesafully")
                    log.info("Reverting:")
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs set %s %s 0 --yes-i-really-mean-it' %
                        (fs_name, attrs[3]))
                    if 'inline data disabled' in rc.read():
                        log.info("inline data disabled successfully")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})

                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("inline data attr failed")
                        return self.return_counts, 1
                else:
                    self.return_counts.update(
                        {node.hostname: node.exit_status})
                    print self.return_counts
                    log.error("inline data attr failed")
                    return self.return_counts, 1

            print "----------------------------------------------------------"
            if attrs[4]:
                out, rc = node.exec_command(
                    cmd='sudo ceph fs set %s %s 1' %
                    (fs_name, attrs[4]))
                if 'marked down' in rc.read():
                    log.info("cluster_down attr set successfully")
                    log.info("Reverting:")
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs set %s %s 0' %
                        (fs_name, attrs[4]))
                    if 'marked up' in rc.read():
                        log.info("cluster_down attr reverted successfully")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("cluster_down attr set failed")
                        return self.return_counts, 1

                else:
                    self.return_counts.update(
                        {node.hostname: node.exit_status})
                    print self.return_counts
                    log.error("cluster_down attr set failed")
                    return self.return_counts, 1

            print "----------------------------------------------------------"
            if attrs[5]:
                out, rc = node.exec_command(
                    cmd='sudo ceph fs set %s  %s 1' %
                    (fs_name, attrs[5]))
                if 'enabled creation of more than 1 active MDS' in rc.read():
                    log.info("allow_multimds attr set successfully")
                    log.info("Reverting:")
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs set %s %s 0' %
                        (fs_name, attrs[5]))
                    if 'disallowed increasing the cluster size past ' \
                       '1' in rc.read():
                        log.info("allow_multimds attr reverted successfully")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})

                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("allow_multimds attr failed")
                        return self.return_counts, 1

                else:
                    self.return_counts.update(
                        {node.hostname: node.exit_status})
                    print self.return_counts
                    log.error("allow_multimds attr failed")
                    return self.return_counts, 1

            print "----------------------------------------------------------"
            if attrs[6]:
                log.info(
                    "Allowing directorty fragmenation for splitting "
                    "and merging")
                out, rc = node.exec_command(
                    cmd='sudo ceph fs set %s  %s 1' %
                    (fs_name, attrs[6]))
                if 'enabled directory fragmentation' in rc.read():
                    log.info("directory fragmentation enabled successfully")
                    log.info("disabling directorty fragmenation")
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs set %s %s 0' %
                        (fs_name, attrs[6]))
                    if 'disallowed new directory fragmentation' in rc.read():
                        log.info(
                            "directorty fragmenation disabled successfully")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})

                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("allow_dirfrags set attr failed")
                        return self.return_counts, 1
                else:
                    self.failure_info.update({node.hostname: node.exit_status})
                    print self.failure_info
                    log.error("allow_dirfrags set attr failed")
                    return self.return_counts, 1

            print "----------------------------------------------------------"
            if attrs[7]:
                log.info("setting the metadata load balancer")
                out, rc = node.exec_command(
                    cmd='sudo ceph fs set %s  %s 2' %
                    (fs_name, attrs[7]))
                out, rc = node.exec_command(
                    cmd='sudo ceph fs get %s| grep %s' %
                    (fs_name, attrs[7]))
                out = out.read().rstrip()
                if 'balancer	2' in out:
                    log.info("metadata load balancer attr set successfully ")
                    log.info("reverting:")
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs set %s %s 1' %
                        (fs_name, attrs[7]))
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs get %s| grep %s' %
                        (fs_name, attrs[7]))
                    out = out.read().rstrip()

                    if 'balancer	1' in out:
                        log.info(
                            "metadata load balancer attr reverted "
                            "successfully ")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("metadata load balancer attr failed")
                        return self.return_counts, 1
                else:
                    self.return_counts.update(
                        {node.hostname: node.exit_status})
                    print self.return_counts
                    log.error("metadata load balancer attr failed")
                    return self.return_counts, 1

            print "----------------------------------------------------------"
            if attrs[8]:
                log.info("setting standby_count_wanted")
                node.exec_command(
                    cmd='sudo ceph fs set %s %s 2' %
                    (fs_name, attrs[8]))
                out, rc = node.exec_command(
                    cmd='sudo ceph fs get %s' %
                    (fs_name))
                out = out.read().rstrip()
                if 'standby_count_wanted	2' in out:
                    log.info("standby_count_wanted attr set successfully")
                    log.info("Reverting:")
                    node.exec_command(
                        cmd='sudo ceph fs set %s %s 1' %
                        (fs_name, attrs[8]))
                    out, rc = node.exec_command(
                        cmd='sudo ceph fs get %s' %
                        (fs_name))
                    out = out.read().rstrip()
                    if 'standby_count_wanted	1' in out:
                        log.info(
                            "standby_count_wanted attr reverted successfully")
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                    else:
                        self.return_counts.update(
                            {node.hostname: node.exit_status})
                        print self.return_counts
                        log.error("standby_count_wanted setting failed")
                        return self.return_counts, 1

                else:
                    self.return_counts.update(
                        {node.hostname: node.exit_status})
                    print self.return_counts
                    log.error("standby_count_wanted setting failed")
                    return self.return_counts, 1

            return self.return_counts, 0

    def rsync(self, clients, source_dir, dest_dir):
        for client in clients:
            client.exec_command(
                cmd='sudo rsync -zvh %s %s' %
                (source_dir, dest_dir))
            if client.exit_status == 0:
                log.info("Files synced successfully")
            else:
                raise CommandFailed('File sync failed')
            break
        return self.return_counts, 0

    def auto_evict(self, active_mds_node, clients, rank):
        grep_cmd = """
        sudo ceph tell mds.%d client ls | grep '"hostname":'
        """
        op, rc = active_mds_node.exec_command(cmd=grep_cmd % (rank))
        op = op.read().split('\n')
        hostname = op[0].strip('"hostname": ').strip('"').strip('",')
        print hostname
        grep_pid_cmd = """
               sudo ceph tell mds.%d client ls | grep '"pid":'
               """
        out, rc = active_mds_node.exec_command(cmd=grep_pid_cmd % (rank))
        out = out.read()
        client_pid = re.findall(r"\d+", out)
        for client in clients:
            if client.hostname == hostname:
                while True:
                    try:
                        for id in client_pid:
                            client.exec_command(cmd='sudo kill -9 %s' % (id))
                            return 0
                    except Exception as e:
                        print e
                        pass

    def manual_evict(self, active_mds_node, rank):
        grep_cmd = """
        sudo ceph tell mds.%d client ls | grep '"id":'
        """
        out, rc = active_mds_node.exec_command(cmd=grep_cmd % (rank))
        out = out.read()
        client_ids = re.findall(r"\d+", out)
        print '--------------------------------'
        grep_cmd = """
               sudo ceph tell mds.%d client ls | grep '"inst":'
               """
        log.info("Getting IP address of Evicted client")
        out, rc = active_mds_node.exec_command(cmd=grep_cmd % (rank))
        out = out.read()
        op = re.findall(r"\d+.+\d+.", out)
        ip_add = op[0]
        ip_add = ip_add.split(' ')
        ip_add = ip_add[1].strip('",')
        print '------------------------'
        id_cmd = 'sudo ceph tell mds.%d client evict id=%s'
        for id in client_ids:
            active_mds_node.exec_command(cmd=id_cmd % (rank, id))
            break

        return ip_add

    def osd_blacklist(self, active_mds_node, ip_add):
        out, rc = active_mds_node.exec_command(
            cmd='sudo ceph osd blacklist ls')
        if ip_add in out.read():
            active_mds_node.exec_command(
                cmd='sudo ceph osd blacklist rm %s' %
                (ip_add))
            if 'listed 0 entries' in out.read():
                log.info(
                    "Evicted client %s unblacklisted successfully" %
                    (ip_add))
        return 0

    def config_blacklist_auto_evict(self, active_mds_node, rank, **kwargs):
        if kwargs:
            active_mds_node.exec_command(
                cmd='sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok'
                    ' config set mds_session_blacklist_on_timeout true' %
                (active_mds_node.hostname))
            return 0
        else:
            active_mds_node.exec_command(
                cmd='sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok '
                    'config set mds_session_blacklist_on_timeout false' %
                (active_mds_node.hostname))
            self.auto_evict(active_mds_node, self.fuse_clients, rank)
            log.info("Waiting 300 seconds for auto eviction---")
            time.sleep(300)
            return 0

    def config_blacklist_manual_evict(self, active_mds_node, rank, **kwargs):
        if kwargs:
            active_mds_node.exec_command(
                cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok"
                    " config set mds_session_blacklist_on_evict true" %
                (active_mds_node.hostname))
            return 0
        else:
            active_mds_node.exec_command(
                cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok "
                "config set mds_session_blacklist_on_evict false" %
                (active_mds_node.hostname))
            ip_add = self.manual_evict(active_mds_node, rank)
            print '----------------------------'
            out, rc = active_mds_node.exec_command(
                cmd='sudo ceph osd blacklist ls')
            print out.read()
            print '----------------------------'
            if ip_add not in out.read():
                return 0

    def getfattr(self, clients, mounting_dir, file_name):
        for client in clients:
            client.exec_command(
                cmd='sudo touch %s%s' %
                (mounting_dir, file_name))
            out, rc = client.exec_command(
                cmd='sudo getfattr -n ceph.file.layout %s%s' %
                (mounting_dir, file_name))
            out = out.read()
            out = out.split()
            out[3] = out[3].strip('ceph.file.layout=')
            self.result_vals.update({'stripe_unit': out[3]})
            self.result_vals.update({'stripe_count': out[4]})
            self.result_vals.update({'object_size': out[5]})
            self.result_vals.update({'pool': out[6]})
            return self.result_vals, 0

    def setfattr(self, clients, ops, val, mounting_dir, file_name):
        for client in clients:
            client.exec_command(
                cmd='sudo setfattr -n ceph.file.layout.%s -v %s %s%s' %
                (ops, val, mounting_dir, file_name))
            return 0

    def client_clean_up(
            self,
            fuse_clients,
            kernel_clients,
            mounting_dir,
            *args):
        for client in fuse_clients:
            log.info("Removing files:")
            while True:
                client.exec_command(
                    cmd='sudo find %s -type f -delete' %
                    (mounting_dir), long_running=True)
                client.exec_command(
                    cmd='sudo rm -rf %s*' %
                    (mounting_dir), long_running=True)
                out, rc = client.exec_command(
                    cmd='sudo ls -l %s' %
                    (mounting_dir))
                op = out.read().rstrip('\n')
                if 'total 0' in op:
                    break
            if args:
                if 'umount' in args:
                    log.info("Unmounting fuse client:")
                    client.exec_command(
                        cmd='sudo fusermount -u %s -z' %
                        (mounting_dir))
                    log.info("Removing mounting directory:")
                    client.exec_command(cmd='sudo rmdir %s' % (mounting_dir))
                    log.info("Removing keyring file:")
                    client.exec_command(
                        cmd="sudo rm -rf /etc/ceph/ceph.client.%s.keyring" %
                        (client.hostname))
                    log.info("Removing permissions:")
                    client.exec_command(
                        cmd="sudo ceph auth rm client.%s" %
                        (client.hostname))
                    client.exec_command(
                        cmd='sudo find /home/cephuser/ -type f -delete',
                        long_running=True)
                    client.exec_command(
                        cmd='sudo rm -rf /home/cephuser/*',
                        long_running=True)
                    client.exec_command(cmd='sudo iptables -F')

        for client in kernel_clients:
            log.info("Removing files:")
            while True:
                client.exec_command(
                    cmd='sudo find %s -type f -delete' %
                    (mounting_dir), long_running=True)
                client.exec_command(
                    cmd='sudo rm -rf %s*' %
                    (mounting_dir), long_running=True)
                out, rc = client.exec_command(
                    cmd='sudo ls -l %s' %
                    (mounting_dir))
                op = out.read().rstrip('\n')
                if 'total 0' in op:
                    break
            if args:
                if 'umount' in args:
                    log.info("Unmounting kernel client:")
                    client.exec_command(
                        cmd='sudo umount %s -l' %
                        (mounting_dir))
                    client.exec_command(cmd='sudo rmdir %s' % (mounting_dir))
                    log.info("Removing keyring file:")
                    client.exec_command(
                        cmd="sudo rm -rf /etc/ceph/ceph.client.%s.keyring" %
                        (client.hostname))
                    log.info("Removing permissions:")
                    client.exec_command(
                        cmd="sudo ceph auth rm client.%s" %
                        (client.hostname))
                    client.exec_command(
                        cmd='sudo find /home/cephuser/ -type f -delete',
                        long_running=True)
                    client.exec_command(
                        cmd='sudo rm -rf /home/cephuser/*',
                        long_running=True)

        return 0

    def mds_cleanup(self, mds_nodes, dir_fragmentation):
        log.info("Deactivating Multiple MDSs")
        for node in mds_nodes:
            log.info("Deactivating Multiple MDSs")
            node.exec_command(
                cmd="sudo ceph fs set cephfs allow_multimds false "
                    "--yes-i-really-mean-it")
            log.info("Setting Max mds to 1:")
            node.exec_command(cmd="sudo ceph fs set cephfs max_mds 1")
            if dir_fragmentation is not None:
                log.info("Disabling directorty fragmenation")
                node.exec_command(
                    cmd='sudo ceph fs set cephfs allow_dirfrags 0')
            break
        time.sleep(120)
        return 0


class MkdirPinning(FsUtils):
    def __init__(self, ceph_nodes, pin_val):
        super(MkdirPinning, self).__init__(ceph_nodes)
        self.pin_val = pin_val

    def mkdir_pinning(self, clients, range1, range2, mounting_dir, dir_name):
        super(
            MkdirPinning,
            self).mkdir(
            clients,
            range1,
            range2,
            mounting_dir,
            dir_name)
        for client in clients:
            for num in range(range1, range2):
                client.exec_command(
                    cmd='sudo setfattr -n ceph.dir.pin -v %s %s%s_%d' %
                    (self.pin_val, mounting_dir, dir_name, num))
            return 0
