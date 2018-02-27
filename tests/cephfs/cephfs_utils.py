import logging
import random
import time

log = logging.getLogger(__name__)


class FsUtils(object):
    def __init__(self, ceph_nodes):
        self.ceph_nodes = ceph_nodes
        self.clients = []
        self.result_vals = {}
        self.osd_nodes = []
        self.mds_nodes = []
        self.return_counts = []
        self.failure_info = {}
        self.active_mds_1 = ''
        self.active_mds_2 = ''
        self.active_mds_node_1 = ''
        self.active_mds_node_2 = ''

    def get_clients(self):
        log.info("Getting Clients")
        for node in self.ceph_nodes:
            if node.role == 'client':
                self.clients.append(node)
            if node.role == 'mds':
                self.mds_nodes.append(node)
            if node.role == 'osd':
                self.osd_nodes.append(node)

        # Identifying MON node
        for node in self.ceph_nodes:
            if node.role == 'mon':
                self.mon_node = node
                out, err = self.mon_node.exec_command(cmd='sudo hostname -I')
                self.mon_node_ip = out.read().rstrip('\n')
                break

        self.fuse_clients = self.clients[0:2]  # seperating clients for fuse and kernel
        self.kernel_clients = self.clients[2:4]
        self.result_vals.update({'clients': self.clients})
        self.result_vals.update({'fuse_clients': self.fuse_clients})
        self.result_vals.update({'kernel_clients': self.kernel_clients})
        self.result_vals.update({'mon_node_ip': self.mon_node_ip})
        self.result_vals.update({'mon_node': self.mon_node})
        self.result_vals.update({'osd_nodes': self.osd_nodes})
        self.result_vals.update({'mds_nodes': self.mds_nodes})

        return self.result_vals

    @staticmethod
    def auth_list(clients, mon_node):

        for node in clients:
            log.info("Giving required permissions for clients from MON node:")
            mon_node.exec_command(
                cmd="sudo ceph auth get-or-create client.%s mon 'allow *' mds 'allow *, allow rw path=/' osd "
                    "'allow rw pool=cephfs_data' -o /etc/ceph/ceph.client.%s.keyring" % (node.hostname, node.hostname))
            out, err = mon_node.exec_command(
                sudo=True, cmd='cat /etc/ceph/ceph.client.%s.keyring' % node.hostname)
            keyring = out.read()
            key_file = node.write_file(
                sudo=True,
                file_name='/etc/ceph/ceph.client.%s.keyring' % node.hostname,
                file_mode='w')
            key_file.write(keyring)

            key_file.flush()

            node.exec_command(cmd="sudo chmod 644 /etc/ceph/ceph.client.%s.keyring" % node.hostname)

    def fuse_mount(self, fuse_clients, mounting_dir):
        try:
            for client in fuse_clients:
                log.info("Creating mounting dir:")
                client.exec_command(cmd='sudo mkdir %s' % mounting_dir)
                log.info("Mounting fs with ceph-fuse on client %s:" % client.hostname)
                client.exec_command(cmd="sudo ceph-fuse -n client.%s %s" % (client.hostname, mounting_dir))
                out, err = client.exec_command(cmd='mount')
                mount_output = out.read()
                mount_output.split()
                log.info("Checking if fuse mount is is passed of failed:")
                if 'fuse' in mount_output:
                    log.info("ceph-fuse mounting passed")
                else:
                    log.error("ceph-fuse mounting failed")
        except Exception as e:
            log.error(e)

    def kernel_mount(self, kernel_clients, mounting_dir, mon_node_ip):
        try:
            for client in kernel_clients:
                log.info("Creating mounting dir:")
                client.exec_command(cmd='sudo mkdir %s' % mounting_dir)
                out, err = client.exec_command(cmd='sudo ceph auth get-key client.%s' % client.hostname)
                secret_key = out.read().rstrip('\n')
                mon_node_ip = mon_node_ip.replace(" ", "")
                client.exec_command(
                    cmd='sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s' % (
                        mon_node_ip, mounting_dir, client.hostname, secret_key))
                out, err = client.exec_command(cmd='mount')
                mount_output = out.read()
                mount_output.split()
                log.info("Checking if kernel mount is is passed of failed:")
                if '%s:6789:/' % mon_node_ip in mount_output:
                    log.info("kernel mount passed")
                else:
                    log.error("kernel mount failed")
        except Exception as e:
            log.error(e)

    def fuse_client_io(self, client, mounting_dir):
        try:
            rand_count = random.randint(1, 10)
            log.info("Performing IOs on fuse-clients")
            client.exec_command(cmd="sudo crefi %s --fop create -n %d --random --min=1M --max=512M -t=sparse" % (
                mounting_dir, rand_count), long_running=True)

        except Exception as e:
            log.error(e)

    def kernel_client_io(self, client, mounting_dir):
        rand_count = random.randint(1, 6)
        rand_bs = random.randint(100, 500)
        client.exec_command(
            cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d" %
                (mounting_dir, client.hostname, rand_bs, rand_count),
            long_running=True)

    def fuse_client_md5(self, fuse_clients, mounting_dir):
        md5sum_list1 = []
        for client in fuse_clients:
            md5sum_list1.append(
                client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % mounting_dir, long_running=True))
        return md5sum_list1

    def kernel_client_md5(self, kernel_clients, mounting_dir):
        md5sum_list2 = []
        for client in kernel_clients:
            md5sum_list2.append(
                client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % mounting_dir, long_running=True))
        return md5sum_list2

    # checking file locking mechanism

    def file_locking(self, client, mounting_dir):
        try:
            to_lock_file = """
import fcntl
import subprocess
import time
try:
    f = open('%sto_test_file_lock', 'w+')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print "locking file:--------------------------------"
    subprocess.check_output(["sudo","dd","if=/dev/zero","of=%sto_test_file_lock","bs=1M","count=2"])
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
            out, err = client.exec_command(cmd="sudo python /home/cephuser/file_lock.py")
            output = out.read()
            output.split()
            if 'Errno 11' in output:
                log.info("File locking achieved, data is not corrupted")
            elif 'locking' in output:
                log.info("File locking achieved, data is not corrupted")
            else:
                log.error("Data is corrupted")

            out, err = client.exec_command(cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" % mounting_dir)

            md5sum_file_lock = out.read()

            return md5sum_file_lock

        except Exception as e:
            log.error(e)

    def activate_multiple_mdss(self, mds_nodes):
        try:
            log.info("Activating Multiple MDSs")
            for node in mds_nodes:
                node.exec_command(cmd="sudo ceph fs set cephfs allow_multimds true --yes-i-really-mean-it")
                node.exec_command(cmd="sudo ceph fs set cephfs max_mds 2")
                break

        except Exception as e:
            log.error(e)

    def mkdir_pinning(self, clients, range1, range2, mounting_dir, dir_name, pin_val):
        try:
            log.info("Creating Directories and Pinning to MDS %s" % pin_val)
            for client in clients:
                for num in range(range1, range2):
                    out, err = client.exec_command(cmd='sudo mkdir %s%s_%d' % (mounting_dir, dir_name, num))
                    if pin_val != '':
                        client.exec_command(
                            cmd='sudo setfattr -n ceph.dir.pin -v %s %s%s_%d' % (pin_val, mounting_dir, dir_name, num))
                    else:
                        print "Pin val not given"
                    print out.read()
                    print time.time()
                break
        except Exception as e:
            log.error(e)

    def allow_dir_fragmentation(self, mds_nodes):
        try:
            log.info("Allowing directorty fragmenation for splitting")
            for node in mds_nodes:
                node.exec_command(cmd='sudo ceph fs set cephfs allow_dirfrags 1')
                break
        except Exception as e:
            log.error(e)

    def mds_fail_over(self, mds_nodes):
        try:
            rand = random.randint(0, 1)
            for node in mds_nodes:
                log.info("Failing MDS {:d}".format(rand))
                node.exec_command(cmd='sudo ceph mds fail {:d}'.format(rand))
                break

        except Exception as e:
            log.error(e)

    def get_active_mdss(self, mds_nodes):
        for node in mds_nodes:
            out, err = node.exec_command(cmd="sudo ceph mds stat | grep -o -P '(?<=0=).*(?==up:active,)'")
            self.active_mds_1 = out.read().rstrip('\n')
            out, err = node.exec_command(cmd="sudo ceph mds stat | grep -o -P '(?<=1=).*(?==up:active)'")
            self.active_mds_2 = out.read().rstrip('\n')
            break

        for node in mds_nodes:
            if node.hostname == self.active_mds_1:
                self.active_mds_node_1 = node
            if node.hostname == self.active_mds_2:
                self.active_mds_node_2 = node

        return self.active_mds_node_1, self.active_mds_node_2

    def get_info(self, active_mds_node_1, active_mds_node_2):
        try:
            out_1, err_1 = active_mds_node_1.exec_command(
                cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok get subtrees | grep path" %
                    active_mds_node_1.hostname)
            out_2, err_2 = active_mds_node_2.exec_command(
                cmd="sudo ceph --admin-daemon /var/run/ceph/ceph-mds.%s.asok get subtrees | grep path" %
                    active_mds_node_2.hostname)

            return out_1.read().rstrip('\n'), out_2.read().rstrip('\n')

        except Exception as e:
            log.error(e)

    def stress_io(self, clients, mounting_dir, dir_name):
        try:
            for client in clients:
                n = 5000
                while n != 0:
                    client.exec_command(cmd='sudo touch %s%s/file_%d' % (mounting_dir, dir_name, n))
                    n -= 1
                break
        except Exception as e:
            log.error(e)

    def pinned_dir_io(self, clients, mounting_dir, mds_fail_over, mds_nodes, num_of_files, dir_name, range1, range2):
        try:
            log.info("Performing IOs and MDSfailovers on clients")
            for client in clients:
                for num in range(range1, range2):
                    if mds_fail_over != '':
                        mds_fail_over(mds_nodes)
                    out, err = client.exec_command(
                        cmd='sudo crefi -n %d %s%s_%d' % (num_of_files, mounting_dir, dir_name, num))
                    rc = out.channel.recv_exit_status()
                    self.return_counts.append(rc)
                    if rc == 0:
                        log.info("Client IO is going on,success")
                    else:
                        log.error("Client IO got interrupted")
                        self.failure_info.update({client: err.read()})
                break
            return self.return_counts, self.failure_info

        except Exception as e:
            log.error(e)

    def rc_verify(self, tc, return_counts):
        return_codes_set = set(return_counts)
        if len(return_codes_set) == 1:
            out = "Test case {} Passed".format(tc)
            return out
        else:
            out = "Test case {} Failed".format(tc)
            return out

    def clean_up(self, fuse_clients, kernel_clients, mounting_dir, umount=None):
        try:
            for client in fuse_clients:
                log.info("Removing files:")
                client.exec_command(cmd='sudo rm -rf {}*'.format(mounting_dir))
                if umount is not None:
                    log.info("Unmounting fuse client:")
                    client.exec_command(cmd='sudo fusermount -u {}'.format(mounting_dir))
                    log.info("Removing mounting directory:")
                    client.exec_command(cmd='sudo rmdir {}'.format(mounting_dir))
                    break
            for client in kernel_clients:
                log.info("Unmounting kernel client:")
                if umount is not None:
                    client.exec_command(cmd='sudo umount {}'.format(mounting_dir))
                    client.exec_command(cmd='sudo rmdir {}'.format(mounting_dir))
                break

        except Exception as e:
            log.error(e)
