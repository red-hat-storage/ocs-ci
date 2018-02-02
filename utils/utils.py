import random
import logging
import time

logger = logging.getLogger(__name__)
log = logger

# variables
mounting_dir = '/mnt/cephfs/'
clients = []
md5sum_list1 = []
md5sum_list2 = []
fuse_clients = []
kernel_clients = []
mon_node = ''
mon_node_ip = ''
mds_nodes = []
md5sum_file_lock = []
active_mdss = []
RC = []
failure = {}
output = []

# function for getting the clients
def get_client_info(ceph_nodes, clients):
    log.info("Getting Clients")
    for node in ceph_nodes:
        if node.role == 'client':
            clients.append(node)
    # Identifying MON node
    for node in ceph_nodes:
        if node.role == 'mon':
            mon_node = node
            out, err = mon_node.exec_command(cmd='sudo hostname -I')
            mon_node_ip = out.read().rstrip('\n')
            break
    for node in ceph_nodes:
        if node.role == 'mds':
            mds_nodes.append(node)
    for node in clients:
        node.exec_command(cmd='sudo yum install -y attr')

    fuse_clients = clients[0:2]  # seperating clients for fuse and kernel
    kernel_clients = clients[2:4]
    return fuse_clients, kernel_clients,mon_node,mounting_dir, mds_nodes, md5sum_file_lock,mon_node_ip


# function for providing authorization to the clients from MON ndoe
def auth_list(clients, mon_node):
    for node in clients:
        log.info("Giving required permissions for clients from MON node:")
        mon_node.exec_command(
            cmd="sudo ceph auth get-or-create client.%s mon 'allow *' mds 'allow *, allow rw path=/' osd 'allow rw pool=cephfs_data' -o /etc/ceph/ceph.client.%s.keyring" % (
                node.hostname, node.hostname))
        out, err = mon_node.exec_command(
            sudo=True, cmd='cat /etc/ceph/ceph.client.%s.keyring' % (node.hostname))
        keyring = out.read()
        key_file = node.write_file(
            sudo=True,
            file_name='/etc/ceph/ceph.client.%s.keyring' % (node.hostname),
            file_mode='w')
        key_file.write(keyring)

        key_file.flush()

        node.exec_command(cmd="sudo chmod 644 /etc/ceph/ceph.client.%s.keyring" % (node.hostname))
        # creating mounting directory
        node.exec_command(cmd='sudo mkdir %s' % (mounting_dir))


# MOunting single FS with ceph-fuse
def fuse_mount(fuse_clients, mounting_dir):
    try:
        for client in fuse_clients:
            log.info("Creating mounting dir:")
            log.info("Mounting fs with ceph-fuse on client %s:" % (client.hostname))
            client.exec_command(cmd="sudo ceph-fuse -n client.%s %s" % (client.hostname, mounting_dir))
            out, err = client.exec_command(cmd='mount')
            mount_output = out.read()
            mount_output.split()
            log.info("Checking if fuse mount is is passed of failed:")
            if 'fuse' in mount_output:
                log.info("ceph-fuse mounting passed")
            else:
                log.error("ceph-fuse mounting failed")
        return md5sum_list1
    except Exception as e:
        log.error(e)


def kernel_mount(mounting_dir, mon_node_ip, kernel_clients):
    try:
        for client in kernel_clients:
            out, err = client.exec_command(cmd='sudo ceph auth get-key client.%s' % (client.hostname))
            secret_key = out.read().rstrip('\n')
            mon_node_ip = mon_node_ip.replace(" ", "")
            client.exec_command(
                cmd='sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s' % (
                mon_node_ip, mounting_dir, client.hostname, secret_key))
            out, err = client.exec_command(cmd='mount')
            mount_output = out.read()
            mount_output.split()
            log.info("Checking if kernel mount is is passed of failed:")
            if '%s:6789:/' % (mon_node_ip) in mount_output:
                log.info("kernel mount passed")
            else:
                log.error("kernel mount failed")
        return md5sum_list2
    except Exception as e:
        log.error(e)


def fuse_client_io(client, mounting_dir):
    try:
        rand_count = random.randint(1, 5)
        rand_bs = random.randint(100, 300)
        log.info("Performing IOs on fuse-clients")
        client.exec_command(cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d" % (
        mounting_dir, client.hostname, rand_bs, rand_count),
                            long_running=True)
    except Exception as e:
        log.error(e)

def kernel_client_io(client, mounting_dir):
    try:
        rand_count = random.randint(1, 6)
        rand_bs = random.randint(100, 500)
        log.info("Performing IOs on kernel-clients")
        client.exec_command(cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d" % (
        mounting_dir, client.hostname, rand_bs, rand_count),
                            long_running=True)
    except Exception as e:
        log.error(e)

def fuse_client_md5(fuse_clients, md5sum_list1):
    try:
        log.info("Calculating MD5 sums of files in fuse-clients:")
        for client in fuse_clients:
            md5sum_list1.append(
                client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir), long_running=True))

    except Exception as e:
        log.error(e)

def kernel_client_md5(kernel_clients, md5sum_list2):
    try:
        log.info("Calculating MD5 sums of files in kernel-clients:")
        for client in kernel_clients:
            md5sum_list2.append(
                client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir), long_running=True))
    except Exception as e:
        log.error(e)

# checking file locking mechanism
def file_locking(client):
    try:
        to_lock_file = """
import fcntl
import subprocess
import time
try:
    f = open('/mnt/cephfs/to_test_file_lock', 'w+')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print "locking file:--------------------------------"
    subprocess.check_output(["sudo","dd","if=/dev/zero","of=/mnt/cephfs/to_test_file_lock","bs=1M","count=2"])
except IOError as e:
    print e
finally:
    print "Unlocking file:------------------------------"
    fcntl.lockf(f,fcntl.LOCK_UN)
            """
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

        out, err = client.exec_command(cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" % (mounting_dir))

        md5sum_file_lock.append(out.read())

    except Exception as e:
        log.error(e)


def activate_multiple_mdss(mds_nodes):
    try:
        log.info("Activating Multiple MDSs")
        for node in mds_nodes:
            out1,err = node.exec_command(cmd="sudo ceph fs set cephfs allow_multimds true --yes-i-really-mean-it")
            out2 ,err =node.exec_command(cmd="sudo ceph fs set cephfs max_mds 2")
            break

    except Exception as e:
        log.error(e)

def mkdir_pinning(clients,range1,range2,dir_name,pin_val):
    try:
        log.info("Creating Directories and Pinning to MDS %s" %(pin_val))
        for client in clients:
            for num in range(range1,range2):
                out,err= client.exec_command(cmd='sudo mkdir %s%s_%d' %(mounting_dir,dir_name,num))
                if pin_val !='':
                    client.exec_command(cmd='sudo setfattr -n ceph.dir.pin -v %s %s%s_%d' % (pin_val,mounting_dir,dir_name,num))
                else:
                    print "PIn val not given"
                print out.read()
                print time.time()
            break
    except Exception as e:
        log.error(e)
def allow_dir_fragmentation(mds_nodes):
    try:
        log.info("Allowing directorty fragmenation for splitting")
        for node in mds_nodes:
            node.exec_command(cmd='sudo ceph fs set cephfs allow_dirfrags 1')
            break
    except Exception as e:
        log.error(e)


def mds_fail_over(mds_nodes):
    try:
        rand = random.randint(0,1)
        for node in mds_nodes:
            log.info("Failing MDS %d" %(rand))
            node.exec_command(cmd='sudo ceph mds fail %d' %(rand))
            break

    except Exception as e:
        log.error(e)

def pinned_dir_io(clients,mds_fail_over,num_of_files,range1,range2):
    try:
        log.info("Performing IOs and MDSfailovers on clients")
        for client in clients:
            client.exec_command(cmd='sudo pip install crefi')
            for num in range(range1,range2):
                    if mds_fail_over !='':
                        mds_fail_over(mds_nodes)
                    out,err = client.exec_command(cmd='sudo crefi -n %d %sdir_%d' %(num_of_files,mounting_dir,num))
                    rc = out.channel.recv_exit_status()
                    print out.read()
                    RC.append(rc)
                    print time.time()
                    if rc == 0:
                        log.info("Client IO is going on,success")
                    else:
                        log.error("Client IO got interrupted")
                        failure.update({client:out})
                        break
            break

    except Exception as e:
        log.error(e)


def rc_verify(tc,RC):
    return_codes_set = set(RC)

    if len(return_codes_set) == 1:

        out = "Test case %s Passed" %(tc)

        return out
    else:
        out = "Test case %s Failed" %(tc)

        return out

# colors for pass and fail status
# class Bcolors:
#     HEADER = '\033[95m'
#     OKGREEN = '\033[92m'
#     FAIL = '\033[91m'
#     ENDC = '\033[0m'
#     BOLD = '\033[1m'
