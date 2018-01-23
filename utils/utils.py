import random
import logging
logger = logging.getLogger(__name__)
log = logger

#variables
mounting_dir = '/mnt/cephfs/'
clients = []
md5sum_list1 = []
md5sum_list2 = []
fuse_clients = []
kernel_clients = []
mon_node = ''
mon_node_ip = ''
#function for getting the clients
def GetClients(ceph_nodes, clients):
    log.info("Getting Clients")
    for node in ceph_nodes:
        if node.role == 'client':
            clients.append(node)
    #Identifying MON node
    for node in ceph_nodes:
        if node.role == 'mon':
            mon_node = node
            break
    fuse_clients = clients[0:2]#seperating clients for fuse and kernel
    kernel_clients = clients[2:4]

    return fuse_clients,kernel_clients,mon_node,mounting_dir
#function for providing authorization to the clients from MON ndoe
def AuthList(clients, mon_node):
    for node in clients:
        log.info("Giving required permissions for clients from MON node:")
        mon_node.exec_command(
            cmd="sudo ceph auth get-or-create client.%s mon 'allow r' mds 'allow r, allow rw path=/' osd 'allow rw pool=cephfs_data' -o /etc/ceph/ceph.client.%s.keyring" % (
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
        #creating mounting directory
        log.info("Creating mounting dir:")

        node.exec_command(cmd='sudo mkdir %s' %(mounting_dir))

#MOunting single FS with ceph-fuse
def FuseMount(fuse_clients,mounting_dir):
    try:
        for client in fuse_clients:
            log.info("Mounting fs with ceph-fuse on client %s:" % (client.hostname))
            client.exec_command(cmd="sudo ceph-fuse -n client.%s %s" % (client.hostname, mounting_dir))
            out,err = client.exec_command(cmd='mount')
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

#Getting IP of Mon node
def Mon_IP( mon_node):
    out, err = mon_node.exec_command(cmd='sudo hostname -I')
    return out.read().rstrip('\n')


def KernelMount(mounting_dir,mon_node_ip,kernel_clients):
    try:
        for client in kernel_clients:
            out, err = client.exec_command(cmd='sudo ceph auth get-key client.%s' % (client.hostname))
            secret_key = out.read().rstrip('\n')
            mon_node_ip = mon_node_ip.replace(" ", "")
            client.exec_command(
            cmd='sudo mount -t ceph %s:6789:/ %s -o name=%s,secret=%s' %(mon_node_ip,mounting_dir,client.hostname,secret_key))
            out,err = client.exec_command(cmd='mount')
            mount_output = out.read()
            mount_output.split()
            log.info("Checking if kernel mount is is passed of failed:")
            if '%s:6789:/' %(mon_node_ip) in mount_output:
                log.info("kernel mount passed")
            else:
                log.error("kernel mount failed")
        return md5sum_list2
    except Exception as e:
        log.error(e)


def FuseIO(client, mounting_dir):
         rand_count = random.randint(1,5)
         rand_bs = random.randint(100,200)
         client.exec_command(cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d" % (mounting_dir, client.hostname,rand_bs,rand_count),
                            long_running=True)
def KernelIO(client, mounting_dir):
        rand_count = random.randint(1,10)
        rand_bs = random.randint(100,200)
        client.exec_command(cmd="sudo dd if=/dev/zero of=%snewfile_%s bs=%dM count=%d" % (mounting_dir, client.hostname,rand_bs,rand_count),
                            long_running=True)

def FuseFilesMd5(fuse_clients,md5sum_list1):
    for client in fuse_clients:
        md5sum_list1.append(client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir), long_running=True))


def KernelFilesMd5(kernel_clients,md5sum_list2):
    for client in kernel_clients:
        md5sum_list2.append(client.exec_command(cmd="sudo md5sum %s* | awk '{print $1}' " % (mounting_dir), long_running=True))

#checking file locking mechanism
def FileLocking(client,md5sum):
    to_lock_file = """
import fcntl
import subprocess
import time
try:
    f = open('/mnt/cephfs/to_test_lock_file1', 'w+')
    fcntl.lockf(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print "locking file:--------------------------------"
    time.sleep(5)
    subprocess.check_output(["sudo","dd","if=/dev/zero","of=/mnt/cephfs/to_test_file_lock","bs=10M","count=2"])
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
    out,err = client.exec_command(cmd="sudo python /home/cephuser/file_lock.py")
    output = out.read()
    output.split()
    if 'Errno 11' in output:
        log.info("File locking achieved, data is not corrupted")
    elif 'locking' in output:
        log.info("File locking achieved, data is not corrupted")
    else:
        log.error("Data is corrupted")

    out,err = client.exec_command(cmd="sudo md5sum %sto_test_file_lock | awk '{print $1}'" % (mounting_dir))
    md5sum.append(out.read())
#colors for pass and fail status
class Bcolors:

    HEADER = '\033[95m'
    OKGREEN = '\033[92m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'