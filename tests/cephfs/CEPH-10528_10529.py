from tests.cephfs.cephfs_utils import *
from ceph.parallel import *
import timeit


def run(**kw):
    start = timeit.default_timer()

    log.info("Running cephfs CRITICAL test")

    ceph_nodes = kw.get('ceph_nodes')

    log.info("Test started for CEPH-10528:")

    mounting_dir = '/mnt/cephfs/'

    fs_util = FsUtils(ceph_nodes)

    client_info = fs_util.get_clients()

    fs_util.auth_list(client_info['clients'],client_info['mon_node'])

    fs_util.fuse_mount(client_info['fuse_clients'], mounting_dir)

    fs_util.kernel_mount(client_info['kernel_clients'], mounting_dir, client_info['mon_node_ip'])

    with parallel() as p:
        for client in client_info['fuse_clients']:
            p.spawn(fs_util.fuse_client_io,client,mounting_dir)

    with parallel() as p:
         for client in client_info['kernel_clients']:
            p.spawn(fs_util.kernel_client_io,client,mounting_dir)

    md5sum_list1 = fs_util.fuse_client_md5(client_info['fuse_clients'],mounting_dir)

    md5sum_list2 = fs_util.kernel_client_md5(client_info['kernel_clients'],mounting_dir)

    sorted(md5sum_list1)

    sorted(md5sum_list2)

    log.info("Test completed for CEPH-10528:")

    if md5sum_list1 == md5sum_list2:
        log.info("Data consistancy found, Test case CEPH-10528 passed")
    else:
        log.error("Test case CEPH-10528 Failed")

    print "#####################################################################################################################"

    log.info("Test for CEPH-10529 will start:")

    md5sum_file_lock = []

    with parallel() as p:

        for client in client_info['fuse_clients']:

            p.spawn(fs_util.file_locking,client,mounting_dir)

        for output in p:

            md5sum_file_lock.append(output)

    print md5sum_file_lock

    if md5sum_file_lock[0] == md5sum_file_lock[1]:

         log.info("File Locking mechanism is working,data is not corrupted,test case CEPH-10529 passed")

    else:
         log.error("File Locking mechanism is failed,data is corrupted,test case CEPH-10529 Failed")


    log.info("Test completed for CEPH-10529")

    log.info("Cleaning up!-----")

    fs_util.clean_up(client_info['fuse_clients'],client_info['kernel_clients'],mounting_dir,umount='doit')

    log.info("Cleaning up successfull")

    print'Script execution time:------'

    stop = timeit.default_timer()

    total_time = stop - start

    mins, secs = divmod(total_time, 60)

    hours, mins = divmod(mins, 60)

    print ("Hours:%d Minutes:%d Seconds:%f" %(hours,mins,secs))

    return 0
