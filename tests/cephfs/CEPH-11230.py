from tests.cephfs.cephfs_utils import *
from ceph.parallel import *
import timeit
import sys


def run(**kw):
    start = timeit.default_timer()

    log.info("Running cephfs CRITICAL test")

    ceph_nodes = kw.get('ceph_nodes')

    config = kw.get('config')

    num_of_dirs = config.get('num_of_dirs')

    mounting_dir = '/mnt/cephfs/'

    dir_name = 'dir'

    output = []

    num_of_dirs = num_of_dirs / 5

    fs_util = FsUtils(ceph_nodes)

    client_info = fs_util.get_clients()

    fs_util.auth_list(client_info['clients'], client_info['mon_node'])

    fs_util.fuse_mount(client_info['fuse_clients'], mounting_dir)

    fs_util.kernel_mount(client_info['kernel_clients'], mounting_dir, client_info['mon_node_ip'])

    with parallel() as p:
        for client in client_info['fuse_clients']:
            p.spawn(fs_util.fuse_client_io, client, mounting_dir)

    with parallel() as p:
        for client in client_info['kernel_clients']:
            p.spawn(fs_util.kernel_client_io, client, mounting_dir)

    md5sum_list1 = fs_util.fuse_client_md5(client_info['fuse_clients'], mounting_dir)

    md5sum_list2 = fs_util.kernel_client_md5(client_info['kernel_clients'], mounting_dir)

    sorted(md5sum_list1)

    sorted(md5sum_list2)

    if md5sum_list1 == md5sum_list2:

        log.info("Data consistancy found")
    else:
        log.error("Data consistancy not found")
        sys.exit(1)

    fs_util.activate_multiple_mdss(client_info['mds_nodes'])

    log.info("Execution of Test case 11130 started:")

    tc = '11130'
    with parallel() as p:

        p.spawn(fs_util.mkdir_pinning,client_info['fuse_clients'],num_of_dirs*21,num_of_dirs*21+25,mounting_dir,dir_name,0)

        p.spawn(fs_util.mkdir_pinning,client_info['kernel_clients'],num_of_dirs*21+25,num_of_dirs*21+50,mounting_dir,dir_name,1)

    with parallel() as p:

        p.spawn(fs_util.pinned_dir_io,client_info['kernel_clients'],mounting_dir,fs_util.mds_fail_over,client_info['mds_nodes'],10,dir_name,num_of_dirs*21,num_of_dirs*21+25)

        p.spawn(fs_util.pinned_dir_io,client_info['kernel_clients'],mounting_dir,fs_util.mds_fail_over,client_info['mds_nodes'],20,dir_name,num_of_dirs*21+25,num_of_dirs*21+50)

        for op in p:
            return_counts, failure_info = op

    log.info("Execution of Test case 11228 ended:")

    print "Results:"

    rc_op = fs_util.rc_verify(tc, return_counts)
    output.append(rc_op)

    if len(failure_info) != 0:
        print "Failure info"
        print failure_info

    for i in output:
        print i


    log.info("Cleaning up!-----")

    fs_util.clean_up(client_info['fuse_clients'],client_info['kernel_clients'],mounting_dir,umount='doit')

    log.info("Cleaning up successfull")

    print'Script execution time:------'

    stop = timeit.default_timer()
    total_time = stop - start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print ("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))

    return 0
