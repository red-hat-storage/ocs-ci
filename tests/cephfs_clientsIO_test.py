from utility import utils
from ceph.parallel import parallel
import timeit
import logging

log = logging.getLogger(__name__)


def run(**kw):
    start = timeit.default_timer()
    log.info("Running cephfs CRITICAL test")
    ceph_nodes = kw.get('ceph_nodes')
    fuse_clients, kernel_clients, mon_node, mounting_dir, mds_nodes, md5sum_file_lock, mon_node_ip = \
        utils.get_client_info(ceph_nodes, utils.clients)
    utils.auth_list(utils.clients, mon_node)
    md5sum_list1 = utils.fuse_mount(fuse_clients, mounting_dir)
    md5sum_list2 = utils.kernel_mount(mounting_dir, mon_node_ip, kernel_clients)
    log.info("Test started for CEPH-10528:")

    with parallel() as p:
        for client in fuse_clients:
            p.spawn(utils.fuse_client_io, client, mounting_dir)

    with parallel() as p:
        for client in kernel_clients:
            p.spawn(utils.kernel_client_io, client, mounting_dir)

    utils.fuse_client_md5(fuse_clients, md5sum_list1)

    utils.kernel_client_md5(kernel_clients, md5sum_list2)

    sorted(md5sum_list1)

    sorted(md5sum_list2)

    log.info("Test completed for CEPH-10528:")
    print md5sum_list1
    print md5sum_list2
    if md5sum_list1 == md5sum_list2:
        log.info("Data consistancy found, Test case CEPH-10528 passed")
    else:
        log.error("Test case CEPH-10528 Failed")
    print("#" * 120)

    log.info("Test for CEPH-10529 will start:")
    with parallel() as p:
        for client in fuse_clients:
            p.spawn(utils.file_locking, client)
    print "-----------------------------------------"

    print md5sum_file_lock

    if md5sum_file_lock[0] == md5sum_file_lock[1]:

        log.info("File Locking mechanism is working,data is not corrupted,test case CEPH-10529 passed")

    else:
        log.error("File Locking mechanism is failed,data is corruptedtTest case CEPH-10529 Failed")

    log.info("Test completed for CEPH-10529")

    print'Script execution time:------'

    stop = timeit.default_timer()
    total_time = stop - start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print ("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))

    return 0
