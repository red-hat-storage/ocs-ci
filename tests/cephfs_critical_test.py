from utils.utils import *
from ceph.parallel import *
import timeit
def run(**kw):
    start = timeit.default_timer()
    log.info("Running cephfs CRITICAL test")
    ceph_nodes = kw.get('ceph_nodes')
    fuse_clients,kernel_clients,mon_node,mounting_dir,mds_nodes,md5sum_file_lock = GetClients(ceph_nodes,clients)
    AuthList(clients,mon_node)
    mon_node_ip = Mon_IP(mon_node)
    md5sum_list1 = FuseMount(fuse_clients,mounting_dir)
    md5sum_list2 = KernelMount(mounting_dir,mon_node_ip,kernel_clients)

    with parallel() as p:
        for client in fuse_clients:
            p.spawn(FuseIO,client,mounting_dir)

    with parallel() as p:
         for client in kernel_clients:
            p.spawn(KernelIO, client, mounting_dir)

    FuseFilesMd5(fuse_clients,md5sum_list1)

    KernelFilesMd5(kernel_clients,md5sum_list2)

    sorted(md5sum_list1)

    sorted(md5sum_list2)

    log.info("Test completed for CEPH-10529:")

    if md5sum_list1 == md5sum_list2:
        log.info(Bcolors.OKGREEN+Bcolors.BOLD+"Data consistancy found, Test case CEPH-10528 passed"+Bcolors.ENDC)
    else:
        log.error(Bcolors.FAIL+Bcolors.BOLD+"Test case CEPH-10528 Failed"+Bcolors.ENDC)
    print "#####################################################################################################################"

    log.info("Test for CEPH-10529 will start:")
    with parallel() as p:
        for client in fuse_clients:
            p.spawn(FileLocking,client)
    print "-----------------------------------------"

    print md5sum_file_lock

    if md5sum_file_lock[0] == md5sum_file_lock[1]:

         log.info(Bcolors.OKGREEN+Bcolors.BOLD+"File Locking mechanism is working,data is not corrupted,test case CEPH-10529 passed"+Bcolors.ENDC)

    else:
         log.error(Bcolors.FAIL+Bcolors.BOLD+"File Locking mechanism is failed,data is corruptedtTest case CEPH-10529 Failed"+Bcolors.ENDC)

    print'Script execution time:------'
    stop = timeit.default_timer()
    total_time = stop - start

    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print ("Hours:%d Minutes:%d Seconds:%f" %(hours,mins,secs))

    return 0
