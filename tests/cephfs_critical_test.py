from utils.utils import *
from ceph.parallel import *
import timeit
def run(**kw):
    start = timeit.default_timer()
    
    log.info("Running cephfs CRITICALtest")
    
    ceph_nodes = kw.get('ceph_nodes')
    
    fuse_clients,kernel_clients,mon_node,mounting_dir = GetClients(ceph_nodes,clients)
    
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
    
        log.info(Bcolors.OKGREEN+"Data consistancy found, Test case CEPH-10528 passed"+Bcolors.ENDC)
    else:
    
        log.error(Bcolors.FAIL+"Test case CEPH-10528 Failed"+Bcolors.ENDC)
    
    print "###########################################################"

    log.info("Test for CEPH-10529 will start:")
    
    md5sum =[]
    
    with parallel() as p:
        for client in fuse_clients:
            p.spawn(FileLocking,client,md5sum)
    
    if md5sum[0] == md5sum[1]:
    
        log.info(Bcolors.OKGREEN+"Test case CEPH-10529 passed"+Bcolors.ENDC)

    else:
    
        log.error(Bcolors.FAIL+"Test case CEPH-10528 Failed"+Bcolors.ENDC)

    print'Script execution time:------'
    stop = timeit.default_timer()
    total_time = stop - start
    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)
    print ("Hours:%d Minutes:%d Seconds:%f" %(hours,mins,secs))

    return 0