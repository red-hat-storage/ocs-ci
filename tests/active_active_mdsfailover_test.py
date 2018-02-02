from utils.utils import *
from ceph.parallel import *
import timeit

def run(**kw):
    start = timeit.default_timer()
    log.info("Running cephfs CRITICAL test")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')
    num_of_dirs = config.get('num_of_dirs')
    fuse_clients, kernel_clients,mon_node, mounting_dir, mds_nodes,_,mon_node_ip = get_client_info(ceph_nodes,clients)
    num_of_dirs = num_of_dirs/5
    activate_multiple_mdss(mds_nodes)
    print "###################################################################################"
    log.info("Execution of Test case 11229 started:")
    tc = '11129'
    with parallel() as p:
        p.spawn(mkdir_pinning,fuse_clients,0,num_of_dirs,0,0)
        p.spawn(mkdir_pinning,fuse_clients,num_of_dirs,num_of_dirs*2,0)
        p.spawn(mkdir_pinning,fuse_clients,num_of_dirs*2,num_of_dirs*3,0)
        p.spawn(mkdir_pinning,fuse_clients,num_of_dirs*3,num_of_dirs*4,0)
        p.spawn(mkdir_pinning,kernel_clients,num_of_dirs*4,num_of_dirs*5,1)

    with parallel() as p:
        p.spawn(pinned_dir_io,kernel_clients,mds_fail_over,1,0,num_of_dirs/2)
        p.spawn(pinned_dir_io,kernel_clients,mds_fail_over,2,num_of_dirs/2,num_of_dirs)
        p.spawn(pinned_dir_io,kernel_clients,mds_fail_over,1,num_of_dirs*4,num_of_dirs*4+10)

    log.info("Execution of Test case 11229 ended:")
    tc = rc_verify(tc,RC)
    output.append(tc)

    print "###################################################################################"
    log.info("Execution of Test case 11228 started:")
    tc = '11128'

    with parallel() as p:
        p.spawn(mkdir_pinning,fuse_clients,num_of_dirs*6,num_of_dirs*11,0)
        p.spawn(mkdir_pinning,kernel_clients,num_of_dirs*11,num_of_dirs*21,1)


    with parallel()  as p:
        p.spawn(pinned_dir_io,kernel_clients,mds_fail_over,10,num_of_dirs*6,num_of_dirs*7)
        p.spawn(pinned_dir_io,kernel_clients,mds_fail_over,20,num_of_dirs*7,num_of_dirs*8)

    log.info("Execution of Test case 11228 ended:")
    tc = rc_verify(tc, RC)
    output.append(tc)
    print "###################################################################################"
    log.info("Execution of Test case 11230 started:")
    tc = '11130'
    with parallel() as p:

        p.spawn(mkdir_pinning,fuse_clients,num_of_dirs*21,num_of_dirs*21+25,0)

        p.spawn(mkdir_pinning,kernel_clients,num_of_dirs*21+25,num_of_dirs*21+50,1)

    with parallel() as p:

        p.spawn(pinned_dir_io,kernel_clients,mds_fail_over,10,num_of_dirs*21,num_of_dirs*21+25)

        p.spawn(pinned_dir_io,kernel_clients,mds_fail_over,20,num_of_dirs*21+25,num_of_dirs*21+50)

    log.info("Execution of Test case 11230 ended:")

    tc = rc_verify(tc, RC)
    output.append(tc)
    print "###################################################################################"
    print "TESTS completed"
    print "Results:"

    for i in output:
        print i
    for i,j in failure.iteritems():
        print "client: %s Failure:%s " %(i,j)


    print'Script execution time:------'
    stop = timeit.default_timer()
    total_time = stop - start

    mins, secs = divmod(total_time, 60)
    hours, mins = divmod(mins, 60)

    print ("Hours:%d Minutes:%d Seconds:%f" %(hours,mins,secs))

    return 0
