from tests.cephfs.cephfs_utils import FsUtils
from ceph.parallel import parallel
import timeit
from ceph.ceph import CommandFailed
import traceback
import logging

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        tc = '11242'
        dir_name = 'dir'
        config = kw.get('config')
        num_of_dirs = config.get('num_of_dirs')
        num_of_dirs = num_of_dirs / 5
        log.info("Running cephfs %s test case" % (tc))
        ceph_nodes = kw.get('ceph_nodes')
        fs_util = FsUtils(ceph_nodes)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client1 = []
        client2 = []
        client3 = []
        client4 = []
        client1.append(client_info['fuse_clients'][0])
        client2.append(client_info['fuse_clients'][1])
        client3.append(client_info['kernel_clients'][0])
        client4.append(client_info['kernel_clients'][1])

        rc1 = fs_util.auth_list(client1, client_info['mon_node'])
        rc2 = fs_util.auth_list(client2, client_info['mon_node'])
        rc3 = fs_util.auth_list(client3, client_info['mon_node'])
        rc4 = fs_util.auth_list(client4, client_info['mon_node'])
        print rc1, rc2, rc3, rc4
        if rc1 == 0 and rc2 == 0 and rc3 == 0 and rc4 == 0:
            log.info("got auth keys")
        else:
            raise CommandFailed("auth list failed")

        rc1 = fs_util.fuse_mount(client1, client_info['mounting_dir'])
        rc2 = fs_util.fuse_mount(client2, client_info['mounting_dir'])

        if rc1 == 0 and rc2 == 0:
            log.info("Fuse mount passed")
        else:
            raise CommandFailed("Fuse mount failed")

        rc3 = fs_util.kernel_mount(
            client3,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        rc4 = fs_util.kernel_mount(
            client4,
            client_info['mounting_dir'],
            client_info['mon_node_ip'])
        if rc3 == 0 and rc4 == 0:
            log.info("kernel mount passed")
        else:
            raise CommandFailed("kernel mount failed")
        rc = fs_util.activate_multiple_mdss(client_info['mds_nodes'])
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            raise CommandFailed("Activate multiple mdss failed")
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')
            p.spawn(fs_util.read_write_IO, client2,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                2,
                iotype='crefi'
            )
            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'g', 'readwrite')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'])
            for op in p:
                return_counts, rc = op

        result = fs_util.rc_verify('', return_counts)
        if result == 'Data validation success':
            print "Data validation success"
            log.info("Execution of Test cases %s started:" % (tc))
            with parallel() as p:
                p.spawn(
                    fs_util.mkdir,
                    client1,
                    0,
                    num_of_dirs * 1,
                    client_info['mounting_dir'],
                    dir_name)
                p.spawn(
                    fs_util.mkdir,
                    client2,
                    num_of_dirs * 1,
                    num_of_dirs * 2,
                    client_info['mounting_dir'],
                    dir_name)
                p.spawn(
                    fs_util.mkdir,
                    client3,
                    num_of_dirs * 2,
                    num_of_dirs * 3,
                    client_info['mounting_dir'],
                    dir_name)
                p.spawn(
                    fs_util.mkdir,
                    client4,
                    num_of_dirs * 3,
                    num_of_dirs * 4,
                    client_info['mounting_dir'],
                    dir_name)
                p.spawn(
                    fs_util.mkdir,
                    client3,
                    num_of_dirs * 4,
                    num_of_dirs * 5,
                    client_info['mounting_dir'],
                    dir_name)

            with parallel() as p:
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client1,
                    client_info['mounting_dir'],
                    dir_name,
                    0,
                    num_of_dirs * 1,
                    1,
                    fs_util.mds_fail_over,
                    client_info['mds_nodes'])
                p.spawn(
                    fs_util.filesystem_utilities,
                    client2,
                    client_info['mounting_dir'],
                    dir_name,
                    0,
                    num_of_dirs * 1)
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client3,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 1,
                    num_of_dirs * 2,
                    1,
                    fs_util.mds_fail_over,
                    client_info['mds_nodes'])
                p.spawn(
                    fs_util.filesystem_utilities,
                    client4,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 1,
                    num_of_dirs * 2)
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client1,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 2,
                    num_of_dirs * 3,
                    1,
                    fs_util.mds_fail_over,
                    client_info['mds_nodes'])
                p.spawn(
                    fs_util.filesystem_utilities,
                    client2,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 2,
                    num_of_dirs * 3)
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client3,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 3,
                    num_of_dirs * 4,
                    1,
                    fs_util.mds_fail_over,
                    client_info['mds_nodes'])
                p.spawn(
                    fs_util.filesystem_utilities,
                    client3,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 3,
                    num_of_dirs * 4)
                p.spawn(
                    fs_util.pinned_dir_io_mdsfailover,
                    client4,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 4,
                    num_of_dirs * 5,
                    1,
                    fs_util.mds_fail_over,
                    client_info['mds_nodes'])
                p.spawn(
                    fs_util.filesystem_utilities,
                    client1,
                    client_info['mounting_dir'],
                    dir_name,
                    num_of_dirs * 4,
                    num_of_dirs * 5)
                for op in p:
                    return_counts, rc = op
            log.info("Execution of Test case CEPH-%s ended:" % (tc))
            print "Results:"
            result = fs_util.rc_verify(tc, return_counts)
            print result
            log.info("Cleaning up!-----")
            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'],
                'umount')
            if rc == 0:
                log.info("Client Cleaning up successfull")
            else:
                raise CommandFailed("Client cleanup failed")

            rc = fs_util.mds_cleanup(client_info['mds_nodes'], None)
            if rc == 0:
                log.info("MDS Cleaning up successfull")
            else:
                raise CommandFailed("MDS cleanup failed")

        else:
            print "Data validation failed"
            log.info("Cleaning up!-----")
            fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'],
                'umount')
            fs_util.mds_cleanup(client_info['mds_nodes'], None)
            log.info("Cleaning up successfull")
            return 1

        print'Script execution time:------'
        stop = timeit.default_timer()
        total_time = stop - start
        mins, secs = divmod(total_time, 60)
        hours, mins = divmod(mins, 60)
        print ("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))

        return 0

    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
