import logging
import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from tests.cephfs.cephfs_utils import FsUtils

logger = logging.getLogger(__name__)
log = logger


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        tc = '11232 and 11233'
        dir_name = 'dir'
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
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

        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)
        rc4 = fs_util.auth_list(client4)
        print(rc1, rc2, rc3, rc4)
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
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                '',
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
        if 'Data validation success' in result:
            print("Data validation success")
            tc = '11232 and 11233'
            log.info("Execution of Test cases %s started:" % (tc))
            fs_util.allow_dir_fragmentation(client_info['mds_nodes'])
            log.info("Creating directory:")
            for node in client_info['fuse_clients']:
                out, rc = node.exec_command(
                    cmd='sudo mkdir %s%s' %
                        (client_info['mounting_dir'], dir_name))
                print(out.read().decode())
                break
            active_mds_node_1, active_mds_node_2, rc = fs_util.get_active_mdss(
                client_info['mds_nodes'])
            if rc == 0:
                log.info("Got active mdss")
            else:
                raise CommandFailed("getting active-mdss failed")
            node1_before_io, _, rc = fs_util.get_mds_info(
                active_mds_node_1, active_mds_node_2, info='get subtrees')
            if rc == 0:
                log.info("Got mds subtree info")
            else:
                raise CommandFailed("Mds info command failed")

            with parallel() as p:
                p.spawn(
                    fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dir_name,
                    0,
                    1000,
                    iotype='touch')
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dir_name,
                    1000,
                    2000,
                    iotype='touch')
                p.spawn(
                    fs_util.stress_io,
                    client2,
                    client_info['mounting_dir'],
                    dir_name,
                    2000,
                    3000,
                    iotype='touch')
                p.spawn(
                    fs_util.stress_io,
                    client4,
                    client_info['mounting_dir'],
                    dir_name,
                    3000,
                    4000,
                    iotype='touch')
                p.spawn(
                    fs_util.stress_io,
                    client3,
                    client_info['mounting_dir'],
                    dir_name,
                    4000,
                    5000,
                    iotype='touch')

            node1_after_io, _, rc = fs_util.get_mds_info(
                active_mds_node_1, active_mds_node_2, info='get subtrees')
            if rc == 0:
                log.info("Got mds subtree info")
            else:
                raise CommandFailed("Mds info command failed")

            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'])
            if rc == 0:
                log.info("Cleaning mount success")
            else:
                raise CommandFailed("Cleaning mount failed")

            node1_after_del, _, rc = fs_util.get_mds_info(
                active_mds_node_1, active_mds_node_2, info='get subtrees')
            if rc == 0:
                log.info("Got mds subtree info")
            else:
                raise CommandFailed("Mds info command failed")

            log.info("Execution of Test case 11232 and 11233 ended:")
            print("Results:")
            if node1_before_io != node1_after_io and \
                    node1_after_io != node1_after_del:
                log.info("Test case %s Passed" % (tc))
            else:
                return 1

            if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
                rc_client = fs_util.client_clean_up(
                    client_info['fuse_clients'],
                    client_info['kernel_clients'],
                    client_info['mounting_dir'],
                    'umount')
                rc_mds = fs_util.mds_cleanup(client_info['mds_nodes'], None)

            else:
                rc_client = fs_util.client_clean_up(
                    client_info['fuse_clients'], '',
                    client_info['mounting_dir'], 'umount')
                rc_mds = fs_util.mds_cleanup(client_info['mds_nodes'], None)

            if rc_client == 0 and rc_mds == 0:
                log.info('Cleaning up successfull')
            else:
                return 1
        print('Script execution time:------')
        stop = timeit.default_timer()
        total_time = stop - start
        mins, secs = divmod(total_time, 60)
        hours, mins = divmod(mins, 60)

        print("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))

        return 0
    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         client_info['kernel_clients'],
                                         client_info['mounting_dir'], 'umount')
        else:
            rc = fs_util.client_clean_up(client_info['fuse_clients'],
                                         '',
                                         client_info['mounting_dir'], 'umount')
        if rc == 0:
            log.info('Cleaning up successfull')
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
