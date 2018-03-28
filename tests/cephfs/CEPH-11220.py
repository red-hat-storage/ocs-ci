from tests.cephfs.cephfs_utils import FsUtils
import timeit
from ceph.ceph import CommandFailed
import traceback
import logging
logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        tc = '11220'
        dir_name = 'dir'
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

        log.info("Creating directory:")
        for node in client1:
            out, rc = node.exec_command(
                cmd='sudo mkdir %s%s' %
                (client_info['mounting_dir'], dir_name))
            print out.read()
            break

        return_counts1, rc1 = fs_util.stress_io(
            client1, client_info['mounting_dir'],
            dir_name, 0, 1, iotype='crefi')
        return_counts2, rc2 = fs_util.stress_io(
            client2, client_info['mounting_dir'], dir_name, 0, 1, iotype='fio')
        return_counts3, rc3 = fs_util.read_write_IO(
            client3, client_info['mounting_dir'], dir_name=dir_name)
        if rc1 == 0 and rc2 == 0 and rc3 == 0:
            log.info("IOs on clients successfull")
            log.info("Testcase %s passed" % (tc))
            log.info('Cleaning up!-----')
            if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
                rc = fs_util.client_clean_up(
                    client_info['fuse_clients'],
                    client_info['kernel_clients'],
                    client_info['mounting_dir'],
                    'umount')
            else:
                rc = fs_util.client_clean_up(
                    client_info['fuse_clients'],
                    '',
                    client_info['mounting_dir'],
                    'umount')
            if rc == 0:
                log.info('Cleaning up successfull')
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
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            fs_util.client_clean_up(client_info['fuse_clients'],
                                    client_info['kernel_clients'],
                                    client_info['mounting_dir'], 'umount')
        else:
            fs_util.client_clean_up(client_info['fuse_clients'],
                                    '',
                                    client_info['mounting_dir'], 'umount')
        return 1

    except Exception as e:

        log.info(e)

        log.info(traceback.format_exc())

        return 1
