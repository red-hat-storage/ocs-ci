from tests.cephfs.cephfs_utils import FsUtils
import timeit
from ceph.ceph import CommandFailed
import traceback
import random
import string
import logging
from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        tc = '11222'
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
        cluster_health_beforeIO = check_ceph_healthly(
            client_info['mon_node'], 12, 1, None, 300)

        dir1 = ''.join(
            random.choice(
                string.lowercase +
                string.digits) for _ in range(10))
        for client in client_info['clients']:
            log.info("Creating directory:")
            client.exec_command(
                cmd='sudo mkdir %s%s' %
                (client_info['mounting_dir'], dir1))
            log.info("Creating directories with breadth and depth:")
            out, rc = client.exec_command(
                cmd='sudo crefi %s%s --fop create --multi -b 10 -d 10 '
                    '--random --min=1K --max=10K' %
                (client_info['mounting_dir'], dir1))
            print out.read()
            break

        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dir1,
                0,
                5,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dir1,
                0,
                100,
                iotype='touch')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir1,
                0,
                5,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir1,
                0,
                5,
                iotype='crefi')
            for op in p:
                return_counts, rc = op
        result1 = fs_util.rc_verify('', return_counts)
        print result1

        for client in client_info['clients']:
            client.exec_command(
                cmd='sudo rm -rf %s%s' %
                (client_info['mounting_dir'], dir1))
            break

        for client in client_info['clients']:
            log.info("Creating directories with breadth and depth:")
            out, rc = client.exec_command(
                cmd='sudo crefi %s%s --fop create --multi -b 10 -d 10 '
                    '--random --min=1K --max=10K' %
                (client_info['mounting_dir'], dir1))
            print out.read()
            log.info("Renaming the dirs:")
            out, rc = client.exec_command(
                cmd='sudo crefi '
                '%s%s --fop rename --multi -b 10 -d 10 --random '
                '--min=1K --max=10K' %
                (client_info['mounting_dir'], dir1))
            print out.read()

            break
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dir1,
                0,
                5,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dir1,
                0,
                100,
                iotype='touch')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir1,
                0,
                5,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir1,
                0,
                5,
                iotype='crefi')
            for op in p:
                return_counts, rc = op
        result2 = fs_util.rc_verify('', return_counts)
        print result2
        cluster_health_afterIO = check_ceph_healthly(
            client_info['mon_node'], 12, 1, None, 300)
        if cluster_health_beforeIO == cluster_health_afterIO:
            print "Testcase %s passed" % (tc)
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
