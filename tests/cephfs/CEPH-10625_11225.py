from tests.cephfs.cephfs_utils import FsUtils
from ceph.parallel import parallel
import timeit
import time
from ceph.ceph import CommandFailed
import traceback
import logging
logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        tc = '11335'
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
        active_mds_node_1, active_mds_node_2, rc = fs_util.get_active_mdss(
            client_info['mds_nodes'])
        if rc == 0:
            log.info("Got active mdss")
        else:
            raise CommandFailed("getting active-mdss failed")
        #
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'm', 'write')
            p.spawn(fs_util.read_write_IO, client2,
                    client_info['mounting_dir'], 'm', 'read')
            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'm', 'readwrite')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'], 'm', 'readwrite')
            for op in p:
                return_counts, rc = op

        result = fs_util.rc_verify('', return_counts)
        print result

        log.info("Performing Auto Eviction:")
        mds1_before_evict, _, rc = fs_util.get_mds_info(
            active_mds_node_1, active_mds_node_2, info='session ls')
        rc = fs_util.auto_evict(active_mds_node_1, client_info['clients'], 0)
        if rc == 0:
            log.info("client process killed successfully for auto eviction")
        else:
            raise CommandFailed(
                "client process killing failed for auto eviction")
        log.info("Waiting 300 seconds for auto eviction---")
        time.sleep(300)
        mds1_after_evict, _, rc = fs_util.get_mds_info(
            active_mds_node_1, active_mds_node_2, info='session ls')
        if mds1_before_evict != mds1_after_evict:
            log.info("Auto eviction Passed")
        else:
            raise CommandFailed("Auto eviction Failed")
        print "-------------------------------------------------------"
        if client3[0].pkg_type == 'deb' and client4[0].pkg_type == 'deb':
            for client in client_info['fuse_clients']:
                client.exec_command(
                    cmd='sudo fusermount -u %s -z' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                    (client_info['mounting_dir']))
        else:
            for client in client_info['fuse_clients']:
                client.exec_command(
                    cmd='sudo fusermount -u %s -z' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                    (client_info['mounting_dir']))

            for client in client_info['kernel_clients']:
                client.exec_command(
                    cmd='sudo umount %s -l' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                    (client_info['mounting_dir']))

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
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'm', 'write')
            p.spawn(fs_util.read_write_IO, client2,
                    client_info['mounting_dir'], 'm', 'read')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                '',
                0,
                1,
                iotype='crefi'
            )
            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'm', 'readwrite')
            for op in p:
                return_counts, rc = op

        result = fs_util.rc_verify('', return_counts)
        print result
        log.info("Performing Manual eviction:")
        ip_addr = fs_util.manual_evict(active_mds_node_1, 0)
        mds1_after_evict, _, rc = fs_util.get_mds_info(
            active_mds_node_1, active_mds_node_2, info='session ls')
        print mds1_before_evict
        print '------------------------'
        print mds1_after_evict
        print '-----------------------'
        if mds1_before_evict != mds1_after_evict:
            log.info("Manual eviction success")
        else:
            raise CommandFailed("Manual Eviction Failed")
        log.info("Removing client from OSD blacklisting:")
        rc = fs_util.osd_blacklist(active_mds_node_1, ip_addr)
        if rc == 0:
            log.info("Removing client from OSD blacklisting successfull")
        else:
            raise CommandFailed("Removing client from OSD blacklisting Failed")
        print '-' * 10

        if client3[0].pkg_type == 'deb' and client4[0].pkg_type == 'deb':
            for client in client_info['fuse_clients']:
                client.exec_command(
                    cmd='sudo fusermount -u %s -z' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                    (client_info['mounting_dir']))
        else:
            for client in client_info['fuse_clients']:
                client.exec_command(
                    cmd='sudo fusermount -u %s -z' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                    (client_info['mounting_dir']))

            for client in client_info['kernel_clients']:
                client.exec_command(
                    cmd='sudo umount %s -l' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                    (client_info['mounting_dir']))

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
        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'm', 'write')
            p.spawn(fs_util.read_write_IO, client2,
                    client_info['mounting_dir'], 'm', 'read')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                '',
                0,
                1,
                iotype='crefi'
            )
            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'm', 'readwrite')
            for op in p:
                return_counts, rc = op

        result = fs_util.rc_verify('', return_counts)
        print result
        log.info("Performing configuring blacklisting:")
        rc = fs_util.config_blacklist_manual_evict(active_mds_node_1, 0)
        if rc == 0:
            log.info("Configure blacklisting for manual evict success")
            rc = fs_util.config_blacklist_manual_evict(
                active_mds_node_1, 0, revert=True)
        else:
            raise CommandFailed(
                "Configure blacklisting for manual evict failed")
        print '-' * 10
        rc = fs_util.config_blacklist_auto_evict(active_mds_node_1, 0)
        if rc == 0:
            log.info("Configure blacklisting for auto evict success")
            rc = fs_util.config_blacklist_auto_evict(
                active_mds_node_1, 0, revert=True)
            if rc == 0:
                log.info("Reverted successfully")
            else:
                raise CommandFailed(
                    "Configure blacklisting for auto evict failed")
        else:
            raise CommandFailed("Configure blacklisting for auto evict failed")

        if client3[0].pkg_type == 'deb' and client4[0].pkg_type == 'deb':
            for client in client_info['fuse_clients']:
                client.exec_command(
                    cmd='sudo rm -rf %s*' %
                    (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo fusermount -u %s -z' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                        (client_info['mounting_dir']))
        else:
            for client in client_info['fuse_clients']:
                client.exec_command(
                    cmd='sudo fusermount -u %s -z' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                        (client_info['mounting_dir']))

            for client in client_info['kernel_clients']:
                client.exec_command(
                    cmd='sudo umount %s -l' %
                        (client_info['mounting_dir']))
                client.exec_command(
                    cmd='sudo rm -rf %s' %
                        (client_info['mounting_dir']))
        print 'Script execution time:------'
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
