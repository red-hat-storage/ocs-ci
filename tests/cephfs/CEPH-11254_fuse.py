from tests.cephfs.cephfs_utils import FsUtils
from ceph.parallel import parallel
import timeit
from ceph.ceph import CommandFailed
import traceback
import logging
from ceph.utils import check_ceph_healthly

logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        start = timeit.default_timer()
        tc = '11254-fuse_clients'
        dir_name = 'dir'
        log.info("Running cephfs %s test case" % (tc))
        ceph_nodes = kw.get('ceph_nodes')
        config = kw.get('config')
        num_of_osds = config.get('num_of_osds')
        fs_util = FsUtils(ceph_nodes)
        client_info, rc = fs_util.get_clients()
        if rc == 0:
            log.info("Got client info")
        else:
            log.error("fetching client info failed")
            return 1
        client1, client2, client3, client4 = ([] for _ in range(4))
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
            log.error("auth list failed")
            return 1

        rc1 = fs_util.fuse_mount(client1, client_info['mounting_dir'])
        rc2 = fs_util.fuse_mount(client2, client_info['mounting_dir'])

        if rc1 == 0 and rc2 == 0:
            log.info("Fuse mount passed")
        else:
            log.error("Fuse mount failed")
            return 1

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
            log.error("kernel mount failed")
            return 1
        cluster_health_beforeIO = check_ceph_healthly(
            client_info['mon_node'][0], num_of_osds, len(
                client_info['mon_node']), None, 300)
        rc = fs_util.activate_multiple_mdss(client_info['mds_nodes'])
        if rc == 0:
            log.info("Activate multiple mdss successfully")
        else:
            raise CommandFailed("Activate multiple mdss failed")
        rc = fs_util.standby_rank(
            client_info['mds_nodes'],
            client_info['mon_node'],
            todo='add_rank')
        if rc == 0:
            log.info("Added standby ranks")
        else:
            raise Exception("Adding standby ranks failed")

        client1[0].exec_command(
            cmd='sudo mkdir %s%s' %
                (client_info['mounting_dir'], dir_name))
        if client1[0].exit_status == 0:
            log.info("Dir created")
        else:
            fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'])
        rc1 = fs_util.fstab_entry(
            client1,
            client_info['mounting_dir'],
            action='doEntry')
        rc2 = fs_util.fstab_entry(
            client2,
            client_info['mounting_dir'],
            action='doEntry')
        if rc1 == 0 and rc2 == 0:
            log.info("FSentry for clients are done")
        else:
            raise CommandFailed("FsEntry failed")

        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')
            p.spawn(fs_util.read_write_IO, client3,
                    client_info['mounting_dir'], 'g', 'read')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir_name,
                0,
                50,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                1,
                iotype='smallfile_create', fnum=1000, fsize=100)
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dir_name,
                0,
                1,
                iotype='crefi')
            p.spawn(fs_util.reboot, client1[0])

        with parallel() as p:
            p.spawn(fs_util.read_write_IO, client1,
                    client_info['mounting_dir'], 'g', 'write')

            p.spawn(fs_util.read_write_IO, client4,
                    client_info['mounting_dir'], 'g', 'read')

            p.spawn(fs_util.stress_io,
                    client1,
                    client_info['mounting_dir'],
                    dir_name,
                    0,
                    1,
                    iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                10,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dir_name,
                0,
                500,
                iotype='touch')
            p.spawn(fs_util.reboot, client2[0])

        cluster_health_afterIO = check_ceph_healthly(
            client_info['mon_node'][0], num_of_osds, len(
                client_info['mon_node']), None, 300)
        if cluster_health_afterIO == cluster_health_beforeIO:
            log.info('cluster is healthy')
        else:
            log.error("cluster is not healty")
            return 1
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dir_name,
                0,
                10,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir_name,
                0,
                10,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                500,
                iotype='touch')
            for node in client_info['mon_node']:
                p.spawn(fs_util.reboot, node)

        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dir_name,
                0,
                10,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir_name,
                0,
                10,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                500,
                iotype='touch')
            for node in client_info['mon_node']:
                fs_util.network_disconnect(node)
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dir_name,
                0,
                10,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dir_name,
                0,
                10,
                iotype='dd')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dir_name,
                0,
                500,
                iotype='touch')
            for node in client_info['mon_node']:
                fs_util.pid_kill(node, 'mon')

        cluster_health_afterIO = check_ceph_healthly(
            client_info['mon_node'][0], num_of_osds, len(
                client_info['mon_node']), None, 300)
        if cluster_health_beforeIO == cluster_health_afterIO:
            log.info("Cluster is healthy")
        else:
            return 1
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                client_info['kernel_clients'],
                client_info['mounting_dir'],
                'umount')
            rc = fs_util.standby_rank(
                client_info['mds_nodes'],
                client_info['mon_node'],
                todo='add_rank_revert')
            if rc == 0:
                log.info("removed standby ranks")
            rc1 = fs_util.fstab_entry(
                client1,
                client_info['mounting_dir'],
                action='revertEntry')
            rc2 = fs_util.fstab_entry(
                client2,
                client_info['mounting_dir'],
                action='revertEntry')
            if rc1 == 0 and rc2 == 0:
                log.info("FSentry for clients are done")
            else:
                return 1
        else:
            rc = fs_util.client_clean_up(
                client_info['fuse_clients'],
                '',
                client_info['mounting_dir'],
                'umount')

            rc = fs_util.standby_rank(
                client_info['mds_nodes'],
                client_info['mon_node'],
                todo='add_rank_revert')
            if rc == 0:
                log.info("removed standby ranks")
            else:
                return 1
            rc1 = fs_util.fstab_entry(
                client1,
                client_info['mounting_dir'],
                action='revertEntry')
            rc2 = fs_util.fstab_entry(
                client2,
                client_info['mounting_dir'],
                action='revertEntry')
            if rc1 == 0 and rc2 == 0:
                log.info("FSentry for clients are done")
            else:
                return 1
            if rc == 0:
                log.info('Cleaning up successfull')
        log.info("Execution of Test cases CEPH-%s ended:" % (tc))
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
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
