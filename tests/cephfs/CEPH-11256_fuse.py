import logging
import timeit
import traceback

from ceph.ceph import CommandFailed
from ceph.parallel import parallel
from ceph.utils import check_ceph_healthly
from tests.cephfs.cephfs_utils import FsUtils

logger = logging.getLogger(__name__)
log = logger


# mds


def run(ceph_cluster, **kw):
    try:
        start = timeit.default_timer()
        tc = '11256-fuse'
        dir_name = 'dir'
        log.info("Running cephfs %s test case" % (tc))
        fs_util = FsUtils(ceph_cluster)
        config = kw.get('config')
        num_of_osds = config.get('num_of_osds')
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
        rc1 = fs_util.auth_list(client1)
        rc2 = fs_util.auth_list(client2)
        rc3 = fs_util.auth_list(client3)
        rc4 = fs_util.auth_list(client4)
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
            raise Exception("kernel mount failed")
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

        dirs, rc = fs_util.mkdir(
            client1, 0, 4, client_info['mounting_dir'], dir_name)
        if rc == 0:
            log.info("Directories created")
        else:
            raise CommandFailed("Directory creation failed")
        dirs = dirs.split('\n')
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dirs[1],
                0,
                1,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dirs[0],
                0,
                1,
                iotype='crefi')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dirs[2],
                '',
                '',
                iotype='smallfile_create', fnum=1000, fsize=1024)
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dirs[3],
                0,
                10,
                iotype='dd')
            for node in client_info['mds_nodes']:
                p.spawn(fs_util.reboot, node)
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dirs[2],
                0,
                1,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dirs[0],
                0,
                1,
                iotype='crefi')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dirs[1],
                '',
                '',
                iotype='smallfile_create', fnum=1000, fsize=1024)
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dirs[3],
                0,
                10,
                iotype='dd')
            for node in client_info['mds_nodes']:
                p.spawn(
                    fs_util.daemon_systemctl,
                    node,
                    'mds',
                    'active_mds_restart')
        with parallel() as p:
            for node in client_info['mds_nodes']:
                p.spawn(fs_util.heartbeat_map, node)

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
                dirs[3],
                0,
                1,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dirs[0],
                0,
                1,
                iotype='crefi')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dirs[2],
                0,
                1,
                iotype='smallfile_create', fnum=10, fsize=1024)
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dirs[1],
                0,
                1,
                iotype='dd')
            for node in client_info['mds_nodes']:
                fs_util.network_disconnect(node)
        with parallel() as p:
            for node in client_info['mds_nodes']:
                p.spawn(fs_util.heartbeat_map, node)
        with parallel() as p:
            p.spawn(
                fs_util.stress_io,
                client1,
                client_info['mounting_dir'],
                dirs[0],
                0,
                1,
                iotype='fio')
            p.spawn(
                fs_util.stress_io,
                client2,
                client_info['mounting_dir'],
                dirs[1],
                0,
                1,
                iotype='crefi')
            p.spawn(
                fs_util.stress_io,
                client3,
                client_info['mounting_dir'],
                dirs[3],
                0,
                1,
                iotype='smallfile_create', fnum=10, fsize=1024)
            p.spawn(
                fs_util.stress_io,
                client4,
                client_info['mounting_dir'],
                dirs[2],
                0,
                1,
                iotype='dd')
            for node in client_info['mds_nodes']:
                fs_util.pid_kill(node, 'mds')
        with parallel() as p:
            for node in client_info['mds_nodes']:
                p.spawn(fs_util.heartbeat_map, node)
        cluster_health_afterIO = check_ceph_healthly(
            client_info['mon_node'][0], num_of_osds, len(
                client_info['mon_node']), None, 300)
        if cluster_health_beforeIO == cluster_health_afterIO:
            log.info("Cluster is healthy")
        else:
            return 1
        log.info('Cleaning up!-----')
        if client3[0].pkg_type != 'deb' and client4[0].pkg_type != 'deb':
            fs_util.client_clean_up(
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
        fs_util.standby_rank(
            client_info['mds_nodes'],
            client_info['mon_node'],
            todo='add_rank_revert')
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
            rc = fs_util.standby_rank(
                client_info['mds_nodes'],
                client_info['mon_node'],
                todo='add_rank_revert')

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
            log.info('Cleaning up successfull')
        return 1
    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
