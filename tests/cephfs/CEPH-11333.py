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
        tc = '11333'
        log.info("Running cephfs %s test" % (tc))
        ceph_nodes = kw.get('ceph_nodes')
        fs_util = FsUtils(ceph_nodes)
        client_info = fs_util.get_clients()
        if 0 in client_info:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        client_info = client_info[0]
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

        return_counts_del_fs, rc = fs_util.del_cephfs(
            client_info['mds_nodes'], 'cephfs')
        if rc == 0:
            log.info("FS deletion successful")
        else:
            raise CommandFailed("FS deletion failed")

        return_counts_create_fs, rc = fs_util.create_fs(
            client_info['mds_nodes'], 'new_cephfs')
        if rc == 0:
            log.info("Fs Creation successful")
        else:
            raise CommandFailed("FS creation failed")

        return_counts_del_fs_again, rc = fs_util.del_cephfs(
            client_info['mds_nodes'], 'new_cephfs')
        if rc == 0:
            log.info("FS deletion successful")
        else:
            raise CommandFailed("FS deletion failed")

        return_counts_create_fs_again, rc = fs_util.create_fs(
            client_info['mds_nodes'], 'new_cephfs_again')
        if rc == 0:
            log.info("Fs Creation successful")
        else:
            raise CommandFailed("FS creation failed")

        return_counts_set_attr, rc = fs_util.set_attr(
            client_info['mds_nodes'], 'new_cephfs_again')
        if rc == 0:
            log.info("setting fs attrs success")
        else:
            raise CommandFailed("Setting Fs attr failed")

        return_counts_add_pool, rc = fs_util.add_pool(
            client_info['mon_node'], 'new_cephfs_again', 'new_data_pool')
        if rc == 0:
            log.info("Pool added to fs successfully")
        else:
            raise CommandFailed("Pool add to fs failed")

        return_counts_del_pool, rc = fs_util.remove_pool(
            client_info['mon_node'], 'new_cephfs_again', 'new_data_pool')
        if rc == 0:
            log.info("Pool removing success")
        else:
            raise CommandFailed("Pool removing  failed")
        return_counts = return_counts_del_fs.values() +\
            return_counts_create_fs.values() + \
            return_counts_del_fs_again.values() + \
            return_counts_create_fs_again.values() + \
            return_counts_set_attr.values() +\
            return_counts_add_pool.values() + \
            return_counts_del_pool.values()
        rc_set = set(return_counts)
        if len(rc_set) == 1:
            print "Tc %s passed" % (tc)
        return_counts_del_fs, rc = fs_util.del_cephfs(
            client_info['mds_nodes'], 'new_cephfs_again')
        return_counts_create_fs, rc = fs_util.create_fs(
            client_info['mds_nodes'], 'cephfs')
        reverting = return_counts_del_fs.values() +\
            return_counts_create_fs.values()
        rc_revert_set = set(reverting)
        if len(rc_revert_set) == 1:
            log.info("Reverted successfully")
            print'Script execution time:------'
            stop = timeit.default_timer()
            total_time = stop - start
            mins, secs = divmod(total_time, 60)
            hours, mins = divmod(mins, 60)
            print ("Hours:%d Minutes:%d Seconds:%f" % (hours, mins, secs))
            return 0
        else:
            return 1
    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
