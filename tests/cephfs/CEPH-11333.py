from tests.cephfs.cephfs_utils import FsUtils
from ceph.ceph import CommandFailed
import traceback
import logging


logger = logging.getLogger(__name__)
log = logger


def run(**kw):
    try:
        log.info("Running  11333 test")
        ceph_nodes = kw.get('ceph_nodes')
        fs_util = FsUtils(ceph_nodes)
        client_info, rc = fs_util.get_clients()
        config = kw.get('config')
        k_and_m = config.get('ec-pool-k-m')
        new_fs_name = 'cephfs_new'
        new_fs_datapool = 'data_pool'
        new_pool = 'new_pool'
        fs_info = fs_util.get_fs_info(client_info['mon_node'][0])
        if rc == 0:
            log.info("Got client info")
        else:
            raise CommandFailed("fetching client info failed")
        if k_and_m:
            fs_util.del_cephfs(
                client_info['mds_nodes'], fs_info.get('fs_name'))
            profile_name = fs_util.create_erasure_profile(
                client_info['mon_node'][0], 'ec_profile_new', k_and_m[0], k_and_m[2])
            fs_util.create_pool(
                client_info['mon_node'][0],
                new_fs_datapool,
                64,
                64,
                pool_type='erasure',
                profile_name=profile_name)
            fs_util.create_fs(
                client_info['mds_nodes'],
                new_fs_name,
                new_fs_datapool,
                fs_info.get('metadata_pool_name'),
                pool_type='erasure_pool')
            fs_util.del_cephfs(client_info['mds_nodes'], new_fs_name)
            fs_util.create_fs(
                client_info['mds_nodes'],
                new_fs_name,
                new_fs_datapool,
                fs_info.get('metadata_pool_name'),
                pool_type='erasure_pool')
            fs_util.set_attr(client_info['mds_nodes'], new_fs_name)
            fs_util.create_pool(
                client_info['mon_node'][0],
                new_pool,
                64,
                64,
                pool_type='erasure',
                profile_name=profile_name)
            fs_util.add_pool_to_fs(
                client_info['mon_node'][0], new_fs_name, new_pool)
            fs_util.remove_pool_from_fs(
                client_info['mon_node'][0], new_fs_name, new_pool)
            fs_util.del_cephfs(client_info['mds_nodes'], new_fs_name)
            fs_util.create_fs(
                client_info['mds_nodes'],
                new_fs_name,
                new_fs_datapool,
                fs_info.get('metadata_pool_name'),
                pool_type='erasure_pool')
        else:
            fs_util.del_cephfs(
                client_info['mds_nodes'],
                fs_info.get('fs_name'))
            fs_util.create_pool(
                client_info['mon_node'][0], new_fs_datapool, 64, 64)
            fs_util.create_fs(
                client_info['mds_nodes'],
                new_fs_name,
                new_fs_datapool,
                fs_info.get('metadata_pool_name'))
            fs_util.del_cephfs(client_info['mds_nodes'], new_fs_name)
            fs_util.create_fs(
                client_info['mds_nodes'],
                new_fs_name,
                new_fs_datapool,
                fs_info.get('metadata_pool_name'))
            fs_util.set_attr(client_info['mds_nodes'], new_fs_name)
            fs_util.create_pool(client_info['mon_node'][0], new_pool, 64, 64)
            fs_util.add_pool_to_fs(
                client_info['mon_node'][0], new_fs_name, new_pool)
            fs_util.remove_pool_from_fs(
                client_info['mon_node'][0], new_fs_name, new_pool)
            fs_util.del_cephfs(client_info['mds_nodes'], new_fs_name)
            fs_util.create_fs(
                client_info['mds_nodes'],
                new_fs_name,
                new_fs_datapool,
                fs_info.get('metadata_pool_name'))

        return 0
    except CommandFailed as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1

    except Exception as e:
        log.info(e)
        log.info(traceback.format_exc())
        return 1
