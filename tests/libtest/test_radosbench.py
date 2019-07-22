import logging
import random


from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


def run(**kw):
    """ A task for radosbench

        Runs radosbench command on specified pod . If parameters are
        not provided task assumes few default parameters.This task
        runs command in synchronous fashion.


        Args:
            **kw: Needs a dictionary of various radosbench parameters.
                ex: pool_name:pool
                    pg_num:number of pgs for pool
                    op: type of operation {read, write}
                    cleanup: True OR False


        Returns:
            ret: return value of radosbench command
    """

    log.info("Running radosbench task")
    ceph_pods = kw.get('ceph_pods')  # list of pod objects of ceph cluster
    config = kw.get('config')

    clients = []
    role = config.get('role', 'client')
    clients = [cpod for cpod in ceph_pods if role in cpod.roles]

    idx = config.get('idx', 0)
    client = clients[idx]
    pg_num = config.get('pg_num', 64)
    op = config.get('op', 'write')
    cleanup = ['--no-cleanup', '--cleanup'][config.get('cleanup', True)]
    pool = config.get('pool', 'test_pool' + str(random.randint(10, 999)))

    # FIXME: replace pool create with library function
    pool_create = (
        f"ceph osd pool create "
        f"{pool} "
        f"{pg_num} "
    )
    try:
        ret = client.exec_ceph_cmd(ceph_cmd=pool_create)
    except CommandFailed as ex:
        log.error(f"Pool creation failed for {pool}\nError is: {ex}")
        return False
    log.info(f"Pool {pool} created")
    log.info(ret)

    block = str(config.get('size', 4 << 20))
    time = config.get('time', 120)
    time = str(time)

    rados_bench = (
        f"rados --no-log-to-stderr "
        f"-b {block} "
        f"-p {pool} "
        f"bench "
        f"{time} "
        f"{op} "
        f"{cleanup} "
    )
    try:
        ret = client.exec_ceph_cmd(ceph_cmd=rados_bench)
    except CommandFailed as ex:
        log.error(f"Rados bench failed\n Error is: {ex}")
        return False

    log.info(ret)
    log.info("Finished radosbench")
    return ret
