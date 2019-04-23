import logging
import random

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
    out, err, ret = client.exec_command(cmd=pool_create, check_ec=True)
    if ret:
        log.error(f"Pool creation failed for {pool}")
        log.error(err)
        return ret
    log.info(f"Pool {pool} created")
    log.info(out)

    block = str(config.get('size', 4 << 20))
    time = config.get('time', 120)
    timeout = time + 10
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
    out, err, ret = client.exec_command(
        cmd=rados_bench,
        check_ec=True,
        long_running=True,
        timeout=timeout
    )
    if ret:
        log.error("Rados bench failed")
        log.error(err)
        return ret

    log.info(out)
    log.info("Finished radosbench")
    return ret
