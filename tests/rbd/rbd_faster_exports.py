import logging

from tests.rbd.rbd_utils import Rbd
from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    try:
        log.info("Running rbd export tests")
        rbd = Rbd(kw.get('ceph_nodes'))
        pool = rbd.random_string()
        image = rbd.random_string()
        snap = rbd.random_string()
        dir_name = rbd.random_string()
        clone = rbd.random_string()

        rbd.exec_cmd(cmd='mkdir {}'.format(dir_name))
        rbd.exec_cmd(cmd='sudo ceph osd pool create {} 128 128'.format(pool))
        rbd.exec_cmd(cmd='sudo rbd create -s {} {}/{}'.format('10G', pool, image))
        rbd.exec_cmd(cmd='sudo rbd bench-write {}/{}'.format(pool, image))
        rbd.exec_cmd(cmd='sudo rbd snap create {}/{}@{}'.format(pool, image, snap))
        rbd.exec_cmd(cmd='sudo rbd snap protect {}/{}@{}'.format(pool, image, snap))
        rbd.exec_cmd(cmd='sudo rbd clone {pool}/{}@{} {pool}/{}'
                     .format(image, snap, clone, pool=pool))

        with parallel() as p:
            p.spawn(rbd.exec_cmd, cmd='sudo rbd bench-write {}/{}'.format(pool, clone))
            p.spawn(rbd.exec_cmd, cmd='sudo rbd export {}/{} {}'.format(pool, clone, dir_name + '/export9876'))
            rbd.check_cmd_ec(p)

        with parallel() as p:
            p.spawn(rbd.exec_cmd, cmd='sudo rbd resize -s {} {}/{}'.format('20G', pool, image))
            p.spawn(rbd.exec_cmd, cmd='sudo rbd export {}/{} {}'.format(pool, image, dir_name + '/export9877_1'))
            rbd.check_cmd_ec(p)

        with parallel() as p:
            p.spawn(rbd.exec_cmd, cmd='sudo rbd resize -s {} --allow-shrink {}/{}'.format('8G', pool, image))
            p.spawn(rbd.exec_cmd, cmd='sudo rbd export {}/{} {}'.format(pool, image, dir_name + '/export9877_2'))
            rbd.check_cmd_ec(p)

        with parallel() as p:
            p.spawn(rbd.exec_cmd, cmd='sudo rbd flatten {}/{}'.format(pool, clone))
            p.spawn(rbd.exec_cmd, cmd='sudo rbd export {}/{} {}'.format(pool, image, dir_name + '/export9878'))
            rbd.check_cmd_ec(p)

        with parallel() as p:
            p.spawn(rbd.exec_cmd, cmd='sudo rbd export {}/{} {}'.format(pool, clone, dir_name + '/export9879'))
            p.spawn(rbd.exec_cmd, cmd='sudo rbd lock add {}/{} lok'.format(pool, clone))
            rbd.check_cmd_ec(p)

        with parallel() as p:
            p.spawn(rbd.exec_cmd, cmd='sudo rbd export {}/{} {}'.format(pool, clone, dir_name + '/export9880_1'))
            p.spawn(rbd.exec_cmd, cmd='sudo rbd resize -s {} {}/{}'.format('20G', pool, image))
            rbd.check_cmd_ec(p)

        with parallel() as p:
            p.spawn(rbd.exec_cmd, cmd='sudo rbd export {}/{} {}'.format(pool, clone, dir_name + '/export9880_2'))
            p.spawn(rbd.exec_cmd, cmd='sudo rbd resize -s {} --allow-shrink {}/{}'.format('8G', pool, image))
            rbd.check_cmd_ec(p)

        rbd.exec_cmd(cmd='rm -rf {}'.format(dir_name))
        rbd.exec_cmd(cmd='sudo ceph osd pool delete {pool} {pool} --yes-i-really-really-mean-it'
                     .format(pool=pool))

        return 0

    except Exception as e:
        log.info(e)
        return 1
