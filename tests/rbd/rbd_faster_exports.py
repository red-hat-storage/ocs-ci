import logging

from tests.rbd.rbd_utils import Rbd
from ceph.parallel import parallel

log = logging.getLogger(__name__)


def run(**kw):
    log.info("Running rbd export tests")
    rbd = Rbd(**kw)
    config = kw.get('config')
    pool = rbd.random_string()
    image = rbd.random_string()
    snap = rbd.random_string()
    dir_name = rbd.random_string()
    clone = rbd.random_string()

    rbd.exec_cmd(cmd='mkdir {}'.format(dir_name))
    rbd.create_pool(poolname=pool)
    rbd.exec_cmd(cmd='rbd create -s {} {}/{}'
                 .format('10G', pool, image))
    rbd.exec_cmd(cmd='rbd bench-write --io-total {} {}/{}'
                 .format(config.get('io-total'), pool, image))
    rbd.exec_cmd(cmd='rbd snap create {}/{}@{}'.format(pool, image, snap))
    rbd.exec_cmd(cmd='rbd snap protect {}/{}@{}'.format(pool, image, snap))
    rbd.exec_cmd(cmd='rbd clone {pool}/{}@{} {pool}/{}'
                 .format(image, snap, clone, pool=pool))

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd='rbd bench-write --io-total {} {}/{}'
                .format(config.get('io-total'), pool, clone))
        p.spawn(rbd.exec_cmd, cmd='rbd export {}/{} {}'
                .format(pool, clone, dir_name + '/export9876'))

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd='rbd resize -s {} {}/{}'
                .format('20G', pool, image))
        p.spawn(rbd.exec_cmd, cmd='rbd export {}/{} {}'
                .format(pool, image, dir_name + '/export9877_1'))

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd='rbd resize -s {} --allow-shrink {}/{}'
                .format('8G', pool, image))
        p.spawn(rbd.exec_cmd, cmd='rbd export {}/{} {}'
                .format(pool, image, dir_name + '/export9877_2'))

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd='rbd flatten {}/{}'.format(pool, clone))
        p.spawn(rbd.exec_cmd, cmd='rbd export {}/{} {}'
                .format(pool, image, dir_name + '/export9878'))

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd='rbd export {}/{} {}'
                .format(pool, clone, dir_name + '/export9879'))
        p.spawn(rbd.exec_cmd, cmd='rbd lock add {}/{} lok'.format(pool, clone))

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd='rbd export {}/{} {}'
                .format(pool, clone, dir_name + '/export9880_1'))
        p.spawn(rbd.exec_cmd, cmd='rbd resize -s {} {}/{}'
                .format('20G', pool, image))

    with parallel() as p:
        p.spawn(rbd.exec_cmd, cmd='rbd export {}/{} {}'
                .format(pool, clone, dir_name + '/export9880_2'))
        p.spawn(rbd.exec_cmd, cmd='rbd resize -s {} --allow-shrink {}/{}'
                .format('8G', pool, image))

    rbd.clean_up(dir_name=dir_name, pools=[pool])

    return rbd.flag
