import logging
import random
import traceback

from ceph.rados_utils import RadosHelper

log = logging.getLogger(__name__)


def run(**kw):
    """
        1. Create a LRC profile and then create a ec pool
        #ceph osd erasure-code-profile set $profile \
        plugin=lrc \
        k=  m= l= \
        ruleset-failure-domain=osd

        try different values for k, m and l
        # ceph osd pool create $poolname 1 1  erasure $profile

        2. perform I/O

        #rados put -p poolname obj /path/
    """

    log.info("Running CEPH-9322")
    log.info(run.__doc__)

    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')

    mons = []
    osds = []

    role = 'client'
    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    role = 'osd'
    for osd in ceph_nodes:
        if osd.role == role:
            osds.append(osd)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(
        cmon=ctrlr.hostname))

    helper = RadosHelper(ctrlr, config, log)
    '''beacause of limited machines resorting to following config'''
    lrc_config = [(4, 2, 3), (2, 1, 3), (2, 2, 2)]

    for conf in lrc_config:
        (k, m, l) = conf
        suffix = "{k}_{m}_{l}".format(k=k, m=m, l=l)
        prof_name = "LRCprofile{suf}".format(suf=suffix)
        profile = "osd erasure-code-profile set {LRC} \
                plugin=lrc\
                k={k} m={m} l={l}\
                ruleset-failure-domain=osd\
                crush-failure-domain=osd".format(LRC=prof_name,
                                                 k=k, m=m, l=l)
        try:
            (out, err) = helper.raw_cluster_cmd(profile)
            outbuf = out.read().decode()
            log.info(outbuf)
            log.info("created profile {LRC}".format(
                LRC=prof_name))
        except Exception:
            log.error("LRC profile creation failed")
            log.error(traceback.format_exc())
            return 1

        '''create LRC ec pool'''
        pname = "lrcpool{rand}{suf}".format(
            rand=random.randint(0, 10000), suf=suffix)
        try:
            helper.create_pool(pname, 1, prof_name)
            log.info("Pool {pname} created".format(pname=pname))
        except Exception:
            log.error("failed to create lrcpool")
            log.error(traceback.format_exc())
            return 1

        '''check whether pool exists'''
        try:
            helper.get_pool_num(pname)
        except Exception:
            log.error("Unable to find pool")
            log.error(traceback.format_exc())
            return 1
    return 0
