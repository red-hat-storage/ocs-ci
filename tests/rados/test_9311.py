import json
import logging
import random
import time
import traceback

from ceph.rados_utils import RadosHelper

log = logging.getLogger(__name__)


def run(**kw):
    """
     CEPH-9311 - RADOS: Pyramid erasure codes (Local Repai     rable erasure codes):
     Bring down 2 osds (in case of k=4) from 2 localities      so that recovery happens from local repair code

     1. Create a LRC profile and then create a ec pool
     #ceph osd erasure-code-profile set $profile \
        plugin=lrc \
        k=4 m=2 l=3 \
        ruleset-failure-domain=osd
     # ceph osd pool create $poolname 1 1  erasure $profile

    2. start writing objects to the pool

    # rados -p poolname bench 1000 write --no-cleanup

    3. Bring down 2 osds from 2 different localities which    contains data chunk:(for this we need to figure out
    mapping) for ex: with k=4, m=2, l=3 mapping looks like
    chunk nr    01234567
    step 1      _cDD_cDD    (Here DD are data chunks )
    step 2      cDDD____
    step 3      ____cDDD

    from "step 1" in the above mapping we can see that
    data chunk is divided into 2 localities which is
    anlogous to 2 data center. so in our case for ex
    we have to bring down (3,7) OR (2,7) OR (2,6) OR (3,6)    ."""

    log.info("Running test ceph-9311")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')

    mons = []
    osds = []
    role = 'mon'

    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    role = 'osd'
    for osd in ceph_nodes:
        if osd.role == role:
            osds.append(osd)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(cmon=ctrlr.hostname))

    Helper = RadosHelper(ctrlr, config, log)

    '''Create an LRC profile'''
    sufix = random.randint(0, 10000)
    prof_name = "LRCprofile{suf}".format(suf=sufix)
    profile = "osd erasure-code-profile set {LRCprofile} \
            plugin=lrc\
            k=4 m=2 l=3 \
            ruleset-failure-domain=osd \
            crush-failure-domain=osd".format(LRCprofile=prof_name)
    try:
        (out, err) = Helper.raw_cluster_cmd(profile)
        outbuf = out.read()
        log.info(outbuf)
        log.info("created profile {LRCprofile}".format(
            LRCprofile=prof_name))
    except Exception:
        log.error("LRC profile creation failed")
        log.error(traceback.format_exc())
        return 1
    '''create LRC ec pool'''
    pool_name = "lrcpool{suf}".format(suf=sufix)
    try:
        Helper.create_pool(pool_name, 1, prof_name)
        log.info("Pool {pname} created".format(pname=pool_name))
    except Exception:
        log.error("lrcpool create failed")
        log.error(traceback.format_exc())
        return 1

    ''' Bringdown 2 osds which contains a 'D' from both localities
        we will be chosing osd at 2 and 7 from the given active set list
    '''
    oname = "UNIQUEOBJECT{i}".format(i=random.randint(0, 10000))
    cmd = "osd map {pname} {obj} --format json".format(pname=pool_name, obj=oname)
    (out, err) = Helper.raw_cluster_cmd(cmd)
    outbuf = out.read()
    log.info(outbuf)
    cmdout = json.loads(outbuf)
    # targt_pg = cmdout['pgid']
    tosds = []
    for i in [2, 7]:
        tosds.append(cmdout['up'][i])

    # putobj = "sudo rados -p {pool} put {obj} {path}".format(
    #     pool=pool_name, obj=oname, path="/etc/hosts"
    # )
    for i in range(10):
        putobj = "sudo rados -p {pool} put {obj} {path}".format(
            pool=pool_name, obj="{oname}{i}".format(oname=oname, i=i),
            path="/etc/hosts"
        )
        (out, err) = ctrlr.exec_command(cmd=putobj)
    '''Bringdown tosds'''
    for osd in tosds:
        Helper.get_osd_obj(osd, osds)
        Helper.kill_osd(osd, "SIGTERM", osds)
        time.sleep(5)

        outbuf = "degrade"
        timeout = 10
        found = 0
        status = '-s --format json'
        while timeout:
            if 'active' not in outbuf:
                (out, err) = Helper.raw_cluster_cmd(status)
                outbuf = out.read()
                time.sleep(1)
                timeout = timeout - 1
            else:
                found = 1
                break
        if timeout == 0 and found == 0:
            log.error("cluster didn't become active+clean..timeout")
            return 1

    '''check whether read/write can be done on the pool'''
    for i in range(10):
        putobj = "sudo rados -p {pool} put {obj} {path}".format(
            pool=pool_name, obj="{oname}{i}".format(oname=oname, i=i),
            path="/etc/hosts"
        )
        (out, err) = ctrlr.exec_command(cmd=putobj)
        log.info(out.read())
    for i in range(10):
        putobj = "sudo rados -p {pool} get {obj} {path}".format(
            pool=pool_name, obj="{oname}{i}".format(oname=oname, i=i),
            path="/tmp/{obj}{i}".format(obj=oname, i=i)
        )
        (out, err) = ctrlr.exec_command(cmd=putobj)
        log.info(out.read())
    '''donewith the test ,revive osds'''
    for osd in tosds:
        Helper.revive_osd(osd, osds)

    return 0
