import json
import logging
import random
import time
import traceback

from ceph.rados_utils import RadosHelper

log = logging.getLogger(__name__)


def run(**kw):

    log.info("Running CEPH-9924")
    log.info(run.__doc__)
    """
    CEPH-9925 - [RADOS]:
    Rewrite a known omap item of a replica and list-inconsistent-obj
    Steps:
        1. create an object in a replica pool
        2. add some omap keys and corresponding values to the object
        3. chose one of the replica and using ceph-objectstore-rool corrupt
         omap key or value
        4. Run deep-scrub  >scrub should report inconsistency
        5. run rados list-inconsistent-pg <pool> >should list the pg in
        which object is inconsistent
        6. Run rados list-inconsistent-obj <pg>	>shud report omap
        digest mismarch error
    """

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
    log.info("chosing mon {cmon} as ctrlrmon".format(
        cmon=ctrlr.hostname))
    helper = RadosHelper(ctrlr, config, log)

    '''create an replica pool'''
    pname0 = "replica_pool_{rand}".format(rand=random.randint(0, 10000))
    pname = pname0
    try:
        helper.create_pool(pname, 128)
        log.info("Pool {pname} is create".format(pname=pname))
    except Exception:
        log.error("failed to create pool")
        log.error(traceback.format_exc())
        return 1
    '''check whether pool exists'''
    try:
        helper.get_pool_num(pname)
    except Exception:
        log.error("Unable to find pool")
        log.error(traceback.format_exc())
        return 1

    time.sleep(10)

    oname = "OBJ_{pname}".format(pname=pname)

    putobj = "sudo rados -p {pool} put {obj} {path}".format(
        pool=pname, obj=oname, path="/etc/hosts"
    )
    (out, err) = ctrlr.exec_command(cmd=putobj)
    ''' creating omap key/value pairs for an object'''

    for i in range(4):
        omapcmd = "sudo rados -p {pool} setomapval {obj} {keey} {valu}".format(
            pool=pname, obj=oname, keey="key" + str(i), valu="value" + str(i))
        (out, err) = ctrlr.exec_command(cmd=omapcmd)
        log.info("put {obj}, omap key {keey} value {valu}".format(
            obj=oname, keey="key" + str(i), valu="value" + str(i)))

    '''
    Goto destination osd, stop the osd service to
    use ceph-objectstore-tool to corrupt
    omap keys
    '''

    cmd = "osd map {pname} {obj} --format json".format(
        pname=pname, obj=oname
    )
    (out, err) = helper.raw_cluster_cmd(cmd)
    outbuf = out.read()
    log.info(outbuf)
    cmdout = json.loads(outbuf)
    targt_pg = cmdout['pgid']
    '''Considering non primary osd'''
    targt_osd = cmdout['up'][1]
    ctx = helper.get_osd_obj(targt_osd, osds)
    helper.kill_osd(targt_osd, "SIGTERM", osds)
    time.sleep(10)
    slist_cmd = "sudo ceph-objectstore-tool --data-path \
            /var/lib/ceph/osd/ceph-{id} --journal-path \
            /var/lib/ceph/osd/ceph-{id}/journal \
            --pgid {pgid} {obj} list-omap".format(id=targt_osd,
                                                  obj=oname, pgid=targt_pg)
    (out, err) = ctx.exec_command(cmd=slist_cmd)
    outbuf = out.read()
    keylist = outbuf.split()
    log.info(outbuf)
    '''corrupting an omap key by rewriting the omap key with different value'''
    corrupt_cmd = "sudo ceph-objectstore-tool --data-path \
            /var/lib/ceph/osd/ceph-{id} --journal-path \
            /var/lib/ceph/osd/ceph-{id}/journal \
                   --pgid {pgid} {obj} set-omap \
                   {outbuf} {path}".format(id=targt_osd,
                                           obj=oname, pgid=targt_pg,
                                           outbuf=keylist[0],
                                           path='/etc/hosts')
    (out, err) = ctx.exec_command(cmd=corrupt_cmd)
    outbuf = out.read()
    log.info(outbuf)

    helper.revive_osd(targt_osd, osds)
    time.sleep(10)
    run_scrub = "pg deep-scrub {pgid}".format(pgid=targt_pg)
    (out, err) = helper.raw_cluster_cmd(run_scrub)
    outbuf = out.read()
    log.info(outbuf)

    while 'HEALTH_ERR' and 'active+clean+inconsistent' not in outbuf:
        status = "-s --format json"
        (out, err) = helper.raw_cluster_cmd(status)
        outbuf = out.read()
    log.info("HEALTH_ERR found as expected")
    log.info("inconsistent found as expected")

    timeout = 100
    found = 0
    while timeout:
        incon_pg = "sudo rados list-inconsistent-pg {pname}".format(
            pname=pname)
        (out, err) = ctrlr.exec_command(cmd=incon_pg)
        outbuf = out.read()
        log.info(outbuf)
        if targt_pg not in outbuf:
            time.sleep(1)
            timeout = timeout - 1
        else:
            found = 1
            break
    if timeout == 0 and found == 0:
        log.error("pg not listed as inconsistent")
        return 1

    timeout = 100
    found = 0
    while timeout:
        incon_obj = "sudo rados list-inconsistent-obj {pg}".format(pg=targt_pg)
        (out, err) = ctrlr.exec_command(cmd=incon_obj)
        outbuf = out.read()
        log.info(outbuf)
        if oname not in outbuf:
            time.sleep(1)
            timeout = timeout - 1
        else:
            found = 1
            break
    if timeout == 0 and found == 0:
        log.error("object is not listed in inconsistent obj")
        return 1

    return 0
