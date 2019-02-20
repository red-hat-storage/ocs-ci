import hashlib
import logging
import random
import time
import traceback

from ceph.parallel import parallel
from ceph.rados_utils import RadosHelper

log = logging.getLogger(__name__)

fcsum = "" '''checksum to verify in read'''
objlist = []


def prepare_sdata(mon):
    """
    create a 4MB obj, same obj will be put nobj times
    because in do_rados_get we have to verify checksum
    """
    global fcsum
    sdata = "/tmp/sdata.txt"
    DSTR = "hello world"
    dbuf = DSTR * 419430
    sfd = None

    try:
        sfd = mon.write_file(file_name=sdata, file_mode='w+')
        sfd.write(dbuf)
        sfd.flush()
    except Exception:
        log.error("file creation failed")
        log.error(traceback.format_exc())

    sfd.seek(0)
    fcsum = hashlib.md5(sfd.read().decode()).hexdigest()
    log.info("md5 digest = {fcsum}".format(fcsum=fcsum))
    sfd.close()

    return sdata


def do_rados_put(mon, pool, nobj):
    """
    write nobjs to cluster with sdata as source
    """
    src = prepare_sdata(mon)
    log.info("src file is {src}".format(src=src))

    for i in range(nobj):
        print("running command on {mon}".format(mon=mon.hostname))
        put_cmd = "sudo rados put -p {pname} obj{i} {src}".format(
            pname=pool, i=i, src=src)
        log.info("cmd is {pcmd}".format(pcmd=put_cmd))
        try:
            (out, err) = mon.exec_command(cmd=put_cmd)
            out.read().decode()
        except Exception:
            log.error(traceback.format_exc)
            return 1
        objlist.append("obj{i}".format(i=i))

    return 0


def do_rados_get(mon, pool, niter):
    """
    scan the pool and get all objs verify checksum with
    fcsum
    """
    global fcsum
    for i in range(niter):
        pool_ls = "sudo rados -p {pool} ls".format(pool=pool)
        (out, err) = mon.exec_command(cmd=pool_ls)
        out.readlines()

        while not fcsum:
            pass
            '''
            read objects one by one from the previous list
            and compare checksum of each object
            '''
        for obj in objlist:
            file_name = "/tmp/{obj}".format(obj=obj)
            get_cmd = "sudo rados -p {pool} get  {obj} {file_name}".format(
                pool=pool, obj=obj, file_name=file_name)
            try:
                mon.exec_command(cmd=get_cmd)
                outbuf = out.readlines()
                log.info(outbuf)
            except Exception:
                log.error("rados get failed for {obj}".format(
                    obj=obj))
                log.error(traceback.format_exc)
            dfd = mon.write_file(file_name=file_name, file_mode='r')
            dcsum = hashlib.md5(dfd.read().decode()).hexdigest()
            log.info("csum of obj {objname}={dcsum}".format(
                objname=obj, dcsum=dcsum))
            print(type(fcsum))
            print("fcsum=", fcsum)
            print(type(dcsum))
            if fcsum != dcsum:
                log.error("checksum mismatch for obj {obj}".format(
                    obj=obj))
                dfd.close()
                return 1
            dfd.close()


def run(ceph_cluster, **kw):
    """
     1. Create a LRC profile and then create a ec pool
            #ceph osd erasure-code-profile set $profile \
            plugin=lrc \
            k=4 m=2 l=3 \
            ruleset-failure-domain=osd
             # ceph osd pool create $poolname 1 1  erasure $profile

    2. start writing a large object so that we will get \
            sometime to fail the osd while the reads and writes are
            in progress on an object

    # rados put -p lrcpool obj1 /src/path
    #rados get -p lrcpool obj1 /tmp/obj1

    while above command is in progress kill primary
    osd responsible for the PG.
    primary can be found from
    # ceph pg dump

    3. Bring back primary

    4. Repeat the step 2 but this time kill some secondary osds

    Args:
        ceph_cluster (ceph.ceph.Ceph):
    """

    log.info("Running test CEPH-9281")
    ceph_nodes = kw.get('ceph_nodes')
    config = kw.get('config')

    mons = []
    role = 'client'

    for mnode in ceph_nodes:
        if mnode.role == role:
            mons.append(mnode)

    ctrlr = mons[0]
    log.info("chosing mon {cmon} as ctrlrmon".format(cmon=ctrlr.hostname))

    helper = RadosHelper(ctrlr, config, log)

    ''' create LRC profile '''
    sufix = random.randint(0, 10000)
    prof_name = "LRCprofile{suf}".format(suf=sufix)
    profile = "osd erasure-code-profile set {LRCprofile} \
        plugin=lrc\
        k=4 m=2 l=3 \
        ruleset-failure-domain=osd \
        crush-failure-domain=osd".format(LRCprofile=prof_name)
    try:
        (out, err) = helper.raw_cluster_cmd(profile)
        outbuf = out.read().decode()
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
        helper.create_pool(pool_name, 1, prof_name)
        log.info("Pool {pname} created".format(pname=pool_name))
    except Exception:
        log.error("lrcpool create failed")
        log.error(traceback.format_exc())
        return 1

    '''rados put and get in a parallel task'''
    with parallel() as p:
        p.spawn(do_rados_put, ctrlr, pool_name, 20)
        p.spawn(do_rados_get, ctrlr, pool_name, 10)

        for res in p:
            log.info(res)

    try:
        pri_osd_id = helper.get_pg_primary(pool_name, 0)
        log.info("PRIMARY={pri}".format(pri=pri_osd_id))
    except Exception:
        log.error("getting primary failed")
        log.error(traceback.format_exc())
        return 1

    log.info("SIGTERM osd")
    pri_osd = ceph_cluster.get_osd_by_id(pri_osd_id)
    pri_osd_node = pri_osd.node
    pri_osd_service = ceph_cluster.get_osd_service_name(pri_osd_id)
    try:
        helper.kill_osd(pri_osd_node, pri_osd_service)
        log.info("osd killed")
    except Exception:
        log.error("killing osd failed")
        log.error(traceback.format_exc())
    time.sleep(10)
    if helper.is_up(pri_osd_id):
        log.error("unexpected! osd is still up")
        return 1
    time.sleep(5)
    log.info("Reviving osd {osd}".format(osd=pri_osd_id))

    try:
        if helper.revive_osd(pri_osd_node, pri_osd_service):
            log.error("revive failed")
            return 1
    except Exception:
        log.error("revive failed")
        log.error(traceback.format_exc())
        return 1
    time.sleep(10)
    if helper.is_up(pri_osd_id):
        log.info("osd is UP")
    else:
        log.error("osd is DOWN")
        return 1

    time.sleep(10)
    try:
        rand_osd_id = helper.get_pg_random(pool_name, 0)
        log.info("RANDOM OSD={rosd}".format(rosd=rand_osd_id))
    except Exception:
        log.error("getting  random osd failed")
        log.error(traceback.format_exc())
        return 1
    log.info("SIGTERM osd")
    rand_osd = ceph_cluster.get_osd_by_id(rand_osd_id)
    rand_osd_node = rand_osd.node
    rand_osd_service = ceph_cluster.get_osd_service_name(rand_osd_id)
    try:
        helper.kill_osd(rand_osd_node, rand_osd_service)
        log.info("osd killed")
    except Exception:
        log.error("killing osd failed")
        log.error(traceback.format_exc())
    time.sleep(10)
    if helper.is_up(rand_osd_id):
        log.error("unexpected! osd is still up")
        return 1
    time.sleep(5)
    log.info("Reviving osd {osd}".format(osd=rand_osd_id))
    try:
        if helper.revive_osd(rand_osd_node, rand_osd_service):
            log.error("revive failed")
            return 1
    except Exception:
        log.error("revive failed")
        log.error(traceback.format_exc())
        return 1
    time.sleep(30)
    if helper.is_up(pri_osd_id):
        log.info("osd is UP")
    else:
        log.error("osd is DOWN")
        return 1

    return 0
