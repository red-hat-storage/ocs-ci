import json
import logging
import random
import time
import traceback

logger = logging.getLogger(__name__)


class RadosHelper:
    def __init__(self, mon, config=None, log=None,
                 cluster='ceph'):
        self.mon = mon
        self.config = config
        if log:
            self.log = lambda x: log.info(x)
        self.num_pools = self.get_num_pools()
        self.cluster = cluster
        pools = self.list_pools()
        self.pools = {}
        for pool in pools:
            self.pools[pool] = self.get_pool_property(pool, 'pg_num')

    def raw_cluster_cmd(self, *args):
        """
        :return: (stdout, stderr)
        """
        ceph_args = [
            'sudo',
            'ceph',
            '--cluster',
            self.cluster,
        ]

        ceph_args.extend(args)
        print ceph_args
        clstr_cmd = " ".join(str(x) for x in ceph_args)
        print clstr_cmd
        (stdout, stderr) = self.mon.exec_command(cmd=clstr_cmd)
        return stdout, stderr

    def get_num_pools(self):
        """
        :returns: number of pools in the
                cluster
           """
        '''TODO'''

    def get_osd_dump_json(self):
        """
        osd dump --format=json converted to a python object
        :returns: the python object
        """
        (out, err) = self.raw_cluster_cmd('osd', 'dump', '--format=json')
        print type(out)
        outbuf = out.read()
        return json.loads('\n'.join(outbuf.split('\n')[1:]))

    def create_pool(self, pool_name, pg_num=16,
                    erasure_code_profile_name=None,
                    min_size=None,
                    erasure_code_use_overwrites=False):
        """
        Create a pool named from the pool_name parameter.
        :param pool_name: name of the pool being created.
        :param pg_num: initial number of pgs.
        :param erasure_code_profile_name: if set and !None create an
            erasure coded pool using the profile
        :param erasure_code_use_overwrites: if true, allow overwrites
        """
        assert isinstance(pool_name, basestring)
        assert isinstance(pg_num, int)
        assert pool_name not in self.pools
        self.log("creating pool_name %s" % (pool_name,))
        if erasure_code_profile_name:
            self.raw_cluster_cmd('osd', 'pool', 'create',
                                 pool_name, str(pg_num), str(pg_num),
                                 'erasure', erasure_code_profile_name)
        else:
            self.raw_cluster_cmd('osd', 'pool', 'create',
                                 pool_name, str(pg_num))
        if min_size is not None:
            self.raw_cluster_cmd(
                'osd', 'pool', 'set', pool_name,
                'min_size',
                str(min_size))
        if erasure_code_use_overwrites:
            self.raw_cluster_cmd(
                'osd', 'pool', 'set', pool_name,
                'allow_ec_overwrites',
                'true')
        self.raw_cluster_cmd(
            'osd', 'pool', 'application', 'enable',
            pool_name, 'rados', '--yes-i-really-mean-it',
        )
        self.pools[pool_name] = pg_num
        time.sleep(1)

    def list_pools(self):
        """
        list all pool names
        """
        osd_dump = self.get_osd_dump_json()
        self.log(osd_dump['pools'])
        return [str(i['pool_name']) for i in osd_dump['pools']]

    def get_pool_property(self, pool_name, prop):
        """
        :param pool_name: pool
        :param prop: property to be checked.
        :returns: property as an int value.
        """
        assert isinstance(pool_name, basestring)
        assert isinstance(prop, basestring)
        (output, err) = self.raw_cluster_cmd(
            'osd',
            'pool',
            'get',
            pool_name,
            prop)
        outbuf = output.read()
        return int(outbuf.split()[1])

    def get_pool_dump(self, pool):
        """
        get the osd dump part of a pool
        """
        osd_dump = self.get_osd_dump_json()
        for i in osd_dump['pools']:
            if i['pool_name'] == pool:
                return i
        assert False

    def get_pool_num(self, pool):
        """
        get number for pool (e.g., data -> 2)
        """
        return int(self.get_pool_dump(pool)['pool'])

    def get_pgid(self, pool, pgnum):
        """
        :param pool: pool name
        :param pgnum: pg number
        :returns: a string representing this pg.
        """
        poolnum = self.get_pool_num(pool)
        pg_str = "{poolnum}.{pgnum}".format(
            poolnum=poolnum,
            pgnum=pgnum)
        return pg_str

    def get_pg_primary(self, pool, pgnum):
        """
        get primary for pool, pgnum (e.g. (data, 0)->0
        """
        pg_str = self.get_pgid(pool, pgnum)
        (output, err) = self.raw_cluster_cmd("pg", "map", pg_str, '--format=json')
        outbuf = output.read()
        j = json.loads('\n'.join(outbuf.split('\n')[1:]))
        return int(j['acting'][0])
        assert False

    def get_pg_random(self, pool, pgnum):
        """
        get random osd for pool, pgnum (e.g. (data, 0)->0
        """
        pg_str = self.get_pgid(pool, pgnum)
        (output, err) = self.raw_cluster_cmd("pg", "map", pg_str, '--format=json')
        outbuf = output.read()
        j = json.loads('\n'.join(outbuf.split('\n')[1:]))
        return int(j['acting'][random.randint(0, len(j['acting']) - 1)])
        assert False

    def get_osd_host(self, osd_id):
        """
        :returns: hostname which has this osd
        """
        self.log("Inside get_osd_host")
        (out, err) = self.raw_cluster_cmd("osd", "tree", '--format=json')
        tree = json.loads(out.read())
        print tree
        for node in tree['nodes']:
            if node['type'] == 'host':
                if osd_id in node['children']:
                    return node['name']
        self.log("couldn't get osd host")
        return None

    def get_osd_obj(self, id, osds):
        """
        :returns: osd object from the list corresponding to osd id
        """
        self.log("Inside get osd obj")
        osd_host = self.get_osd_host(id)
        if osd_host is None:
            self.log("coudn't get osd obj")
            return None

        for osd in osds:
            if osd.hostname == osd_host:
                return osd
        return None

    def kill_osd(self, osd_id, sig_type, osds=[]):
        """
        :params: id , type of signal, list of osd objects
            type: "SIGKILL", "SIGTERM", "SIGHUP" etc.
        :returns: 1 or 0
        """
        self.log("Inside KILL_OSD")
        osd_node = self.get_osd_obj(osd_id, osds)
        if osd_node is None:
            self.log("coudn't get osd node, abort killing")
            return 1
        kill_cmd = 'sudo systemctl kill -s {s_type} ceph-osd@{id}'.format(
            s_type=sig_type, id=osd_id)
        self.log("kill cmd will be run on {osd}".format(osd=osd_node.hostname))
        print kill_cmd
        try:
            osd_node.exec_command(cmd=kill_cmd)
            return 0
        except Exception:
            self.log("failed to kill osd")
            self.log(traceback.format_exc())
            return 1

    def is_up(self, osd_id):
        """
        :return 1 if up, 0 if down
        """
        (output, err) = self.raw_cluster_cmd("osd", "dump", '--format=json')
        outbuf = output.read()
        jbuf = json.loads(outbuf)
        self.log(jbuf)

        for osd in jbuf['osds']:
            if osd_id == osd['osd']:
                return osd['up']

    def revive_osd(self, osd_id, osds):
        """
        :returns: 0 if revive success,1 if fail
        """
        if self.is_up(osd_id):
            return 0
        osd_host = self.get_osd_obj(osd_id, osds)
        if osd_host:
            revive_cmd = 'sudo systemctl start ceph-osd@{id}'.format(
                id=osd_id)
            print revive_cmd
            try:
                osd_host.exec_command(cmd=revive_cmd)
                return 0
            except Exception:
                self.log("failed to revive")
                self.log(traceback.format_exc())
                return 1
        return 1
