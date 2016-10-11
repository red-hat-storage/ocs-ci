import yaml
import sys
import os
import logging
import importlib
import pickle
import time
from docopt import docopt
from ceph.ceph import CephNode, Ceph
from ceph.utils import create_ceph_nodes, cleanup_ceph_nodes, keep_alive
from ceph.utils import setup_cdn_repos

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --rhbuild BUILD --global-conf FILE --suite FILE [--use-cdn ]
        [--rhs-con-repo <repo> --rhs-ceph-repo <repo>]
        [--add-repo <repo>]
        [--store]
        [--reuse <file>]
        [--skip-cluster]


Options:
  -h --help                         show this screen
  -v --version                      run version
  -s <smoke> --suite <smoke>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --global-conf <file>              global configuration file
  --rhbuild <1.3.0>                 ceph downstream version
                                    eg: 1.3.0, 2.0, 2.1 etc
  --use-cdn                         whether to use cdn or not [deafult: false]
  --rhs-con-repo <repo>             location of rhs console repo
                                    Top level location of console compose
  --rhs-ceph-repo <repo>            location of rhs-ceph repo
                                    Top level location of compose
  --add-repo <repo>                 Any additional repo's need to be enabled
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --skip-cluster                    skip cluster creation from ansible/ceph-deploy
"""

logger = logging.getLogger(__name__)
log = logger
root = logging.getLogger()
root.setLevel(logging.INFO)

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
root.addHandler(ch)

def create_nodes(global_yaml):
    logger.info("Creating ceph nodes")
    cleanup_ceph_nodes(global_yaml)
    ceph_vmnodes = create_ceph_nodes(global_yaml)
    logger.info("Running test")
    ceph_nodes = []
    for node_key in ceph_vmnodes.iterkeys():
        node = ceph_vmnodes[node_key]
        ceph = CephNode(username='cephuser',
                        password='cephuser',
                        root_password='passwd',
                        role=node.role,
                        no_of_volumes=node.no_of_volumes,
                        ip_address=node.ip_address,
                        hostname=node.hostname,
                        ceph_vmnode=node)
        ceph.connect()
        ceph_nodes.append(ceph)
    return ceph_nodes

def print_results(tc):
    header = '{name:<20s}   {desc:50s}   {duration:20s}   {status:>15s}'.format(
    name='TEST NAME',
    desc='TEST DESCRIPTION',
    duration='DURATION',
    status='STATUS'
    )
    print header
    for test in tc:
        if test.get('duration'):
            dur = str(test['duration'])
        else:
            dur = '0s'
        line = '{name:<20s}   {desc:50s}   {duration:20s}   {status:>15s}'.format(
            name=test['name'],
            desc=test['desc'],
            duration=dur,
            status=test['status'],
        )
        print line


def run(args):
    glb_file = args['--global-conf']
    suite_file = args['--suite']
    store = args.get('--store', False)
    reuse = args.get('--reuse', None)
    base_url = args.get('--rhs-ceph-repo', None)
    installer_url = args.get('--rhs-con-repo', None)
    rhbuild = args.get('--rhbuild')
    use_cdn = args.get('--use-cdn', False)
    g_yaml = os.path.abspath(glb_file)
    suites = os.path.abspath(suite_file)
    skip_setup=args.get('--skip-cluster', False)
    if reuse is None:
        ceph_nodes = create_nodes(g_yaml)
    else:
        ceph_store_nodes = open(reuse, 'rb')
        ceph_nodes = pickle.load(ceph_store_nodes)
        ceph_store_nodes.close()
        for node in ceph_nodes:
            node.reconnect()
    if store:
        (_,_,node_num,_,_) = ceph_nodes[0].hostname.split('-')
        ceph_nodes_file = 'rerun/ceph-nodes-' + node_num
        cn = open(ceph_nodes_file, 'wb')
        pickle.dump(ceph_nodes, cn)
        cn.close()
        log.info("ceph_nodes_file %s", ceph_nodes_file)
    suites_yaml = yaml.safe_load(open(suites))
    sys.path.append(os.path.abspath('tests'))
    tests = suites_yaml.get('tests')
    tcs=[]
    jenkins_rc=0
    if use_cdn is True:
         setup_cdn_repos(ceph_nodes=ceph_nodes, build=rhbuild)
    for test in tests:
        test = test.get('test')
        tc=dict()
        tc['name'] = test.get('name')
        tc['desc'] = test.get('desc')
        tc['file'] = test.get('module')
        test_file = tc['file']
        config = test.get('config', {})
        if not config.get('base_url'):
            config['base_url'] = base_url
        if not config.get('installer_url'):
            config['installer_url'] = installer_url
        config['rhbuild'] = rhbuild
        if not config.get('use_cdn'):
            config['use_cdn'] = use_cdn
        if skip_setup is True:
            config['skip_setup'] = True
        if args.get('--add-repo'):
                repo = args.get('--add-repo')        
                if repo.startswith('http'):
                    config['add-repo'] = repo
        mod_file_name = os.path.splitext(test_file)[0]
        test_mod = importlib.import_module(mod_file_name)
        log.info("Running test %s", test_file)
        tc['duration'] = '0s'
        tc['status'] = 'Not Executed'
        start = time.time()
        rc = test_mod.run(ceph_nodes=ceph_nodes, config=config)
        elapsed = (time.time() - start)
        tc['duration'] = elapsed
        if rc==0:
            log.info("Test %s passed" % test_mod)
            tc['status'] =  'Pass'
        else:
            tc['status'] = 'Failed'
            log.info("Test %s failed" % test_mod)
            jenkins_rc=1
            if test.get('abort-on-fail', False):
                log.info("Aborting on test failure")
                break
        if test.get('destroy-cluster') is True:
                (nodename,uid, node_num,_,_) = ceph_nodes[0].hostname.split('-')
                cleanup_name = nodename + "-" + uid 
                cleanup_ceph_nodes(g_yaml, name=cleanup_name)
        if test.get('recreate-cluster') is True:
                ceph_nodes = create_nodes(g_yaml)
        tcs.append(tc)
                
    print_results(tcs)
    return jenkins_rc

if __name__ == '__main__':
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)