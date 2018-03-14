#!/usr/bin/env python
from gevent import monkey
monkey.patch_all()
import yaml
import sys
import os
import json
import logging
import importlib
import pickle
import time
import uuid
import shutil
from docopt import docopt
from ceph.ceph import CephNode
from ceph.utils import create_ceph_nodes, cleanup_ceph_nodes
from ceph.utils import setup_cdn_repos
from utils.polarion import post_to_polarion

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --rhbuild BUILD --global-conf FILE --suite FILE [--use-cdn ]
        [--osp-cred <file>]
        [--rhs-con-repo <repo> --rhs-ceph-repo <repo>]
        [--add-repo <repo>]
        [--kernel-repo <repo>]
        [--store]
        [--reuse <file>]
        [--skip-cluster]
        [--cleanup <name>]
        [--docker-registry <registry>]
        [--docker-image <image>]
        [--docker-tag <tag>]
        [--insecure-registry]
        [--post-results]


Options:
  -h --help                         show this screen
  -v --version                      run version
  -s <smoke> --suite <smoke>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --global-conf <file>              global configuration file
  --osp-cred <file>                 openstack credentials as separate file
  --rhbuild <1.3.0>                 ceph downstream version
                                    eg: 1.3.0, 2.0, 2.1 etc
  --use-cdn                         whether to use cdn or not [deafult: false]
  --rhs-con-repo <repo>             location of rhs console repo
                                    Top level location of console compose
  --rhs-ceph-repo <repo>            location of rhs-ceph repo
                                    Top level location of compose
  --add-repo <repo>                 Any additional repo's need to be enabled
  --ubuntu-repo <repo>              http location of downstream ubuntu repo
  --kernel-repo <repo>              Zstream Kernel Repo location
  --cleanup <name>                  cleanup nodes on OSP with names that start
                                    with 'name' , returns after node cleanup
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --skip-cluster                    skip cluster creation from ansible/ceph-deploy
  --docker-registry <registry>      Docker registry, deafult value is taken from ansible config
  --docker-image <image>            Docker image, deafult value is taken from ansible config
  --docker-tag <tag>                Docker tag, default value is 'latest'
  --insecure-registry               Disable security check for docker registry
  --post-results                    Post results to polarion, needs Polarion IDs
                                    in test suite yamls.
"""

log = logging.getLogger(__name__)
root = logging.getLogger()
root.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
root.addHandler(ch)

temp_startup_log = os.path.join("/tmp/", "startup-{uuid}.log".format(uuid=uuid.uuid4().hex))
print("Temporary startup log location: {location}\n".format(location=temp_startup_log))
handler = logging.FileHandler(temp_startup_log)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
root.addHandler(handler)


def create_nodes(global_yaml, osp_cred):
    log.info("Creating ceph nodes")
    cleanup_ceph_nodes(osp_cred)
    ceph_vmnodes = create_ceph_nodes(global_yaml, osp_cred)
    log.info("Running test")
    ceph_nodes = []
    for node_key in ceph_vmnodes.iterkeys():
        node = ceph_vmnodes[node_key]
        ceph = CephNode(username='cephuser',
                        password='cephuser',
                        root_password='passwd',
                        root_login=node.root_login,
                        role=node.role,
                        no_of_volumes=node.no_of_volumes,
                        ip_address=node.ip_address,
                        hostname=node.hostname,
                        ceph_vmnode=node)
        ceph_nodes.append(ceph)
    log.info("Waiting for Floating IPs to be available")
    log.info("Sleeping 15 Seconds")
    time.sleep(15)
    for ceph in ceph_nodes:
        ceph.connect()
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
            name=test['name'], desc=test['desc'], duration=dur, status=test['status'], )
        print line


def run(args):
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    run_id = str(int(time.time()))
    glb_file = args['--global-conf']
    osp_cred = args['--osp-cred']
    suite_file = args['--suite']
    store = args.get('--store', False)
    reuse = args.get('--reuse', None)
    base_url = args.get('--rhs-ceph-repo', None)
    ubuntu_repo = args.get('--ubuntu-repo', None)
    kernel_repo = args.get('--kernel-repo', None)
    rhbuild = args.get('--rhbuild')
    docker_registry = args.get('--docker-registry', None)
    docker_image = args.get('--docker-image', None)
    docker_tag = args.get('--docker-tag', None)
    docker_insecure_registry = args.get('--insecure-registry', False)
    post_results = args.get('--post-results')
    if rhbuild.startswith('2'):
        if base_url is None:
            # use latest as default when nothing is specified in cli
            base_url = 'http://download.engineering.redhat.com/rcm-guest/ceph-drops/2/latest-RHCEPH-2.4-Ubuntu/'
        if ubuntu_repo is None:
            log.info("Using latest ubuntu repo since no default value provided")
            ubuntu_repo = 'http://download-node-02.eng.bos.redhat.com/rcm-guest/ceph-drops/2/latest-Ceph-2-Ubuntu/'
    elif rhbuild.startswith('3'):
        if base_url is None:
            # default to latest RHCeph build 3.0
            base_url = 'http://download.eng.bos.redhat.com/composes/auto/ceph-3.0-rhel-7/latest-RHCEPH-3-RHEL-7/'
            # we dont need installer repo
            installer_url = None
        if ubuntu_repo is None:
            ubuntu_repo = \
                'http://download-node-02.eng.bos.redhat.com/rcm-guest/ceph-drops/3.0/latest-RHCEPH-3.0-Ubuntu/'
    installer_url = args.get('--rhs-con-repo', None)
    if rhbuild.startswith('2'):
        if installer_url is None:
            # default installer repo points to latest
            installer_url = 'http://download.eng.bos.redhat.com/composes/auto/rhscon-2-rhel-7/latest-RHSCON-2-RHEL-7/'
    if os.environ.get('TOOL') is not None:
        ci_message = json.loads(os.environ['CI_MESSAGE'])
        compose_id = ci_message['compose_id']
        compose_url = ci_message['compose_url'] + "/"
        product_name = ci_message.get('product_name', None)
        product_version = ci_message.get('product_version', None)
        log.info("COMPOSE_URL = %s ", compose_url)
        if os.environ['TOOL'] == 'pungi':
            # is a rhel compose
            log.info("trigger on CI RHEL Compose")
        elif os.environ['TOOL'] == 'rhcephcompose':
            # is a ubuntu compose
            log.info("trigger on CI Ubuntu Compose")
            ubuntu_repo = compose_url
            log.info("using ubuntu repo" + ubuntu_repo)
        elif os.environ['TOOL'] == 'bucko':
            # is a docker compose
            log.info("Trigger on CI Docker Compose")
            docker_registry, docker_image_tag = ci_message['repositories'][0].split('/')
            docker_image, docker_tag = docker_image_tag.split(':')
            log.info("\nUsing docker registry from ci message: {registry} \nDocker image: {image}\nDocker tag:{tag}"
                     .format(registry=docker_registry, image=docker_image, tag=docker_tag))
            log.warn('Using Docker insecure registry setting')
            docker_insecure_registry = True
        if product_name == 'ceph':
            # is a rhceph compose
            base_url = compose_url
            log.info("using base url" + base_url)
        elif product_name == 'rhscon':
            # is a rhcon
            installer_url = compose_url
            log.info("using console repo" + installer_url)

    use_cdn = args.get('--use-cdn', False)
    g_yaml = os.path.abspath(glb_file)
    suites = os.path.abspath(suite_file)
    skip_setup = args.get('--skip-cluster', False)
    cleanup_name = args.get('--cleanup', None)
    if cleanup_name is not None:
        cleanup_ceph_nodes(osp_cred, cleanup_name)
        return 0
    if reuse is None:
        ceph_nodes = create_nodes(glb_file, osp_cred)
    else:
        ceph_store_nodes = open(reuse, 'rb')
        ceph_nodes = pickle.load(ceph_store_nodes)
        ceph_store_nodes.close()
        for node in ceph_nodes:
            node.reconnect()
    if store:
        (_, _, node_num, _, _) = ceph_nodes[0].hostname.split('-')
        ceph_nodes_file = 'rerun/ceph-nodes-' + node_num
        if not os.path.exists(os.path.dirname(ceph_nodes_file)):
            os.makedirs(os.path.dirname(ceph_nodes_file))
        cn = open(ceph_nodes_file, 'w+b')
        pickle.dump(ceph_nodes, cn)
        cn.close()
        log.info("ceph_nodes_file %s", ceph_nodes_file)
    suites_yaml = yaml.safe_load(open(suites))
    sys.path.append(os.path.abspath('tests'))
    sys.path.append(os.path.abspath('tests/rados'))
    sys.path.append(os.path.abspath('tests/rbd'))
    sys.path.append(os.path.abspath('tests/cephfs'))
    sys.path.append(os.path.abspath('tests/iscsi'))
    tests = suites_yaml.get('tests')
    tcs = []
    jenkins_rc = 0
    if use_cdn is True and reuse is None:
        setup_cdn_repos(ceph_nodes, build=rhbuild)
    # use ceph_test_data to pass around dynamic data between tests
    ceph_test_data = dict()
    for test in tests:
        test = test.get('test')
        tc = dict()
        tc['name'] = test.get('name')
        tc['desc'] = test.get('desc')
        tc['file'] = test.get('module')
        tc['polarion-id'] = test.get('polarion-id')
        tc['rhbuild'] = rhbuild
        test_file = tc['file']
        config = test.get('config', {})
        tc['log-link'] = configure_logger(tc['name'], run_id)
        if not config.get('base_url'):
            config['base_url'] = base_url
        if not config.get('installer_url'):
            config['installer_url'] = installer_url
        config['rhbuild'] = rhbuild
        if 'ubuntu_repo' in locals():
            config['ubuntu_repo'] = ubuntu_repo
        if not config.get('use_cdn'):
            config['use_cdn'] = use_cdn
        if skip_setup is True:
            config['skip_setup'] = True
        if args.get('--add-repo'):
            repo = args.get('--add-repo')
            if repo.startswith('http'):
                config['add-repo'] = repo
        config['docker-insecure-registry'] = docker_insecure_registry
        if config and config.get('ansi_config'):
            if docker_registry:
                config.get('ansi_config')['ceph_docker_registry'] = str(docker_registry)
            if docker_image:
                config.get('ansi_config')['ceph_docker_image'] = str(docker_image)
            if docker_tag:
                config.get('ansi_config')['ceph_docker_image_tag'] = str(docker_tag)
        if kernel_repo is not None:
            config['kernel-repo'] = kernel_repo
        # if Kernel Repo is defined in ENV then set the value in config
        if os.environ.get('KERNEL-REPO-URL') is not None:
            config['kernel-repo'] = os.environ.get('KERNEL-REPO-URL')
        mod_file_name = os.path.splitext(test_file)[0]
        test_mod = importlib.import_module(mod_file_name)
        print("Running test {test_name}\n".format(test_name=tc['name']))
        print("Test logfile location: {log_url}\n".format(log_url=tc['log-link']))
        log.info("Running test %s", test_file)
        tc['duration'] = '0s'
        tc['status'] = 'Not Executed'
        start = time.time()
        rc = test_mod.run(ceph_nodes=ceph_nodes, config=config,
                          test_data=ceph_test_data)
        elapsed = (time.time() - start)
        tc['duration'] = elapsed
        if rc == 0:
            log.info("Test %s passed" % test_mod)
            tc['status'] = 'Pass'
            if post_results:
                post_to_polarion(tc=tc)
        else:
            tc['status'] = 'Failed'
            log.info("Test %s failed" % test_mod)
            jenkins_rc = 1
            if post_results:
                post_to_polarion(tc=tc)
            if test.get('abort-on-fail', False):
                log.info("Aborting on test failure")
                tcs.append(tc)
                break
        if test.get('destroy-cluster') is True:
            cleanup_ceph_nodes(osp_cred)
        if test.get('recreate-cluster') is True:
            ceph_nodes = create_nodes(glb_file, osp_cred)
        tcs.append(tc)

    print_results(tcs)
    return jenkins_rc


def configure_logger(test_name, run_id, level=logging.INFO):
    """
    Configures a new FileHandler for the root logger depending on the run_id and test_name.

    Args:
        test_name: name of the test being executed. used for naming the logfile
        run_id: id of the test run. passed through to the directory creation
        level: logging level

    Returns:
        URL where the log file can be viewed
    """
    _root = logging.getLogger()

    run_dir = create_run_dir(run_id)

    if os.path.exists(temp_startup_log):
        shutil.move(temp_startup_log, os.path.join(run_dir, "startup.log"))

    base_name = "_".join(test_name.split())
    num = 0
    while os.path.exists(os.path.join(run_dir, "{base_name}_{num}.log".format(base_name=base_name, num=num))):
        num += 1
    full_log_name = "{base_name}_{num}.log".format(base_name=base_name, num=num)
    test_logfile = os.path.join(run_dir, full_log_name)

    _root.handlers = [h for h in _root.handlers if not isinstance(h, logging.FileHandler)]
    _handler = logging.FileHandler(test_logfile)
    _handler.setLevel(level)
    _handler.setFormatter(formatter)
    _root.addHandler(_handler)

    url_base = "http://magna002.redhat.com/cephci-jenkins"
    run_dir_name = run_dir.split('/')[-1]
    log_url = "{url_base}/{run_dir}/{log_name}".format(url_base=url_base, run_dir=run_dir_name, log_name=full_log_name)

    log.info("Completed log configuration")
    return log_url


def create_run_dir(run_id):
    """
    Create the directory where test logs will be placed.

    Args:
        run_id: id of the test run. used to name the directory

    Returns:
        Full path of the created directory
    """
    dir_name = "cephci-run-{run_id}".format(run_id=run_id)
    base_dir = "/ceph/cephci-jenkins"
    run_dir = os.path.join(base_dir, dir_name)
    try:
        os.makedirs(run_dir)
        log.info("Created run directory: {run_dir}".format(run_dir=run_dir))
    except OSError:
        pass

    return run_dir


if __name__ == '__main__':
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)
