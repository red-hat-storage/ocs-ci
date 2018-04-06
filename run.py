#!/usr/bin/env python
from gevent import monkey

monkey.patch_all()
import traceback
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
import re
import requests
import textwrap
from docopt import docopt
from reportportal_client import ReportPortalServiceAsync
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
        [--report-portal]
        [--log-level <LEVEL>]


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
                                    in test suite yamls. Requires config file, see README.
  --report-portal                   Post results to report portal. Requires config file, see README.
  --log-level <LEVEL>                       Set logging level
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

test_names = []


def create_nodes(conf, osp_cred, report_portal_session=None):
    if report_portal_session:
        name = create_unique_test_name("ceph node creation", test_names)
        test_names.append(name)
        desc = "Ceph cluster preparation"
        report_portal_session.start_test_item(name=name,
                                              description=desc,
                                              start_time=timestamp(),
                                              item_type="STEP")
    log.info("Destroying existing osp instances")
    for cluster in conf.get('globals'):
        cleanup_ceph_nodes(osp_cred)
    ceph_cluster_dict = {}
    log.info('Creating osp instances')
    for cluster in conf.get('globals'):
        ceph_vmnodes = create_ceph_nodes(cluster, osp_cred)
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
        ceph_cluster_dict[cluster.get('ceph-cluster').get('name', 'ceph')] = ceph_nodes
    log.info('Done creating osp instances')
    log.info("Waiting for Floating IPs to be available")
    log.info("Sleeping 15 Seconds")
    time.sleep(15)
    for cluster_name, cluster in ceph_cluster_dict.iteritems():
        for inctance in cluster:
            try:
                inctance.connect()
            except BaseException:
                if report_portal_session:
                    report_portal_session.finish_test_item(end_time=timestamp(), status="FAILED")
                raise
    if report_portal_session:
        report_portal_session.finish_test_item(end_time=timestamp(), status="PASSED")
    return ceph_cluster_dict


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
    osp_cred_file = args['--osp-cred']
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
    use_cdn = args.get('--use-cdn', False)
    skip_setup = args.get('--skip-cluster', False)
    cleanup_name = args.get('--cleanup', None)
    post_to_report_portal = args.get('--report-portal', False)
    console_log_level = args.get('--log-level')
    suites_path = os.path.abspath(suite_file)
    conf_path = os.path.abspath(glb_file)

    if console_log_level:
        ch.setLevel(logging.getLevelName(console_log_level.upper()))

    with open(conf_path, 'r') as conf_stream:
        conf = yaml.safe_load(conf_stream)
    with open(suites_path, 'r') as suite_stream:
        suite = yaml.safe_load(suite_stream)
    with open(osp_cred_file, 'r') as osp_cred_stream:
        osp_cred = yaml.safe_load(osp_cred_stream)

    compose_id = None
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

    ceph_version = []
    ceph_ansible_version = []
    distro = []
    for cluster in conf.get('globals'):
        image_name = cluster.get('ceph-cluster').get('image-name')
        if 'rhel' in image_name:
            distro.append("RHEL")
            # get COMPOSE ID and ceph version
            id = requests.get(base_url + "/COMPOSE_ID")
            compose_id = id.text
            ceph_pkgs = requests.get(base_url +
                                     "/compose/Tools/x86_64/os/Packages/")
            m = re.search(r'ceph-common-(.*?)cp', ceph_pkgs.text)
            ceph_version.append(m.group(1))
            m = re.search(r'ceph-ansible-(.*?)cp', ceph_pkgs.text)
            ceph_ansible_version.append(m.group(1))
            log.info("Compose id is: " + compose_id)
        else:
            distro.append("Ubuntu")
            ubuntu_pkgs = requests.get(ubuntu_repo +
                                       "/Tools/dists/xenial/main/binary-amd64/Packages")
            m = re.search(r'ceph\nVersion: (.*)', ubuntu_pkgs.text)
            ceph_version.append(m.group(1))
            m = re.search(r'ceph-ansible\nVersion: (.*)', ubuntu_pkgs.text)
            ceph_ansible_version.append(m.group(1))

    distro = ', '.join(list(set(distro)))
    ceph_version = ', '.join(list(set(ceph_version)))
    ceph_ansible_version = ', '.join(list(set(ceph_ansible_version)))
    log.info("Testing Ceph Version: " + ceph_version)
    log.info("Testing Ceph Ansible Version: " + ceph_ansible_version)

    service = None
    if post_to_report_portal:
        log.info("Creating report portal session")
        service = create_report_portal_session()
        suite_name = os.path.basename(suite_file).split(".")[0]
        launch_name = "{suite_name} ({distro})".format(suite_name=suite_name, distro=distro)
        launch_desc = textwrap.dedent(
            """
            ceph version: {ceph_version}
            ceph-ansible version: {ceph_ansible_version}
            compose-id: {compose_id}
            """.format(ceph_version=ceph_version,
                       ceph_ansible_version=ceph_ansible_version,
                       compose_id=compose_id))
        service.start_launch(name=launch_name, start_time=timestamp(), description=launch_desc)

    if cleanup_name is not None:
        cleanup_ceph_nodes(osp_cred, cleanup_name)
        return 0
    if reuse is None:
        ceph_cluster_dict = create_nodes(conf, osp_cred, service)
    else:
        ceph_store_nodes = open(reuse, 'rb')
        ceph_cluster_dict = pickle.load(ceph_store_nodes)
        ceph_store_nodes.close()
        for cluster_name, cluster in ceph_cluster_dict.iteritems():
            for node in cluster:
                node.reconnect()
    if store:
        ceph_clusters_file = 'rerun/ceph-snapshot-' + timestamp()
        if not os.path.exists(os.path.dirname(ceph_clusters_file)):
            os.makedirs(os.path.dirname(ceph_clusters_file))
        cn = open(ceph_clusters_file, 'w+b')
        pickle.dump(ceph_cluster_dict, cn)
        cn.close()
        log.info("ceph_clusters_file %s", ceph_clusters_file)

    sys.path.append(os.path.abspath('tests'))
    sys.path.append(os.path.abspath('tests/rados'))
    sys.path.append(os.path.abspath('tests/rbd'))
    sys.path.append(os.path.abspath('tests/cephfs'))
    sys.path.append(os.path.abspath('tests/iscsi'))
    tests = suite.get('tests')
    tcs = []
    jenkins_rc = 0
    if use_cdn is True and reuse is None:
        for cluster_name, cluster in ceph_cluster_dict.itreritems():
            setup_cdn_repos(cluster, build=rhbuild)
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
        unique_test_name = create_unique_test_name(tc['name'], test_names)
        test_names.append(unique_test_name)
        tc['log-link'] = configure_logger(unique_test_name, run_id)
        mod_file_name = os.path.splitext(test_file)[0]
        test_mod = importlib.import_module(mod_file_name)
        print("Running test {test_name}\n".format(test_name=tc['name']))
        print("Test logfile location: {log_url}\n".format(log_url=tc['log-link']))
        log.info("Running test %s", test_file)
        tc['duration'] = '0s'
        tc['status'] = 'Not Executed'
        start = time.time()
        for cluster_name in test.get('clusters', ceph_cluster_dict):
            if test.get('clusters'):
                config = test.get('clusters').get(cluster_name).get('config', {})
            else:
                config = test.get('config', {})
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
            try:
                if post_to_report_portal:
                    service.start_test_item(
                        name=unique_test_name, description=tc['desc'], start_time=timestamp(), item_type="STEP")
                    service.log(time=timestamp(), message="Logfile location: {}".format(tc['log-link']), level="INFO")
                    service.log(time=timestamp(), message="Polarion ID: {}".format(tc['polarion-id']), level="INFO")
                rc = test_mod.run(ceph_nodes=ceph_cluster_dict[cluster_name], config=config, test_data=ceph_test_data)
            except BaseException:
                log.error(traceback.format_exc())
                rc = 1
            if rc != 0:
                break
        elapsed = (time.time() - start)
        tc['duration'] = elapsed
        if rc == 0:
            log.info("Test %s passed" % test_mod)
            tc['status'] = 'Pass'
            if post_to_report_portal:
                service.finish_test_item(end_time=timestamp(), status="PASSED")
            if post_results:
                post_to_polarion(tc=tc)
        else:
            tc['status'] = 'Failed'
            log.info("Test %s failed" % test_mod)
            jenkins_rc = 1
            if post_to_report_portal:
                service.finish_test_item(end_time=timestamp(), status="FAILED")
            if post_results:
                post_to_polarion(tc=tc)
            if test.get('abort-on-fail', False):
                log.info("Aborting on test failure")
                tcs.append(tc)
                break
        if test.get('destroy-cluster') is True:
            cleanup_ceph_nodes(osp_cred)
        if test.get('recreate-cluster') is True:
            ceph_nodes = create_nodes(conf, osp_cred)
        tcs.append(tc)
    close_and_remove_filehandlers()
    if post_to_report_portal:
        service.finish_launch(end_time=timestamp())
        service.terminate()
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

    full_log_name = "{test_name}.log".format(test_name=test_name)
    test_logfile = os.path.join(run_dir, full_log_name)

    close_and_remove_filehandlers()
    _handler = logging.FileHandler(test_logfile)
    _handler.setLevel(level)
    _handler.setFormatter(formatter)
    _root.addHandler(_handler)

    url_base = "http://magna002.ceph.redhat.com/cephci-jenkins"
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


def close_and_remove_filehandlers(logger=logging.getLogger()):
    """
    Close FileHandlers and then remove them from the loggers handlers list.

    Args:
        logger: the logger in which to remove the handlers from, defaults to root logger

    Returns:
        None
    """
    handlers = logger.handlers[:]
    for h in handlers:
        if isinstance(h, logging.FileHandler):
            h.close()
            logger.removeHandler(h)


def create_report_portal_session():
    """
    Configures and creates a session to the Report Portal instance.

    Returns:
        The session object
    """
    home_dir = os.path.expanduser("~")
    cfg_file = os.path.join(home_dir, ".cephci.yaml")
    try:
        with open(cfg_file, "r") as yml:
            cfg = yaml.load(yml)['report-portal']
    except IOError:
        log.error("Please create ~/.cephci.yaml from the cephci.yaml.template. See README for more information.")
        raise

    return ReportPortalServiceAsync(
        endpoint=cfg['endpoint'], project=cfg['project'], token=cfg['token'], error_handler=error_handler)


def timestamp():
    """
    The current epoch timestamp in milliseconds as a string.

    Returns:
        The timestamp
    """
    return str(int(time.time() * 1000))


def error_handler(exc_info):
    """
    Error handler for the Report Portal session.

    Returns:
        None
    """
    print("Error occurred: {}".format(exc_info[1]))
    traceback.print_exception(*exc_info)


def create_unique_test_name(test_name, name_list):
    """
    Creates a unique test name using the actual test name and an increasing integer for each duplicate test name.

    Args:
        test_name: name of the test
        name_list: list of names to compare test name against

    Returns:
        unique name for the test case
    """
    base = "_".join(test_name.split())
    num = 0
    while "{base}_{num}".format(base=base, num=num) in name_list:
        num += 1
    return "{base}_{num}".format(base=base, num=num)


if __name__ == '__main__':
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)
