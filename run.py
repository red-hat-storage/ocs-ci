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
import re
import requests
import textwrap
from docopt import docopt
from getpass import getuser
from libcloud.common.types import LibcloudError
from ceph.ceph import CephNode, Ceph
from ceph.clients import WinNode
from ceph.utils import create_ceph_nodes, cleanup_ceph_nodes, setup_cdn_repos
from utility.polarion import post_to_polarion
from utility.retry import retry
from utility.utils import timestamp, create_run_dir, create_unique_test_name, create_report_portal_session, \
    configure_logger, close_and_remove_filehandlers, get_latest_container, email_results

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --rhbuild BUILD --global-conf FILE --inventory FILE --suite FILE [--use-cdn ]
        [--osp-cred <file>]
        [--rhs-con-repo <repo> --rhs-ceph-repo <repo>]
        [ --ubuntu-repo <repo>]
        [--add-repo <repo>]
        [--kernel-repo <repo>]
        [--store]
        [--reuse <file>]
        [--skip-cluster]
        [--skip-subscription]
        [--docker-registry <registry>]
        [--docker-image <image>]
        [--docker-tag <tag>]
        [--insecure-registry]
        [--post-results]
        [--report-portal]
        [--log-level <LEVEL>]
        [--instances-name <name>]
        [--osp-image <image>]
        [--filestore]
        [--use-ec-pool <k,m>]
        [--hotfix-repo <repo>]
        [--ignore-latest-container]
        [--skip-version-compare]
        [--custom-config <key>=<value>]...
        [--custom-config-file <file>]
  run.py --cleanup=name [--osp-cred <file>]
        [--log-level <LEVEL>]

Options:
  -h --help                         show this screen
  -v --version                      run version
  -s <smoke> --suite <smoke>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --global-conf <file>              global cloud configuration file
  --inventory <file>                hosts inventory file
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
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --skip-cluster                    skip cluster creation from ansible/ceph-deploy
  --skip-subscription               skip subscription manager if using beta rhel images
  --docker-registry <registry>      Docker registry, deafult value is taken from ansible config
  --docker-image <image>            Docker image, deafult value is taken from ansible config
  --docker-tag <tag>                Docker tag, default value is 'latest'
  --insecure-registry               Disable security check for docker registry
  --post-results                    Post results to polarion, needs Polarion IDs
                                    in test suite yamls. Requires config file, see README.
  --report-portal                   Post results to report portal. Requires config file, see README.
  --log-level <LEVEL>               Set logging level
  --instances-name <name>           Name that will be used for instances creation
  --osp-image <image>               Image for osp instances, default value is taken from conf file
  --filestore                       To specify filestore as osd object store
  --use-ec-pool <k,m>               To use ec pools instead of replicated pools
  --hotfix-repo <repo>              To run sanity on hotfix build
  --ignore-latest-container         Skip getting latest nightly container
  --skip-version-compare            Skip verification that ceph versions change post upgrade
  -c --custom-config <name>=<value> Add a custom config key/value to ceph_conf_overrides
  --custom-config-file <file>       Add custom config yaml to ceph_conf_overrides
"""
log = logging.getLogger(__name__)
root = logging.getLogger()
root.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.ERROR)
ch.setFormatter(formatter)
root.addHandler(ch)

run_id = timestamp()
run_dir = create_run_dir(run_id)
startup_log = os.path.join(run_dir, "startup.log")
print("Startup log location: {}".format(startup_log))
handler = logging.FileHandler(startup_log)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
root.addHandler(handler)

test_names = []


@retry(LibcloudError, tries=5, delay=15)
def create_nodes(conf, inventory, osp_cred, run_id, report_portal_session=None, instances_name=None):
    if report_portal_session:
        name = create_unique_test_name("ceph node creation", test_names)
        test_names.append(name)
        desc = "Ceph cluster preparation"
        report_portal_session.start_test_item(name=name,
                                              description=desc,
                                              start_time=timestamp(),
                                              item_type="STEP")
    log.info("Destroying existing osp instances")
    cleanup_ceph_nodes(osp_cred, instances_name)
    ceph_cluster_dict = {}
    log.info('Creating osp instances')
    for cluster in conf.get('globals'):
        ceph_vmnodes = create_ceph_nodes(cluster, inventory, osp_cred, run_id, instances_name)
        ceph_nodes = []
        clients = []
        for node in ceph_vmnodes.itervalues():
            if node.role == 'win-iscsi-clients':
                clients.append(WinNode(ip_address=node.ip_address,
                                       private_ip=node.get_private_ip()))
            else:
                ceph = CephNode(username='cephuser',
                                password='cephuser',
                                root_password='passwd',
                                root_login=node.root_login,
                                role=node.role,
                                no_of_volumes=node.no_of_volumes,
                                ip_address=node.ip_address,
                                private_ip=node.get_private_ip(),
                                hostname=node.hostname,
                                ceph_vmnode=node)
                ceph_nodes.append(ceph)
        cluster_name = cluster.get('ceph-cluster').get('name', 'ceph')
        ceph_cluster_dict[cluster_name] = Ceph(cluster_name, ceph_nodes)
    # TODO: refactor cluster dict to cluster list
    log.info('Done creating osp instances')
    log.info("Waiting for Floating IPs to be available")
    log.info("Sleeping 15 Seconds")
    time.sleep(15)
    for cluster_name, cluster in ceph_cluster_dict.iteritems():
        for instance in cluster:
            try:
                instance.connect()
            except BaseException:
                if report_portal_session:
                    report_portal_session.finish_test_item(end_time=timestamp(), status="FAILED")
                raise
    if report_portal_session:
        report_portal_session.finish_test_item(end_time=timestamp(), status="PASSED")
    return ceph_cluster_dict, clients


def print_results(tc):
    header = '\n{name:<20s}   {desc:50s}   {duration:20s}   {status:>15s}'.format(
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
    glb_file = args['--global-conf']
    inventory_file = args['--inventory']
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
    skip_subscription = args.get('--skip-subscription', False)
    cleanup_name = args.get('--cleanup', None)
    post_to_report_portal = args.get('--report-portal', False)
    console_log_level = args.get('--log-level')
    instances_name = args.get('--instances-name')
    osp_image = args.get('--osp-image')
    filestore = args.get('--filestore', False)
    ec_pool_vals = args.get('--use-ec-pool', None)
    ignore_latest_nightly_container = args.get('--ignore-latest-container', False)
    skip_version_compare = args.get('--skip-version-compare', False)
    custom_config = args.get('--custom-config')
    custom_config_file = args.get('--custom-config-file')
    if console_log_level:
        ch.setLevel(logging.getLevelName(console_log_level.upper()))

    if glb_file:
        conf_path = os.path.abspath(glb_file)
        with open(conf_path, 'r') as conf_stream:
            conf = yaml.safe_load(conf_stream)

    if inventory_file:
        inventory_path = os.path.abspath(inventory_file)
        with open(inventory_path, 'r') as inventory_stream:
            inventory = yaml.safe_load(inventory_stream)

    if suite_file:
        suites_path = os.path.abspath(suite_file)
        with open(suites_path, 'r') as suite_stream:
            suite = yaml.safe_load(suite_stream)
    if osp_cred_file:
        with open(osp_cred_file, 'r') as osp_cred_stream:
            osp_cred = yaml.safe_load(osp_cred_stream)

    if cleanup_name is not None:
        cleanup_ceph_nodes(osp_cred, cleanup_name)
        return 0

    if osp_image and inventory.get('instance').get('create'):
        inventory.get('instance').get('create').update({'image-name': osp_image})

    compose_id = None
    if rhbuild.startswith('2'):
        if base_url is None:
            # use latest as default when nothing is specified in cli
            # base_url = 'http://download.engineering.redhat.com/rcm-guest/ceph-drops/2/latest-RHCEPH-2.4-Ubuntu/'
            base_url = 'http://download.eng.bos.redhat.com/composes/auto/ceph-2-rhel-7/latest-RHCEPH-2-RHEL-7/'
        if ubuntu_repo is None:
            log.info("Using latest ubuntu repo since no default value provided")
            ubuntu_repo = 'http://download-node-02.eng.bos.redhat.com/rcm-guest/ceph-drops/2/latest-Ceph-2-Ubuntu/'
    elif rhbuild == '3.1':
        if base_url is None:
            # default to latest RHCeph build 3.1
            base_url = 'http://download.eng.bos.redhat.com/composes/auto/ceph-3.1-rhel-7/latest-RHCEPH-3-RHEL-7/'
        if ubuntu_repo is None:
            ubuntu_repo = \
                'http://download.engineering.redhat.com/rcm-guest/ceph-drops/3.1/latest-RHCEPH-3.1-Ubuntu/'
    elif rhbuild.startswith('3'):
        if base_url is None:
            # default to latest RHCeph build 3.2
            base_url = 'http://download.eng.bos.redhat.com/composes/auto/ceph-3.2-rhel-7/latest-RHCEPH-3-RHEL-7/'
        if ubuntu_repo is None:
            ubuntu_repo = \
                'http://download.eng.bos.redhat.com/rcm-guest/ceph-drops/3.2/latest-RHCEPH-3.2-Ubuntu/'
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
            docker_registry, docker_image_tag = ci_message['repository'].split('/')
            docker_image, docker_tag = docker_image_tag.split(':')
            log.info("\nUsing docker registry from ci message: {registry} \nDocker image: {image}\nDocker tag:{tag}"
                     .format(registry=docker_registry, image=docker_image, tag=docker_tag))
            log.warn('Using Docker insecure registry setting')
            docker_insecure_registry = True
        if product_name == 'ceph':
            # is a rhceph compose
            base_url = compose_url
            log.info("using base url" + base_url)

    image_name = inventory.get('instance').get('create').get('image-name')
    ceph_version = []
    ceph_ansible_version = []
    distro = []
    if inventory.get('instance').get('create'):
        distro.append(inventory.get('instance').get('create').get('image-name'))
    for cluster in conf.get('globals'):
        if cluster.get('ceph-cluster').get('inventory'):
            cluster_inventory_path = os.path.abspath(cluster.get('ceph-cluster').get('inventory'))
            with open(cluster_inventory_path, 'r') as inventory_stream:
                cluster_inventory = yaml.safe_load(inventory_stream)
            image_name = cluster_inventory.get('instance').get('create').get('image-name')
            distro.append(image_name.replace('.iso', ''))
        # get COMPOSE ID and ceph version
        id = requests.get(base_url + "/COMPOSE_ID")
        compose_id = id.text
        if 'rhel' in image_name.lower():
            ceph_pkgs = requests.get(base_url +
                                     "/compose/Tools/x86_64/os/Packages/")
            m = re.search(r'ceph-common-(.*?)cp', ceph_pkgs.text)
            ceph_version.append(m.group(1))
            m = re.search(r'ceph-ansible-(.*?)cp', ceph_pkgs.text)
            ceph_ansible_version.append(m.group(1))
            log.info("Compose id is: " + compose_id)
        else:
            ubuntu_pkgs = requests.get(ubuntu_repo +
                                       "/Tools/dists/xenial/main/binary-amd64/Packages")
            m = re.search(r'ceph\nVersion: (.*)', ubuntu_pkgs.text)
            ceph_version.append(m.group(1))
            m = re.search(r'ceph-ansible\nVersion: (.*)', ubuntu_pkgs.text)
            ceph_ansible_version.append(m.group(1))

    distro = ','.join(list(set(distro)))
    ceph_version = ', '.join(list(set(ceph_version)))
    ceph_ansible_version = ', '.join(list(set(ceph_ansible_version)))
    log.info("Testing Ceph Version: " + ceph_version)
    log.info("Testing Ceph Ansible Version: " + ceph_ansible_version)

    if not os.environ.get('TOOL') and not ignore_latest_nightly_container:
        major_version = re.match('RHCEPH-(\d+\.\d+)', compose_id).group(1)
        try:
            latest_container = get_latest_container(major_version)
        except ValueError:
            latest_container = get_latest_container('.'.join([major_version.split('.')[0], 'x']))
        docker_registry = latest_container.get('docker_registry') if not docker_registry else docker_registry
        docker_image = latest_container.get('docker_image') if not docker_image else docker_image
        docker_tag = latest_container.get('docker_tag') if not docker_tag else docker_tag
        log.info("Using latest nightly docker image \nRegistry: {registry} \nDocker image: {image}\nDocker tag:{tag}"
                 .format(registry=docker_registry, image=docker_image, tag=docker_tag))
        docker_insecure_registry = True
        log.warn('Using Docker insecure registry setting')

    service = None
    suite_name = os.path.basename(suite_file).split(".")[0]
    if post_to_report_portal:
        log.info("Creating report portal session")
        service = create_report_portal_session()
        launch_name = "{suite_name} ({distro})".format(suite_name=suite_name, distro=distro)
        launch_desc = textwrap.dedent(
            """
            ceph version: {ceph_version}
            ceph-ansible version: {ceph_ansible_version}
            compose-id: {compose_id}
            invoked-by: {user}
            """.format(ceph_version=ceph_version,
                       ceph_ansible_version=ceph_ansible_version,
                       user=getuser(),
                       compose_id=compose_id))
        if docker_image and docker_registry and docker_tag:
            launch_desc = launch_desc + textwrap.dedent(
                """
                docker registry: {docker_registry}
                docker image: {docker_image}
                docker tag: {docker_tag}
                invoked-by: {user}
                """.format(docker_registry=docker_registry,
                           docker_image=docker_image,
                           user=getuser(),
                           docker_tag=docker_tag))
        service.start_launch(name=launch_name, start_time=timestamp(), description=launch_desc)

    if reuse is None:
        ceph_cluster_dict, clients = create_nodes(conf, inventory, osp_cred, run_id, service, instances_name)
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
        store_cluster_state(ceph_cluster_dict, ceph_clusters_file)

    sys.path.append(os.path.abspath('tests'))
    sys.path.append(os.path.abspath('tests/rados'))
    sys.path.append(os.path.abspath('tests/rbd'))
    sys.path.append(os.path.abspath('tests/rbd-mirror'))
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
    ceph_test_data['custom-config'] = custom_config
    ceph_test_data['custom-config-file'] = custom_config_file

    for test in tests:
        test = test.get('test')
        tc = dict()
        tc['docker-containers-list'] = []
        tc['name'] = test.get('name')
        tc['desc'] = test.get('desc')
        tc['file'] = test.get('module')
        tc['polarion-id'] = test.get('polarion-id')
        tc['rhbuild'] = rhbuild
        tc['ceph-version'] = ceph_version
        tc['ceph-ansible-version'] = ceph_ansible_version
        tc['compose-id'] = compose_id
        tc['distro'] = distro
        tc['suite-name'] = suite_name
        test_file = tc['file']
        report_portal_description = tc['desc'] or ''
        log.info("rp desc type: {}".format(type(report_portal_description)))
        unique_test_name = create_unique_test_name(tc['name'], test_names)
        test_names.append(unique_test_name)
        tc['log-link'] = configure_logger(unique_test_name, run_dir)
        mod_file_name = os.path.splitext(test_file)[0]
        test_mod = importlib.import_module(mod_file_name)
        print("\nRunning test: {test_name}".format(test_name=tc['name']))
        if tc.get('log-link'):
            print("Test logfile location: {log_url}".format(log_url=tc['log-link']))
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
            config['rhbuild'] = rhbuild
            if 'ubuntu_repo' in locals():
                config['ubuntu_repo'] = ubuntu_repo
            if not config.get('use_cdn'):
                config['use_cdn'] = use_cdn
            if skip_setup is True:
                config['skip_setup'] = True
            if skip_subscription is True:
                config['skip_subscription'] = True
            if args.get('--add-repo'):
                repo = args.get('--add-repo')
                if repo.startswith('http'):
                    config['add-repo'] = repo
            config['docker-insecure-registry'] = docker_insecure_registry
            config['skip_version_compare'] = skip_version_compare
            if config and config.get('ansi_config'):
                if docker_registry:
                    config.get('ansi_config')['ceph_docker_registry'] = str(docker_registry)
                if docker_image:
                    config.get('ansi_config')['ceph_docker_image'] = str(docker_image)
                if docker_tag:
                    config.get('ansi_config')['ceph_docker_image_tag'] = str(docker_tag)
                cluster_docker_registry = config.get('ansi_config').get('ceph_docker_registry')
                cluster_docker_image = config.get('ansi_config').get('ceph_docker_image')
                cluster_docker_tag = config.get('ansi_config').get('ceph_docker_image_tag')
                if cluster_docker_registry:
                    cluster_docker_registry = config.get('ansi_config').get('ceph_docker_registry')
                    report_portal_description = report_portal_description + '\ndocker registry: {docker_registry}' \
                        .format(docker_registry=cluster_docker_registry)
                if cluster_docker_image:
                    cluster_docker_image = config.get('ansi_config').get('ceph_docker_image')
                    report_portal_description = report_portal_description + '\ndocker image: {docker_image}' \
                        .format(docker_image=cluster_docker_image)
                if cluster_docker_tag:
                    cluster_docker_tag = config.get('ansi_config').get('ceph_docker_image_tag')
                    report_portal_description = report_portal_description + '\ndocker tag: {docker_tag}' \
                        .format(docker_tag=cluster_docker_tag)
                if cluster_docker_image and cluster_docker_registry:
                    tc['docker-containers-list'].append('{docker_registry}/{docker_image}:{docker_tag}'.format(
                        docker_registry=cluster_docker_registry, docker_image=cluster_docker_image,
                        docker_tag=cluster_docker_tag))
            if filestore:
                config['filestore'] = filestore
            if ec_pool_vals:
                config['ec-pool-k-m'] = ec_pool_vals
            if args.get('--hotfix-repo'):
                hotfix_repo = args.get('--hotfix-repo')
                if hotfix_repo.startswith('http'):
                    config['hotfix_repo'] = hotfix_repo
            if kernel_repo is not None:
                config['kernel-repo'] = kernel_repo
            if osp_cred:
                config['osp_cred'] = osp_cred
            # if Kernel Repo is defined in ENV then set the value in config
            if os.environ.get('KERNEL-REPO-URL') is not None:
                config['kernel-repo'] = os.environ.get('KERNEL-REPO-URL')
            try:
                if post_to_report_portal:
                    service.start_test_item(name=unique_test_name, description=report_portal_description,
                                            start_time=timestamp(), item_type="STEP")
                    service.log(time=timestamp(), message="Logfile location: {}".format(tc['log-link']), level="INFO")
                    service.log(time=timestamp(), message="Polarion ID: {}".format(tc['polarion-id']), level="INFO")
                rc = test_mod.run(ceph_cluster=ceph_cluster_dict[cluster_name],
                                  ceph_nodes=ceph_cluster_dict[cluster_name], config=config, test_data=ceph_test_data,
                                  ceph_cluster_dict=ceph_cluster_dict, clients=clients)
            except BaseException:
                if post_to_report_portal:
                    service.log(time=timestamp(), message=traceback.format_exc(), level="ERROR")
                log.error(traceback.format_exc())
                rc = 1
            finally:
                if store:
                    store_cluster_state(ceph_cluster_dict, ceph_clusters_file)
            if rc != 0:
                break
        elapsed = (time.time() - start)
        tc['duration'] = elapsed
        if rc == 0:
            tc['status'] = 'Pass'
            msg = "Test {} passed".format(test_mod)
            log.info(msg)
            print(msg)
            if post_to_report_portal:
                service.finish_test_item(end_time=timestamp(), status="PASSED")
            if post_results:
                post_to_polarion(tc=tc)
        else:
            tc['status'] = 'Failed'
            msg = "Test {} failed".format(test_mod)
            log.info(msg)
            print(msg)
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
            cleanup_ceph_nodes(osp_cred, instances_name)
        if test.get('recreate-cluster') is True:
            ceph_cluster_dict, clients = create_nodes(conf, inventory, osp_cred, run_id, service, instances_name)
        tcs.append(tc)
    close_and_remove_filehandlers()
    if post_to_report_portal:
        service.finish_launch(end_time=timestamp())
        service.terminate()
    url_base = "http://magna002.ceph.redhat.com/cephci-jenkins"
    run_dir_name = run_dir.split('/')[-1]
    print("\nAll test logs located here: {base}/{dir}".format(base=url_base, dir=run_dir_name))
    print_results(tcs)
    send_to_cephci = post_results or post_to_report_portal
    email_results(tcs, run_id, send_to_cephci)
    return jenkins_rc


def store_cluster_state(ceph_cluster_object, ceph_clusters_file_name):
    cn = open(ceph_clusters_file_name, 'w+b')
    pickle.dump(ceph_cluster_object, cn)
    cn.close()
    log.info("ceph_clusters_file %s", ceph_clusters_file_name)


if __name__ == '__main__':
    args = docopt(doc)
    rc = run(args)
    log.info("final rc of test run %d" % rc)
    sys.exit(rc)
