#!/usr/bin/env python
from gevent import monkey

monkey.patch_all()
import traceback
import yaml
import sys
import os
import logging
import importlib
import time
import textwrap
import urllib3
from docopt import docopt
from getpass import getuser
from utility.polarion import post_to_polarion
from utility.utils import timestamp, create_run_dir, create_unique_test_name, create_report_portal_session, \
    configure_logger, close_and_remove_filehandlers, email_results

doc = """
A simple test suite wrapper that executes tests based on yaml test configuration

 Usage:
  run.py --suite FILE
        [--conf <FILE>]
        [--store]
        [--reuse <FILE>]
        [--post-results]
        [--report-portal]
        [--log-level <LEVEL>]
        [--cluster-name <NAME>]
        [--no-email]
  run.py --cleanup=NAME [--osp-cred <FILE>]
        [--log-level <LEVEL>]

Options:
  -h --help                         show this screen
  -c <conf> --conf <conf>           cluster configuration file to override defaults
  -s <suite> --suite <suite>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --post-results                    Post results to Polarion, needs Polarion IDs
                                    in test suite yaml. Requires config file, see README.
  --report-portal                   Post results to report portal. Requires config file, see README.
  --log-level <LEVEL>               Set logging level
  --cluster-name <name>             Name that will be used for cluster creation
  --no-email                        Do not send results email at the end of the run
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


def print_results(tc):
    header = '\n{name:<20s}   {desc:50s}   {duration:20s}   {status:>15s}'.format(
        name='TEST NAME',
        desc='TEST DESCRIPTION',
        duration='DURATION',
        status='STATUS'
    )
    print(header)
    for test in tc:
        if test.get('duration'):
            dur = str(test['duration'])
        else:
            dur = '0s'
        name = test['name']
        desc = test['desc'] or "None"
        status = test['status']
        line = f'{name:<20s}   {desc:50s}   {dur:20s}   {status:>15s}'
        print(line)


def run(args):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    suite_file = args['--suite']
    conf_file = args['--conf']
    store = args.get('--store', False)
    reuse = args.get('--reuse', None)
    post_results = args.get('--post-results')
    cleanup_name = args.get('--cleanup', None)
    post_to_report_portal = args.get('--report-portal', False)
    console_log_level = args.get('--log-level')
    cluster_name = args.get('--cluster-name')
    send_email = not args.get('--no-email', False)

    if console_log_level:
        ch.setLevel(logging.getLevelName(console_log_level.upper()))

    if cleanup_name:
        pass  # TODO: cleanup cluster and skip test execution

    if suite_file:
        suites_path = os.path.abspath(suite_file)
        with open(suites_path, 'r') as suite_stream:
            suite = yaml.safe_load(suite_stream)

    cluster_conf = dict()
    if conf_file:
        with open(conf_file) as f:
            cluster_conf = yaml.safe_load(f)

    # TODO: determine ci-message structure and necessity for OCS testing
    if os.environ.get("TOOL") is not None:
        pass  # TODO: determine ci-message structure and utilize for OCS if necessary, otherwise remove logic

    rp_service = None
    suite_name = os.path.basename(suite_file).split(".")[0]
    if post_to_report_portal:
        log.info("Creating report portal session")
        rp_service = create_report_portal_session()
        launch_name = "{suite_name}".format(suite_name=suite_name)
        # TODO: add appropriate values to report portal test description for OCS
        launch_desc = textwrap.dedent(
            """
            invoked-by: {user}
            """.format(user=getuser()))
        rp_service.start_launch(name=launch_name, start_time=timestamp(), description=launch_desc)

    if reuse:
        pass  # TODO: build cluster object and skip install -- potentially with additional kwargs read in by test(s)
    if store:
        pass  # TODO: store cluster data for non-aws installations, standardize location for cluster info

    sys.path.append(os.path.abspath('tests'))
    tests = suite.get('tests')
    tcs = []
    jenkins_rc = 0
    test_data = dict()
    if cluster_name:
        test_data['cluster-name'] = cluster_name

    for test in tests:
        test = test.get('test')
        tc = dict()
        tc['docker-containers-list'] = []
        tc['name'] = test.get('name')
        tc['desc'] = test.get('desc')
        tc['file'] = test.get('module')
        tc['polarion-id'] = test.get('polarion-id')
        tc['suite-name'] = suite_name
        test_file = tc['file']
        report_portal_description = tc['desc'] or ''
        unique_test_name = create_unique_test_name(tc['name'])
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
        rc = 1
        config = test.get('config', {})

        test_kwargs = dict()
        test_kwargs.update({'config': config})
        test_kwargs.update({'test_data': test_data})
        if cluster_conf:
            test_kwargs.update({'cluster_conf': cluster_conf})

        try:
            if post_to_report_portal:
                rp_service.start_test_item(name=unique_test_name,
                                           description=report_portal_description,
                                           start_time=timestamp(),
                                           item_type="STEP")
                rp_service.log(time=timestamp(), message="Logfile location: {}".format(tc['log-link']), level="INFO")
                rp_service.log(time=timestamp(), message="Polarion ID: {}".format(tc['polarion-id']), level="INFO")

            rc = test_mod.run(**test_kwargs)
        except Exception:
            if post_to_report_portal:
                rp_service.log(time=timestamp(), message=traceback.format_exc(), level="ERROR")
            log.error(traceback.format_exc())
            rc = 1
        finally:
            if store:
                pass  # TODO: store cluster data for non-aws installations?

        elapsed = (time.time() - start)
        tc['duration'] = elapsed
        if rc == 0:
            tc['status'] = 'Pass'
            msg = "Test {} passed".format(test_mod)
            log.info(msg)
            print(msg)
            if post_to_report_portal:
                rp_service.finish_test_item(end_time=timestamp(), status="PASSED")
            if post_results:
                post_to_polarion(tc=tc)
        else:
            tc['status'] = 'Failed'
            msg = "Test {} failed".format(test_mod)
            log.info(msg)
            print(msg)
            jenkins_rc = 1
            if post_to_report_portal:
                rp_service.finish_test_item(end_time=timestamp(), status="FAILED")
            if post_results:
                post_to_polarion(tc=tc)
            if test.get('abort-on-fail', False):
                log.info("Aborting on test failure")
                tcs.append(tc)
                break
        tcs.append(tc)
    close_and_remove_filehandlers()
    if post_to_report_portal:
        rp_service.finish_launch(end_time=timestamp())
        rp_service.terminate()
    url_base = "http://magna002.ceph.redhat.com/cephci-jenkins"  # TODO: need a new directory for ocs test logs?
    run_dir_name = run_dir.split('/')[-1]
    print("\nAll test logs located here: {base}/{dir}".format(base=url_base, dir=run_dir_name))
    print_results(tcs)
    send_to_qe = post_results or post_to_report_portal
    if send_email:
        email_results(tcs, run_id, send_to_qe)
    return jenkins_rc


if __name__ == '__main__':
    args = docopt(doc)
    rc = run(args)
    log.info("Final rc of test run %d" % rc)
    sys.exit(rc)
