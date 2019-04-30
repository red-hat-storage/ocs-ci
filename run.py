#!/usr/bin/env python
from gevent import monkey

monkey.patch_all()
import yaml
import sys
import os
import platform
import logging
import textwrap
import urllib3
from docopt import docopt
from getpass import getuser
from ocsci.enums import ReturnCode, TestStatus
from utility.utils import (
    timestamp, create_run_dir, create_report_portal_session, email_results,
    close_and_remove_filehandlers, configure_logger,
)
from ocsci.framework import TestCase


doc = """
A simple test suite wrapper that executes tests based on yaml test config.

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
  -c <conf> --conf <conf>           cluster configuration file to override
                                    defaults
  -s <suite> --suite <suite>        test suite to run
                                    eg: -s smoke or -s rbd
  -f <tests> --filter <tests>       filter tests based on the patter
                                    eg: -f 'rbd' will run tests that have 'rbd'
  --store                           store the current vm state for reuse
  --reuse <file>                    use the stored vm state for rerun
  --post-results                    Post results to Polarion.
                                    Needs Polarion IDs in test suite yaml.
                                    Requires config file, see README.
  --report-portal                   Post results to report portal.
                                    Requires config file, see README.
  --log-level <LEVEL>               Set logging level
  --cluster-name <name>             Name that will be used for cluster creation
  --no-email                        Do not send results email
"""

log = logging.getLogger(__name__)

run_id = timestamp()
run_dir = create_run_dir(run_id)


def set_logger(console_log_level=logging.INFO):
    """
    Set all related to logger

    Args:
        console_log_level (int) = Log level number (default: logging.INFO)
    """
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    logging.basicConfig(level=logging.DEBUG)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    if console_log_level:
        log_level = logging.getLevelName(console_log_level.upper())
        stream_handler.setLevel(log_level)
    root_logger.addHandler(stream_handler)
    configure_logger("startup", run_dir)


def print_results(tc):
    header = (
        '\n{name:<20s}   {desc:50s}   {duration:20s}   {status:>15s}'.format(
            name='TEST NAME',
            desc='TEST DESCRIPTION',
            duration='DURATION',
            status='STATUS'
        )
    )
    print(header)
    for test in tc:
        if test.duration:
            dur = str(test.duration)
        else:
            dur = '0s'
        name = test.name
        desc = test.desc or "None"
        status = test.status.name
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
    cluster_name = args.get('--cluster-name')
    send_email = not args.get('--no-email', False)

    if cleanup_name:
        pass  # TODO: cleanup cluster and skip test execution

    # Check if we are running on windows and bail out sooner
    if platform.system() == "Windows":
        log.error("Windows OS is not supported by Openshift Installer")
        return ReturnCode.UNSUPPORTED_WINDOWS_RUN

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
        pass
        # TODO: determine ci-message structure and utilize for OCS if necessary
        #  otherwise remove logic

    rp_service = None
    suite_name = str(os.path.basename(suite_file).split(".")[0])
    if post_to_report_portal:
        log.info("Creating report portal session")
        rp_service = create_report_portal_session()
        # TODO: add appropriate values to report portal test description
        launch_desc = textwrap.dedent(
            f"""
            invoked-by: {getuser()}
            """
        )
        rp_service.start_launch(
            name=suite_name,
            start_time=timestamp(),
            description=launch_desc
        )

    if reuse:
        pass
        # TODO: build cluster object and skip install
        #  potentially with additional kwargs read in by test(s)
    if store:
        pass
        # TODO: store cluster data for non-aws installations,
        #  standardize location for cluster info

    sys.path.append(os.path.abspath('tests'))
    tests = suite.get('tests')
    tcs = []
    jenkins_rc = TestStatus.FAILED
    test_data = dict()
    if cluster_name:
        test_data['cluster-name'] = cluster_name

    for test in tests:
        config = test.get('test').get('config', {})
        test_kwargs = dict()
        test_kwargs.update({'config': config})
        test_kwargs.update({'test_data': test_data})
        if cluster_conf:
            test_kwargs.update({'cluster_conf': cluster_conf})
        tc = TestCase(
            test.get('test'),
            suite_name,
            run_dir,
            test_kwargs,
            rp_service
        )
        tc.execute()
        tcs.append(tc)
        if tc.abort_on_fail and tc.status == TestStatus.FAILED:
            log.info("Aborting on test failure")
            break

    if all([tc.status == TestStatus.PASSED for tc in tcs]):
        jenkins_rc = TestStatus.PASSED

    close_and_remove_filehandlers()
    if post_to_report_portal:
        rp_service.finish_launch(end_time=timestamp())
        rp_service.terminate()
    # TODO: need a new directory for ocs test logs?
    # Once fixed, we can reuse code commented out below.
    # url_base = "http://magna002.ceph.redhat.com/cephci-jenkins"
    # run_dir_name = run_dir.split('/')[-1]
    # print(f"\nAll test logs located here: {url_base}/{run_dir_name}")
    print(f"\nAll test logs located here: {run_dir}/")
    print_results(tcs)
    send_to_qe = post_results or post_to_report_portal
    if send_email:
        email_results(tcs, run_id, send_to_qe)
    return jenkins_rc


if __name__ == '__main__':
    args = docopt(doc)
    console_log_level = args.get('--log-level')
    set_logger(console_log_level=console_log_level)
    rc = run(args)
    log.info("Final rc of test run %d", rc.value)
    sys.exit(rc.value)
