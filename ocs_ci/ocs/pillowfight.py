"""
Pillowfight Class to run various workloads and scale tests
"""
import logging
import tempfile
import re
from os import listdir
from os.path import isfile, join
from shutil import rmtree

from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs import constants
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern

log = logging.getLogger(__name__)


class PillowFight(object):
    """
      Workload operation using PillowFight
      This class was modelled after the RipSaw class in this directory.
    """

    WAIT_FOR_TIME = 600
    COUCHBASE_OPERATOR = 'couchbase-operator-namespace'
    MIN_ACCEPTABLE_OPS_PER_SEC = 1000
    MAX_ACCEPTABLE_RESPONSE_TIME = 1000

    def __init__(self, **kwargs):
        """
        Initializer function

        Args:
            kwargs (dict):
                Following kwargs are valid
                repo: PillowFight repo to used - a github link
                branch: branch to use from the repo
                namespace: namespace for the operator

        Example Usage:
            r1 = PillowFight()
            r1.run_pillowfights()
            # To run a private yaml
            my_custom_bench = my_custom_bench.yaml
            run_cmd('oc apply -f my_custom_bench')
            # To get pillowfight data from log file
            data = r1.extract_data(log_file)
            # To do basic sanity checking of data
            r1.sanity_check(data)

        """
        self.args = kwargs
        self.namespace = self.args.get('namespace', 'couchbase-operator-namespace')
        self.ocp = OCP()
        self.ns_obj = OCP(kind='namespace')
        self.pod_obj = OCP(kind='pod')
        self.up_check = OCP(namespace=self.COUCHBASE_OPERATOR)
        self.logs = tempfile.mkdtemp(prefix='pf_logs_')

    def run_pillowfights(self):
        """
        loop through all the yaml files extracted from the pillowfight repo
        and run them.  Run oc logs on the results and save the logs in self.logs
        directory

        """
        ocp_local = OCP(namespace=self.namespace)
        pf_files = listdir(constants.TEMPLATE_PILLOWFIGHT_DIR)
        for pf_yaml in pf_files:
            pf_fullpath = join(constants.TEMPLATE_PILLOWFIGHT_DIR, pf_yaml)
            if not pf_fullpath.endswith('.yaml'):
                continue
            if not isfile(pf_fullpath):
                continue
            pfight = templating.load_yaml(pf_fullpath)
            lpillowfight = OCS(**pfight)
            lpillowfight.create()
            pf_completion_info = ''
            pf_pod = ''
            for pillowfight_pod in TimeoutSampler(
                self.WAIT_FOR_TIME,
                3,
                get_pod_name_by_pattern,
                'pillowfight',
                self.COUCHBASE_OPERATOR
            ):
                try:
                    pf_pod = pillowfight_pod[0]
                    pod_info = self.up_check.exec_oc_cmd(
                        f"get pods {pf_pod} -o json"
                    )
                    pf_status = pod_info['status']['containerStatuses'][0]['state']
                    if 'terminated' in pf_status:
                        pf_completion_info = pf_status['terminated']['reason']
                        break
                except IndexError:
                    log.info(f"Pillowfight {pf_yaml} not yet completed")
            if pf_completion_info == 'Error':
                raise Exception(
                    f"Pillowfight {pf_yaml} failed to complete"
                )
            if pf_completion_info == 'Completed':
                pf_prefix = pf_yaml[0:pf_yaml.find(".")]
                pf_endlog = f'{pf_prefix}.log'
                pf_log = join(self.logs, pf_endlog)
                data_from_log = ocp_local.exec_oc_cmd(
                    f"logs -f {pf_pod} --ignore-errors",
                    out_yaml_format=False
                )
                data_from_log = data_from_log.replace('\x00', '')
                with open(pf_log, 'w') as fd:
                    fd.write(data_from_log)

    def analyze_all(self):
        """
        Analyze the data extracted into self.logs files

        """
        for path in listdir(self.logs):
            full_path = join(self.logs, path)
            with open(full_path, 'r') as fdesc:
                data_from_log = fdesc.read()
            log_data = self.parse_pillowfight_log(data_from_log)
            self.sanity_check(log_data)

    def sanity_check(self, stats):
        """
        Make sure the worst cases for ops per second and response times are
        within an acceptable range.

        """
        stat1 = min(stats['opspersec'])
        if stat1 < self.MIN_ACCEPTABLE_OPS_PER_SEC:
            raise Exception(
                f"Worst OPS/SEC value reported is {stat1}"
            )
        stat2 = max(stats['resptimes'].keys()) / 1000
        if stat2 > self.MAX_ACCEPTABLE_RESPONSE_TIME:
            raise Exception(
                f"Worst response time reported is {stat2} milliseconds"
            )

    def parse_pillowfight_log(self, data_from_log):
        """
        Run oc logs on the pillowfight pod passed in.  Cleanup the output
        from oc logs to handle peculiarities in the couchbase log results,
        and generate a summary of the results.

        The dictionary returned has two values; 'opspersec' and 'resptimes'.
        opspersec is a list of ops per second numbers reported.'
        resptimes is a dictionary index by the max response time of a range.
        Each entry in resptimes contains a minimum response time for that range,
        and a count of how many messages fall within that range.

        Args:
            data_from_log (str): log data

        Returns:
            dict: ops per sec and response time information

        """
        # The data in the couchbase logs is kind of abnormal.
        # It contains histograms with invalid unicode charaters for yaml
        # output (which is why out_yaml_format=False is used).
        # It also seems to write a block of text inside another block at
        # an unpredictable location.  The value good_txt below is the output
        # of the log with that data removed..
        #
        # So what's left is a list of OPS/SEC values and a histogram of
        # response times.  This routine organizes that data.
        ops_per_sec = []
        resp_hist = {}
        log.info(
            "*******Couchbase raw output log*********\n"
            f"{data_from_log}"
        )
        lines = data_from_log.split("\n")
        for dline in lines:
            try:
                if dline.startswith("OPS/SEC"):
                    dfields = dline.split(" ")
                    dnumb = int(dfields[-1].strip())
                    ops_per_sec.append(dnumb)
                if re.match('^\\[\\d+ +- \\d+ *\\][um]s \\|#* - \\d+', dline):
                    for element in ["[", "]", "|", "-", "#"]:
                        dline = dline.replace(element, " ")
                    parts = dline.split()
                    i1 = int(parts[0])
                    i2 = int(parts[1])
                    if parts[2] == 'ms':
                        i1 *= 1000
                        i2 *= 1000
                    resp_hist[i2] = {'minindx': i1, 'number': int(parts[3])}
            except ValueError:
                log.info(f"{dline} -- contains invalid data")
        ret_data = {'opspersec': ops_per_sec, 'resptimes': resp_hist}
        return ret_data

    def cleanup(self):
        """
        Remove pillowfight pods and temp files

        """
        pf_files = listdir(constants.TEMPLATE_PILLOWFIGHT_DIR)
        for pf_yaml in pf_files:
            pf_fullpath = join(constants.TEMPLATE_PILLOWFIGHT_DIR, pf_yaml)
            if not pf_fullpath.endswith('.yaml'):
                continue
            if not isfile(pf_fullpath):
                continue
            pfight = templating.load_yaml(pf_fullpath)
            lpillowfight = OCS(**pfight)
            try:
                lpillowfight.delete()
            except CommandFailed:
                log.info(f"{pf_fullpath} object is already deleted")
        rmtree(self.logs)
        nsinfo = self.pod_obj.exec_oc_cmd(command="get namespace")
        if self.COUCHBASE_OPERATOR in nsinfo:
            self.pod_obj.exec_oc_cmd(
                command=f"delete namespace {self.COUCHBASE_OPERATOR}"
            )
