"""
Couchbase Workload Module
"""
import logging
import os
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP, switch_to_project
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.exceptions import CommandFailed

log = logging.getLogger(__name__)


@workloads
class TestCouchbaseWorkload(E2ETest):
    """
    Main couchbase workload class
    """
    MIN_ACCEPTABLE_OPS_PER_SEC = 1000
    MAX_ACCEPTABLE_RESPONSE_TIME = 1000
    COUCHBASE_OPERATOR = 'couchbase-operator-namespace'
    DOCKER_ENV_VAR_USERNAME = 'CBRHELUSER'
    DOCKER_ENV_VAR_PASSWORD = 'CBRHELPASSWORD'
    DOCKER_ENV_VAR_EMAIL = "CBRHELEMAIL"
    admission_parts = [
        constants.COUCHBASE_ADMISSION_SERVICE_ACCOUNT_YAML,
        constants.COUCHBASE_ADMISSION_CLUSTER_ROLE_YAML,
        constants.COUCHBASE_ADMISSION_CLUSTER_ROLE_BINDING_YAML,
        constants.COUCHBASE_ADMISSION_SECRET_YAML,
        constants.COUCHBASE_ADMISSION_DEPLOYMENT_YAML,
        constants.COUCHBASE_ADMISSION_SERVICE_YAML,
        constants.COUCHBASE_MUTATING_WEBHOOK_YAML,
        constants.COUCHBASE_VALIDATING_WEBHOOK_YAML
    ]
    admission_objs = []
    pod_obj = OCP(kind='pod')
    couchbase_pod = OCP(kind='pod')
    admission_pod = []
    secrets_array = []
    secret_options = [
        ["docker-username", DOCKER_ENV_VAR_USERNAME],
        ["docker-password", DOCKER_ENV_VAR_PASSWORD],
        ["docker-email", DOCKER_ENV_VAR_EMAIL]
    ]

    def gen_secret(self, entry):
        """
        Each entry consists of a parameter name and an environment variable.
        Generate text to add to the docker command if the environment variable
        is set.
        """
        envar = os.environ.get(entry[1])
        if envar:
            return f" --{entry[0]}={envar}"
        return False

    def create_secret(self):
        """
        Create a docker-registry secret.  This call uses environment variables
        CBRHELUSER for the username, CBRHELPASSWORD for the password, and
        CBRHELEMAIL for the email address.  If any of these values are not
        set, create_secret still attempts to run the create command without
        these options specified.
        """
        docker_cmd = 'create secret docker-registry rh-catalog '
        docker_cmd += '--docker-server=registry.connect.redhat.com'
        secret_array = []
        for entry in self.secret_options:
            new_create_cmd_line_part = self.gen_secret(entry)
            if new_create_cmd_line_part:
                secret_array.append(entry[0])
                docker_cmd += new_create_cmd_line_part
        self.pod_obj.exec_oc_cmd(command=docker_cmd, secrets=secret_array)

    def add_serviceaccount_secret(self, in_text):
        """
        Execute secrets add for serviceaccount
        """
        self.secretsadder.exec_oc_cmd(
            f"secrets add serviceaccount/{in_text} secrets/rh-catalog --for=pull"
        )

    def is_up_and_running(self, pod_name, ocp_value):
        """
        Test if the pod specified by pod is up and running.
        """
        if not pod_name:
            return False
        pod_info = ocp_value.exec_oc_cmd(f"get pods {pod_name} -o json")
        if pod_info['status']['containerStatuses'][0]['ready']:
            if 'running' in pod_info['status']['containerStatuses'][0]['state']:
                return True
        return False

    def parse_couchbase_log(self, pf_pod):
        """
        Run oc logs on the pillowfight pod passed in.  Cleanup the output
        from oc logs to handle peculiarities in the couchbase log results,
        and generate a summary of the results.

        Returns: a dictionary with two values; 'opspersec' and 'resptimes'.
        opspersec is a list of ops per second numbers reported.'
        resptimes is a dictionary index by the max response time of a range.
        Each entry in resptimes contains a minimum response time for that range,
        and a count of how many messages fall within that range.
        """
        data_from_log = self.up_check.exec_oc_cmd(
            f"logs -f {pf_pod} --ignore-errors",
            out_yaml_format=False
        )
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
        end_okay_txt = data_from_log.rfind('\n{')
        start_okay_txt = data_from_log.rfind('orph: 0\n') + 8
        raw_txt = data_from_log[:end_okay_txt] + data_from_log[start_okay_txt:]
        good_txt = raw_txt.replace("\0", "")
        lines = good_txt.split("\n")
        collect_response_times = False
        for dline in lines:
            if dline.startswith("OPS/SEC"):
                dfields = dline.split(" ")
                dnumb = int(dfields[-1].strip())
                ops_per_sec.append(dnumb)
            if collect_response_times:
                if dline.startswith('['):
                    for element in ["[", "]", "|", "-", "#"]:
                        dline = dline.replace(element, " ")
                    parts = dline.split()
                    i1 = int(parts[0])
                    i2 = int(parts[1])
                    if parts[2] == 'ms':
                        i1 *= 1000
                        i2 *= 1000
                    resp_hist[i2] = {'minindx': i1, 'number': int(parts[3])}
            if "+---------+" in dline:
                collect_response_times = True
            if "+-----------------" in dline:
                collect_response_times = False
        ret_data = {'opspersec': ops_per_sec, 'resptimes': resp_hist}
        return ret_data

    def test_couchbase_workload_simple(self):
        """
        Deploy a Couchbase server and pillowfight workload using operator
        """
        # Create admission controller
        log.info("Create admission controller process for Couchbase")

        self.admission_objs = []
        self.up_adm_chk = OCP(namespace="default")
        self.up_check = OCP(namespace=self.COUCHBASE_OPERATOR)
        for adm_yaml in self.admission_parts:
            adm_data = templating.load_yaml(adm_yaml)
            adm_obj = OCS(**adm_data)
            adm_obj.create()
            self.admission_objs.append(adm_obj)

        # Wait for admission pod to be created
        for adm_pod in TimeoutSampler(
            300,
            3,
            get_pod_name_by_pattern,
            'couchbase-operator-admission',
            'default'
        ):
            try:
                if self.is_up_and_running(adm_pod[0], self.up_adm_chk):
                    self.admission_pod = adm_pod[0]
                    break
            except IndexError:
                log.info("Admission pod is not ready yet")

        # Wait for admission pod to be running
        log.info("Waiting for admission pod to be running")
        self.pod_obj.wait_for_resource(
            condition='Running',
            resource_name=self.admission_pod,
            timeout=800,
            sleep=10,
        )
        self.pod_obj.new_project(self.COUCHBASE_OPERATOR)
        couchbase_data = templating.load_yaml(
            constants.COUCHBASE_CRD_YAML
        )
        self.couchbase_obj = OCS(**couchbase_data)
        self.couchbase_obj.create()
        self.rhcatalogOk = True
        try:
            self.create_secret()
        except CommandFailed:
            log.info("rh-catalog creation failed. Check DOCKER variables")
            self.rhcatalogOk = False
            raise Exception(
                "CBRHELUSER, CBRHELPASSWORD, and CBRHELEMAIL are probably not set"
            )
        op_data = templating.load_yaml(constants.COUCHBASE_OPERATOR_ROLE)
        self.operator_role = OCS(**op_data)
        self.operator_role.create()
        self.serviceaccount = OCP(namespace=self.COUCHBASE_OPERATOR)
        self.serviceaccount.exec_oc_cmd(
            "create serviceaccount couchbase-operator"
        )
        self.secretsadder = OCP()
        self.add_serviceaccount_secret("couchbase-operator")
        self.add_serviceaccount_secret("default")
        self.rolebinding = OCP(namespace=self.COUCHBASE_OPERATOR)
        rolebind_cmd = "".join([
            "create rolebinding couchbase-operator-rolebinding ",
            "--role couchbase-operator ",
            "--serviceaccount couchbase-operator-namespace:couchbase-operator"
        ])
        self.rolebinding.exec_oc_cmd(rolebind_cmd)
        dep_data = templating.load_yaml(constants.COUCHBASE_OPERATOR_DEPLOY)
        self.cb_deploy = OCS(**dep_data)
        self.cb_deploy.create()
        # Wait for couchbase operator pod to be running
        for couchbase_pod in TimeoutSampler(
            300,
            3,
            get_pod_name_by_pattern,
            'couchbase-operator',
            self.COUCHBASE_OPERATOR
        ):
            try:
                if self.is_up_and_running(couchbase_pod[0], self.up_check):
                    break
            except IndexError:
                log.info("Couchbase operator is not up")
        cb_work = templating.load_yaml(constants.COUCHBASE_WORKER_SECRET)
        self.cb_worker = OCS(**cb_work)
        self.cb_worker.create()
        cb_example = templating.load_yaml(constants.COUCHBASE_WORKER_EXAMPLE)
        self.cb_examples = OCS(**cb_example)
        self.cb_examples.create()
        # Wait for last of three workers to be running.
        for cb_wrk_pod in TimeoutSampler(
            300,
            3,
            get_pod_name_by_pattern,
            'cb-example-0002',
            self.COUCHBASE_OPERATOR
        ):
            try:
                if self.is_up_and_running(cb_wrk_pod[0], self.up_check):
                    # once last pod is up, make sure all are ready
                    counter = 0
                    for wpodn in range(0, 3):
                        cbw_pod = f"cb-example-{wpodn:04}"
                        if self.is_up_and_running(cbw_pod, self.up_check):
                            counter += 1
                    if counter == 3:
                        break
            except IndexError:
                log.info("Couchbase workers are not up")
        pfight = templating.load_yaml(constants.COUCHBASE_BASIC_PILLOWFIGHT)
        pillowfight = OCS(**pfight)
        pillowfight.create()
        pf_completion_info = ''
        pf_pod = ''
        for pillowfight_pod in TimeoutSampler(
            300,
            3,
            get_pod_name_by_pattern,
            'pillowfight',
            self.COUCHBASE_OPERATOR
        ):
            try:
                pf_pod = pillowfight_pod[0]
                pod_info = self.up_check.exec_oc_cmd(f"get pods {pf_pod} -o json")
                pf_status = pod_info['status']['containerStatuses'][0]['state']
                if 'terminated' in pf_status:
                    pf_completion_info = pf_status['terminated']['reason']
                    break
            except IndexError:
                log.info("Pillowfight not yet completed")
        if pf_completion_info == 'Error':
            raise Exception(
                "Basic pillowfight failed to complete"
            )
        if pf_completion_info == 'Completed':
            stats = self.parse_couchbase_log(pf_pod)
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
            return
        raise Exception("Invalid return from basic pillowfight")

    def teardown(self):
        """
        Delete objects created in roughly reverse order of how they were created.
        """
        if self.rhcatalogOk:
            self.cb_examples.delete()
            self.cb_worker.delete()
            self.cb_deploy.delete()
            self.pod_obj.exec_oc_cmd(
                command="delete rolebinding couchbase-operator-rolebinding"
            )
            self.pod_obj.exec_oc_cmd(
                command="delete serviceaccount couchbase-operator"
            )
            self.operator_role.delete()
            self.pod_obj.exec_oc_cmd(command="delete secret rh-catalog")
        self.couchbase_obj.delete()
        switch_to_project('default')
        self.pod_obj.delete_project(self.COUCHBASE_OPERATOR)
        for adm_obj in self.admission_objs:
            adm_obj.delete()
