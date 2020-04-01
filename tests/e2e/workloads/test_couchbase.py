"""
Couchbase Workload Module
"""
import logging
import pytest
from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP, switch_to_project
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs import constants
from ocs_ci.framework.testlib import E2ETest, workloads, ignore_leftovers
from ocs_ci.ocs.pillowfight import PillowFight

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def pillowfight(request):

    # Create RipSaw Operator
    pillowfight = PillowFight()

    def teardown():
        pillowfight.cleanup()
    request.addfinalizer(teardown)
    return pillowfight


@ignore_leftovers
@workloads
class TestCouchbaseWorkload(E2ETest):
    """
    Main couchbase workload class
    """
    COUCHBASE_OPERATOR = 'couchbase-operator-namespace'
    WAIT_FOR_TIME = 600
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
    pod_obj = OCP(kind='pod')
    couchbase_pod = OCP(kind='pod')
    secretsadder = OCP(kind='pod')
    admission_pod = []
    cb_worker = OCS()
    cb_examples = OCS()

    def add_serviceaccount_secret(self, acct_name, dockerstr):
        """
        Add secret for serviceaccount

        Args:
            acct_name (str): Name of the service account
            dockerstr (str): Docker secret

        """
        self.secretsadder.exec_oc_cmd(
            f"secrets add serviceaccount/{acct_name} secrets/{dockerstr} --for=pull"
        )

    def is_up_and_running(self, pod_name, ocp_value):
        """
        Test if the pod specified is up and running.

        Args:
            pod_name (str): Name of pod being checked.
            ocp_value (OCP): object used for running oc commands

        Returns:
            bool; True if pod is running, False otherwise

        """
        if not pod_name:
            return False
        pod_info = ocp_value.exec_oc_cmd(f"get pods {pod_name} -o json")
        if pod_info['status']['containerStatuses'][0]['ready']:
            if 'running' in pod_info['status']['containerStatuses'][0]['state']:
                return True
        return False

    def test_couchbase_workload_simple(self, pillowfight):
        """
        Deploy a Couchbase server and pillowfight workload using operator

        The couchbase workers do not come up unless there is an admission controller
        running.  The admission controller is started from the default project prior
        to bringing up the operator.  Secrets, rolebindings and serviceaccounts
        need to also be generated.

        Once the couchbase operator is running, we need to wait for the three
        worker pods to also be up.  Then a pillowfight task is started.

        After the pillowfight task has finished, the log is collected and
        analyzed.

        Raises:
            Exception: If pillowfight results indicate that a minimum performance
                level is not reached (1 second response time, less than 1000 ops
                per second)

        """
        # Create admission controller
        log.info("Create admission controller process for Couchbase")

        switch_to_project('default')
        self.up_adm_chk = OCP(namespace="default")
        self.up_check = OCP(namespace=self.COUCHBASE_OPERATOR)
        for adm_yaml in self.admission_parts:
            adm_data = templating.load_yaml(adm_yaml)
            adm_obj = OCS(**adm_data)
            adm_obj.create()

        # Wait for admission pod to be created
        for adm_pod in TimeoutSampler(
            self.WAIT_FOR_TIME,
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
            timeout=self.WAIT_FOR_TIME,
            sleep=10,
        )
        self.pod_obj.new_project(self.COUCHBASE_OPERATOR)
        couchbase_data = templating.load_yaml(
            constants.COUCHBASE_CRD_YAML
        )
        self.couchbase_obj = OCS(**couchbase_data)
        self.couchbase_obj.create()
        op_data = templating.load_yaml(constants.COUCHBASE_OPERATOR_ROLE)
        self.operator_role = OCS(**op_data)
        self.operator_role.create()
        self.serviceaccount = OCP(namespace=self.COUCHBASE_OPERATOR)
        self.serviceaccount.exec_oc_cmd(
            "create serviceaccount couchbase-operator"
        )

        dockercfgs = self.serviceaccount.exec_oc_cmd("get secrets")
        startloc = dockercfgs.find('couchbase-operator-dockercfg')
        newdockerstr = dockercfgs[startloc:]
        endloc = newdockerstr.find(' ')
        dockerstr = newdockerstr[:endloc]
        self.add_serviceaccount_secret("couchbase-operator", dockerstr)
        self.add_serviceaccount_secret("default", dockerstr)
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
            self.WAIT_FOR_TIME,
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
            self.WAIT_FOR_TIME,
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

        pillowfight.run_pillowfights()
        pillowfight.analyze_all()

    def teardown(self):
        """
        Delete objects created in roughly reverse order of how they were created.

        """
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
        self.couchbase_obj.delete()
        switch_to_project('default')
        self.pod_obj.delete_project(self.COUCHBASE_OPERATOR)
        for adm_yaml in self.admission_parts:
            adm_data = templating.load_yaml(adm_yaml)
            adm_obj = OCS(**adm_data)
            adm_obj.delete()
        # Before the code below was added, the teardown task would sometimes
        # fail with the leftover objects because it would still see one of the
        # couchbase pods.
        for admin_pod in TimeoutSampler(
            self.WAIT_FOR_TIME,
            3,
            get_pod_name_by_pattern,
            'couchbase',
            'default'
        ):
            if admin_pod:
                continue
            else:
                break
