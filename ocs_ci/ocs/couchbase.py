"""
Couchbase workload class
"""
import logging
import random
from concurrent.futures.thread import ThreadPoolExecutor

from ocs_ci.ocs.resources.ocs import OCS
from ocs_ci.ocs.ocp import OCP, switch_to_project
from ocs_ci.utility import templating
from ocs_ci.utility.utils import TimeoutSampler
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.ocs import constants
from ocs_ci.ocs.pillowfight import PillowFight
from ocs_ci.ocs.ocp import switch_to_default_rook_cluster_project
from ocs_ci.ocs.resources.pod import get_pod_obj
from tests.helpers import wait_for_resource_state

log = logging.getLogger(__name__)


class CouchBase(PillowFight):
    """
    CouchBase workload operation
    """
    WAIT_FOR_TIME = 1800
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

    def __init__(self, **kwargs):
        """
        Initializer function

        """
        super().__init__(**kwargs)

    def is_up_and_running(self, pod_name, ocp_value):
        """
        Test if the pod specified is up and running.

        Args:
            pod_name (str): Name of pod being checked.
            ocp_value (object): object used for running oc commands

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

    def setup_cb(self):
        """
        Creating admission parts,couchbase operator pod, couchbase worker secret

        """
        # Create admission controller
        log.info("Create admission controller process for Couchbase")
        switch_to_project('default')
        self.up_adm_chk = OCP(namespace="default")
        self.up_check = OCP(namespace=constants.COUCHBASE_OPERATOR)
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
        self.pod_obj.new_project(constants.COUCHBASE_OPERATOR)
        couchbase_data = templating.load_yaml(
            constants.COUCHBASE_CRD_YAML
        )
        self.couchbase_obj = OCS(**couchbase_data)
        self.couchbase_obj.create()
        op_data = templating.load_yaml(constants.COUCHBASE_OPERATOR_ROLE)
        self.operator_role = OCS(**op_data)
        self.operator_role.create()
        self.serviceaccount = OCP(namespace=constants.COUCHBASE_OPERATOR)
        self.serviceaccount.exec_oc_cmd(
            "create serviceaccount couchbase-operator"
        )

        dockercfgs = self.serviceaccount.exec_oc_cmd("get secrets")
        startloc = dockercfgs.find('couchbase-operator-dockercfg')
        newdockerstr = dockercfgs[startloc:]
        endloc = newdockerstr.find(' ')
        dockerstr = newdockerstr[:endloc]
        self.secretsadder.exec_oc_cmd(
            f"secrets link serviceaccount/couchbase-operator secrets/{dockerstr}"
        )
        self.rolebinding = OCP(namespace=constants.COUCHBASE_OPERATOR)
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
            constants.COUCHBASE_OPERATOR
        ):
            try:
                if self.is_up_and_running(couchbase_pod[0], self.up_check):
                    break
            except IndexError:
                log.info("Couchbase operator is not up")

        cb_work = templating.load_yaml(constants.COUCHBASE_WORKER_SECRET)
        self.cb_worker = OCS(**cb_work)
        self.cb_worker.create()

    def create_couchbase_worker(self, replicas=1):
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
        logging.info('Creating pods..')
        cb_example = templating.load_yaml(constants.COUCHBASE_WORKER_EXAMPLE)
        cb_example['spec']['servers'][0]['size'] = replicas
        self.cb_examples = OCS(**cb_example)
        self.cb_examples.create()

        # Wait for last of three workers to be running.

        logging.info('Waiting for the pods to Running')
        for cb_wrk_pods in TimeoutSampler(
            self.WAIT_FOR_TIME,
            3,
            get_pod_name_by_pattern,
            'cb-example',
            constants.COUCHBASE_OPERATOR
        ):
            try:
                if len(cb_wrk_pods) == replicas:
                    counter = 0
                    for cb_pod in cb_wrk_pods:
                        if self.is_up_and_running(cb_pod, self.up_check):
                            counter += 1
                            logging.info(f'Couchbase worker {cb_pod} is up')
                    if counter == replicas:
                        break
            except IndexError:
                logging.info(
                    f'Expected number of couchbase pods are {replicas} '
                    f'but only found {len(cb_wrk_pods)}'
                )

    def run_workload(self, replicas, num_items=None, num_threads=None, run_in_bg=False):
        """
        Running workload with pillow fight operator
        Args:
            replicas (int): Number of pods
            num_items (int): Number of items to be loaded to the cluster
            num_threads (int): Number of threads
            run_in_bg (bool) : Optional run IOs in background

        """
        self.result = None
        logging.info('Running IOs...')
        if run_in_bg:
            executor = ThreadPoolExecutor(1)
            self.result = executor.submit(PillowFight.run_pillowfights, self, replicas=replicas,
                                          num_items=num_items, num_threads=num_threads
                                          )
            return self.result
        PillowFight.run_pillowfights(self, replicas=replicas, num_items=num_items, num_threads=num_threads)

    def analyze_run(self, skip_analyze=False):
        """
        Analyzing the workload run logs

        Args:
            skip_analyze (bool): Option to skip logs analysis

        """
        if not skip_analyze:
            logging.info('Analyzing  workload run logs..')
            PillowFight.analyze_all(self)

    def respin_couchbase_app_pod(self):
        """
        Respin the couchbase app pod

        Returns:
            pod status

        """
        app_pod_list = get_pod_name_by_pattern('cb-example', constants.COUCHBASE_OPERATOR)
        app_pod = app_pod_list[random.randint(0, len(app_pod_list) - 1)]
        logging.info(f"respin pod {app_pod}")
        app_pod_obj = get_pod_obj(app_pod, namespace=constants.COUCHBASE_OPERATOR)
        app_pod_obj.delete(wait=True, force=False)
        wait_for_resource_state(
            resource=app_pod_obj, state=constants.STATUS_RUNNING, timeout=300
        )

    def get_couchbase_nodes(self):
        """
        Get nodes that contain a couchbase app pod

        Returns:
            list: List of nodes

        """
        app_pods_list = get_pod_name_by_pattern('cb-example', constants.COUCHBASE_OPERATOR)
        app_pod_objs = list()
        for pod in app_pods_list:
            app_pod_objs.append(get_pod_obj(pod, namespace=constants.COUCHBASE_OPERATOR))

        log.info("Create a list of nodes that contain a couchbase app pod")
        nodes_set = set()
        for pod in app_pod_objs:
            logging.info(
                f"pod {pod.name} located on "
                f"node {pod.get().get('spec').get('nodeName')}"
            )
            nodes_set.add(pod.get().get('spec').get('nodeName'))
        return list(nodes_set)

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
        self.pod_obj.delete_project(constants.COUCHBASE_OPERATOR)
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
        PillowFight.cleanup(self)
        switch_to_default_rook_cluster_project()
