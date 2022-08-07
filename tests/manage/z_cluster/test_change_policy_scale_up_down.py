import logging
import pytest

from ocs_ci.ocs import constants, ocp
from ocs_ci.utility.utils import TimeoutSampler, run_cmd
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import wait_for_resource_state
from ocs_ci.ocs.utils import get_pod_name_by_pattern
from ocs_ci.framework.testlib import ManageTest, tier2, bugzilla
from ocs_ci.utility import templating
from ocs_ci.ocs.resources.pod import get_pod_obj

log = logging.getLogger(__name__)


@tier2
@bugzilla("2024870")
class TestChangePolicyScaleUpDown(ManageTest):
    """
    Test Change Policy Scale Up/Down
    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        def finalizer():
            self.delete_pod(pod_name="simple-app", namespace=self.project_name)

        request.addfinalizer(finalizer)

    def test_change_policy_scale_up_down(
        self,
        teardown_project_factory,
        pvc_factory,
    ):
        self.project_name = "test-project"
        self.ocp_obj = ocp.OCP(namespace=self.project_name)
        project_obj = helpers.create_project(project_name=self.project_name)
        teardown_project_factory(project_obj)

        self.pvc_obj = pvc_factory(
            interface=constants.CEPHBLOCKPOOL,
            project=project_obj,
            access_mode=constants.ACCESS_MODE_RWO,
            size=10,
            status=constants.STATUS_BOUND,
        )
        log.info("Create service_account to get privilege for deployment pods")
        sa_obj = helpers.create_serviceaccount(self.pvc_obj.project.namespace)
        helpers.add_scc_policy(
            sa_name=sa_obj.name, namespace=self.pvc_obj.project.namespace
        )

        simple_app_data = templating.load_yaml(constants.SIMPLE_APP_POD_YAML)
        simple_app_data["metadata"]["namespace"] = self.pvc_obj.project.namespace
        simple_app_data["spec"]["template"]["spec"]["serviceAccountName"] = sa_obj.name
        simple_app_data["spec"]["template"]["spec"]["volumes"][0][
            "persistentVolumeClaim"
        ]["claimName"] = self.pvc_obj.name
        helpers.create_resource(**simple_app_data)

        sample = TimeoutSampler(
            timeout=300,
            sleep=5,
            func=self.verify_simple_app_pod_on_running_state,
            pod_name="simple-app",
            namespace=self.pvc_obj.project.namespace,
        )
        if not sample.wait_for_func_status(result=True):
            log.error("'simple-app' pod does not created after 100 seconds")
            raise TimeoutExpiredError(
                "'simple-app' pod does not created after 100 seconds"
            )

        bash_loop = "{1..2}"
        self.app_pod_obj.exec_cmd_on_pod(
            f"for i in {bash_loop}; do dd if=/dev/urandom of=file$i bs=1k count=1 ; done"
        )
        for mode in ("2770", "0770", "0775", "2755"):
            log.error(mode)
        node_name = self.app_pod_obj.pod_data["spec"]["nodeName"]
        cmd_start = f"oc debug nodes/{node_name} -- chroot /host /bin/bash -c "
        cmd = f"df -h | grep {self.pvc_obj.data['spec']['volumeName']}"
        cmd = f'{cmd_start} "{cmd}"'
        out = run_cmd(cmd=cmd)
        log.info(out)
        path = out.split()[5]

        cmd = f"{cmd_start} 'chmod 0770 {path}'"
        out = run_cmd(cmd=cmd)
        log.info(out)

        log.info(f"Scaling down simple-app deployment to 0")
        ocp_obj = ocp.OCP(namespace=self.pvc_obj.project.namespace)
        ocp_obj.exec_oc_cmd(f"scale --replicas=0 deployment/simple-app")
        try:
            pod_names = get_pod_name_by_pattern(
                pattern="simple-app", namespace=self.pvc_obj.project.namespace
            )
            self.app_pod_obj = get_pod_obj(pod_names[0], namespace=pod_names[0])
            self.app_pod_obj.delete(force=True)
        except Exception as e:
            log.error(e)

        log.info(f"Scaling up simple-app deployment to 1")
        self.ocp_obj.exec_oc_cmd(f"scale --replicas=1 deployment/simple-app")

    def verify_simple_app_pod_on_running_state(self, pod_name, namespace):
        pod_names = get_pod_name_by_pattern(pattern=pod_name, namespace=namespace)
        if len(self.pod_names) == 0:
            return False
        self.app_pod_obj = get_pod_obj(pod_names[0], namespace=namespace)
        try:
            wait_for_resource_state(
                resource=self.app_pod_obj, state=constants.STATUS_RUNNING, timeout=30
            )
        except Exception as e:
            log.error(e)
            return False
        return True

    def delete_pod(self, pod_name, namespace):
        log.info(f"Scaling down simple-app deployment to 0")
        self.ocp_obj.exec_oc_cmd(f"scale --replicas=0 deployment/simple-app")
        try:
            pod_names = get_pod_name_by_pattern(pattern=pod_name, namespace=namespace)
            self.app_pod_obj = get_pod_obj(pod_names[0], namespace=pod_names[0])
            self.app_pod_obj.delete(force=True)
        except Exception as e:
            log.error(e)

    def change_mode(self, mode):
        node_name = self.app_pod_obj.pod_data["spec"]["nodeName"]
        cmd_start = f"oc debug nodes/{node_name} -- chroot /host /bin/bash -c "
        cmd = f"df -h | grep {self.pvc_obj.data['spec']['volumeName']}"
        cmd = f'{cmd_start} "{cmd}"'
        out = run_cmd(cmd=cmd)
        log.info(out)
        path = out.split()[5]
        cmd = f"{cmd_start} 'chmod {mode} {path}'"
        out = run_cmd(cmd=cmd)
        log.info(out)
