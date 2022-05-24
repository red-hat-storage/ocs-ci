import logging
from ocs_ci.ocs import constants, node
from ocs_ci.framework.testlib import E2ETest, tier1
from ocs_ci.framework.testlib import bugzilla
from ocs_ci.ocs.exceptions import ResourceWrongStatusException
from ocs_ci.ocs.node import get_worker_nodes
from ocs_ci.ocs.resources import pod
from concurrent.futures import ThreadPoolExecutor
from ocs_ci.helpers import helpers


log = logging.getLogger(__name__)


@tier1
@bugzilla("2075068")
class TestKernelCrash(E2ETest):
    """
    Tests to verify kernel crash
    """

    pvc_size = 1

    def test_node_kernel_crash_ceph_fsync(
        self, pvc_factory, teardown_factory, random=None
    ):
        """
        Create PVCs and pods
        """

        original_dir = "/var/lib/www/html/"
        result_dir = "mydir"
        worker_nodes_list = get_worker_nodes()

        # Create a RWX PVC
        pvc_obj = pvc_factory(
            interface=constants.CEPHFILESYSTEM,
            access_mode=constants.ACCESS_MODE_RWX,
            status=constants.STATUS_BOUND,
        )

        # Confirm PVCs are Bound
        log.info("Verifying the CephFS PVC are Bound")
        helpers.wait_for_resource_state(
            resource=pvc_obj, state=constants.STATUS_BOUND, timeout=240
        )
        pvc_obj.reload()
        log.info("Verified: CephFS PVC are Bound")

        # Set interface argument for reference
        pvc_obj.interface = constants.CEPHFILESYSTEM

        # Create a pod on a particular node
        selected_node = random.choice(worker_nodes_list)
        log.info(f"Creating a pod on node: {selected_node} with pvc {pvc_obj.name}")

        pod_obj = helpers.create_pod(
            interface_type=constants.CEPHFILESYSTEM,
            pvc_name=pvc_obj.name,
            namespace=pvc_obj.namespace,
            node_name=selected_node,
            pod_dict_path=constants.NGINX_POD_YAML,
        )

        # Confirm that the pod is running on the selected_node
        helpers.wait_for_resource_state(
            resource=pod_obj, state=constants.STATUS_RUNNING, timeout=180
        )
        pod_obj.reload()
        assert pod.verify_node_name(
            pod_obj, selected_node
        ), "Pod is running on a different node than the selected node"

        files = [constants.FILE_CREATE_DELETE, constants.FSYNC]
        for file in files:
            cmd = f"oc cp {file} {pvc_obj.namespace}/{pod_obj.name}:/"
            helpers.run_cmd(cmd=cmd)
        log.info("Files copied successfully ")

        commands = ["mkdir" + " " + original_dir + result_dir, "apt-get update"]
        for cmd in commands:
            pod_obj.exec_cmd_on_pod(command=f"{cmd}")

        pod_obj.exec_sh_cmd_on_pod(command=f"apt-get install gcc -y")
        pod_obj.exec_sh_cmd_on_pod(command=f"gcc fsync.c -o fsync")
        log.info("Starting creation and deletion of files on volume")

        executor = ThreadPoolExecutor(max_workers=3)
        thread1 = executor.submit(
            pod_obj.exec_sh_cmd_on_pod, command=f"nohup bash files_create_delete.sh"
        )
        thread2 = executor.submit(pod_obj.exec_sh_cmd_on_pod, command=f"./fsync")
        thread1.result()
        thread2.result()

        try:
            node.wait_for_nodes_status(
                node=selected_node, status=constants.NODE_NOT_READY, timeout=3600
            )
        except ResourceWrongStatusException as ex:
            log.info(f"Node in NotReady status found, hence TC is failed. ")
            raise ex
        else:
            log.info(f"Node in Ready status found, hence TC is Passed. ")

        teardown_factory(pod_obj)
