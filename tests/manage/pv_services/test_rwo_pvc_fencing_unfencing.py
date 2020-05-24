import logging
import pytest
import random
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from ocs_ci.framework import config
from ocs_ci.framework.testlib import (
    ignore_leftovers, ManageTest, tier4, tier4a, tier4b, tier4c
)
from ocs_ci.ocs import constants, machine, node, ocp
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.exceptions import ResourceWrongStatusException, UnexpectedBehaviour
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check, get_az_count
from tests import disruption_helpers, helpers

logger = logging.getLogger(__name__)

ISSUE_SKIP = pytest.mark.skip(
    'Skip test due to https://github.com/red-hat-storage/ocs-ci/issues/2101'
)


@tier4
@ignore_leftovers
class TestRwoPVCFencingUnfencing(ManageTest):
    """
    KNIP-677 OCS support for Automated fencing/unfencing RWO PV
    """
    pvc_size = 5  # size in Gi

    # Pods of each interface type to be run on nodes which are going to fail
    num_of_app_pods_per_node = 2

    short_nw_fail_time = 300  # Duration in seconds for short network failure

    prolong_nw_fail_time = 900  # Duration in seconds for prolong network failure

    @pytest.fixture()
    def setup(
        self, request, scenario, num_of_nodes, num_of_fail_nodes,
        disrupt_provisioner, project_factory, multi_pvc_factory, dc_pod_factory
    ):
        """
        Identify the nodes and start DeploymentConfig based app pods using
        PVC with ReadWriteOnce (RWO) access mode on selected nodes

        Args:
            scenario (str): Scenario of app pods running on OCS or dedicated nodes
                (eg., 'colocated', 'dedicated')
            num_of_nodes (int): number of nodes required for running test
            num_of_fail_nodes (int): number of nodes to make unresponsive during test
            disrupt_provisioner (bool): True to disrupt the leader provisioner
                pods if not running on selected nodes, else False
            project_factory: A fixture to create new project
            multi_pvc_factory: A fixture create a set of new PVCs
            dc_pod_factory: A fixture to create deploymentconfig pods

        Returns:
            tuple: containing the params used in test cases
        """
        ocs_nodes, non_ocs_nodes = self.identify_and_add_nodes(
            scenario, num_of_nodes
        )
        test_nodes = ocs_nodes if (scenario == "colocated") else non_ocs_nodes
        logger.info(f"Using nodes {test_nodes} for running test")

        def finalizer():
            helpers.remove_label_from_worker_node(
                node_list=test_nodes, label_key="nodetype"
            )

        request.addfinalizer(finalizer)

        if len(ocs_nodes) > 4 and float(config.ENV_DATA['ocs_version']) >= 4.3:
            pod_obj = ocp.OCP(
                kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
            )
            assert pod_obj.wait_for_resource(
                condition=constants.STATUS_RUNNING,
                selector=constants.MON_APP_LABEL, resource_count=5, timeout=900
            )

        ceph_cluster = CephCluster()
        project = project_factory()

        # Select nodes for running app pods and inducing network failure later
        app_pod_nodes = self.select_nodes_for_app_pods(
            scenario, ceph_cluster, ocs_nodes, non_ocs_nodes,
            num_of_fail_nodes
        )

        # Create multiple RBD and CephFS backed PVCs with RWO accessmode
        num_of_pvcs = self.num_of_app_pods_per_node * num_of_fail_nodes
        rbd_pvcs = multi_pvc_factory(
            interface=constants.CEPHBLOCKPOOL, project=project, size=self.pvc_size,
            access_modes=[constants.ACCESS_MODE_RWO], num_of_pvc=num_of_pvcs
        )
        cephfs_pvcs = multi_pvc_factory(
            interface=constants.CEPHFILESYSTEM, project=project, size=self.pvc_size,
            access_modes=[constants.ACCESS_MODE_RWO], num_of_pvc=num_of_pvcs
        )

        # Create deploymentconfig based pods
        dc_pods = []
        # Start app-pods on selected node(s)
        for node_name in app_pod_nodes:
            logger.info(f"Starting app pods on the node {node_name}")
            helpers.label_worker_node(
                node_list=[node_name], label_key="nodetype",
                label_value="app-pod"
            )

            for num in range(self.num_of_app_pods_per_node):
                dc_pods.append(
                    dc_pod_factory(
                        interface=constants.CEPHBLOCKPOOL, pvc=rbd_pvcs.pop(0),
                        node_selector={'nodetype': 'app-pod'}
                    )
                )
                assert pod.verify_node_name(dc_pods[-1], node_name), (
                    f"Pod {dc_pods[-1].name} is not running on labeled node {node_name}"
                )
                dc_pods.append(
                    dc_pod_factory(
                        interface=constants.CEPHFILESYSTEM, pvc=cephfs_pvcs.pop(0),
                        node_selector={'nodetype': 'app-pod'}
                    )
                )
                assert pod.verify_node_name(dc_pods[-1], node_name), (
                    f"Pod {dc_pods[-1].name} is not running on labeled node {node_name}"
                )
            helpers.remove_label_from_worker_node(
                node_list=[node_name], label_key="nodetype"
            )

        # Label other test nodes to be able to run app pods later
        helpers.label_worker_node(
            node_list=test_nodes, label_key="nodetype", label_value="app-pod"
        )

        # Get ceph mon,osd pods running on selected node if colocated scenario
        # and extra OCS nodes are present
        ceph_pods = []
        if scenario == "colocated" and len(test_nodes) > len(ceph_cluster.osds):
            pods_to_check = ceph_cluster.osds
            # Skip mon pods if mon_count is 5 as there may not be enough nodes
            # for all mons to run after multiple node failures
            if ceph_cluster.mon_count == 3:
                pods_to_check.extend(ceph_cluster.mons)
            for pod_obj in pods_to_check:
                if pod.get_pod_node(pod_obj).name in app_pod_nodes[0]:
                    ceph_pods.append(pod_obj)
            logger.info(
                f"Colocated Mon, OSD pods: {[pod_obj.name for pod_obj in ceph_pods]}"
            )

        disruptor = []
        if disrupt_provisioner:
            disruptor = self.disrupt_plugin_provisioner_pods(app_pod_nodes)

        return ceph_cluster, dc_pods, ceph_pods, app_pod_nodes, test_nodes, disruptor

    @pytest.fixture()
    def teardown(self, request, nodes):
        """
        Make sure all nodes are up again
        Make sure that all cluster's nodes are in 'Ready' state and if not,
        change them back to 'Ready' state by restarting the nodes
        """
        def finalizer():
            # Start the powered off nodes
            nodes.restart_nodes_teardown()
            try:
                node.wait_for_nodes_status(status=constants.NODE_READY)
            except ResourceWrongStatusException:
                # Restart the nodes if in NotReady state
                not_ready_nodes = [
                    n for n in node.get_node_objs() if n
                    .ocp.get_resource_status(n.name) == constants.NODE_NOT_READY
                ]
                if not_ready_nodes:
                    logger.info(
                        f"Nodes in NotReady status found: {[n.name for n in not_ready_nodes]}"
                    )
                    nodes.restart_nodes(not_ready_nodes)
                    node.wait_for_nodes_status(status=constants.NODE_READY)

            # Check ceph health
            assert ceph_health_check(), "Ceph cluster health is not OK"
            logger.info("Ceph cluster health is OK")

        request.addfinalizer(finalizer)

    def identify_and_add_nodes(self, scenario, num_of_nodes):
        """
        Fetches info about the worker nodes and add nodes (if required)

        Args:
            scenario (str): Scenario of app pods running on OCS or dedicated nodes
                (eg., 'colocated', 'dedicated')
            num_of_nodes (int): number of nodes required for running test

        Returns:
            tuple: tuple containing:
                list: list of OCS nodes name
                list: list of non-OCS nodes name

        """
        nodes_to_add = 0
        initial_worker_nodes = helpers.get_worker_nodes()
        ocs_nodes = machine.get_labeled_nodes(constants.OPERATOR_NODE_LABEL)
        non_ocs_nodes = list(set(initial_worker_nodes) - set(ocs_nodes))

        if 'colocated' in scenario and len(ocs_nodes) < num_of_nodes:
            nodes_to_add = num_of_nodes - len(initial_worker_nodes)

        if 'dedicated' in scenario and len(non_ocs_nodes) < num_of_nodes:
            nodes_to_add = num_of_nodes - len(non_ocs_nodes)

        if nodes_to_add > 0:
            logger.info(f"{nodes_to_add} extra workers nodes needed")
            if config.ENV_DATA['deployment_type'] == 'ipi':
                machine_name = machine.get_machine_from_node_name(
                    random.choice(initial_worker_nodes)
                )
                machineset_name = machine.get_machineset_from_machine_name(
                    machine_name
                )
                machineset_replica_count = machine.get_replica_count(
                    machineset_name
                )
                machine.add_node(
                    machineset_name,
                    count=machineset_replica_count + nodes_to_add
                )
                logger.info("Waiting for the new node(s) to be in ready state")
                machine.wait_for_new_node_to_be_ready(machineset_name)
            else:
                # TODO: Add required num of nodes instead of skipping
                # https://github.com/red-hat-storage/ocs-ci/issues/1291
                pytest.skip("Add node not implemented for UPI, github issue #1291")

            new_worker_nodes = helpers.get_worker_nodes()
            new_nodes_added = list(set(new_worker_nodes) - set(initial_worker_nodes))
            assert len(new_nodes_added) > 0, 'Extra nodes not added in the cluster'
            non_ocs_nodes += new_nodes_added

        if 'colocated' in scenario and len(ocs_nodes) < num_of_nodes:
            logger.info('Adding OCS storage label to Non-OCS workers')
            node_obj = ocp.OCP(kind=constants.NODE)
            nodes_to_label = non_ocs_nodes[0:(num_of_nodes - len(ocs_nodes))]
            for node_name in nodes_to_label:
                node_obj.add_label(
                    resource_name=node_name, label=constants.OPERATOR_NODE_LABEL
                )
                ocs_nodes.append(node_name)
            non_ocs_nodes = list(set(non_ocs_nodes) - set(ocs_nodes))

        logger.info(f"The OCS nodes are : {ocs_nodes}")
        logger.info(f"The Non-OCS nodes are: {non_ocs_nodes}")
        return ocs_nodes, non_ocs_nodes

    def select_nodes_for_app_pods(
        self, scenario, ceph_cluster, ocs_nodes, non_ocs_nodes, num_of_nodes
    ):
        """
        Select nodes for running app pods
        Colocated scenario: Select 1 OCS node where osd and/or mon is running,
            select other nodes where mon/osd are not running
        Dedicated scenario: Select non-OCS nodes

        Args:
            scenario (str): Scenario of app pods running on OCS or dedicated nodes
                (eg., 'colocated', 'dedicated')
            ceph_cluster (obj): CephCluster object
            ocs_nodes (list): list of OCS nodes name
            non_ocs_nodes (list): list of non-OCS nodes name
            num_of_nodes (int): number of nodes to be selected

        Returns:
            list: list of selected nodes name for running app pods
        """
        selected_nodes = []
        if scenario == "colocated":
            logger.info(f"Selecting {num_of_nodes} OCS node from {ocs_nodes}")
            if len(ocs_nodes) == 3:
                selected_nodes.append(random.choice(ocs_nodes))
            else:
                az_count = get_az_count()
                logger.info(f"AZ count: {az_count}")
                if az_count == 1:
                    label_to_search = 'topology.rook.io/rack'
                else:
                    label_to_search = 'failure-domain.beta.kubernetes.io/zone'

                mon_pod_nodes = [
                    pod.get_pod_node(pod_obj).name for pod_obj in ceph_cluster.mons
                ]
                logger.info(f"Mon pods are running on {mon_pod_nodes}")
                osd_pod_nodes = [
                    pod.get_pod_node(pod_obj).name for pod_obj in ceph_cluster.osds
                ]
                logger.info(f"OSD pods are running on {osd_pod_nodes}")

                # Nodes having both mon and osd pods
                ceph_pod_nodes = list(set(mon_pod_nodes) & set(osd_pod_nodes))

                fd_worker_nodes = {}
                nodes_objs = node.get_node_objs(ocs_nodes)
                for wnode in nodes_objs:
                    fd = wnode.get().get('metadata').get('labels').get(label_to_search)
                    fd_node_list = fd_worker_nodes.get(fd, [])
                    fd_node_list.append(wnode.name)
                    fd_worker_nodes[fd] = fd_node_list

                fd_sorted = sorted(
                    fd_worker_nodes, key=lambda k: len(fd_worker_nodes[k]),
                    reverse=True
                )

                worker_nodes = fd_worker_nodes.get(fd_sorted[0])
                logger.info(
                    f"Selecting 1 OCS node where OSD and/or Mon are running from {worker_nodes}"
                )
                common_nodes = list(set(worker_nodes) & set(ceph_pod_nodes))
                if len(common_nodes) == 0:
                    common_nodes = list(set(worker_nodes) & set(osd_pod_nodes))
                selected_nodes.append(random.choice(common_nodes))

                logger.info(f"Selected 1 OCS node {selected_nodes}")

                if num_of_nodes > 1:
                    available_nodes = list()
                    for fd in fd_sorted:
                        worker_nodes = fd_worker_nodes.get(fd)
                        # Remove already selected node and 1 extra node for later
                        # osd pod to move over that node
                        if selected_nodes[0] in worker_nodes:
                            worker_nodes.remove(selected_nodes[0])
                            worker_nodes = worker_nodes[1:]
                        available_nodes += worker_nodes

                    logger.info(f"Selecting {num_of_nodes - 1} OCS node from {available_nodes}")
                    preferred_nodes = list(set(available_nodes) - set(osd_pod_nodes))
                    if len(preferred_nodes) < (num_of_nodes - 1):
                        preferred_nodes += list(set(available_nodes) - set(preferred_nodes))

                    selected_nodes += preferred_nodes[0:num_of_nodes - 1]
                    logger.info(f"Selected {num_of_nodes - 1} OCS node {selected_nodes[1:]}")

        else:
            logger.info(f"Selecting {num_of_nodes} non-OCS node from {non_ocs_nodes}")
            selected_nodes += non_ocs_nodes[0:num_of_nodes]

        logger.info(
            f"Selected nodes for running app pods: {selected_nodes}"
        )
        return selected_nodes

    def run_and_verify_io(
        self, pod_list, fio_filename='io_file', return_md5sum=True,
        run_io_in_bg=False
    ):
        """
        Start IO on the pods and verify IO results
        Calculates md5sum of the io files which can be used to verify data
            integrity later

        Args:
            pod_list (list): list of pod objects to run ios
            fio_filename (str): name of the file for fio
            return_md5sum (bool): True if md5sum of fio file to be calculated,
                else False
            run_io_in_bg (bool): True if more background ios to be run, else False

        Returns:
            list: list of md5sum values for the fio file if return_md5sum is
                True
        """
        # Start IO on the pods
        logger.info(f"Starting IO on {len(pod_list)} app pods")
        with ThreadPoolExecutor(max_workers=4) as executor:
            for pod_obj in pod_list:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                executor.submit(
                    pod_obj.run_io, storage_type='fs', size='1G', runtime=30,
                    fio_filename=fio_filename
                )
        logger.info(f"IO started on all {len(pod_list)} app pods")

        # Verify IO results
        for pod_obj in pod_list:
            pod.get_fio_rw_iops(pod_obj)

        if run_io_in_bg:
            logger.info(
                f"Starting IO in background on {len(pod_list)} app pods"
            )
            for pod_obj in pod_list:
                logger.info(f"Starting IO on pod {pod_obj.name}")
                pod_obj.run_io(
                    storage_type='fs', size='100M', runtime=600,
                    fio_filename='bg_io_file'
                )
            logger.info(
                f"IO started in background on all {len(pod_list)} app pods"
            )

        # Calculate md5sum of io files
        md5sum_data = []
        if return_md5sum:
            with ThreadPoolExecutor() as executor:
                for pod_obj in pod_list:
                    md5sum_data.append(
                        executor.submit(pod.cal_md5sum, pod_obj, fio_filename)
                    )
            md5sum_data = [future_obj.result() for future_obj in md5sum_data]

        return md5sum_data

    def disrupt_plugin_provisioner_pods(self, node_list):
        """
        Set leader plugin-provisioner resources for disruption, skip if running
        on node from the node_list

        Args:
            node_list (list): list of node names to check

        Returns:
            list: list of Disruption objects
        """
        provisioner_resource = []
        for interface in [constants.CEPHBLOCKPOOL, constants.CEPHFILESYSTEM]:
            provisioner_pod = pod.plugin_provisioner_leader(interface=interface)
            node_name = pod.get_pod_node(provisioner_pod).name
            if node_name not in node_list:
                if interface == constants.CEPHBLOCKPOOL:
                    provisioner_resource.append('rbdplugin_provisioner')
                else:
                    provisioner_resource.append('cephfsplugin_provisioner')

        disruptor = []
        for resource in provisioner_resource:
            disruption = disruption_helpers.Disruptions()
            disruption.set_resource(resource=resource)
            disruptor.append(disruption)

        return disruptor

    def get_new_pods(self, pod_list):
        """
        Fetches info about the respun pods in the cluster

        Args:
            pod_list (list): list of previous pod objects

        Returns:
            list : list of respun pod objects
        """
        new_pods = []
        for pod_obj in pod_list:
            if any(str in pod_obj.name for str in ['mon', 'osd']):
                pod_label = pod_obj.labels.get('pod-template-hash')
                label_selector = f'pod-template-hash={pod_label}'
            else:
                pod_label = pod_obj.labels.get('deploymentconfig')
                label_selector = f'deploymentconfig={pod_label}'

            pods_data = pod.get_pods_having_label(
                label_selector, pod_obj.namespace
            )
            for pod_data in pods_data:
                pod_name = pod_data.get('metadata').get('name')
                if '-deploy' not in pod_name and pod_name not in pod_obj.name:
                    new_pods.append(
                        pod.get_pod_obj(pod_name, pod_obj.namespace)
                    )
        logger.info(
            f"Previous pods: {[pod_obj.name for pod_obj in pod_list]}"
        )
        logger.info(
            f"Respun pods: {[pod_obj.name for pod_obj in new_pods]}"
        )
        return new_pods

    @retry(UnexpectedBehaviour, tries=10, delay=10, backoff=1)
    def verify_multi_attach_error(self, pod_list):
        """
        Checks for the expected failure event message in oc describe command

        Args:
            pod_list (list): list of pod objects

        Returns:
            bool: True if Multi-Attach Error is found in oc describe

        Raises:
            UnexpectedBehaviour: If Multi-Attach Error not found in describe command
        """
        failure_str = 'Multi-Attach error for volume'
        for pod_obj in pod_list:
            if failure_str in pod_obj.describe():
                logger.info(
                    f"Multi-Attach error is present in oc describe of {pod_obj.name}"
                )
            else:
                logger.warning(
                    f"Multi-Attach error is not found in oc describe of {pod_obj.name}"
                )
                raise UnexpectedBehaviour(pod_obj.name, pod_obj.describe())

        return True

    @tier4a
    @pytest.mark.parametrize(
        argnames=[
            "scenario", "num_of_nodes", "num_of_fail_nodes",
            "disrupt_provisioner"
        ],
        argvalues=[
            pytest.param(
                *['colocated', 3, 1, False],
                marks=[pytest.mark.polarion_id("OCS-1423"), ISSUE_SKIP]
            ),
            pytest.param(
                *['dedicated', 2, 1, False],
                marks=pytest.mark.polarion_id("OCS-1428")
            ),
            pytest.param(
                *['dedicated', 4, 3, True],
                marks=pytest.mark.polarion_id("OCS-1434")
            ),
            pytest.param(
                *['colocated', 4, 1, False],
                marks=[pytest.mark.polarion_id("OCS-1426"), ISSUE_SKIP]
            ),
            pytest.param(
                *['colocated', 5, 3, True],
                marks=[pytest.mark.polarion_id("OCS-1424"), ISSUE_SKIP]
            )
        ]
    )
    def test_rwo_pvc_fencing_node_short_network_failure(
        self, nodes, setup, teardown
    ):
        """
        OCS-1423/OCS-1428/OCS-1426:
        - Start DeploymentConfig based app pods on 1 OCS/Non-OCS node
        - Make the node (where app pods are running) unresponsive
            by bringing its main network interface down
        - Check new app pods and/or mon, osd pods scheduled on another node
            are stuck due to Multi-Attach error.
        - Reboot the unresponsive node
        - When unresponsive node recovers, run IOs on new app pods

        OCS-1424/OCS-1434:
        - Start DeploymentConfig based app pods on multiple node
            Colocated scenario: Select 1 node where osd and/or mon is running,
                select other 2 nodes where mon/osd are not running
            Dedicated scenario: 3 Non-OCS nodes
        - Disrupt the leader provisioner pods if not running on above selected
            nodes
        - Make the nodes (where app pods are running) unresponsive
            by bringing their main network interface down
        - Check new app pods and/or mon, osd pods scheduled on another node and
            are stuck due to Multi-Attach error.
        - Reboot the unresponsive nodes
        - When unresponsive nodes recover, run IOs on new app pods
        """
        ceph_cluster, dc_pods, ceph_pods, app_pod_nodes, test_nodes, disruptor = setup

        # Run IO on pods
        md5sum_data = self.run_and_verify_io(
            pod_list=dc_pods, fio_filename='io_file1', run_io_in_bg=True
        )

        # OCS-1424/OCS-1434
        # Disrupt leader plugin-provisioner pods, skip if running on node to be failed
        if disruptor:
            [disruption.delete_resource() for disruption in disruptor]

        # Induce network failure on the nodes
        node.node_network_failure(app_pod_nodes)
        logger.info(f"Waiting for {self.short_nw_fail_time} seconds")
        sleep(self.short_nw_fail_time)

        # Wait for pods to be rescheduled
        for pod_obj in (dc_pods + ceph_pods):
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_TERMINATING,
                resource_name=pod_obj.name, timeout=600, sleep=30
            )

        # Fetch info of new pods and verify Multi-Attach error
        new_dc_pods = self.get_new_pods(dc_pods)
        assert len(new_dc_pods) == len(dc_pods), 'Unexpected number of app pods'
        self.verify_multi_attach_error(new_dc_pods)

        if ceph_pods:
            new_ceph_pods = self.get_new_pods(ceph_pods)
            assert len(new_ceph_pods) > 0, 'Unexpected number of osd pods'
            self.verify_multi_attach_error(new_ceph_pods)

        # Reboot the unresponsive node(s)
        logger.info(f"Rebooting the unresponsive node(s): {app_pod_nodes}")
        nodes.restart_nodes(node.get_node_objs(app_pod_nodes))
        node.wait_for_nodes_status(
            node_names=app_pod_nodes, status=constants.NODE_READY
        )

        # Wait for new app pods to reach Running state
        for pod_obj in new_dc_pods:
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING, resource_name=pod_obj.name,
                timeout=1200, sleep=30
            ), (
                f"App pod with name {pod_obj.name} did not reach Running state"
            )

        # Wait for mon and osd pods to reach Running state
        selectors_to_check = {
            constants.MON_APP_LABEL: ceph_cluster.mon_count,
            constants.OSD_APP_LABEL: ceph_cluster.osd_count
        }
        for selector, count in selectors_to_check.items():
            assert ceph_cluster.POD.wait_for_resource(
                condition=constants.STATUS_RUNNING, selector=selector,
                resource_count=count, timeout=1800, sleep=60
            ), (
                f"{count} expected pods with selector {selector} are not in Running state"
            )

        assert ceph_health_check(), "Ceph cluster health is not OK"
        logger.info("Ceph cluster health is OK")

        # Verify data integrity from new pods
        for num, pod_obj in enumerate(new_dc_pods):
            assert pod.verify_data_integrity(
                pod_obj=pod_obj, file_name='io_file1',
                original_md5sum=md5sum_data[num]
            ), 'Data integrity check failed'

        # Run IO on new pods
        self.run_and_verify_io(
            pod_list=new_dc_pods, fio_filename='io_file2', return_md5sum=False
        )

    @tier4b
    @pytest.mark.parametrize(
        argnames=[
            "scenario", "num_of_nodes", "num_of_fail_nodes",
            "disrupt_provisioner"
        ],
        argvalues=[
            pytest.param(
                *['dedicated', 2, 1, False],
                marks=pytest.mark.polarion_id("OCS-1429")
            ),
            pytest.param(
                *['dedicated', 4, 3, True],
                marks=pytest.mark.polarion_id("OCS-1435")
            ),
            pytest.param(
                *['colocated', 4, 1, False],
                marks=[pytest.mark.polarion_id("OCS-1427"), ISSUE_SKIP]
            ),
            pytest.param(
                *['colocated', 6, 3, True],
                marks=[pytest.mark.polarion_id("OCS-1430"), ISSUE_SKIP]
            )
        ]
    )
    def test_rwo_pvc_fencing_node_prolonged_network_failure(
        self, nodes, setup, teardown
    ):
        """
        OCS-1427/OCS-1429:
        - Start DeploymentConfig based app pods on 1 OCS/Non-OCS node
        - Make the node (where app pods are running) unresponsive
            by bringing its main network interface down
        - Check new app pods and/or mon, osd pods scheduled on another node
            are stuck due to Multi-Attach error.
        - Power off the unresponsive node
        - Force delete the app pods and/or mon,osd pods on the unresponsive node
        - Check new app pods and/or mon, osd pods scheduled on another node comes
            into Running state
        - Run IOs on new app pods

        OCS-1430/OCS-1435:
        - Start DeploymentConfig based app pods on multiple node
            Colocated scenario: Select 1 node where osd and/or mon is running,
                select other 2 nodes where mon/osd are not running
            Dedicated scenario: 3 Non-OCS nodes
        - Disrupt the leader provisioner pods if not running on above selected
            nodes
        - Make the nodes (where app pods are running) unresponsive
            by bringing their main network interface down
        - Check new app pods and/or mon, osd pods scheduled on another node
            are stuck due to Multi-Attach error.
        - Power off the unresponsive nodes
        - Force delete the app pods and/or mon,osd pods on the unresponsive node
        - Check new app pods and/or mon, osd pods scheduled on another node comes
            into Running state
        - Run IOs on new app pods
        """
        ceph_cluster, dc_pods, ceph_pods, app_pod_nodes, test_nodes, disruptor = setup

        # Run IO on pods
        md5sum_data = self.run_and_verify_io(
            pod_list=dc_pods, fio_filename='io_file1', run_io_in_bg=True
        )

        # OCS-1430/OCS-1435
        # Disrupt leader plugin-provisioner pods, skip if running on node to be failed
        if disruptor:
            [disruption.delete_resource() for disruption in disruptor]

        # Induce network failure on the nodes
        node.node_network_failure(app_pod_nodes)
        logger.info(f"Waiting for {self.prolong_nw_fail_time} seconds")
        sleep(self.prolong_nw_fail_time)

        # Wait for pods to be rescheduled
        for pod_obj in (dc_pods + ceph_pods):
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_TERMINATING,
                resource_name=pod_obj.name
            )

        # Fetch info of new pods and verify Multi-Attach error
        new_dc_pods = self.get_new_pods(dc_pods)
        assert len(new_dc_pods) == len(dc_pods), 'Unexpected number of app pods'
        self.verify_multi_attach_error(new_dc_pods)

        if ceph_pods:
            new_ceph_pods = self.get_new_pods(ceph_pods)
            assert len(new_ceph_pods) > 0, 'Unexpected number of osd pods'
            self.verify_multi_attach_error(new_ceph_pods)

        logger.info("Executing manual recovery steps")
        # Power off the unresponsive node(s)
        logger.info(
            f"Powering off the unresponsive node(s): {app_pod_nodes}"
        )
        nodes.stop_nodes(node.get_node_objs(app_pod_nodes))

        # Force delete the app pods and/or mon,osd pods on the unresponsive node
        if ceph_cluster.mon_count == 5:
            for pod_obj in ceph_cluster.mons:
                if pod.get_pod_node(pod_obj).name in app_pod_nodes:
                    ceph_pods.append(pod_obj)

        for pod_obj in (dc_pods + ceph_pods):
            pod_obj.delete(force=True)

        # Wait for new app pods to reach Running state
        for pod_obj in new_dc_pods:
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING, resource_name=pod_obj.name,
                timeout=1200, sleep=30
            ), (
                f"App pod with name {pod_obj.name} did not reach Running state"
            )

        # Wait for mon and osd pods to reach Running state
        selectors_to_check = [constants.MON_APP_LABEL, constants.OSD_APP_LABEL]
        for selector in selectors_to_check:
            assert ceph_cluster.POD.wait_for_resource(
                condition=constants.STATUS_RUNNING, selector=selector,
                resource_count=3, timeout=1800, sleep=60
            ), (
                f"3 expected pods with selector {selector} are not in Running state"
            )

        if ceph_cluster.mon_count == 3:
            # Check ceph health
            toolbox_status = ceph_cluster.POD.get_resource_status(
                ceph_cluster.toolbox.name
            )
            if toolbox_status == constants.STATUS_TERMINATING:
                ceph_cluster.toolbox.delete(force=True)

            assert ceph_health_check(), "Ceph cluster health is not OK"
            logger.info("Ceph cluster health is OK")

        # Verify data integrity from new pods
        for num, pod_obj in enumerate(new_dc_pods):
            assert pod.verify_data_integrity(
                pod_obj=pod_obj, file_name='io_file1',
                original_md5sum=md5sum_data[num]
            ), 'Data integrity check failed'

        # Run IO on new pods
        self.run_and_verify_io(
            pod_list=new_dc_pods, fio_filename='io_file2', return_md5sum=False
        )

    @tier4c
    @pytest.mark.parametrize(
        argnames=[
            "scenario", "num_of_nodes", "num_of_fail_nodes",
            "disrupt_provisioner"
        ],
        argvalues=[
            pytest.param(
                *['dedicated', 3, 1, True],
                marks=pytest.mark.polarion_id("OCS-1436")
            ),
            pytest.param(
                *['colocated', 4, 1, True],
                marks=[pytest.mark.polarion_id("OCS-1431"), ISSUE_SKIP]
            )
        ]
    )
    def test_rwo_pvc_fencing_node_prolonged_and_short_network_failure(
        self, nodes, setup, teardown
    ):
        """
        OCS-1431/OCS-1436:
        - Start DeploymentConfig based app pods on 1 node
        - Make the node (where app pods are running) unresponsive
            by bringing its main network interface down
        - Disrupt the leader provisioner pods if not running on above selected
            node
        - Check new app pods and/or mon, osd pods scheduled on another node
            are stuck due to Multi-Attach error.
        - Power off the unresponsive node
        - Force delete the app pods and/or mon,osd pods on the unresponsive node
        - Check new app pods and/or mon, osd pods scheduled on another node comes
            into Running state
        - Run IOs on new app pods
        - Again make the node (where app pods are running) unresponsive
            by bringing its main network interface down
        - Check new app pods scheduled on another node are stuck due to
            Multi-Attach error.
        - Reboot the unresponsive node
        - When unresponsive node recovers, run IOs on new app pods
        """
        ceph_cluster, dc_pods, ceph_pods, app_pod_nodes, test_nodes, disruptor = setup

        extra_nodes = list(set(test_nodes) - set(app_pod_nodes))
        helpers.remove_label_from_worker_node(
            node_list=extra_nodes[:-1], label_key="nodetype"
        )

        # Run IO on pods
        md5sum_data = self.run_and_verify_io(
            pod_list=dc_pods, fio_filename='io_file1', run_io_in_bg=True
        )

        # Disrupt leader plugin-provisioner pods, skip if running on node to be failed
        if disruptor:
            [disruption.delete_resource() for disruption in disruptor]

        # Induce network failure on the nodes
        node.node_network_failure(app_pod_nodes)
        logger.info(f"Waiting for {self.prolong_nw_fail_time} seconds")
        sleep(self.prolong_nw_fail_time)

        # Wait for pods to be rescheduled
        for pod_obj in (dc_pods + ceph_pods):
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_TERMINATING,
                resource_name=pod_obj.name
            )

        # Fetch info of new pods and verify Multi-Attach error
        new_dc_pods = self.get_new_pods(dc_pods)
        assert len(new_dc_pods) == len(dc_pods), 'Unexpected number of app pods'
        self.verify_multi_attach_error(new_dc_pods)

        new_ceph_pods = []
        if ceph_pods:
            new_ceph_pods = self.get_new_pods(ceph_pods)
            assert len(new_ceph_pods) > 0, 'Unexpected number of osd pods'
            self.verify_multi_attach_error(new_ceph_pods)

        logger.info("Executing manual recovery steps")
        # Power off the unresponsive node
        logger.info(
            f"Powering off the unresponsive node: {app_pod_nodes}"
        )
        nodes.stop_nodes(node.get_node_objs(app_pod_nodes))

        # Force delete the app pods and/or mon,osd pods on the unresponsive node
        for pod_obj in (dc_pods + ceph_pods):
            pod_obj.delete(force=True)

        # Wait for new app pods to reach Running state
        for pod_obj in new_dc_pods:
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING, resource_name=pod_obj.name,
                timeout=1200, sleep=30
            ), (
                f"App pod with name {pod_obj.name} did not reach Running state"
            )

        # Wait for mon and osd pods to reach Running state
        selectors_to_check = [constants.MON_APP_LABEL, constants.OSD_APP_LABEL]
        for selector in selectors_to_check:
            assert ceph_cluster.POD.wait_for_resource(
                condition=constants.STATUS_RUNNING, selector=selector,
                resource_count=3, timeout=1800, sleep=60
            ), (
                f"3 expected pods with selector {selector} are not in Running state"
            )

        if ceph_cluster.mon_count == 3:
            # Check ceph health
            toolbox_status = ceph_cluster.POD.get_resource_status(
                ceph_cluster.toolbox.name
            )
            if toolbox_status == constants.STATUS_TERMINATING:
                ceph_cluster.toolbox.delete(force=True)

            assert ceph_health_check(), "Ceph cluster health is not OK"
            logger.info("Ceph cluster health is OK")

        # Verify data integrity from new pods
        for num, pod_obj in enumerate(new_dc_pods):
            assert pod.verify_data_integrity(
                pod_obj=pod_obj, file_name='io_file1',
                original_md5sum=md5sum_data[num]
            ), 'Data integrity check failed'

        # Run IO on new pods
        md5sum_data2 = self.run_and_verify_io(
            pod_list=new_dc_pods, fio_filename='io_file2', run_io_in_bg=True
        )

        helpers.label_worker_node(
            node_list=extra_nodes[:-1], label_key="nodetype", label_value="app-pod"
        )

        # Induce network failure on the node
        node.node_network_failure(extra_nodes[-1])
        logger.info(f"Waiting for {self.short_nw_fail_time} seconds")
        sleep(self.short_nw_fail_time)

        # Wait for pods to be rescheduled
        for pod_obj in new_dc_pods:
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_TERMINATING,
                resource_name=pod_obj.name, timeout=600, sleep=30
            )

        # Fetch info of new pods and verify Multi-Attach error
        new_dc_pods2 = self.get_new_pods(new_dc_pods)
        assert len(new_dc_pods2) == len(new_dc_pods), 'Unexpected number of app pods'
        self.verify_multi_attach_error(new_dc_pods2)

        # Reboot the unresponsive node
        logger.info(f"Rebooting the unresponsive node: {extra_nodes[-1]}")
        nodes.restart_nodes(node.get_node_objs([extra_nodes[-1]]))
        node.wait_for_nodes_status(
            node_names=[extra_nodes[-1]], status=constants.NODE_READY
        )

        # Wait for new app pods to reach Running state
        for pod_obj in new_dc_pods2:
            pod_obj.ocp.wait_for_resource(
                condition=constants.STATUS_RUNNING, resource_name=pod_obj.name,
                timeout=1200, sleep=30
            ), (
                f"App pod with name {pod_obj.name} did not reach Running state"
            )

        # Wait for mon and osd pods to reach Running state
        for selector in selectors_to_check:
            assert ceph_cluster.POD.wait_for_resource(
                condition=constants.STATUS_RUNNING, selector=selector,
                resource_count=3, timeout=1800, sleep=60
            ), (
                f"3 expected pods with selector {selector} are not in Running state"
            )

        if ceph_cluster.mon_count == 3:
            # Check ceph health
            assert ceph_health_check(), "Ceph cluster health is not OK"
            logger.info("Ceph cluster health is OK")

        # Verify data integrity from new pods
        for num, pod_obj in enumerate(new_dc_pods2):
            assert pod.verify_data_integrity(
                pod_obj=pod_obj, file_name='io_file2',
                original_md5sum=md5sum_data2[num]
            ), 'Data integrity check for files written before second node failures failed'

        for num, pod_obj in enumerate(new_dc_pods2):
            assert pod.verify_data_integrity(
                pod_obj=pod_obj, file_name='io_file1',
                original_md5sum=md5sum_data[num]
            ), 'Data integrity check for files written before first node failures failed'

        # Run IO on new pods
        self.run_and_verify_io(
            pod_list=new_dc_pods2, fio_filename='io_file3', return_md5sum=False
        )
