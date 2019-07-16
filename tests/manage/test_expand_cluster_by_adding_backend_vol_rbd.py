import os
import pytest
import logging
from time import sleep
import json

from ocs_ci.ocs import constants, defaults, cluster
from ocs_ci.utility.templating import load_yaml_to_dict
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.resources import pvc
from ocs_ci.framework import config
from ocs_ci.framework.testlib import ManageTest, tier1
from ocs_ci.utility.aws import AWS
from tests import helpers
from ocs_ci.ocs.exceptions import TimeoutExpiredError


logger = logging.getLogger(__name__)


def setup(self):
    """
    This function just performs the following pre-requisite steps of this test.
    1) Create secret
    2) Create storageclass
    3) Create a namespace specific to this test
    4) Check the status of all the ocs pods - should be either in Running or Completed state
    5) Check overall health of the ceph cluster
    6) Create PVCs
    7) Create Pods
    :
    :
    """

    # Create the secret
    self.secret = helpers.create_secret(constants.CEPHBLOCKPOOL)

    # Create storage class
    self.sc = helpers.create_storage_class(constants.CEPHBLOCKPOOL, 'rbd', self.secret.name)

    # TBD: create a namespace dedicated for app pods
    # It's a good practice to run the app-pods in a project/namespace other than the openshift-storage namespace.

    # Check status of all the ocs pods in openshift-storage namespace.
    # They should be either in Running or Completed state
    assert check_ocs_pods_status()

    # Determine the health of the ceph cluster. If unhealthy do not proceed with testing.
    # Note: Unhealthy = HEALTH_ERR, HEALTH_WARN
    assert (cluster.CephCluster()).cluster_health_check()

    # Create the rbd based PVCs so that app pods can run on them
    number_of_pvcs_to_create = 2  # TBD: change this to parameterized format later. Accept it from cmdline
    pvc_data = load_yaml_to_dict(constants.CSI_RBD_PVC_YAML)
    pvc_data['spec']['storageClassName'] = self.sc.name

    self.pvcs = pvc.create_multiple_pvc(number_of_pvcs_to_create, **pvc_data)

    # Create the app pods on these PVCs and Run fio based IOs from them
    self.pods = []

    # change the following loop contents to use new way of creating pods. get new pod.py
    number_of_pods_to_create = 2  # TBD: change this to parameterized format later. Accept it from cmdline
    for i in range(number_of_pods_to_create):
        self.pods.append(helpers.create_pod(
                                                constants.CEPHBLOCKPOOL,
                                                self.pvcs[i].name,
                                                constants.STATUS_RUNNING,
                                                True
                                        ))


def check_ocs_pods_status():
    """
    This function checks the status of all the pods in the openshift-storage namespace. All pods should be either
    in 'Running' or 'Completed' state.

    Returns :
        pod_health: boolean value. True if all the pods are in either 'Running' or 'Completed/Succeeded' state.
                    False, otherwise

    """
    list_of_pods = pod.get_all_pods(defaults.ROOK_CLUSTER_NAMESPACE)
    pod_health = True
    for p in list_of_pods:
        pod_dict = p.get()
        if pod_dict['status']['phase'] not in constants.STATUS_RUNNING:
            if pod_dict['status']['phase'] not in constants.STATUS_SUCCEEDED:
                pod_health = False
                logging.error(f"pod {p.name} in Unhealthy")
    return pod_health


def get_pod_restarts_count():
    """
    Gets the dictionary of pod and its restart count for all the ocs pods

    Returns:
        restart_dict: Dictionary of pod name and its corresponding restart count

    """
    list_of_pods = pod.get_all_pods(defaults.ROOK_CLUSTER_NAMESPACE)
    restart_dict = {}
    for p in list_of_pods:
        pod_dict = p.get()
        # we don't want to compare osd-prepare pod as it gets created freshly when an osd need to be added.
        if "rook-ceph-osd-prepare" not in p.name:
            restart_dict[p.name] = pod_dict['status']['containerStatuses'][0]['restartCount']

    return restart_dict


def add_osd_aws(
    number_of_osds,
    original_osd_count,
    size=100
):
    """
    Create volumes on workers

    Args:
        number_of_osds (int): number of osds to be added
        original_osd_count (int): number of osds that were present before osd addition
        size (int): Size in GB (default: 100)
    """
    osds_added = 0
    cluster_path = config.ENV_DATA['cluster_path']

    with open(os.path.join(cluster_path, "terraform.tfvars")) as f:
        tfvars = json.load(f)

    cluster_id = tfvars['cluster_id']
    worker_pattern = f'{cluster_id}-worker*'
    logger.info(f'Worker pattern: {worker_pattern}')

    # aws = AWS(default.AWS_REGION)
    aws = AWS(config.ENV_DATA['region'])

    """
    per https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html, the available devices names for
    HVM based instances are from /dev/sdf to /dev/sdp.
    """
    device_letters_allowed = []
    for c in range(ord('f'), ord('p') + 1):
        device_letters_allowed.append(chr(c))

    #  while osds_added != number_of_osds:
    #      worker_instances = aws.get_instances_by_name_pattern(worker_pattern)

    worker_instances = aws.get_instances_by_name_pattern(worker_pattern)
    for worker in worker_instances:
        logging.info(worker)

        # following is a new fuction added in aws.py as part of this script.
        vols = aws.get_volumes_attached_to_instance(worker['id'])
        """
        Following lines of code dynamically determine which device should be used.
        a) get the list of allowed device letters
        b) get the list of already 'in-use' device letters
        c) check if a letter from allowed device letters is not used already. 
          c.1) If yes, use that device letter to form the device name.
          c.2) If no, then we have used all the allowed device letters and hence cannot add more devices
        """
        device_letters_used = []
        device_name = 'None'
        for i in range(len(vols)):
            device_letters_used.append((vols[i]['attachments'][0]['Device'])[-1])
            logger.info(device_letters_used)
        for d in device_letters_allowed:
            if d not in device_letters_used:
                logger.info("Available drive letter : "+str(d))
                device_name = '/dev/sd'+str(d)
                break

        assert device_name != 'None', (
            'All the allowed device letters are already in use and hence cannot add more devices'
        )
        logger.info("I will use device: "+device_name)

        logger.info(f"Creating and attaching {size} GB volume to {worker['name']}")
        aws.create_volume_and_attach(
            availability_zone=worker['avz'],
            instance_id=worker['id'],
            name=f"{worker['name']}_extra_volume",
            size=size,
            timeout=150,
            # device='/dev/sdf',
            device=device_name,
        )
        time_out = 1
        pod_obj = pod.get_ceph_tools_pod()
        #  Wait for 10 mins in this loop to see if all osds including the newly added are in 'up' and 'in' state and
        #  cluster is in healthy state
        for i in range(60):
            cmdoutput = pod_obj.exec_ceph_cmd('ceph -s')
            up_osds = cmdoutput['osdmap']['osdmap']['num_up_osds']
            in_osds = cmdoutput['osdmap']['osdmap']['num_in_osds']
            pg_state = cmdoutput['pgmap']['pgs_by_state'][0]['state_name']
            if up_osds == original_osd_count + osds_added + 1 and \
                    in_osds == original_osd_count + osds_added + 1 and \
                    pg_state == 'active+clean':
                osds_added += 1
                time_out = 0
                logger.info("All osds including newly added are in 'up' and 'in' state and cluster is healthy")
                break
            else:
                logger.info("Waiting for all osds including newly added to be in 'up' and 'in'. "
                            "Waiting for 10 more seconds.....")
                sleep(10)

        if time_out:
            raise TimeoutExpiredError(
                "Timeout exceeded waiting for all the osds including the newly added to be 'up' and 'in' state"
            )

        logger.info("osd Addition status - Added : "+str(osds_added) + ". Requested: "+str(number_of_osds))
        if osds_added == number_of_osds:
            logger.info("Requested number of osds are now added to the system")
            return


def check_pod_restarts_count(before, after):
    """
    Compares the restart count of pods before and after adding backend volume. If restarted then returns True
    Args:
        before (dict): Dictionary of pods and restart count for each pod
        after (dict): Dictionary of pods and restart count for each pod

    Returns:
        pod_restarted: boolean. True, if restart count changed. False, otherwise.
    """
    pod_restarted = False

    for p in before:
        if before[p] != after[p]:
            pod_restarted = True
            logging.error(f"The pod {p} has restarted. Restart count = {after[p]}")
    return pod_restarted


def check_osd_pods_added(before, after, expected_osd_pod_count):
    """
    Checks if osd pod corresponding to the newly added osd is shown in oc get pod output or not.

    Args:
        before (list): list of osd pods that were existing before the addition
        after (list): list of  names after the osd was added
        expected_osd_pod_count (int): number of expected osd pod count

    Returns:
        True, if the expected_osd_pod_count matches with the actual_osd_count observed.
    """
    actual_pods_added = 0

    for pod_after in after:
        found = False
        for pod_before in before:
            logging.info(f"pod after = {pod_after.name}, pod before = {pod_before.name}")
            if pod_after.name in pod_before.name:
                found = True
                break
        if not found:  # we found the newly added osd pod
            pod_status = pod_after.get()
            if (pod_status['status']['containerStatuses'][0]['restartCount'] == 0 and
                    pod_status['status']['phase'] == constants.STATUS_RUNNING):
                logging.info(f"Found the newly added pod {pod_after.name} and is running successfully")
                actual_pods_added += 1
    if expected_osd_pod_count == actual_pods_added:
        return True
    else:
        logging.error(f"Expected to find {expected_osd_pod_count} osd pods but found {actual_pods_added}")
        return False


@pytest.fixture(scope='class')
def test_fixture(request):
    self = request.node.cls

    def finalizer():
        tear_down(self)
    request.addfinalizer(finalizer)
    setup(self)


def tear_down(self):

    # Delete app pods
    for p in self.pods:
        assert p.delete()

    # Delete the PVCs
    assert pvc.delete_pvcs(self.pvcs)

    # TBD: Delete app-pods namespace. Waiting for the patch

    # Delete storage class
    helpers.delete_all_storageclass()

    # Delete secret
    self.secret.delete()

    # TBD: To remove the backend disk that was added as part of this test and its corresponding osd pod.
    #       NOTE: We do not have the rook way of removing the osd pod after its backend disk is removed.


@tier1
@pytest.mark.usefixtures(
    test_fixture.__name__,
)
class TestExpandClusterByAddingBackendVol(ManageTest):
    @pytest.mark.polarion_id("OCS-356")
    def test_expand_cluster_by_adding_backend_vol_rbd(self):
        """
        This script automates the following test case:
        https://polarion.engineering.redhat.com/polarion/#/project/OpenShiftContainerStorage/workitem?id=OCS-356
        """
        number_of_osds_to_add = 2  # TBD: change this to parameterized format later. Accept it from cmdline

        # Get the osd pod names, osd count and restart count for all pods before adding new osd
        osd_pods_before = pod.get_osd_pods()
        original_osd_count = len(osd_pods_before)
        pod_restart_count_before = get_pod_restarts_count()

        # Run fio on the pods created
        for p in self.pods:
            # Run fio with data integrity check enabled for 600 seconds.
            p.run_io('fs', '50M', 'wv', 75, 1, 900)

        logger.info("Allowing IOs to continue for few minutes. Sleeping....")
        sleep(60)  # Allow IOs to soak for at least 1-2 minutes before we add new disk.
        logger.info("Woke up from sleep. Let me proceed with next steps")

        # Add the osd at the backend
        add_osd_aws(number_of_osds_to_add, original_osd_count)

        # Sleep for some time to allow IOs to soak after addition of osd.
        sleep(60)

        # Check if ceph -s shows the new osd count.
        cmdoutput = (pod.get_ceph_tools_pod()).exec_ceph_cmd('ceph -s')
        expected_count = original_osd_count + number_of_osds_to_add
        assert expected_count == cmdoutput['osdmap']['osdmap']['num_osds'], \
            "***** Test verification failure: After the osd addition, the ceph is not reflecting the correct osd count"

        # Check if ceph -s shows all the osds as up and in
        assert expected_count == cmdoutput['osdmap']['osdmap']['num_up_osds'], \
            "***** Test verification failure: After the osd addition, the ceph is not reflecting the correct number" \
            " of osds in 'up' state "
        assert expected_count == cmdoutput['osdmap']['osdmap']['num_in_osds'], \
            "***** Test verification failure: After the osd addition, the ceph is not reflecting the correct number" \
            " of osds in 'in' state "

        # Check if all pods are in either Running or Completed state
        assert check_ocs_pods_status(), "***** Test verification failure: After the osd addition, " \
            "one or more pods are not in Running/Completed state"

        # Compare the restart count after osd is added with the one before
        assert not check_pod_restarts_count(pod_restart_count_before, get_pod_restarts_count()), "" \
            "***** Test verification failure: After the osd addition, one or more pods have restarted"

        # Check if the pod for the newly added osd is shown in 'oc get pod' output or not
        assert check_osd_pods_added(osd_pods_before, pod.get_osd_pods(), number_of_osds_to_add), \
            "***** Test verification failure: After the osd addition, the new osd pods are not shown in oc"

        # Print fio results
        for i in range(len(self.pods)):
            logging.info(f"The fio results for pod: {self.pods[i].name} : ")
            fio_result = self.pods[i].get_fio_results()
            assert not fio_result.get('jobs')[0].get('error'), f"FIO has failed on the pod: {self.pods[i].name}"

            logging.info(f"Status of FIO operation: Success")
            logging.info("IOPs after FIO:")
            logging.info(
                f"Read: {fio_result.get('jobs')[0].get('read').get('iops')}"
            )
            logging.info(
                f"Write: {fio_result.get('jobs')[0].get('write').get('iops')}"
            )
