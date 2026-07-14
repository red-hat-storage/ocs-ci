# -*- coding: utf8 -*-
"""
This is a demonstration how to start k8s Job in one test, let it running, and
then revisit it and stop in another one. Original simple draft was created by
mbukatov based on discussion with fbalak, who is expected to tweak it further
to fit into the upgrade scenario (as well as error checking adn logging).
"""

import logging
import textwrap

import pytest

from ocs_ci.framework.pytest_customization.marks import blue_squad
from ocs_ci.framework.testlib import skipif_managed_service
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import TimeoutExpiredError
from ocs_ci.ocs.resources.objectconfigfile import ObjectConfFile
from ocs_ci.utility.utils import run_cmd


logger = logging.getLogger(__name__)
TEST_NS = "namespace-test-fio-continuous-workload"


@blue_squad
@pytest.mark.libtest
@skipif_managed_service
def test_start_fio_job(
    tmp_path,
    fio_pvc_dict,
    fio_job_dict,
    fio_configmap_dict,
):
    """
    Start a fio job performing IO load, check that it's running, and keep
    it running even after the test finishes.
    """
    # creating project directly to set it's name and prevent it's deletion
    project = ocp.OCP(kind="Project", namespace=TEST_NS)
    project.new_project(TEST_NS)

    # size of the volume for fio
    pvc_size = 10  # GiB

    # test uses cephfs based volume, could be either parametrized or we can
    # try to start more jobs
    storage_class_name = "ocs-storagecluster-cephfs"

    # fio config file: random mixed read and write IO will be running for one
    # day (we expect that the other test will stop it), only 1/2 of the volume
    # is used, we don't need to utilize the PV 100%
    fio_size = int(pvc_size / 2)  # GiB
    fio_conf = textwrap.dedent(
        f"""
        [readwrite]
        readwrite=randrw
        buffered=1
        blocksize=4k
        ioengine=libaio
        directory=/mnt/target
        size={fio_size}G
        time_based
        runtime=24h
        """
    )

    # put the dicts together into yaml file of the Job
    fio_configmap_dict["data"]["workload.fio"] = fio_conf
    fio_pvc_dict["spec"]["storageClassName"] = storage_class_name
    fio_pvc_dict["spec"]["resources"]["requests"]["storage"] = f"{pvc_size}Gi"
    fio_objs = [fio_pvc_dict, fio_configmap_dict, fio_job_dict]
    job_file = ObjectConfFile("fio_continuous", fio_objs, project, tmp_path)

    # deploy the Job to the cluster and start it
    job_file.create()

    # wait for a pod for the job to be deployed and running
    ocp_pod = ocp.OCP(kind="Pod", namespace=project.namespace)
    try:
        ocp_pod.wait_for_resource(
            resource_count=1, condition=constants.STATUS_RUNNING, timeout=300, sleep=30
        )
    except TimeoutExpiredError:
        logger.error("pod for fio job wasn't deployed properly")
        raise


@blue_squad
@pytest.mark.libtest
@skipif_managed_service
def test_stop_fio_job():
    """
    Check that the job is still running.
    """
    # check that the pod of the job is still running
    ocp_pod = ocp.OCP(kind="Pod", namespace=TEST_NS)
    try:
        ocp_pod.wait_for_resource(
            resource_count=1, condition=constants.STATUS_RUNNING, timeout=300, sleep=30
        )
    except TimeoutExpiredError:
        logger.error("pod for fio job wasn't deployed properly")
        raise

    # TODO: check the activity of the job during it's whole run could via
    # prometheus query (make sure it was really running)

    # TODO: delete the project properly
    run_cmd(cmd=f"oc delete project/{TEST_NS}", timeout=600)
