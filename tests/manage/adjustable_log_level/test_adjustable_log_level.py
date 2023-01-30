import pytest
import logging

from ocs_ci.utility import loglevel_utils, utils
from ocs_ci.ocs import constants, ocp
from ocs_ci.helpers import helpers
from ocs_ci.framework.pytest_customization.marks import green_squad
from ocs_ci.ocs.resources import pod
from ocs_ci.framework.testlib import (
    skipif_ocs_version,
    ManageTest,
    tier1,
    tier3,
    skipif_ocp_version,
    skipif_managed_service,
    skipif_disconnected_cluster,
    skipif_proxy_cluster,
    ignore_leftovers,
)


log = logging.getLogger(__name__)
# Error message to look in a command output
ERRMSG = "Error in command"


@tier1
@skipif_ocs_version(">4.11")
@skipif_ocp_version(">4.11")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
@green_squad
@ignore_leftovers
class TestSidecarLoglevelUnavailable(ManageTest):
    """
    Test $CSI_SIDECAR_LOG_LEVEL is unavailable and default csi log level is, CSI_LOG_LEVEL: "5" for ODF<4.12
    """

    # @polarion_id("OCS-4669")
    def test_sidecar_loglevel_unavailable_for_previous_versions(self):
        """
        This test is to validate sidecar log level feature is unavailable in previous ODF version
        Steps:
        1:- Check $CSI_SIDECAR_LOG_LEVEL flag unavailable and not suported in previous
        ODF versions (<4.12)
        """
        operator_pod_obj = pod.get_operator_pods()
        rook_ceph_operator = operator_pod_obj[0]

        helpers.wait_for_resource_state(
            rook_ceph_operator, constants.STATUS_RUNNING, timeout=120
        )
        command = (
            f"oc exec -n openshift-storage {operator_pod_obj[0].name} -- bash -c "
            + "'echo $CSI_SIDECAR_LOG_LEVEL'"
        )
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 1
        log.info(result.stdout.decode())
        command = (
            f"oc exec -n openshift-storage {operator_pod_obj[0].name} -- bash -c "
            + "'echo $CSI_LOG_LEVEL'"
        )
        result = utils.exec_cmd(cmd=command)
        assert result.returncode == 0
        assert result.stdout.decode().rstrip() == "5"
        log.info(result.stdout.decode())


@tier1
@skipif_ocs_version("<4.12")
@skipif_ocp_version("<4.12")
@skipif_managed_service
@skipif_disconnected_cluster
@skipif_proxy_cluster
@green_squad
@ignore_leftovers
class TestSidecarLoglevel(ManageTest):
    """
    Test sidecar log level feature for ODF 4.12
    """

    @pytest.fixture(autouse=True)
    def setup(self, project_factory):
        """
        Setup for the class
        """
        log.info("-----Setup-----")
        self.project_name = "sidecar-loglevel"
        project_factory(project_name=self.project_name)
        self.namespace = "openshift-storage"
        self.config_map_obj = ocp.OCP(kind="Configmap", namespace=self.namespace)
        self.pod_obj = ocp.OCP(kind="Pod", namespace=self.namespace)
        self.pv_obj = ocp.OCP(kind=constants.PV, namespace=self.namespace)
        self.rook_ceph_operator = pod.get_operator_pods()[0]

    # @polarion_id("OCS-4674")
    def test_default_sidecar_log_level(self):
        """
        This test is to validate $CSI_SIDECAR_LOG_LEVEL is set with default value "1" for  ODF(4.12) clusters
        Steps:
        1:- Check $CSI_SIDECAR_LOG_LEVEL parameter is set with default value "1"
        """
        default_sidecar_log_level = loglevel_utils.default_sidecar_loglevel()
        log.info(f"default side car log level is, {default_sidecar_log_level}")
        assert (
            default_sidecar_log_level == "1"
        ), "Error: The default value for sidecar log level is not as expected"

    @pytest.mark.parametrize(
        argnames=["fs", "sc_name"],
        argvalues=[
            pytest.param(
                "ocs-storagecluster-cephfilesystem",
                constants.DEFAULT_STORAGECLASS_CEPHFS,
                marks=pytest.mark.polarion_id("OCS-4676"),
            ),
            pytest.param(
                "ocs-storagecluster-cephblockpool",
                constants.DEFAULT_STORAGECLASS_RBD,
                marks=pytest.mark.polarion_id("OCS-4679"),
            ),
        ],
    )
    def test_sidecar_logs_for_default_loglevel(self, snapshot_factory, sc_name):
        """
        This test case verifies with CSI_SIDECAR_LOG_LEVEL : 1 default value in place sidecar logs
        generated for performing---
        1. provision pvc
        2. expansion of a pvc
        3. snapshotting
        4. mount pvc
        """
        # Create pvc object
        pvc_obj = helpers.create_pvc(
            sc_name=sc_name,
            namespace=self.project_name,
            do_reload=True,
            size="1Gi",
        )
        helpers.wait_for_resource_state(pvc_obj, constants.STATUS_BOUND, timeout=600)
        provisioner_success_log = (
            f"Successfully provisioned volume {pvc_obj.backed_pv_obj.name}"
        )
        log_found = loglevel_utils.validate_sidecar_logs(
            sc_name=sc_name,
            container="csi-provisioner",
            expected_log=provisioner_success_log,
        )
        assert (
            log_found
        ), f"Error: {provisioner_success_log} msg not found in csi-provisioner logs."

        # Expand PVC to new size
        log.info(f"Starting expanding PVC size to 3Gi")
        pvc_obj.resize_pvc(3, True)
        resize_success_log = f"'VolumeResizeSuccessful' Resize volume succeeded"
        log_found = loglevel_utils.validate_sidecar_logs(
            sc_name=sc_name, container="csi-resizer", expected_log=resize_success_log
        )
        assert (
            log_found
        ), f"Error: {resize_success_log} msg not found in csi-resizer logs."

        # Create a volume snapshot
        snap_obj = snapshot_factory(pvc_obj, wait=True)
        log.info(f"snap object created {snap_obj.name}")

    @tier3
    @pytest.mark.parametrize(
        argnames=["flag_value"],
        argvalues=[
            pytest.param("100"),
            pytest.param("level3"),
            pytest.param("sidecar-loglevel"),
        ],
    )
    # @polarion_id("OCS-4682")
    def test_negative_values_for_CSI_SIDECAR_LOG_LEVEL_flag(self, flag_value):
        """
        Validate negative scenarios by providing various un acceptable values for, CSI_SIDECAR_LOG_LEVEL flag.
        1. numeric value for more than accepted
        2. alphanumeric value
        3. string values
        4. Update a wrong flag name as, CSI_SIDE_CAR_LOG_LEVEL flag
        Default log level set for CSI_SIDECAR_LOG_LEVEL_flag will be considered in such cases when
        unacceptable values are set for CSI_SIDECAR_LOG_LEVEL_flag in rook-ceph-operator-config

        Steps:
            1. Set CSI_SIDECAR_LOG_LEVEL_flag flag value as numeric value more than accepted
            2. Set CSI_SIDECAR_LOG_LEVEL_flag flag value as alphanumeric value
            3. Set string values for CSI_SIDECAR_LOG_LEVEL_flag flag
        """
        # Set numeric value for CSI_SIDECAR_LOG_LEVEL_flag flag
        params = (
            '{"data":{"CSI_SIDECAR_LOG_LEVEL_flag": ' + '"' + flag_value + '"' + "}}"
        )
        log.info(f"params ----- {params}")

        # Enable CSI_ENABLE_OMAP_GENERATOR flag for rook-ceph-operator-config using patch command
        assert self.config_map_obj.patch(
            resource_name="rook-ceph-operator-config",
            params=params,
        ), "configmap/rook-ceph-operator-config not patched"

        # Check csi-cephfsplugin provisioner and csi-rbdplugin-provisioner pods are up and running
        assert self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=csi-cephfsplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )

        assert self.pod_obj.wait_for_resource(
            condition=constants.STATUS_RUNNING,
            selector="app=csi-rbdplugin-provisioner",
            dont_allow_other_resources=True,
            timeout=60,
        )
        default_sidecar_log_level = loglevel_utils.default_sidecar_loglevel()
        log.info(f"default side car log level is, {default_sidecar_log_level}")
        assert (
            default_sidecar_log_level == "1"
        ), "Error: The default value for sidecar log level is not as expected"
