import logging
import pytest
from ocs_ci.framework.testlib import config, tier1
from ocs_ci.ocs import constants
from ocs_ci.helpers.keyrotation_helper import PVKeyrotation
from ocs_ci.helpers.helpers import create_pods

log = logging.getLogger(__name__)

kmsprovider = constants.VAULT_KMS_PROVIDER
argnames = ["kv_version", "kms_provider", "use_vault_namespace"]
if config.ENV_DATA.get("vault_hcp"):
    argvalues = [
        pytest.param("v1", kmsprovider, True, marks=pytest.mark.polarion_id()),
        pytest.param("v2", kmsprovider, True, marks=pytest.mark.polarion_id()),
    ]
else:
    argvalues = [
        pytest.param("v1", kmsprovider, False, marks=pytest.mark.polarion_id()),
        pytest.param("v2", kmsprovider, False, marks=pytest.mark.polarion_id()),
    ]


@tier1
@pytest.mark.parametrize(
    argnames=argnames,
    argvalues=argvalues,
)
class TestDisablePVKeyrotaionOperation:
    @pytest.fixture()
    def setup(
        self,
        kv_version,
        kms_provider,
        pv_encryption_kms_setup_factory,
        project_factory,
        storageclass_factory,
        multi_pvc_factory,
        pod_factory,
        use_vault_namespace,
    ):
        """
        Setup csi-kms-connection-details configmap

        """
        log.info("Setting up csi-kms-connection-details configmap")
        self.kms = pv_encryption_kms_setup_factory(kv_version, use_vault_namespace)
        log.info("csi-kms-connection-details setup successful")

        # Create a project
        self.proj_obj = project_factory()

        # Keyrotation annotations
        keyrotation_annotations = {
            "keyrotation.csiaddons.openshift.io/schedule": "* * * * *"
        }

        # Create an encryption enabled storageclass for RBD
        self.sc_obj = storageclass_factory(
            interface=constants.CEPHBLOCKPOOL,
            encrypted=True,
            encryption_kms_id=self.kms.kmsid,
            annotations=keyrotation_annotations,
        )

        # Create ceph-csi-kms-token in the tenant namespace
        self.kms.vault_path_token = self.kms.generate_vault_token()
        self.kms.create_vault_csi_kms_token(namespace=self.proj_obj.namespace)

        # Create RBD PVCs with volume mode Block
        self.pvc_objs = multi_pvc_factory(
            size=5,
            num_of_pvc=3,
            storageclass=self.sc_obj,
            access_modes=[
                f"{constants.ACCESS_MODE_RWX}-Block",
                f"{constants.ACCESS_MODE_RWO}-Block",
            ],
            wait_each=True,
            project=self.proj_obj
        )

        self.pod_objs = create_pods(
            self.pvc_objs,
            pod_factory,
            constants.CEPHBLOCKPOOL,
            pods_for_rwx=1,
            status=constants.STATUS_RUNNING,
        )

        self.pv_keyrotation_obj = PVKeyrotation(self.sc_obj)

    def test_disable_pv_keyrotatio_operation_globally(
        self,
        setup,
    ):
        """Test disable PV keyrotation globally by anotating storageclass.

        Steps:
            1. Add annotation to the storaegclass to disable keyrotation.
            2. Verify keyrotation jobs are deleted
            3. Remove annotation form the storageclass
            4. verify keyrotation cronjob are created again.

        """

        # Disable Keyrotation globally by annotating the storageclass
        # disable_annotations = {"keyrotation.csiaddons.openshift.io/enable": "false"}
        # self.sc_obj.annotations(disable_annotations)
        state = False
        self.pv_keyrotation_obj.set_keyrotation_state_by_sc_annotation(state)

        # Verify keyrotation jobs are removed.
        for pvc_obj in self.pvc_objs:
            with pytest.raises(ValueError):
                self.pv_keyrotation_obj.get_keyrotationcronjob_for_pvc(pvc_obj)

        # enable keyrotation by changing annotation
        state = True
        self.pv_keyrotation_obj.set_keyrotation_state_by_sc_annotation(state)

        # verify keyrotation is enabled for the pvc
        assert self.pv_keyrotation_obj.wait_till_all_pv_keyrotation_on_vault_kms(
            self.pvc_objs
        ), "Failed PV keyrotation operation."

    # def test_disable_pv_keyrotation_operation_by_RBAC_user(self):
    #     """_summary_"""
