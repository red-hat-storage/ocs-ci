import logging

import pytest
from flaky import flaky

from ocs_ci.framework import config
from ocs_ci.framework.pytest_customization.marks import (
    skipif_ibm_cloud_managed,
    skipif_managed_service,
    black_squad,
    green_squad,
    polarion_id,
    tier2,
    tier3,
    skipif_ocs_version,
    mcg,
    ui,
    skipif_hci_provider_or_client,
    runs_on_provider,
    skipif_disconnected_cluster,
    external_mode_required,
    jira,
)
from ocs_ci.framework.testlib import ManageTest
from ocs_ci.helpers import helpers
from ocs_ci.helpers.helpers import create_unique_resource_name
from ocs_ci.ocs import constants
from ocs_ci.ocs.cluster import get_ec_metadata_pool_name, is_ec_pool_supported
from ocs_ci.ocs.exceptions import CommandFailed, TimeoutExpiredError
from ocs_ci.ocs.node import get_osd_running_nodes
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs.resources import pod
from ocs_ci.ocs.ui.page_objects.page_navigator import PageNavigator
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)


def _force_delete_ec_pool(pool_name, pool_type):
    """
    Force-delete an EC pool via CLI as a teardown fallback.

    Args:
        pool_name (str): Full pool name.
        pool_type (str): 'block' or 'filesystem'.
    """
    namespace = config.ENV_DATA.get("cluster_namespace", "openshift-storage")

    if pool_type == "block":
        try:
            OCP().exec_oc_cmd(
                f"delete cephblockpool {pool_name} -n {namespace} "
                "--force --grace-period=0"
            )
            logger.info(f"Force-deleted CephBlockPool {pool_name}")
        except CommandFailed:
            logger.debug(f"CephBlockPool {pool_name} already deleted or not found")
    else:
        suffix = pool_name.replace("ocs-storagecluster-cephfilesystem-", "")
        try:
            cf = OCP(kind="CephFilesystem", namespace=namespace)
            cf_data = cf.get(resource_name="ocs-storagecluster-cephfilesystem")
            data_pools = cf_data.get("spec", {}).get("dataPools", [])
            for i, dp in enumerate(data_pools):
                if dp.get("name") == suffix:
                    cf.patch(
                        resource_name="ocs-storagecluster-cephfilesystem",
                        params=(
                            f'[{{"op": "remove", ' f'"path": "/spec/dataPools/{i}"}}]'
                        ),
                        format_type="json",
                    )
                    logger.info(
                        f"Force-deleted CephFilesystem data pool {suffix} "
                        f"(index {i})"
                    )
                    break
            else:
                logger.debug(f"CephFilesystem data pool {suffix} not found")
        except CommandFailed:
            logger.debug(
                f"CephFilesystem data pool {suffix} already deleted or not found"
            )


@ui
@tier3
@black_squad
@runs_on_provider
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_ocs_version("<4.13")
class TestErrorMessageImprovements(ManageTest):
    @mcg
    @flaky(max_runs=2)
    @polarion_id("OCS-4865")
    def test_backing_store_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new backing store
            No more than 43 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            A unique name for the BackingStore within the project
        """
        backing_store_tab = (
            PageNavigator().nav_object_storage_page().nav_backing_store_tab()
        )
        backing_store_tab.proceed_resource_creation()
        backing_store_tab.check_error_messages()

    @mcg
    @jira("DFBUGS-410")
    @polarion_id("OCS-4867")
    def test_obc_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new object bucket claim
            No more than 253 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Cannot be used before
        """
        object_bucket_claim_create_tab = (
            PageNavigator().nav_object_storage_page().nav_object_buckets_claims_tab()
        )
        object_bucket_claim_create_tab.proceed_resource_creation()
        object_bucket_claim_create_tab.check_error_messages()

    @mcg
    @flaky(max_runs=2)
    @polarion_id("OCS-4869")
    def test_bucket_class_creation_rules(self, setup_ui_class):
        """
        Test to verify error rules for the name when creating a new bucket class
            3-63 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Avoid using the form of an IP address
            Cannot be used before
        """
        bucket_class_create_tab = (
            PageNavigator().nav_object_storage_page().nav_bucket_class_tab()
        )
        bucket_class_create_tab.proceed_resource_creation()
        bucket_class_create_tab.check_error_messages()

    @mcg
    @flaky(max_runs=2)
    @polarion_id("OCS-4871")
    @skipif_disconnected_cluster
    def test_namespace_store_creation_rules(
        self, cld_mgr, namespace_store_factory, setup_ui_class
    ):
        """
        Test to verify error rules for the name when creating a new namespace store
            No more than 43 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            A unique name for the NamespaceStore within the project

        * check_error_messages function requires 1 existing namespacestore as pre-condition for checking rule
        'A unique name for the NamespaceStore within the project'
        """
        existing_namespace_store_names = OCP().exec_oc_cmd(
            "get namespacestore --all-namespaces -o custom-columns=':metadata.name'"
        )
        if not existing_namespace_store_names:
            logger.info("Create namespace resource")
            nss_tup = ("oc", {"aws": [(1, "us-east-2")]})
            namespace_store_factory(*nss_tup)

        namespace_store_tab = (
            PageNavigator().nav_object_storage_page().nav_namespace_store_tab()
        )
        namespace_store_tab.proceed_resource_creation()
        namespace_store_tab.check_error_messages()

    @polarion_id("OCS-4873")
    def test_blocking_pool_creation_rules(self, cephblockpool_factory_ui_class):
        """
        Test to verify
        * edit Block Pool label warnings
        * error rules for the name when creating a new blocking pool
            No more than 253 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Cannot be used before
        """

        block_pool_obj = cephblockpool_factory_ui_class()

        blocking_pool_tab = PageNavigator().navigate_storage_pools_page()

        blocking_pool_tab.check_edit_labels(block_pool_obj.name)

        blocking_pool_tab.proceed_resource_creation()

        blocking_pool_tab.check_error_messages()

    @polarion_id("OCS-4875")
    @flaky(max_runs=2)
    @external_mode_required
    @skipif_hci_provider_or_client
    def deprecated_storage_class_creation_rules(self, setup_ui_class):
        """
        ! StorageSystem removed from management-console starting from ODF 4.20

        Test to verify error rules for the name when creating a new storage class.
        external_mode_required deco added. Starting from ODF 4.16 this form is available for External mode only,
        where still no clusters StorageSystem was created. Rules are:
            No more than 253 characters
            Starts and ends with a lowercase letter or number
            Only lowercase letters, numbers, non-consecutive periods, or hyphens
            Cannot be used before
        """
        storage_systems_tab = (
            PageNavigator().nav_storage_cluster_default_page().nav_storage_systems_tab()
        )
        storage_systems_tab.proceed_resource_creation()
        storage_systems_tab.fill_backing_storage_form(
            "Use an existing StorageClass", "Next"
        )
        storage_systems_tab.check_error_messages()


@ui
@tier2
@green_squad
@runs_on_provider
@skipif_ibm_cloud_managed
@skipif_managed_service
@skipif_ocs_version("<4.22")
class TestECPoolCreation(ManageTest):
    """
    Test Erasure Coded pool creation, scheme validation, IO, and lifecycle via UI.
    """

    @pytest.fixture(autouse=True)
    def skip_if_ec_not_supported(self):
        if not is_ec_pool_supported():
            pytest.skip("EC pools not supported on this cluster")
        num_osd_hosts = len(get_osd_running_nodes())
        min_k_plus_m = min(p["k"] + p["m"] for p in constants.EC_SUPPORTED_PROFILES)
        if num_osd_hosts < min_k_plus_m:
            pytest.skip(
                f"Cluster has {num_osd_hosts} OSD hosts but smallest EC "
                f"profile requires {min_k_plus_m}"
            )

    @pytest.fixture()
    def ec_pool_teardown(self, request):
        """
        Registers CLI-based force-deletion as a session-end finalizer
        for any EC pools created during the test.
        """
        created_pools = []

        def finalizer():
            for name, ptype in created_pools:
                _force_delete_ec_pool(name, ptype)

        request.addfinalizer(finalizer)
        return created_pools

    @pytest.fixture()
    def ec_io_cleanup(self, request):
        """
        Tracks StorageClass and Secret objects created for IO verification
        and cleans them up in finalizer.
        """
        resources = []

        def finalizer():
            for obj in reversed(resources):
                try:
                    obj.delete()
                    obj.ocp.wait_for_delete(obj.name, timeout=60)
                except (CommandFailed, TimeoutExpiredError):
                    logger.warning(f"Failed to delete {obj.kind}/{obj.name}")

        request.addfinalizer(finalizer)
        return resources

    @pytest.mark.parametrize(
        argnames=["pool_type"],
        argvalues=[
            pytest.param("block"),
            pytest.param("filesystem"),
        ],
    )
    def test_create_ec_pool(
        self,
        setup_ui_class,
        ec_pool_teardown,
        ec_io_cleanup,
        pvc_factory,
        pod_factory,
        pool_type,
    ):
        """
        Create an EC storage pool via UI, verify EC scheme selection,
        run IO on the pool, then delete the pool.

        Steps:
            1. Navigate to Storage pools, click Create
            2. Select volume type (block or filesystem)
            3. Select Erasure coding under Data protection policy
            4. Verify available EC schemes match cluster node count
            5. Verify Recommended badge on appropriate scheme
            6. Enable compression, select Recommended scheme, create pool
            7. Verify pool in list shows correct EC scheme and Ready status
            8. Create StorageClass + PVC + Pod on the new EC pool, run FIO
            9. Verify IO completes without errors
            10. Delete pool via UI
        """
        num_osd_hosts = len(get_osd_running_nodes())
        logger.info(f"Cluster has {num_osd_hosts} OSD hosts")

        expected_schemes = [
            p["scheme"]
            for p in constants.EC_SUPPORTED_PROFILES
            if p["k"] + p["m"] <= num_osd_hosts
        ]
        logger.info(f"Expected available EC schemes: {expected_schemes}")

        storage_pools = PageNavigator().navigate_storage_pools_page()
        storage_pools.proceed_resource_creation(volume_type=pool_type)

        pool_name = create_unique_resource_name("test", "ec-pool")
        storage_pools.do_send_keys(storage_pools.bp_loc["pool_name_input"], pool_name)
        storage_pools.select_data_protection("erasure_coding")

        available = storage_pools.get_available_ec_schemes()
        available_names = [s["scheme"] for s in available]
        assert available_names == expected_schemes, (
            f"EC schemes mismatch: UI shows {available_names}, "
            f"expected {expected_schemes}"
        )

        recommended = [s for s in available if s["recommended"]]
        assert recommended, "No EC scheme marked as Recommended"
        recommended_scheme = recommended[0]["scheme"]
        logger.info(f"Recommended EC scheme: {recommended_scheme}")

        k, m = [int(x) for x in recommended_scheme.split("+")]
        assert k + m + 1 <= num_osd_hosts, (
            f"Recommended scheme {recommended_scheme} requires {k + m + 1} hosts "
            f"but cluster has {num_osd_hosts}"
        )

        non_recommended = [s for s in available if not s["recommended"]]
        for s in non_recommended:
            nk, nm = [int(x) for x in s["scheme"].split("+")]
            assert nk + nm + 1 > num_osd_hosts, (
                f"Scheme {s['scheme']} should be Recommended "
                f"(k+m+1={nk + nm + 1} <= {num_osd_hosts} hosts)"
            )

        storage_pools.do_click(storage_pools.bp_loc["conpression_checkbox"])

        storage_pools.select_ec_scheme(recommended_scheme)
        storage_pools.do_click(storage_pools.bp_loc["pool_confirm_create"])

        full_pool_name = pool_name
        if pool_type == "filesystem":
            full_pool_name = f"ocs-storagecluster-cephfilesystem-{pool_name}"

        ec_pool_teardown.append((full_pool_name, pool_type))

        assert storage_pools.wait_until_expected_text_is_found(
            storage_pools.bp_loc["pool_state_inside_pool"], "Ready", timeout=60
        ), f"EC pool {full_pool_name} did not reach Ready state"

        logger.info(f"EC pool created: {full_pool_name}")

        storage_pools_page = PageNavigator().navigate_storage_pools_page()
        assert storage_pools_page.verify_pool_ec_scheme_in_list(
            full_pool_name, recommended_scheme
        ), (
            f"Pool {full_pool_name} not showing EC scheme "
            f"{recommended_scheme} in list"
        )

        # --- IO verification on the UI-created EC pool ---
        logger.info("Creating StorageClass and running IO on the new EC pool")
        if pool_type == "block":
            interface_type = constants.CEPHBLOCKPOOL
            secret_obj = helpers.create_secret(interface_type=interface_type)
            ec_io_cleanup.append(secret_obj)
            sc_obj = helpers.create_storage_class(
                interface_type=interface_type,
                interface_name=get_ec_metadata_pool_name(),
                secret_name=secret_obj.name,
                data_pool_name=full_pool_name,
            )
            ec_io_cleanup.append(sc_obj)
            access_mode = constants.ACCESS_MODE_RWO
        else:
            interface_type = constants.CEPHFILESYSTEM
            secret_obj = helpers.create_secret(interface_type=interface_type)
            ec_io_cleanup.append(secret_obj)
            sc_obj = helpers.create_storage_class(
                interface_type=interface_type,
                interface_name=full_pool_name,
                secret_name=secret_obj.name,
            )
            ec_io_cleanup.append(sc_obj)
            access_mode = constants.ACCESS_MODE_RWX

        logger.info(f"StorageClass '{sc_obj.name}' created for EC pool")

        pvc_obj = pvc_factory(
            interface=interface_type,
            storageclass=sc_obj,
            access_mode=access_mode,
            size=5,
        )
        logger.info(f"PVC '{pvc_obj.name}' bound")

        pod_obj = pod_factory(interface=interface_type, pvc=pvc_obj)
        logger.info(f"Pod '{pod_obj.name}' running, starting IO")

        pod_obj.run_io(storage_type="fs", size="256M", runtime=30)
        fio_result = pod_obj.get_fio_results()
        err_count = fio_result.get("jobs")[0].get("error")
        assert err_count == 0, (
            f"IO error on EC pool {full_pool_name}. " f"FIO result: {fio_result}"
        )
        logger.info("IO completed without errors on EC pool")

        # --- Clean up IO resources before pool deletion ---
        logger.info("Deleting IO pod, PVC, StorageClass and Secret")
        pod_obj.delete(wait=True)
        pod_obj.ocp.wait_for_delete(pod_obj.name, timeout=120)
        logger.info(f"Pod '{pod_obj.name}' deleted")

        pvc_obj.delete(wait=True)
        pvc_obj.ocp.wait_for_delete(pvc_obj.name, timeout=120)
        logger.info(f"PVC '{pvc_obj.name}' deleted")

        sc_obj.delete()
        sc_obj.ocp.wait_for_delete(sc_obj.name, timeout=60)
        logger.info(f"StorageClass '{sc_obj.name}' deleted")

        secret_obj.delete()
        secret_obj.ocp.wait_for_delete(secret_obj.name, timeout=60)
        logger.info(f"Secret '{secret_obj.name}' deleted")

        ec_io_cleanup.clear()

        # --- Delete pool via UI ---
        storage_pools_page = PageNavigator().navigate_storage_pools_page()
        assert storage_pools_page.delete_block_pool(
            full_pool_name
        ), f"Failed to delete pool {full_pool_name} via UI"

        ct_pod = pod.get_ceph_tools_pod()
        logger.info(f"Waiting for Ceph pool '{full_pool_name}' to be removed")
        for pools in TimeoutSampler(180, 10, ct_pod.exec_ceph_cmd, "ceph osd pool ls"):
            if full_pool_name not in pools:
                logger.info(f"Ceph pool '{full_pool_name}' removed successfully")
                break

        storage_pools_page = PageNavigator().navigate_storage_pools_page()
        storage_pools_page.page_has_loaded()
        assert not storage_pools_page.is_block_pool_exist(
            full_pool_name
        ), f"Pool {full_pool_name} still exists after deletion"

        ec_pool_teardown.clear()
        logger.info(
            f"EC pool {full_pool_name} successfully created, IO verified, and deleted"
        )
