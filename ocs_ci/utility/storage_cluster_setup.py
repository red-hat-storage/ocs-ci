from ocs_ci.deployment.helpers import storage_class
import tempfile
import yaml
from copy import deepcopy
from ocs_ci.utility import (
    templating,
    kms as KMS,
    pgsql,
    version,
)
from ocs_ci.framework import config, merge_dict
import logging
from ocs_ci.deployment.helpers.mcg_helpers import (
    mcg_only_deployment,
)
from ocs_ci.ocs.node import (
    get_node_objs,
)
from ocs_ci.ocs.resources.storage_cluster import (
    setup_ceph_debug,
)
from ocs_ci.ocs.ocp import OCP
from ocs_ci.helpers import helpers
from ocs_ci.framework.logger_helper import log_step
from ocs_ci.deployment.encryption import add_in_transit_encryption_to_cluster_data
from ocs_ci.ocs import constants, ocp, defaults
from ocs_ci.ocs.exceptions import (
    UnsupportedFeatureError,
)
from ocs_ci.utility.utils import (
    get_az_count,
    run_cmd,
)

logger = logging.getLogger(__name__)


# create custom storage class for StorageCluster CR if necessary
class StorageClusterSetup(object):
    def __init__(self):
        self.custom_storage_class_path = None

    def setup_storage_cluster(self):
        if self.custom_storage_class_path is not None:
            self.storage_class = storage_class.create_custom_storageclass(
                self.custom_storage_class_path
            )

        # Set rook log level
        self.set_rook_log_level()

        # creating StorageCluster
        if config.DEPLOYMENT.get("kms_deployment"):
            kms = KMS.get_kms_deployment()
            kms.deploy()

        if config.ENV_DATA["mcg_only_deployment"]:
            mcg_only_deployment()
            return

        log_step("Setup StorageCluster preferences before applying CR")
        cluster_data = templating.load_yaml(constants.STORAGE_CLUSTER_YAML)
        # Figure out all the OCS modules enabled/disabled
        # CLI parameter --disable-components takes the precedence over
        # anything which comes from config file
        if config.ENV_DATA.get("disable_components"):
            for component in config.ENV_DATA["disable_components"]:
                config.COMPONENTS[f"disable_{component}"] = True
                logger.warning(f"disabling: {component}")

        if config.DEPLOYMENT.get("host_network"):
            logger.info("Using host network for ODF operator")
            cluster_data["spec"]["network"] = {"hostNetwork": True}

        if config.ENV_DATA.get("odf_provider_mode_deployment", False):
            cluster_data["spec"]["providerAPIServerServiceType"] = "NodePort"

        # Update cluster_data with respective component enable/disable
        for key in config.COMPONENTS.keys():
            comp_name = constants.OCS_COMPONENTS_MAP[key.split("_")[1]]
            if config.COMPONENTS[key]:
                if "noobaa" in key:
                    merge_dict(
                        cluster_data,
                        {
                            "spec": {
                                "multiCloudGateway": {"reconcileStrategy": "ignore"}
                            }
                        },
                    )
                else:
                    merge_dict(
                        cluster_data,
                        {
                            "spec": {
                                "managedResources": {
                                    f"{comp_name}": {"reconcileStrategy": "ignore"}
                                }
                            }
                        },
                    )

        device_class = config.ENV_DATA.get("device_class")
        arbiter_deployment = config.DEPLOYMENT.get("arbiter_deployment")

        if arbiter_deployment:
            cluster_data["spec"]["arbiter"] = {}
            cluster_data["spec"]["nodeTopologies"] = {}
            cluster_data["spec"]["arbiter"]["enable"] = True
            cluster_data["spec"]["nodeTopologies"][
                "arbiterLocation"
            ] = self.get_arbiter_location()
            cluster_data["spec"]["storageDeviceSets"][0]["replica"] = 4

        cluster_data["metadata"]["name"] = config.ENV_DATA["storage_cluster_name"]
        cluster_data["metadata"]["namespace"] = self.namespace

        deviceset_data = cluster_data["spec"]["storageDeviceSets"][0]
        device_size = int(config.ENV_DATA.get("device_size", defaults.DEVICE_SIZE))
        if device_class:
            deviceset_data["deviceClass"] = device_class

        logger.debug(
            "Flexible scaling is available from version 4.7 on LSO cluster with less than 3 zones"
        )
        zone_num = get_az_count()
        local_storage = config.DEPLOYMENT.get("local_storage")
        ocs_version = version.get_semantic_ocs_version_from_config()
        if (
            local_storage
            and ocs_version >= version.VERSION_4_7
            and zone_num < 3
            and not config.DEPLOYMENT.get("arbiter_deployment")
            and not (self.platform in constants.HCI_PROVIDER_CLIENT_PLATFORMS)
        ):
            cluster_data["spec"]["flexibleScaling"] = True
            # https://bugzilla.redhat.com/show_bug.cgi?id=1921023
            cluster_data["spec"]["storageDeviceSets"][0]["count"] = 3
            cluster_data["spec"]["storageDeviceSets"][0]["replica"] = 1
        elif self.platform in constants.HCI_PROVIDER_CLIENT_PLATFORMS:
            from ocs_ci.deployment.baremetal import disks_available_to_cleanup

            nodes_obj = OCP(
                kind=constants.NODE,
                selector=f"{constants.OPERATOR_NODE_LABEL}",
            )
            nodes_data = nodes_obj.get()["items"]
            node_names = [nodes["metadata"]["name"] for nodes in nodes_data]

            no_of_worker_nodes = len(node_names)
            number_of_disks_available_total = 0
            # count number of disks available on all labeled nodes and divide to number of nodes
            for node in node_names:
                node_obj_list = get_node_objs([node])
                number_of_disks_available_total += len(
                    disks_available_to_cleanup(node_obj_list.pop())
                )

            number_of_disks_available = int(
                number_of_disks_available_total / no_of_worker_nodes
            )

            # with this approach of datermining the number of nodes we assume worker nodes number of disks is equal
            # to master nodes number of disks, in case when config.ENV_DATA.get("mark_masters_schedulable") == True,
            # and we labeled master nodes to serve as a storage nodes
            cluster_data["spec"]["storageDeviceSets"][0][
                "count"
            ] = number_of_disks_available
            cluster_data["spec"]["storageDeviceSets"][0]["replica"] = no_of_worker_nodes
            cluster_data["spec"]["flexibleScaling"] = True

        # set size of request for storage
        if self.platform.lower() in [
            constants.BAREMETAL_PLATFORM,
            constants.HCI_BAREMETAL,
        ]:
            pv_size_list = helpers.get_pv_size(
                storageclass=constants.DEFAULT_STORAGECLASS_LSO
            )
            pv_size_list.sort()
            deviceset_data["dataPVCTemplate"]["spec"]["resources"]["requests"][
                "storage"
            ] = f"{pv_size_list[0]}"
        else:
            deviceset_data["dataPVCTemplate"]["spec"]["resources"]["requests"][
                "storage"
            ] = f"{device_size}Gi"

        # set storage class to OCS default on current platform
        if self.storage_class:
            deviceset_data["dataPVCTemplate"]["spec"][
                "storageClassName"
            ] = self.storage_class

        # StorageCluster tweaks for LSO
        ocp_version = version.get_semantic_ocp_version_from_config()

        if local_storage:
            cluster_data["spec"]["manageNodes"] = False
            cluster_data["spec"]["monDataDirHostPath"] = "/var/lib/rook"
            deviceset_data["name"] = constants.DEFAULT_DEVICESET_LSO_PVC_NAME
            deviceset_data["portable"] = False
            deviceset_data["dataPVCTemplate"]["spec"][
                "storageClassName"
            ] = constants.DEFAULT_STORAGECLASS_LSO
            lso_type = config.DEPLOYMENT.get("type")
            if (
                self.platform.lower() == constants.AWS_PLATFORM
                and not lso_type == constants.AWS_EBS
            ):
                deviceset_data["count"] = 2
            # setting resource limits for AWS i3
            # https://access.redhat.com/documentation/en-us/red_hat_openshift_container_storage/4.6/html-single/deploying_openshift_container_storage_using_amazon_web_services/index#creating-openshift-container-storage-cluster-on-amazon-ec2_local-storage
            if (
                ocs_version >= version.VERSION_4_5
                and config.ENV_DATA.get("worker_instance_type")
                == constants.AWS_LSO_WORKER_INSTANCE
            ):
                deviceset_data["resources"] = {
                    "limits": {"cpu": 2, "memory": "5Gi"},
                    "requests": {"cpu": 1, "memory": "5Gi"},
                }
            if (ocp_version >= version.VERSION_4_6) and (
                ocs_version >= version.VERSION_4_6
            ):
                cluster_data["metadata"]["annotations"] = {
                    "cluster.ocs.openshift.io/local-devices": "true"
                }
            count = config.DEPLOYMENT.get("local_storage_storagedeviceset_count")
            if count is not None:
                deviceset_data["count"] = count

        # Allow lower instance requests and limits for OCS deployment
        # The resources we need to change can be found here:
        # https://github.com/openshift/ocs-operator/blob/release-4.5/pkg/deploy-manager/storagecluster.go#L88-L116
        if config.DEPLOYMENT.get("allow_lower_instance_requirements"):
            none_resources = {"Requests": None, "Limits": None}
            deviceset_data["resources"] = deepcopy(none_resources)
            resources = [
                "mon",
                "mds",
                "rgw",
                "mgr",
                "noobaa-core",
                "noobaa-db",
            ]
            if ocs_version >= version.VERSION_4_5:
                resources.append("noobaa-endpoint")
            cluster_data["spec"]["resources"] = {
                resource: deepcopy(none_resources) for resource in resources
            }
            if ocs_version >= version.VERSION_4_5:
                cluster_data["spec"]["resources"]["noobaa-endpoint"] = {
                    "limits": {"cpu": 1, "memory": "500Mi"},
                    "requests": {"cpu": 1, "memory": "500Mi"},
                }
        else:
            platform = config.ENV_DATA.get("platform", "").lower()
            if local_storage and platform == "aws":
                resources = {
                    "mds": {
                        "limits": {"cpu": 3, "memory": "8Gi"},
                        "requests": {"cpu": 1, "memory": "8Gi"},
                    }
                }
                if ocs_version < version.VERSION_4_5:
                    resources["noobaa-core"] = {
                        "limits": {"cpu": 2, "memory": "8Gi"},
                        "requests": {"cpu": 1, "memory": "8Gi"},
                    }
                    resources["noobaa-db"] = {
                        "limits": {"cpu": 2, "memory": "8Gi"},
                        "requests": {"cpu": 1, "memory": "8Gi"},
                    }
                cluster_data["spec"]["resources"] = resources

        # Enable host network if enabled in config (this require all the
        # rules to be enabled on underlaying platform).
        if config.DEPLOYMENT.get("host_network"):
            cluster_data["spec"]["hostNetwork"] = True
            logger.info("Host network is enabled")
            # follow the rule in bug DFBUGS-2324. UI adds this value by default if the ["spec"]["hostNetwork"] = True
            # this prevents crashes on rgw installation
            cluster_data["spec"].setdefault("managedResources", {}).setdefault(
                "cephObjectStores", {}
            )["hostNetwork"] = False

        cluster_data["spec"]["storageDeviceSets"] = [deviceset_data]
        managed_ibmcloud = (
            config.ENV_DATA["platform"] == constants.IBMCLOUD_PLATFORM
            and config.ENV_DATA["deployment_type"] == "managed"
        )
        if managed_ibmcloud:
            mon_pvc_template = {
                "spec": {
                    "accessModes": ["ReadWriteOnce"],
                    "resources": {"requests": {"storage": "20Gi"}},
                    "storageClassName": self.storage_class,
                    "volumeMode": "Filesystem",
                }
            }
            cluster_data["spec"]["monPVCTemplate"] = mon_pvc_template
            # Need to check if it's needed for ibm cloud to set manageNodes
            cluster_data["spec"]["manageNodes"] = False

        if config.ENV_DATA.get("encryption_at_rest"):
            if ocs_version < version.VERSION_4_6:
                error_message = "Encryption at REST can be enabled only on OCS >= 4.6!"
                logger.error(error_message)
                raise UnsupportedFeatureError(error_message)
            logger.info("Enabling encryption at REST!")
            cluster_data["spec"]["encryption"] = {
                "enable": True,
            }
            if ocs_version >= version.VERSION_4_10:
                cluster_data["spec"]["encryption"] = {
                    "clusterWide": True,
                }
            if config.DEPLOYMENT.get("kms_deployment"):
                cluster_data["spec"]["encryption"]["kms"] = {
                    "enable": True,
                }
            if config.DEPLOYMENT.get("sc_encryption"):
                if not config.DEPLOYMENT.get("kms_deployment"):
                    raise UnsupportedFeatureError(
                        "StorageClass encryption can be enabled only when KMS is enabled!"
                    )
                cluster_data["spec"]["encryption"]["storageClass"] = True

        managed_resources = cluster_data["spec"].setdefault("managedResources", {})
        if config.DEPLOYMENT.get("ceph_debug"):
            setup_ceph_debug()
            managed_resources.setdefault("cephConfig", {}).update(
                {"reconcileStrategy": "ignore"}
            )
        create_public_net = config.ENV_DATA["multus_create_public_net"]
        create_cluster_net = config.ENV_DATA["multus_create_cluster_net"]

        if config.ENV_DATA.get("is_multus_enabled"):
            public_net_name = config.ENV_DATA["multus_public_net_name"]
            public_net_namespace = config.ENV_DATA["multus_public_net_namespace"]
            cluster_net_name = config.ENV_DATA["multus_cluster_net_name"]
            cluster_net_namespace = config.ENV_DATA["multus_cluster_net_namespace"]
            selector_data = {}
            if create_public_net:
                public_selector_data = {
                    "public": f"{public_net_namespace}/{public_net_name}"
                }
                selector_data.update(public_selector_data)
            if create_cluster_net:
                cluster_selector_data = {
                    "cluster": f"{cluster_net_namespace}/{cluster_net_name}"
                }
                selector_data.update(cluster_selector_data)
            cluster_data["spec"]["network"] = {
                "provider": "multus",
                "selectors": selector_data,
            }

        # Enable in-transit encryption.
        cluster_data = add_in_transit_encryption_to_cluster_data(cluster_data)

        # Use Custom Storageclass Names
        if config.ENV_DATA.get("custom_default_storageclass_names"):
            storageclassnames = config.ENV_DATA.get("storageclassnames")

            keys_to_update = [
                constants.OCS_COMPONENTS_MAP["cephfs"],
                constants.OCS_COMPONENTS_MAP["rgw"],
                constants.OCS_COMPONENTS_MAP["blockpools"],
                constants.OCS_COMPONENTS_MAP["cephnonresilentpools"],
            ]

            cluster_data.setdefault("spec", {}).setdefault("managedResources", {})

            for key in keys_to_update:
                if storageclassnames.get(key):
                    cluster_data["spec"]["managedResources"][key] = {
                        "storageClassName": storageclassnames[key]
                    }

            if cluster_data["spec"].get("nfs"):
                cluster_data["spec"]["nfs"] = {
                    "storageClassName": storageclassnames["nfs"]
                }

            if cluster_data["spec"].get("encryption"):
                cluster_data["spec"]["encryption"] = {
                    "storageClassName": storageclassnames["encryption"]
                }
        performance_profile = config.ENV_DATA.get("performance_profile")
        if performance_profile:
            cluster_data["spec"]["resourceProfile"] = performance_profile
        # Bluestore-rdr for RDR greenfield deployments: 4.14 onwards until 4.17
        if (
            (
                version.VERSION_4_14
                <= version.get_semantic_ocs_version_from_config()
                <= version.VERSION_4_17
            )
            and config.multicluster
            and (config.MULTICLUSTER.get("multicluster_mode") == "regional-dr")
            and config.ENV_DATA.get("rdr_osd_deployment_mode")
            == constants.RDR_OSD_MODE_GREENFIELD
        ):
            rdr_bluestore_annotation = {
                "ocs.openshift.io/clusterIsDisasterRecoveryTarget": "true"
            }
            merge_dict(
                cluster_data, {"metadata": {"annotations": rdr_bluestore_annotation}}
            )
        if (
            version.get_semantic_ocs_version_from_config() >= version.VERSION_4_19
            and config.MULTICLUSTER.get("multicluster_mode") == "regional-dr"
        ):
            api_server_exported_address_annotation = {
                "ocs.openshift.io/api-server-exported-address": (
                    f'{config.ENV_DATA["cluster_name"]}.'
                    f"ocs-provider-server.openshift-storage.svc.clusterset.local:50051"
                )
            }
            merge_dict(
                cluster_data,
                {"metadata": {"annotations": api_server_exported_address_annotation}},
            )
        if config.ENV_DATA.get("noobaa_external_pgsql"):
            log_step(
                "Creating external pgsql DB for NooBaa and correct StorageCluster data"
            )
            pgsql_data = config.AUTH["pgsql"]
            user = pgsql_data["username"]
            password = pgsql_data["password"]
            host = pgsql_data["host"]
            port = pgsql_data["port"]
            pgsql_manager = pgsql.PgsqlManager(
                username=user,
                password=password,
                host=host,
                port=port,
            )
            cluster_name = config.ENV_DATA["cluster_name"]
            db_name = f"nbcore_{cluster_name.replace('-', '_')}"
            pgsql_manager.create_database(
                db_name=db_name, extra_params="WITH LC_COLLATE = 'C' TEMPLATE template0"
            )
            create_external_pgsql_secret()
            cluster_data["spec"]["multiCloudGateway"] = {
                "externalPgConfig": {"pgSecretName": constants.NOOBAA_POSTGRES_SECRET}
            }
        # To be able to verify: https://bugzilla.redhat.com/show_bug.cgi?id=2276694
        wait_timeout_for_healthy_osd_in_minutes = config.ENV_DATA.get(
            "wait_timeout_for_healthy_osd_in_minutes"
        )
        # For testing: https://issues.redhat.com/browse/RHSTOR-5929
        ceph_threshold_backfill_full_ratio = config.ENV_DATA.get(
            "ceph_threshold_backfill_full_ratio"
        )
        ceph_threshold_full_ratio = config.ENV_DATA.get("ceph_threshold_full_ratio")
        ceph_threshold_near_full_ratio = config.ENV_DATA.get(
            "ceph_threshold_near_full_ratio"
        )

        osd_maintenance_timeout = config.ENV_DATA.get("osd_maintenance_timeout")

        # For testing: https://issues.redhat.com/browse/RHSTOR-5758
        skip_upgrade_checks = config.ENV_DATA.get("skip_upgrade_checks")
        continue_upgrade_after_checks_even_if_not_healthy = config.ENV_DATA.get(
            "continue_upgrade_after_checks_even_if_not_healthy"
        )
        upgrade_osd_requires_healthy_pgs = config.ENV_DATA.get(
            "upgrade_osd_requires_healthy_pgs"
        )
        wipe_devices_from_other_clusters = config.ENV_DATA.get(
            "wipe_devices_from_other_clusters", False
        )

        set_managed_resources_ceph_cluster = (
            wait_timeout_for_healthy_osd_in_minutes
            or ceph_threshold_backfill_full_ratio
            or ceph_threshold_full_ratio
            or ceph_threshold_near_full_ratio
            or osd_maintenance_timeout
            or skip_upgrade_checks is not None
            or continue_upgrade_after_checks_even_if_not_healthy is not None
            or upgrade_osd_requires_healthy_pgs is not None
            or wipe_devices_from_other_clusters
        )
        if set_managed_resources_ceph_cluster:
            cluster_data.setdefault("spec", {}).setdefault(
                "managedResources", {}
            ).setdefault("cephCluster", {})
            managed_resources_ceph_cluster = cluster_data["spec"]["managedResources"][
                "cephCluster"
            ]
            if wait_timeout_for_healthy_osd_in_minutes:
                managed_resources_ceph_cluster["waitTimeoutForHealthyOSDInMinutes"] = (
                    wait_timeout_for_healthy_osd_in_minutes
                )
            if ceph_threshold_backfill_full_ratio:
                managed_resources_ceph_cluster["backfillFullRatio"] = (
                    ceph_threshold_backfill_full_ratio
                )
            if ceph_threshold_full_ratio:
                managed_resources_ceph_cluster["fullRatio"] = ceph_threshold_full_ratio
            if ceph_threshold_near_full_ratio:
                managed_resources_ceph_cluster["nearFullRatio"] = (
                    ceph_threshold_near_full_ratio
                )

            if osd_maintenance_timeout:
                managed_resources_ceph_cluster["osdMaintenanceTimeout"] = (
                    osd_maintenance_timeout
                )

            if skip_upgrade_checks is not None:
                managed_resources_ceph_cluster["skipUpgradeChecks"] = (
                    skip_upgrade_checks
                )

            if continue_upgrade_after_checks_even_if_not_healthy is not None:
                managed_resources_ceph_cluster[
                    "continueUpgradeAfterChecksEvenIfNotHealthy"
                ] = continue_upgrade_after_checks_even_if_not_healthy

            if upgrade_osd_requires_healthy_pgs is not None:
                managed_resources_ceph_cluster["upgradeOSDRequiresHealthyPGs"] = (
                    upgrade_osd_requires_healthy_pgs
                )
            # Flag to enable wiping devices that were used by other Ceph clusters
            if wipe_devices_from_other_clusters:
                logger.info(
                    "Enabling cleanupPolicy.wipeDevicesFromOtherClusters on CephCluster"
                )
                cp = managed_resources_ceph_cluster.setdefault("cleanupPolicy", {})
                cp["wipeDevicesFromOtherClusters"] = True

        storage_cluster_override = config.DEPLOYMENT.get("storage_cluster_override", {})
        if storage_cluster_override:
            logger.info(
                f"Override storage cluster data with: {storage_cluster_override}"
            )
            merge_dict(cluster_data, storage_cluster_override)
        cluster_data_yaml = tempfile.NamedTemporaryFile(
            mode="w+", prefix="cluster_storage", delete=False
        )
        templating.dump_data_to_temp_yaml(cluster_data, cluster_data_yaml.name)

        log_step("Create StorageCluster CR")

        storage_cluster_obj = ocp.OCP(
            kind=constants.STORAGECLUSTER,
            namespace=config.ENV_DATA["cluster_namespace"],
        )
        is_storagecluster = storage_cluster_obj.is_exist(
            resource_name=constants.DEFAULT_STORAGE_CLUSTER
        )

        with open(cluster_data_yaml.name, "r") as f:
            data = yaml.safe_load(f)

        logger.info(
            "CLUSTER_DATA_YAML:\n%s",
            yaml.dump(data, sort_keys=False, default_flow_style=False),
        )

        if config.ENV_DATA.get("odf_provider_mode_deployment", False):
            if not is_storagecluster:
                run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=1200)
            else:
                logger.info(
                    f"StorageCluster {constants.DEFAULT_STORAGE_CLUSTER} already exists, skipping creation."
                )
        else:
            run_cmd(f"oc create -f {cluster_data_yaml.name}", timeout=1200)

        if config.DEPLOYMENT["infra_nodes"]:
            log_step("Labeling infra nodes")
            _ocp = ocp.OCP(kind="node")
            _ocp.exec_oc_cmd(
                command=f"annotate namespace {config.ENV_DATA['cluster_namespace']} "
                f"{constants.NODE_SELECTOR_ANNOTATION}"
            )


def create_external_pgsql_secret():
    """
    Creates secret for external PgSQL to be used by Noobaa
    """
    secret_data = templating.load_yaml(constants.EXTERNAL_PGSQL_NOOBAA_SECRET_YAML)
    secret_data["metadata"]["namespace"] = config.ENV_DATA["cluster_namespace"]
    pgsql_data = config.AUTH["pgsql"]
    user = pgsql_data["username"]
    password = pgsql_data["password"]
    host = pgsql_data["host"]
    port = pgsql_data["port"]
    cluster_name = config.ENV_DATA["cluster_name"].replace("-", "_")
    secret_data["stringData"][
        "db_url"
    ] = f"postgres://{user}:{password}@{host}:{port}/nbcore_{cluster_name}"

    secret_data_yaml = tempfile.NamedTemporaryFile(
        mode="w+", prefix="external_pgsql_noobaa_secret", delete=False
    )
    templating.dump_data_to_temp_yaml(secret_data, secret_data_yaml.name)
    logger.info("Creating external PgSQL Noobaa secret")
    run_cmd(f"oc create -f {secret_data_yaml.name}")
