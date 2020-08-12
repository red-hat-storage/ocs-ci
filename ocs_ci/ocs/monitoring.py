import logging
import yaml
import json

from ocs_ci.ocs.ocp import OCP
from ocs_ci.utility import templating
from ocs_ci.ocs import constants, defaults
from ocs_ci.ocs.resources.pvc import get_all_pvcs, PVC
from ocs_ci.ocs.resources.pod import get_pod_obj
from tests import helpers
import ocs_ci.utility.prometheus
from ocs_ci.ocs.exceptions import (
    UnexpectedBehaviour,
    ServiceUnavailable,
    CommandFailed,
)
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)

ceph_metrics = [
    "ceph_bluestore_state_aio_wait_lat_sum",
    "ceph_paxos_store_state_latency_sum",
    "ceph_osd_op_out_bytes",
    "ceph_pg_incomplete",
    "ceph_bluestore_submit_lat_sum",
    "ceph_paxos_commit",
    "ceph_paxos_new_pn_latency_count",
    "ceph_osd_op_r_process_latency_count",
    "ceph_osd_flag_norebalance",
    "ceph_bluestore_submit_lat_count",
    "ceph_osd_in",
    "ceph_bluestore_kv_final_lat_sum",
    "ceph_paxos_collect_keys_sum",
    "ceph_paxos_accept_timeout",
    "ceph_paxos_begin_latency_count",
    "ceph_bluefs_wal_total_bytes",
    "ceph_osd_flag_nobackfill",
    "ceph_paxos_refresh",
    "ceph_bluestore_read_lat_count",
    "ceph_pg_degraded",
    "ceph_mon_num_sessions",
    "ceph_objecter_op_rmw",
    "ceph_bluefs_bytes_written_wal",
    "ceph_mon_num_elections",
    "ceph_rocksdb_compact",
    "ceph_bluestore_kv_sync_lat_sum",
    "ceph_osd_op_process_latency_count",
    "ceph_osd_op_w_prepare_latency_count",
    "ceph_pool_stored",
    "ceph_objecter_op_active",
    "ceph_pg_backfill_unfound",
    "ceph_num_objects_degraded",
    "ceph_osd_flag_nodeep_scrub",
    "ceph_osd_apply_latency_ms",
    "ceph_paxos_begin_latency_sum",
    "ceph_osd_flag_noin",
    "ceph_osd_op_r",
    "ceph_osd_op_rw_prepare_latency_sum",
    "ceph_paxos_new_pn",
    "ceph_rgw_qlen",
    "ceph_rgw_req",
    "ceph_rocksdb_get_latency_count",
    "ceph_pool_max_avail",
    "ceph_pool_rd",
    "ceph_rgw_cache_miss",
    "ceph_paxos_commit_latency_count",
    "ceph_bluestore_throttle_lat_count",
    "ceph_paxos_lease_ack_timeout",
    "ceph_bluestore_commit_lat_sum",
    "ceph_paxos_collect_bytes_sum",
    "ceph_cluster_total_used_raw_bytes",
    "ceph_pg_stale",
    "ceph_health_status",
    "ceph_pool_wr_bytes",
    "ceph_osd_op_rw_latency_count",
    "ceph_paxos_collect_uncommitted",
    "ceph_osd_op_rw_latency_sum",
    "ceph_paxos_share_state",
    "ceph_pool_stored_raw",
    "ceph_osd_op_r_prepare_latency_sum",
    "ceph_bluestore_kv_flush_lat_sum",
    "ceph_osd_op_rw_process_latency_sum",
    "ceph_osd_metadata",
    "ceph_rocksdb_rocksdb_write_memtable_time_count",
    "ceph_paxos_collect_latency_count",
    "ceph_pg_undersized",
    "ceph_osd_op_rw_prepare_latency_count",
    "ceph_paxos_collect_latency_sum",
    "ceph_rocksdb_rocksdb_write_delay_time_count",
    "ceph_objecter_op_rmw",
    "ceph_paxos_begin_bytes_sum",
    "ceph_pg_recovering",
    "ceph_pg_peering",
    "ceph_osd_numpg",
    "ceph_osd_flag_noout",
    "ceph_pg_inconsistent",
    "ceph_osd_stat_bytes",
    "ceph_rocksdb_submit_sync_latency_sum",
    "ceph_rocksdb_compact_queue_merge",
    "ceph_paxos_collect_bytes_count",
    "ceph_osd_op",
    "ceph_paxos_commit_keys_sum",
    "ceph_osd_op_rw_in_bytes",
    "ceph_osd_op_rw_out_bytes",
    "ceph_bluefs_bytes_written_sst",
    "ceph_rgw_put",
    "ceph_osd_op_rw_process_latency_count",
    "ceph_rocksdb_compact_queue_len",
    "ceph_pool_wr",
    "ceph_bluestore_throttle_lat_sum",
    "ceph_bluefs_slow_used_bytes",
    "ceph_osd_op_r_latency_sum",
    "ceph_bluestore_kv_flush_lat_count",
    "ceph_rocksdb_compact_range",
    "ceph_osd_op_latency_sum",
    "ceph_mon_session_add",
    "ceph_paxos_share_state_keys_count",
    "ceph_num_objects_misplaced",
    "ceph_paxos_collect",
    "ceph_osd_op_w_in_bytes",
    "ceph_osd_op_r_process_latency_sum",
    "ceph_paxos_start_peon",
    "ceph_cluster_total_bytes",
    "ceph_mon_session_trim",
    "ceph_pg_recovery_wait",
    "ceph_rocksdb_get_latency_sum",
    "ceph_rocksdb_submit_transaction_sync",
    "ceph_osd_op_rw",
    "ceph_paxos_store_state_keys_count",
    "ceph_rocksdb_rocksdb_write_delay_time_sum",
    "ceph_pool_objects",
    "ceph_pg_backfill_wait",
    "ceph_objecter_op_r",
    "ceph_objecter_op_active",
    "ceph_objecter_op_w",
    "ceph_osd_recovery_ops",
    "ceph_bluefs_logged_bytes",
    "ceph_rocksdb_get",
    "ceph_pool_metadata",
    "ceph_bluefs_db_total_bytes",
    "ceph_rgw_put_initial_lat_sum",
    "ceph_pg_recovery_toofull",
    "ceph_osd_op_w_latency_count",
    "ceph_rgw_put_initial_lat_count",
    "ceph_mon_metadata",
    "ceph_bluestore_commit_lat_count",
    "ceph_bluestore_state_aio_wait_lat_count",
    "ceph_pg_unknown",
    "ceph_paxos_begin_bytes_count",
    "ceph_pg_recovery_unfound",
    "ceph_pool_quota_bytes",
    "ceph_pg_snaptrim_wait",
    "ceph_paxos_start_leader",
    "ceph_pg_creating",
    "ceph_mon_election_call",
    "ceph_rocksdb_rocksdb_write_pre_and_post_time_count",
    "ceph_mon_session_rm",
    "ceph_cluster_total_used_bytes",
    "ceph_pg_active",
    "ceph_paxos_store_state",
    "ceph_pg_activating",
    "ceph_paxos_store_state_bytes_count",
    "ceph_osd_op_w_latency_sum",
    "ceph_rgw_keystone_token_cache_hit",
    "ceph_rocksdb_submit_latency_count",
    "ceph_pool_dirty",
    "ceph_paxos_commit_latency_sum",
    "ceph_rocksdb_rocksdb_write_memtable_time_sum",
    "ceph_rgw_metadata",
    "ceph_paxos_share_state_bytes_sum",
    "ceph_osd_op_process_latency_sum",
    "ceph_paxos_begin_keys_sum",
    "ceph_pg_snaptrim_error",
    "ceph_rgw_qactive",
    "ceph_pg_backfilling",
    "ceph_rocksdb_rocksdb_write_pre_and_post_time_sum",
    "ceph_bluefs_wal_used_bytes",
    "ceph_pool_rd_bytes",
    "ceph_pg_deep",
    "ceph_rocksdb_rocksdb_write_wal_time_sum",
    "ceph_osd_op_wip",
    "ceph_pg_backfill_toofull",
    "ceph_osd_flag_noup",
    "ceph_rgw_get_initial_lat_sum",
    "ceph_pg_scrubbing",
    "ceph_num_objects_unfound",
    "ceph_mon_quorum_status",
    "ceph_paxos_lease_timeout",
    "ceph_osd_op_r_out_bytes",
    "ceph_paxos_begin_keys_count",
    "ceph_bluestore_kv_sync_lat_count",
    "ceph_osd_op_prepare_latency_count",
    "ceph_bluefs_bytes_written_slow",
    "ceph_rocksdb_submit_latency_sum",
    "ceph_pg_repair",
    "ceph_osd_op_r_latency_count",
    "ceph_paxos_share_state_keys_sum",
    "ceph_paxos_store_state_bytes_sum",
    "ceph_osd_op_latency_count",
    "ceph_paxos_commit_bytes_count",
    "ceph_paxos_restart",
    "ceph_rgw_get_initial_lat_count",
    "ceph_pg_down",
    "ceph_bluefs_slow_total_bytes",
    "ceph_paxos_collect_timeout",
    "ceph_pg_peered",
    "ceph_osd_commit_latency_ms",
    "ceph_osd_op_w_process_latency_sum",
    "ceph_osd_weight",
    "ceph_paxos_collect_keys_count",
    "ceph_paxos_share_state_bytes_count",
    "ceph_osd_op_w_prepare_latency_sum",
    "ceph_bluestore_read_lat_sum",
    "ceph_osd_flag_noscrub",
    "ceph_osd_stat_bytes_used",
    "ceph_osd_flag_norecover",
    "ceph_pg_clean",
    "ceph_paxos_begin",
    "ceph_mon_election_win",
    "ceph_osd_op_w_process_latency_count",
    "ceph_rgw_get_b",
    "ceph_rgw_failed_req",
    "ceph_rocksdb_rocksdb_write_wal_time_count",
    "ceph_rgw_keystone_token_cache_miss",
    "ceph_disk_occupation",
    "ceph_pg_snaptrim",
    "ceph_paxos_store_state_keys_sum",
    "ceph_osd_numpg_removing",
    "ceph_pg_remapped",
    "ceph_paxos_commit_keys_count",
    "ceph_pg_forced_backfill",
    "ceph_paxos_new_pn_latency_sum",
    "ceph_osd_op_in_bytes",
    "ceph_paxos_store_state_latency_count",
    "ceph_paxos_refresh_latency_count",
    "ceph_rgw_get",
    "ceph_pg_total",
    "ceph_osd_op_r_prepare_latency_count",
    "ceph_rgw_cache_hit",
    "ceph_objecter_op_w",
    "ceph_rocksdb_submit_transaction",
    "ceph_objecter_op_r",
    "ceph_bluefs_num_files",
    "ceph_osd_up",
    "ceph_rgw_put_b",
    "ceph_mon_election_lose",
    "ceph_osd_op_prepare_latency_sum",
    "ceph_bluefs_db_used_bytes",
    "ceph_bluestore_kv_final_lat_count",
    "ceph_pool_quota_objects",
    "ceph_osd_flag_nodown",
    "ceph_pg_forced_recovery",
    "ceph_paxos_refresh_latency_sum",
    "ceph_osd_recovery_bytes",
    "ceph_osd_op_w",
    "ceph_paxos_commit_bytes_sum",
    "ceph_bluefs_log_bytes",
    "ceph_rocksdb_submit_sync_latency_count",
    "ceph_pool_num_bytes_recovered",
    "ceph_pool_num_objects_recovered",
    "ceph_pool_recovering_bytes_per_sec",
    "ceph_pool_recovering_keys_per_sec",
    "ceph_pool_recovering_objects_per_sec"
]


def create_configmap_cluster_monitoring_pod(sc_name=None, telemeter_server_url=None):
    """
    Create a configmap named cluster-monitoring-config based on the arguments.

    Args:
        sc_name (str): Name of the storage class which will be used for
            persistent storage needs of OCP Prometheus and Alert Manager.
            If not defined, the related options won't be present in the
            monitoring config map and the default (non persistent) storage
            will be used for OCP Prometheus and Alert Manager.
        telemeter_server_url (str): URL of Telemeter server where telemeter
            client (running in the cluster) will send it's telemetry data. If
            not defined, related option won't be present in the monitoring
            config map and the default (production) telemeter server will
            receive the metrics data.
    """
    logger.info("Creating configmap cluster-monitoring-config")
    config_map = templating.load_yaml(
        constants.CONFIGURE_PVC_ON_MONITORING_POD
    )
    config = yaml.safe_load(config_map['data']['config.yaml'])
    if sc_name is not None:
        logger.info(f"Setting {sc_name} as storage backed for Prometheus and Alertmanager")
        config['prometheusK8s']['volumeClaimTemplate']['spec']['storageClassName'] = sc_name
        config['alertmanagerMain']['volumeClaimTemplate']['spec']['storageClassName'] = sc_name
    else:
        del config['prometheusK8s']
        del config['alertmanagerMain']
    if telemeter_server_url is not None:
        logger.info(f"Setting {telemeter_server_url} as telemeter server url")
        config['telemeterClient'] = {}
        config['telemeterClient']['telemeterServerURL'] = telemeter_server_url
    config = yaml.dump(config)
    config_map['data']['config.yaml'] = config
    assert helpers.create_resource(**config_map)
    ocp = OCP('v1', 'ConfigMap', defaults.OCS_MONITORING_NAMESPACE)
    assert ocp.get(resource_name='cluster-monitoring-config')
    logger.info("Successfully created configmap cluster-monitoring-config")


@retry((AssertionError, CommandFailed), tries=30, delay=10, backoff=1)
def validate_pvc_created_and_bound_on_monitoring_pods():
    """
    Validate pvc's created and bound in state
    on monitoring pods

    Raises:
        AssertionError: If no PVC are created or if any PVC are not
            in the Bound state

    """
    logger.info("Verify pvc are created")
    pvc_list = get_all_pvcs(namespace=defaults.OCS_MONITORING_NAMESPACE)
    logger.info(f"PVC list {pvc_list}")

    assert pvc_list['items'], (
        f"No PVC created in {defaults.OCS_MONITORING_NAMESPACE} namespace"
    )

    # Check all pvc's are in bound state
    for pvc in pvc_list['items']:
        assert pvc['status']['phase'] == constants.STATUS_BOUND, (
            f"PVC {pvc['metadata']['name']} is not Bound"
        )
    logger.info('Verified: Created PVCs are in Bound state.')


def validate_pvc_are_mounted_on_monitoring_pods(pod_list):
    """
    Validate created pvc are mounted on monitoring pods

    Args:
        pod_list (list): List of the pods where pvc are mounted

    """
    for pod in pod_list:
        pod_obj = get_pod_obj(
            name=pod.name, namespace=defaults.OCS_MONITORING_NAMESPACE
        )
        mount_point = pod_obj.exec_cmd_on_pod(
            command="df -kh", out_yaml_format=False,
        )
        assert "/dev/rbd" in mount_point, f"pvc is not mounted on pod {pod.name}"
    logger.info("Verified all pvc are mounted on monitoring pods")


def get_list_pvc_objs_created_on_monitoring_pods():
    """
    Returns list of pvc objects created on monitoring pods

    Returns:
        list: List of pvc objs

    """
    pvc_list = get_all_pvcs(namespace=defaults.OCS_MONITORING_NAMESPACE)
    ocp_pvc_obj = OCP(
        kind=constants.PVC, namespace=defaults.OCS_MONITORING_NAMESPACE
    )
    pvc_obj_list = []
    for pvc in pvc_list['items']:
        pvc_dict = ocp_pvc_obj.get(resource_name=pvc.get('metadata').get('name'))
        pvc_obj = PVC(**pvc_dict)
        pvc_obj_list.append(pvc_obj)
    return pvc_obj_list


@retry(ServiceUnavailable, tries=60, delay=3, backoff=1)
def get_metrics_persistentvolumeclaims_info():
    """
    Returns the created pvc information on prometheus pod

    Returns:
        response.content (dict): The pvc metrics collected on prometheus pod

    """

    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    response = prometheus.get(
        'query?query=kube_pod_spec_volumes_persistentvolumeclaims_info'
    )
    if response.status_code == 503:
        raise ServiceUnavailable("Failed to handle the request")
    return json.loads(response.content.decode('utf-8'))


@retry(UnexpectedBehaviour, tries=60, delay=3, backoff=1)
def check_pvcdata_collected_on_prometheus(pvc_name):
    """
    Checks whether initially pvc related data is collected on pod

    Args:
        pvc_name (str): Name of the pvc

    Returns:
        True on success, raises UnexpectedBehaviour on failures

    """
    logger.info(
        f"Verify for created pvc {pvc_name} related data is collected on prometheus pod"
    )
    pvcs_data = get_metrics_persistentvolumeclaims_info()
    list_pvcs_data = pvcs_data.get('data').get('result')
    pvc_list = [pvc for pvc in list_pvcs_data if pvc_name == pvc.get('metric').get('persistentvolumeclaim')]
    if not pvc_list:
        raise UnexpectedBehaviour(
            f"On prometheus pod for created pvc {pvc_name} related data is not found"
        )
    logger.info(f"Created pvc {pvc_name} data {pvc_list} is collected on prometheus pod")
    return True


def check_ceph_health_status_metrics_on_prometheus(mgr_pod):
    """
    Check ceph health status metric is collected on prometheus pod

    Args:
        mgr_pod (str): Name of the mgr pod

    Returns:
        bool: True on success, false otherwise

    """
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    response = prometheus.get(
        'query?query=ceph_health_status'
    )
    ceph_health_metric = json.loads(response.content.decode('utf-8'))
    return bool(
        [mgr_pod for health_status in ceph_health_metric.get('data').get(
            'result') if mgr_pod == health_status.get('metric').get('pod')]
    )


@retry(AssertionError, tries=20, delay=3, backoff=1)
def prometheus_health_check(name=constants.MONITORING, kind=constants.CLUSTER_OPERATOR):
    """
    Return true if the prometheus cluster is healthy

    Args:
        name (str) : Name of the resources
        kind (str): Kind of the resource

    Returns:
        bool : True on prometheus health is ok, false otherwise

    """
    ocp_obj = OCP(kind=kind)
    health_info = ocp_obj.get(resource_name=name)
    health_conditions = health_info.get('status').get('conditions')

    # Check prometheus is degraded
    # If degraded, degraded value will be True, AVAILABLE is False
    available = False
    degraded = True
    for i in health_conditions:
        if {('type', 'Available'), ('status', 'True')}.issubset(set(i.items())):
            logging.info("Prometheus cluster available value is set true")
            available = True
        if {('status', 'False'), ('type', 'Degraded')}.issubset(set(i.items())):
            logging.info("Prometheus cluster degraded value is set false")
            degraded = False

    if available and not degraded:
        logging.info("Prometheus health cluster is OK")
        return True

    logging.error(f"Prometheus cluster is degraded {health_conditions}")
    return False


def check_ceph_metrics_available():
    """
    Check ceph metrics available

    Returns:
        bool: True on success, false otherwise

    """
    logger.info('check ceph metrics available')
    prometheus = ocs_ci.utility.prometheus.PrometheusAPI()
    list_of_metrics_without_results = []
    for metric in ceph_metrics:
        result = prometheus.query(metric)
        if len(result) == 0:
            list_of_metrics_without_results.append(metric)
        elif type(result[0]) is not dict:
            list_of_metrics_without_results.append(metric)

    if len(list_of_metrics_without_results) == 0:
        logger.info('Get results for all metrics')
        return True
    else:
        for metric_without_results in list_of_metrics_without_results:
            logger.error(f"failed to get results for {metric_without_results}")
        return False
