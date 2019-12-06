# -*- coding: utf8 -*-
"""
Test cases here performs Prometheus queries directly without a workload, to
check that OCS Monitoring is configured and available as expected.
"""

import logging
from datetime import datetime

import pytest
import yaml

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, ocp
from ocs_ci.ocs.resources import pod
from ocs_ci.utility.prometheus import PrometheusAPI


logger = logging.getLogger(__name__)


@pytest.mark.deployment
@pytest.mark.polarion_id("OCS-1261")
def test_monitoring_enabled():
    """
    OCS Monitoring is enabled after OCS installation (which is why this test
    has a deployment marker) by asking for values of one ceph and one noobaa
    related metrics.
    """
    prometheus = PrometheusAPI()

    # ask for values of ceph_pool_stored metric
    logger.info("Checking that ceph data are provided in OCS monitoring")
    result = prometheus.query('ceph_pool_stored')
    # check that we actually received some values
    assert len(result) > 0
    for metric in result:
        _ , value = metric['value']
        assert int(value) >= 0
    # additional check that values makes at least some sense
    logger.info(
        "Checking that size of ceph_pool_stored result matches number of pools")
    ct_pod = pod.get_ceph_tools_pod()
    ceph_pools = ct_pod.exec_ceph_cmd("ceph osd pool ls")
    assert len(result) == len(ceph_pools)

    # again for a noobaa metric
    logger.info("Checking that MCG/NooBaa data are provided in OCS monitoring")
    result = prometheus.query('NooBaa_bucket_status')
    # check that we actually received some values
    assert len(result) > 0
    for metric in result:
        _ , value = metric['value']
        assert int(value) >= 0


@pytest.mark.polarion_id("OCS-1265")
def test_ceph_mgr_dashboard_not_deployed():
    """
    Check that `ceph mgr dashboard`_ is not deployed after installation of OCS
    (this is upstream rook feature not supported in downstream OCS).

    .. _`ceph mgr dashboard`: https://rook.io/docs/rook/v1.0/ceph-dashboard.html
    """
    logger.info("Checking that there is no ceph mgr dashboard pod deployed")
    ocp_pod = ocp.OCP(
        kind=constants.POD,
        namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    # if there is no "items" in the reply, OCS is very broken
    ocs_pods= ocp_pod.get()['items']
    for pod in ocs_pods:
        # just making the assumptions explicit
        assert pod['kind'] == constants.POD
        pod_name = pod['metadata']['name']
        msg = "ceph mgr dashboard should not be deployed as part of OCS"
        assert "dashboard" not in pod_name, msg
        assert "ceph-mgr-dashboard" not in pod_name, msg

    logger.info("Checking that there is no ceph mgr dashboard route")
    ocp_route = ocp.OCP(kind=constants.ROUTE)
    for route in ocp_route.get(all_namespaces=True)['items']:
        # just making the assumptions explicit
        assert route['kind'] == constants.ROUTE
        route_name = route['metadata']['name']
        msg = "ceph mgr dashboard route should not be deployed as part of OCS"
        assert "ceph-mgr-dashboard" not in route_name, msg


@pytest.mark.bugzilla("1779336")
@pytest.mark.polarion_id("OCS-1267")
def test_ceph_rbd_metrics_available():
    """
    Ceph RBD metrics should be provided via OCP Prometheus as well.
    See also: https://ceph.com/rbd/new-in-nautilus-rbd-performance-monitoring/
    """
    # this is not a full list, but it is enough to check whether we have
    # rbd metrics available via OCS monitoring
    list_of_metrics = [
        "ceph_rbd_write_ops",
        "ceph_rbd_read_ops",
        "ceph_rbd_write_bytes",
        "ceph_rbd_read_bytes",
        "ceph_rbd_write_latency_sum",
        "ceph_rbd_write_latency_count"]

    prometheus = PrometheusAPI()

    list_of_metrics_without_results = []
    for metric in list_of_metrics:
        result = prometheus.query(metric)
        # check that we actually received some values
        if len(result) == 0:
            logger.error(f"failed to get results for {metric}")
            list_of_metrics_without_results.append(metric)
    msg = (
        "OCS Monitoring should provide some value(s) for tested rbd metrics, "
        "so that the list of metrics without results is empty.")
    assert list_of_metrics_without_results == [], msg


@pytest.mark.polarion_id("OCS-1268")
def test_ceph_metrics_available():
    """
    Ceph metrics as listed in KNIP-634 should be provided via OCP Prometheus.

    Ceph Object Gateway https://docs.ceph.com/docs/master/radosgw/ is
    deployed on on-prem platforms only (such as VMWare - see BZ 1763150),
    so this test case ignores failures for ceph_rgw_* and ceph_objecter_*
    metrics when running on cloud platforms (such as AWS).
    """
    # this list is taken from spreadsheet attached to KNIP-634
    list_of_metrics = [
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
        "ceph_objecter_0x5633ca03ff80_op_rmw",
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
        "ceph_pool_recovering_objects_per_sec"]

    platforms_without_rgw = [constants.AWS_PLATFORM]
    current_platform = config.ENV_DATA['platform'].lower()

    prometheus = PrometheusAPI()

    list_of_metrics_without_results = []
    for metric in list_of_metrics:
        result = prometheus.query(metric)
        # check that we actually received some values
        if len(result) == 0:
            # Ceph Object Gateway https://docs.ceph.com/docs/master/radosgw/ is
            # deployed on on-prem platforms only, so we are going to ignore
            # missing metrics from these components on such platforms.
            is_rgw_metric = (
                metric.startswith("ceph_rgw") or
                metric.startswith("ceph_objecter"))
            if current_platform in platforms_without_rgw and is_rgw_metric:
                msg = (
                    f"failed to get results for {metric}, "
                    f"but it is expected on {current_platform}")
                logger.info(msg)
            else:
                logger.error(f"failed to get results for {metric}")
                list_of_metrics_without_results.append(metric)
    msg = (
        "OCS Monitoring should provide some value(s) for all tested metrics, "
        "so that the list of metrics without results is empty.")
    assert list_of_metrics_without_results == [], msg


@pytest.mark.polarion_id("OCS-1302")
def test_monitoring_reporting_ok_when_idle(workload_idle):
    """
    When nothing is happening, OCP Prometheus reports OCS status as OK.

    If this test case fails, the status is either reported wrong or the
    cluster is in a broken state. Either way, a failure here is not good.
    """
    prometheus = PrometheusAPI()

    health_result = prometheus.query_range(
        query='ceph_health_status',
        start=workload_idle['start'],
        end=workload_idle['stop'],
        step=15)
    health_validation = prometheus.check_query_range_result(
        result=health_result,
        good_values=[0],
        bad_values=[1],
        exp_metric_num=1)
    health_msg = "ceph_health_status {} report 0 (health ok) as expected"
    if health_validation:
        health_msg = health_msg.format('does')
        logger.info(health_msg)
    else:
        health_msg = health_msg.format('should')
        logger.error(health_msg)

    mon_result = prometheus.query_range(
        query='ceph_mon_quorum_status',
        start=workload_idle['start'],
        end=workload_idle['stop'],
        step=15)
    mon_validation = prometheus.check_query_range_result(
        result=mon_result,
        good_values=[1],
        bad_values=[0],
        exp_metric_num=workload_idle['result']['mon_num'])
    mon_msg = "ceph_mon_quorum_status {} indicate no problems with quorum"
    if mon_validation:
        mon_msg = mon_msg.format('does')
        logger.info(mon_msg)
    else:
        mon_msg = mon_msg.format('should')
        logger.error(mon_msg)

    osd_validations = []
    for metric in ("ceph_osd_up", "ceph_osd_in"):
        osd_result = prometheus.query_range(
            query=metric,
            start=workload_idle['start'],
            end=workload_idle['stop'],
            step=15)
        osd_validation = prometheus.check_query_range_result(
            result=osd_result,
            good_values=[1],
            bad_values=[0],
            exp_metric_num=workload_idle['result']['osd_num'])
        osd_validations.append(osd_validation)
        osd_msg = "{} metric {} indicate no problems with OSDs"
        if osd_validation:
            osd_msg = osd_msg.format(metric, 'does')
            logger.info(osd_msg)
        else:
            osd_msg = osd_msg.format(metric, 'should')
            logger.error(osd_msg)

    # after logging everything properly, make the test fail if necessary
    # see ERRORs reported in the test log for details
    assert health_validation, health_msg
    assert mon_validation, mon_msg
    osds_msg = "ceph_osd_{up,in} metrics should indicate no OSD issues"
    assert all(osd_validations), osds_msg
