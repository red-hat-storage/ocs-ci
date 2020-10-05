# -*- coding: utf8 -*-
"""
OCS Metrics module.

Code in this module Supports monitoring test cases dealing with OCS metrics.
"""

import logging

from ocs_ci.ocs import constants


logger = logging.getLogger(__name__)


# See: https://ceph.com/rbd/new-in-nautilus-rbd-performance-monitoring/
# This is not a full list, but it is enough to check whether we have
# rbd metrics available via OCS monitoring
ceph_rbd_metrics = (
    "ceph_rbd_write_ops",
    "ceph_rbd_read_ops",
    "ceph_rbd_write_bytes",
    "ceph_rbd_read_bytes",
    "ceph_rbd_write_latency_sum",
    "ceph_rbd_write_latency_count")


# Ceph metrics available since OCS 4.2.
# This list is taken from spreadsheet attached to KNIP-634
ceph_metrics = (
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
    "ceph_pool_recovering_objects_per_sec")


# List of all ceph metrics.
ceph_metrics_all = tuple(ceph_metrics + ceph_rbd_metrics)


def get_missing_metrics(prometheus, metrics, current_platform=None):
    """
    Using given prometheus instance, check that all given metrics which are
    expected to be available on current platform are there.

    Args:
        prometheus (ocs_ci.utility.prometheus.PrometheusAPI): prometheus instance
        metrics (list): list or tuple with metrics to be checked
        current_platform (str): name of current platform (optional)

    Returns:
        list: metrics which were not available but should be

    """
    metrics_without_results = []
    for metric in metrics:
        result = prometheus.query(metric)
        # check that we actually received some values
        if len(result) == 0:
            # Ceph Object Gateway https://docs.ceph.com/docs/master/radosgw/ is
            # deployed on on-prem platforms only, so we are going to ignore
            # missing metrics from these components on such platforms.
            # See BZ 1763150
            is_rgw_metric = (
                metric.startswith("ceph_rgw")
                or metric.startswith("ceph_objecter"))
            if current_platform in constants.CLOUD_PLATFORMS and is_rgw_metric:
                msg = (
                    f"failed to get results for {metric}, "
                    f"but it is expected on {current_platform}")
                logger.info(msg)
            else:
                logger.error(f"failed to get results for {metric}")
                metrics_without_results.append(metric)
    return metrics_without_results
