# -*- coding: utf8 -*-

from unittest.mock import patch, Mock
import logging
import textwrap

import pytest
import yaml
import yaml.parser

from ocs_ci.ocs import fiojob


@pytest.fixture
def fio_json_output():
    """
    Example of fio --output-format=json output. Based on actual test run.
    """
    fio_out = textwrap.dedent("""
        {
          "fio version" : "fio-3.7",
          "timestamp" : 1584531581,
          "timestamp_ms" : 1584531581691,
          "time" : "Wed Mar 18 11:39:41 2020",
          "jobs" : [
            {
              "jobname" : "simple-write",
              "groupid" : 0,
              "error" : 0,
              "eta" : 0,
              "elapsed" : 296,
              "job options" : {
                "rw" : "write",
                "buffered" : "1",
                "bs" : "4k",
                "ioengine" : "libaio",
                "directory" : "/mnt/target",
                "fill_device" : "1"
              },
              "read" : {
                "io_bytes" : 0,
                "io_kbytes" : 0,
                "bw_bytes" : 0,
                "bw" : 0,
                "iops" : 0.000000,
                "runtime" : 0,
                "total_ios" : 0,
                "short_ios" : 0,
                "drop_ios" : 0,
                "slat_ns" : {
                  "min" : 0,
                  "max" : 0,
                  "mean" : 0.000000,
                  "stddev" : 0.000000
                },
                "clat_ns" : {
                  "min" : 0,
                  "max" : 0,
                  "mean" : 0.000000,
                  "stddev" : 0.000000,
                  "percentile" : {
                    "1.000000" : 0,
                    "5.000000" : 0,
                    "10.000000" : 0,
                    "20.000000" : 0,
                    "30.000000" : 0,
                    "40.000000" : 0,
                    "50.000000" : 0,
                    "60.000000" : 0,
                    "70.000000" : 0,
                    "80.000000" : 0,
                    "90.000000" : 0,
                    "95.000000" : 0,
                    "99.000000" : 0,
                    "99.500000" : 0,
                    "99.900000" : 0,
                    "99.950000" : 0,
                    "99.990000" : 0
                  }
                },
                "lat_ns" : {
                  "min" : 0,
                  "max" : 0,
                  "mean" : 0.000000,
                  "stddev" : 0.000000
                },
                "bw_min" : 0,
                "bw_max" : 0,
                "bw_agg" : 0.000000,
                "bw_mean" : 0.000000,
                "bw_dev" : 0.000000,
                "bw_samples" : 0,
                "iops_min" : 0,
                "iops_max" : 0,
                "iops_mean" : 0.000000,
                "iops_stddev" : 0.000000,
                "iops_samples" : 0
              },
              "write" : {
                "io_bytes" : 90280222720,
                "io_kbytes" : 88164280,
                "bw_bytes" : 305955830,
                "bw" : 298784,
                "iops" : 74696.251135,
                "runtime" : 295076,
                "total_ios" : 22041071,
                "short_ios" : 0,
                "drop_ios" : 0,
                "slat_ns" : {
                  "min" : 2273,
                  "max" : 2057165385,
                  "mean" : 12424.378866,
                  "stddev" : 1365344.977980
                },
                "clat_ns" : {
                  "min" : 569,
                  "max" : 1883160,
                  "mean" : 634.690945,
                  "stddev" : 958.191583,
                  "percentile" : {
                    "1.000000" : 580,
                    "5.000000" : 580,
                    "10.000000" : 588,
                    "20.000000" : 588,
                    "30.000000" : 588,
                    "40.000000" : 588,
                    "50.000000" : 588,
                    "60.000000" : 596,
                    "70.000000" : 596,
                    "80.000000" : 596,
                    "90.000000" : 732,
                    "95.000000" : 764,
                    "99.000000" : 988,
                    "99.500000" : 1336,
                    "99.900000" : 5664,
                    "99.950000" : 10816,
                    "99.990000" : 31104
                  }
                },
                "lat_ns" : {
                  "min" : 2922,
                  "max" : 2057170104,
                  "mean" : 13112.945516,
                  "stddev" : 1365360.038857
                },
                "bw_min" : 80,
                "bw_max" : 997232,
                "bw_agg" : 100.000000,
                "bw_mean" : 317039.167266,
                "bw_dev" : 177561.303347,
                "bw_samples" : 556,
                "iops_min" : 20,
                "iops_max" : 249308,
                "iops_mean" : 79259.769784,
                "iops_stddev" : 44390.326602,
                "iops_samples" : 556
              },
              "trim" : {
                "io_bytes" : 0,
                "io_kbytes" : 0,
                "bw_bytes" : 0,
                "bw" : 0,
                "iops" : 0.000000,
                "runtime" : 0,
                "total_ios" : 0,
                "short_ios" : 0,
                "drop_ios" : 0,
                "slat_ns" : {
                  "min" : 0,
                  "max" : 0,
                  "mean" : 0.000000,
                  "stddev" : 0.000000
                },
                "clat_ns" : {
                  "min" : 0,
                  "max" : 0,
                  "mean" : 0.000000,
                  "stddev" : 0.000000,
                  "percentile" : {
                    "1.000000" : 0,
                    "5.000000" : 0,
                    "10.000000" : 0,
                    "20.000000" : 0,
                    "30.000000" : 0,
                    "40.000000" : 0,
                    "50.000000" : 0,
                    "60.000000" : 0,
                    "70.000000" : 0,
                    "80.000000" : 0,
                    "90.000000" : 0,
                    "95.000000" : 0,
                    "99.000000" : 0,
                    "99.500000" : 0,
                    "99.900000" : 0,
                    "99.950000" : 0,
                    "99.990000" : 0
                  }
                },
                "lat_ns" : {
                  "min" : 0,
                  "max" : 0,
                  "mean" : 0.000000,
                  "stddev" : 0.000000
                },
                "bw_min" : 0,
                "bw_max" : 0,
                "bw_agg" : 0.000000,
                "bw_mean" : 0.000000,
                "bw_dev" : 0.000000,
                "bw_samples" : 0,
                "iops_min" : 0,
                "iops_max" : 0,
                "iops_mean" : 0.000000,
                "iops_stddev" : 0.000000,
                "iops_samples" : 0
              },
              "sync" : {
                "lat_ns" : {
                  "min" : 0,
                  "max" : 0,
                  "mean" : 0.000000,
                  "stddev" : 0.000000,
                  "percentile" : {
                    "1.000000" : 0,
                    "5.000000" : 0,
                    "10.000000" : 0,
                    "20.000000" : 0,
                    "30.000000" : 0,
                    "40.000000" : 0,
                    "50.000000" : 0,
                    "60.000000" : 0,
                    "70.000000" : 0,
                    "80.000000" : 0,
                    "90.000000" : 0,
                    "95.000000" : 0,
                    "99.000000" : 0,
                    "99.500000" : 0,
                    "99.900000" : 0,
                    "99.950000" : 0,
                    "99.990000" : 0
                  }
                },
                "total_ios" : 0
              },
              "usr_cpu" : 7.279166,
              "sys_cpu" : 26.498687,
              "ctx" : 24791,
              "majf" : 0,
              "minf" : 21,
              "iodepth_level" : {
                "1" : 100.000000,
                "2" : 0.000000,
                "4" : 0.000000,
                "8" : 0.000000,
                "16" : 0.000000,
                "32" : 0.000000,
                ">=64" : 0.000000
              },
              "latency_ns" : {
                "2" : 0.000000,
                "4" : 0.000000,
                "10" : 0.000000,
                "20" : 0.000000,
                "50" : 0.000000,
                "100" : 0.000000,
                "250" : 0.000000,
                "500" : 0.000000,
                "750" : 92.436298,
                "1000" : 6.599584
              },
              "latency_us" : {
                "2" : 0.744950,
                "4" : 0.044621,
                "10" : 0.117499,
                "20" : 0.040293,
                "50" : 0.015126,
                "100" : 0.010000,
                "250" : 0.010000,
                "500" : 0.010000,
                "750" : 0.010000,
                "1000" : 0.010000
              },
              "latency_ms" : {
                "2" : 0.010000,
                "4" : 0.000000,
                "10" : 0.000000,
                "20" : 0.000000,
                "50" : 0.000000,
                "100" : 0.000000,
                "250" : 0.000000,
                "500" : 0.000000,
                "750" : 0.000000,
                "1000" : 0.000000,
                "2000" : 0.000000,
                ">=2000" : 0.000000
              },
              "latency_depth" : 1,
              "latency_target" : 0,
              "latency_percentile" : 100.000000,
              "latency_window" : 0
            }
          ],
          "disk_util" : [
            {
              "name" : "rbd1",
              "read_ios" : 42,
              "write_ios" : 18382,
              "read_merges" : 0,
              "write_merges" : 18322,
              "read_ticks" : 33031,
              "write_ticks" : 50421049,
              "in_queue" : 50444926,
              "util" : 7.917599
            }
          ]
        }
        """)
    return fio_out


@pytest.fixture
def fio_json_output_with_error(fio_json_output):
    """
    Example of fio --output-format=json output, with io_u error. Based on
    actual test run.
    """
    err_line = (
        "fio: io_u error on file /mnt/target/simple-write.0.0: "
        "No space left on device: write offset=90280222720, buflen=4096")
    return err_line + "\n" + fio_json_output


@pytest.fixture
def fio_json_output_broken():
    """
    Example of broken, unparseable json output.
    """
    fio_out = textwrap.dedent("""
        {
          "fio version" : "fio-3.7",
          "timestamp" : 1584531581,
          "timestamp_ms" : 1584531581691,
          "time" : "Wed Mar 18 11:39:41 2020",
          "jobs" : [
        """)
    return fio_out


def test_fio_to_dict_empty():
    assert fiojob.fio_to_dict("") is None


def test_fio_to_dict_without_error(fio_json_output):
    fio_dict = fiojob.fio_to_dict(fio_json_output)
    assert type(fio_dict) == dict
    assert len(fio_dict['jobs']) == 1


def test_fio_to_dict_with_error(fio_json_output_with_error):
    fio_dict = fiojob.fio_to_dict(fio_json_output_with_error)
    assert type(fio_dict) == dict
    assert len(fio_dict['jobs']) == 1


def test_fio_to_dict_output_broken(fio_json_output_broken, caplog):
    caplog.set_level(logging.ERROR)
    with pytest.raises(yaml.parser.ParserError):
        fiojob.fio_to_dict(fio_json_output_broken)
    assert "json output from fio can't be parsed" in caplog.text


@pytest.fixture
def ceph_df_json_output_clusterempty():
    """
    Example of output from ``ceph df --format json-pretty`` command.
    Based on actual test run:
    jnk-ai3c33-t1/jnk-ai3c33-t1_20200318T095635/logs/ocs-ci-logs-1584528704
    """
    ceph_df_out = textwrap.dedent("""
        {
            "stats": {
                "total_bytes": 6593848541184,
                "total_avail_bytes": 6587904737280,
                "total_used_bytes": 2722578432,
                "total_used_raw_bytes": 5943803904,
                "total_used_raw_ratio": 0.0009014165261760354,
                "num_osds": 3,
                "num_per_pool_osds": 3
            },
            "stats_by_class": {
                "ssd": {
                    "total_bytes": 6593848541184,
                    "total_avail_bytes": 6587904737280,
                    "total_used_bytes": 2722578432,
                    "total_used_raw_bytes": 5943803904,
                    "total_used_raw_ratio": 0.0009014165261760354
                }
            },
            "pools": [
                {
                    "name": "ocs-storagecluster-cephblockpool",
                    "id": 1,
                    "stats": {
                        "stored": 762745112,
                        "objects": 442,
                        "kb_used": 2242417,
                        "bytes_used": 2296234200,
                        "percent_used": 0.00040995955350808799,
                        "max_avail": 1866275880960
                    }
                },
                {
                    "name": "ocs-storagecluster-cephfilesystem-metadata",
                    "id": 2,
                    "stats": {
                        "stored": 566061,
                        "objects": 25,
                        "kb_used": 2064,
                        "bytes_used": 2113536,
                        "percent_used": 3.7749603620795824e-07,
                        "max_avail": 1866275880960
                    }
                },
                {
                    "name": "ocs-storagecluster-cephfilesystem-data0",
                    "id": 3,
                    "stats": {
                        "stored": 140263712,
                        "objects": 41,
                        "kb_used": 411264,
                        "bytes_used": 421134336,
                        "percent_used": 7.5212650699540973e-05,
                        "max_avail": 1866275880960
                    }
                }
            ]
        }
        """)
    return yaml.safe_load(ceph_df_out)


def test_get_storageutilization_size_empty100percent(
    ceph_df_json_output_clusterempty
):
    """
    Checking that when asking for 100% target utilization on an empty cluster,
    the pvc_size matches MAX AVAIL value.
    """
    # Mock ceph tools pod object to push particular return value of it's
    # exec_ceph_cmd() method.
    mp = Mock()
    mp.exec_ceph_cmd.return_value = ceph_df_json_output_clusterempty
    with patch("ocs_ci.ocs.resources.pod.get_ceph_tools_pod", return_value=mp):
        ceph_pool = "ocs-storagecluster-cephblockpool"
        target = 1.0
        pvc_size = fiojob.get_storageutilization_size(target, ceph_pool)
        # with 100% utilization target and an empty cluster, the pvc size
        # necessary to utilize all storage space on the cluster should match
        # the value of MAX AVAIL
        expected_pvc_size_bytes = \
            ceph_df_json_output_clusterempty['pools'][2]['stats']['max_avail']
        expected_pvc_size = int(expected_pvc_size_bytes / 2**30)
        assert pvc_size == expected_pvc_size


def test_get_timeout_basic():
    """
    Writing on 1 GiB volume with 1 GiB/s should give us 1s timeout
    """
    fio_min_mbps = 2**10  # MiB/s
    pvc_size = 1  # GiB
    assert fiojob.get_timeout(fio_min_mbps, pvc_size) == 1
