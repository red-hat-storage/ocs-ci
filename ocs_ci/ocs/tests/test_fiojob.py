# -*- coding: utf8 -*-

from unittest.mock import patch, Mock
import logging
import os
import textwrap

import pytest
import yaml

from ocs_ci.ocs import constants
from ocs_ci.ocs import fiojob


HERE = os.path.abspath(os.path.dirname(__file__))


@pytest.fixture
def fio_json_output():
    """
    Example of fio --output-format=json output. Based on actual test run.
    """
    filename = os.path.join(HERE, "fio.output")
    with open(filename, "r") as fio_output:
        content = fio_output.read()
    return content


@pytest.fixture
def fio_json_output_with_error(fio_json_output):
    """
    Example of fio --output-format=json output, with io_u error. Based on
    actual test run.
    """
    err_line = (
        "fio: io_u error on file /mnt/target/simple-write.0.0: "
        "No space left on device: write offset=90280222720, buflen=4096"
    )
    return err_line + "\n" + fio_json_output


@pytest.fixture
def fio_json_output_broken():
    """
    Example of broken, unparseable json output.
    """
    fio_out = textwrap.dedent(
        """
        {
          "fio version" : "fio-3.7",
          "timestamp" : 1584531581,
          "timestamp_ms" : 1584531581691,
          "time" : "Wed Mar 18 11:39:41 2020",
          "jobs" : [
        """
    )
    return fio_out


def test_fio_to_dict_empty():
    assert fiojob.fio_to_dict("") is None


def test_fio_to_dict_without_error(fio_json_output):
    fio_dict = fiojob.fio_to_dict(fio_json_output)
    assert isinstance(fio_dict, dict)
    assert len(fio_dict["jobs"]) == 1


def test_fio_to_dict_with_error(fio_json_output_with_error):
    fio_dict = fiojob.fio_to_dict(fio_json_output_with_error)
    assert isinstance(fio_dict, dict)
    assert len(fio_dict["jobs"]) == 1


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
    ceph_df_out = textwrap.dedent(
        """
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
        """
    )
    return yaml.safe_load(ceph_df_out)


def test_get_storageutilization_size_empty100percent(ceph_df_json_output_clusterempty):
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
        pool_stats = ceph_df_json_output_clusterempty["pools"][2]["stats"]
        expected_pvc_size = int(pool_stats["max_avail"] / 2**30)  # GiB
        assert pvc_size == expected_pvc_size


def test_get_timeout_basic():
    """
    Writing on 1 GiB volume with 1 GiB/s should give us 1s timeout
    """
    fio_min_mbps = 2**10  # MiB/s
    pvc_size = 1  # GiB
    assert fiojob.get_timeout(fio_min_mbps, pvc_size) == 1


def test_get_sc_name_default():
    """
    Checking that we get correct storage class by default.
    """
    assert fiojob.get_sc_name("foo_rbd") == constants.DEFAULT_STORAGECLASS_RBD
    assert fiojob.get_sc_name("bar_cephfs") == constants.DEFAULT_STORAGECLASS_CEPHFS
