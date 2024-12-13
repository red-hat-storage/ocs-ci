import logging
import re

import botocore
import pytest

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    acceptance,
    performance,
    skipif_mcg_only,
    runs_on_provider,
    red_squad,
    mcg,
)
from ocs_ci.ocs.bucket_utils import sync_object_directory
from ocs_ci.ocs.constants import DEFAULT_STORAGECLASS_RBD, AWSCLI_TEST_OBJ_DIR
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.ocs.resources.objectbucket import BUCKET_MAP
from ocs_ci.ocs.resources.pod import get_pod_logs, get_operator_pods
from ocs_ci.framework.testlib import MCGTest
from ocs_ci.framework.pytest_customization.marks import skipif_managed_service

logger = logging.getLogger(__name__)


@mcg
@red_squad
@tier2
class TestMcgCli(MCGTest):
    """
    Tests for mcg cli commands validation
    """

    def test_bucket_list_command(
        self,
        mcg_obj,
        bucket_factory,
    ):
        """
        Test proper output of MCG CLI bucket list command.
        The method does the following:
         1) runs this command to count the number of existing buckets
         2) creates number of buckets
         3) runs bucket list command again a
         4) verifies that the number of current buckets equals the number of previously existing and the added ones

        """
        bucket_lst_res = mcg_obj.exec_mcg_cmd("bucket list").stdout.split("\n")
        bucket_names = [name.strip() for name in bucket_lst_res if name][
            1:
        ]  # get rid of empty strings and the title
        default_bucket_name = "first.bucket"
        existing_buckets_num = len(bucket_names)
        assert bucket_names[0] == default_bucket_name, (
            f"First bucket name is {bucket_names[0]}, "
            f" expected {default_bucket_name}."
        )
        logger.info(f"{existing_buckets_num} bucket(s) exist")

        count = 3
        logger.info(f"Creating {count} buckets")
        buckets = bucket_factory(count)
        created_bucket_names = (b.name for b in buckets)
        logger.info("Buckets " + ", ".join(created_bucket_names) + " created")

        bucket_lst_res = mcg_obj.exec_mcg_cmd("bucket list").stdout.split("\n")
        bucket_names = [name.strip() for name in bucket_lst_res if name][
            1:
        ]  # get rid of empty strings and the title
        current_buckets_num = len(bucket_names)
        assert current_buckets_num == existing_buckets_num + count, (
            f"'bucket list' command shows"
            f" {current_buckets_num} buckets"
            f" expected {existing_buckets_num + count}."
        )
        logger.info("'bucket list' command finished successfully")
