import pytest

import logging

from ocs_ci.framework.pytest_customization.marks import (
    tier2,
    red_squad,
    mcg,
)
from ocs_ci.framework.testlib import MCGTest

logger = logging.getLogger(__name__)


@mcg
@red_squad
@tier2
class TestMcgCli(MCGTest):
    """
    Tests for mcg cli commands validation
    """

    @pytest.mark.parametrize(
        argnames="count",
        argvalues=[
            pytest.param(3),
        ],
    )
    def test_bucket_list_command(
        self,
        count,
        mcg_obj,
        bucket_factory,
    ):
        """
        Test proper output of MCG CLI bucket list command.
        The method does the following:
         1) runs this command to count the number of existing buckets
         2) creates number of buckets
         3) runs bucket list command again
         4) verifies that the number of current buckets equals the number of previously existing and the added ones

        Args:
           count (int): number of buckets to create

        """
        bucket_lst_res = mcg_obj.exec_mcg_cmd("bucket list").stdout.split("\n")
        bucket_names = [name.strip() for name in bucket_lst_res if name][
            1:
        ]  # get rid of empty strings and the title
        existing_buckets_num = len(bucket_names)
        logger.info(f"{existing_buckets_num} bucket(s) exist")

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
