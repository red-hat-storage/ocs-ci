import logging

import pytest

logger = logging.getLogger(__name__)


class TestOBCQuota:
    """
    Test OBC Quota feature
    """

    @pytest.mark.parametrize(
        argnames="amount,interface,quota",
        argvalues=[
            pytest.param(
                *[1, "RGW-OC", {"maxObjects": "1", "maxSize": "1M"}],
            ),
        ],
    )
    def test_obc_quota(self, rgw_bucket_factory, amount, interface, quota):
        """
        Test OBC quota feature
            * create OBC with some quota set
            * check if the quota works
            * change the quota
            * check if the new quota works
        """
        bucket = rgw_bucket_factory(amount, interface, quota=quota)[0]
        logging.info("Bucket created: {}".format(bucket.name))

        # write objects more than specified in the quota

        # change the quota

        # write objects equal to quota, see if it fails

        # write objects more than the new quota, see if it works
