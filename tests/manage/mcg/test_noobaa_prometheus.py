import logging
import json

from ocs_ci.ocs.exceptions import (
    TimeoutExpiredError,
    ReturnedEmptyResponseException,
)
from ocs_ci.framework.pytest_customization.marks import tier2, bugzilla, polarion_id
from ocs_ci.utility.prometheus import PrometheusAPI
from ocs_ci.ocs.bucket_utils import write_random_test_objects_to_bucket
from ocs_ci.utility.retry import retry

logger = logging.getLogger(__name__)


@retry(ReturnedEmptyResponseException, tries=30, delay=10, backoff=1)
def get_bucket_used_bytes_metric(bucket_name):
    response = json.loads(
        PrometheusAPI()
        .get(f'query?query=NooBaa_bucket_used_bytes{{bucket_name="{bucket_name}"}}')
        .content.decode("utf-8")
    )
    if len(response.get("data").get("result")) == 0:
        raise ReturnedEmptyResponseException
    elif response.get("data").get("result")[0].get("value")[1] == 0:
        raise ReturnedEmptyResponseException
    else:
        value = response.get("data").get("result")[0].get("value")
    return value[1]


class TestNoobaaaPrometheus:
    @tier2
    @bugzilla("2168010")
    @polarion_id("OCS-4928")
    def test_bucket_used_bytes_metric(
        self, bucket_factory, test_directory_setup, awscli_pod_session, mcg_obj_session
    ):
        """
        This test checks if the Noobaa_bucket_used_bytes prometheus metrics
        reflects the number of bytes in the bucket

        """
        amount_of_objs = 10
        bytes_size_in_mb = 2
        bytes_in_mb = 1024 * 1024
        bucket_name = bucket_factory()[0].name
        write_random_test_objects_to_bucket(
            awscli_pod_session,
            bucket_name,
            test_directory_setup.origin_dir,
            amount=amount_of_objs,
            bs=f"{bytes_size_in_mb}M",
            mcg_obj=mcg_obj_session,
        )

        try:
            value = int(get_bucket_used_bytes_metric(bucket_name))
            assert value == (
                bytes_size_in_mb * bytes_in_mb * amount_of_objs
            ), f"Byte size didnt match with actuall bytes were uploaded to the bucket {bucket_name}"
            logger.info(
                f"Prometheus metric Noobaa_bucket_used_bytes value "
                f"matches the bytes uploaded to the bucket {bucket_name}"
            )
        except ReturnedEmptyResponseException:
            raise TimeoutExpiredError(
                "Timed out after retrying for multiple times, no metrics were fetched"
            )
