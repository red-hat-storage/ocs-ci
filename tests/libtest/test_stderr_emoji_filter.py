from ocs_ci.framework.testlib import libtest
from ocs_ci.ocs.exceptions import CommandFailed
from ocs_ci.utility.utils import is_emoji, filter_out_emojis


@libtest
def test_filter_out_emojis():
    """
    Test that the filter_out_emojis function works as expected

    """
    str_with_emojis = "Hello ✅ World! 🌎 How are you? 🤔"
    str_without_emojis = "Hello  World!  How are you? "

    assert filter_out_emojis(str_with_emojis) == str_without_emojis


@libtest
def test_stderr_emoji_filter(mcg_obj):
    """
    Test that exec_mcg_cmd errors do not contain emojis

    """

    try:
        # Send a request that is expected to fail and return emojis in the error
        depricated_schema_payload = {
            "name": "first.bucket",
            "quota": {"unit": "PETABYTE", "size": 1},
        }
        mcg_obj.send_rpc_query(
            "bucket_api",
            "update_bucket",
            depricated_schema_payload,
        )
    except CommandFailed as e:
        # Verify that that error does not contain emojis
        assert all(not is_emoji(c) for c in str(e))
