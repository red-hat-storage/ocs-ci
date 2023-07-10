import logging
import pytest

from ocs_ci.framework.pytest_customization.marks import tier1
from ocs_ci.framework.testlib import MCGTest

logger = logging.getLogger(__name__)


class TestCredentialsReset(MCGTest):
    """
    Test suite for resetting and regenerating MCG related credentials

    """

    @pytest.fixure()
    def original_noobaa_admin_password(self, request, mcg_obj_session):
        """
        Fixture to get the original password from the noobaa-admin secret

        """
        original_password = mcg_obj_session.get_noobaa_admin_credentials_from_secret()[
            "password"
        ]

        def finalizer():
            """
            Reset the password back to the original and retrieve a new RPC token

            """
            mcg_obj_session.reset_admin_pw(new_password=original_password)
            mcg_obj_session.retrieve_nb_token()

        request.addfinalizer(finalizer)
        return original_password

    @tier1
    def test_change_nb_admin_pw(self, mcg_obj_session, original_noobaa_admin_password):
        """
        Test changing the NooBaa admin password

        1. Change the noobaa-admin password
        2. Verify the password changed in the noobaa-admin secret
        3. Verify the old password fails when attempting to generate an RPC token and the new password succeeds

        """

        # Change the noobaa-admin password
        new_password = "new_nb_admin_password"
        mcg_obj_session.reset_admin_pw(new_password=new_password)

        # Verify the password changed in the noobaa-admin secret
        assert (
            new_password
            == mcg_obj_session.get_noobaa_admin_credentials_from_secret()["password"]
        )

        # Verify the original password fails when attempting to generate an RPC token
        mcg_obj_session.password = original_noobaa_admin_password
        with pytest.raises(Exception) as e:
            mcg_obj_session.retrieve_nb_token()
            logger.info(
                f"Failed to retrieve RPC token with the original password as expected: {e}"
            )

        # Verify the new password succeeds when attempting to generate an RPC token
        mcg_obj_session.password = new_password
        try:
            mcg_obj_session.retrieve_nb_token()
            logger.info("Successfully retrieved RPC token with new password")
        except Exception as e:
            logger.error(f"Failed to retrieve RPC token with new password: {e}")
            raise
