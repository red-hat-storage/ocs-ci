import logging
import pytest
import time

from ocs_ci.framework.pytest_customization.marks import cyan_squad
from ocs_ci.framework.testlib import (
    ManageTest,
    tier1,
)
from ocs_ci.helpers.ols_helpers import (
    do_deploy_ols,
    create_ols_secret,
    create_ols_config,
    verify_ols_connects_to_llm,
)

log = logging.getLogger(__name__)


@cyan_squad
@tier1
@pytest.mark.polarion_id("")
class TestRagImageDeploymentAndConfiguration(ManageTest):
    """

    This test case covers the successful deployment of the RAG image and the initial configuration of OLS to use it.
    This will validate the core prerequisites for RAG functionality.

    1. Deploy OLS Operator and verify OLS Operator installed
    2. Create credential secret for LLM provider (i.e IBM watsonx)
    3. Create custom resource "ols-config" file that contains the yaml content for the LLM provider
    4. Verify OLS successfully connects to and utilizes the specified IBM watsonx LLM provider.
       Verify all the OLS pods are up and running.

    """

    def test_ragimage_deployment_and_configuration(self):
        """

        This test case verifies the successful deployment of the RAG image and its initial configuration for OLS.

        """

        # Deploy OLS operator
        assert do_deploy_ols(), "Failed to install OLS Operator"

        # Create credential secret for LLM provider IBM watsonx
        assert (
            create_ols_secret()
        ), "Failed to create credential secret for LLM provider"

        # Create custom resource "ols-config"
        assert create_ols_config(), "Failed to create ols-config"

        # Wait for the resources to be up
        time.sleep(300)

        # Verify OLS successfully connects to and utilizes the specified IBM watsonx LLM provider
        # Verify all OLS pods are up and running
        verify_ols_connects_to_llm()
