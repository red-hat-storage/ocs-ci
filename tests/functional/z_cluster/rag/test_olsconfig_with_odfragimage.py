import logging
import os
import pytest
import time
import uuid

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
    verify_ols_connection_fails,
    verify_ols_pod_logs_contain_expected_errors,
    delete_ols_config_and_secret,
)
from ocs_ci.helpers.ols_qa_answer_validations import (
    load_test_data,
    calculate_accuracy,
    calculate_consistency,
    is_uncertain,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.ols_ui import OLSUI

log = logging.getLogger(__name__)

ACCURACY_THRESHOLD = 0.75
CONSISTENCY_THRESHOLD = 0.6


@cyan_squad
@tier1
class TestRagImageDeploymentAndConfiguration(ManageTest):
    """

    This test case covers the successful deployment of the RAG image and the initial configuration of OLS to use it.
    This will validate the core prerequisites for RAG functionality, and also verifies the response given by OLS.

    """

    @pytest.mark.polarion_id("OCS-7483")
    def test_ragimage_deployment_and_configuration(self):
        """

        This test case covers the successful deployment of the RAG image and the initial configuration of OLS to use it.
        This will validate the core prerequisites for RAG functionality.

        1. Deploy OLS Operator and verify OLS Operator installed
        2. Create credential secret for LLM provider (i.e IBM watsonx)
        3. Create custom resource "ols-config" file that contains the yaml content for the LLM provider
        4. Verify OLS successfully connects to and utilizes the specified IBM watsonx LLM provider.
            Verify all the OLS pods are up and running.

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

    @pytest.mark.polarion_id("OCS-7484")
    def test_data_foundation_answers(self, setup_ui):
        """

        This will validate the core prerequisites for RAG functionality, by validating the response given by OLS

        1. Loads 20 sample QnA is set "qa-expectation.yaml"
        2. Ask each valid, valid but no rag answer, invalid questions one by one
            a. calculate the accuracy based on the keywords of expected answer
            b. calculate the consistency by repeating question twice
            c. results are appended for graph

        """

        results = []
        test_data = load_test_data()

        ols = OLSUI()

        # Open OLS chatbox
        ols.open_ols()

        for item in test_data:
            qid = item["id"]
            qtype = item["type"]
            question = item["question"]
            keywords = item.get("required_terms", [])

            ans1 = ols.ask_question(question)
            # take 3 min to breathe
            log.info(
                "Waiting for 3 mins to repeat same question for verifying consistency"
            )
            time.sleep(180)
            ans2 = ols.ask_question(question)

            accuracy = calculate_accuracy(ans1, keywords)
            consistency = calculate_consistency(ans1, ans2)

            # Store results for graph
            results.append(
                {"id": qid, "accuracy": accuracy, "consistency": consistency}
            )

            assert (
                consistency >= CONSISTENCY_THRESHOLD
            ), f"Q{qid}: Consistency failed ({consistency})"

            if qtype == "valid":
                assert (
                    accuracy >= ACCURACY_THRESHOLD
                ), f"Q{qid}: Accuracy failed ({accuracy})"

            elif qtype == "invalid":
                assert (
                    "data foundation" not in ans1.lower()
                ), f"Q{qid}: Hallucinated answer"

            elif qtype == "no_rag_answer":
                assert is_uncertain(ans1), f"Q{qid}: Should say answer not available"

    @pytest.mark.polarion_id("OCS-7486")
    def test_ols_attach_yaml_and_validate_response(self, setup_ui):
        """

        Attach PVC (or pod) YAML from ocs-ci/conf path, ask a question related to the
        attached YAML, and validate the OLS response meets the expectation.

        1. Load PVC YAML from conf path (conf/ocsci/ols_attached_pvc.yaml).
        2. Attach the YAML as context in the chat (include it in the question).
        3. Ask a question about the attached YAML (e.g. storage class, requested size).
        4. Validate the OLS response contains the expected terms from the YAML.

        """
        pvc_yaml_path = constants.OLS_ATTACHED_PVC_YAML
        assert os.path.isfile(pvc_yaml_path), f"OLS attached PVC YAML not found: {pvc_yaml_path}"

        with open(pvc_yaml_path) as f:
            pvc_yaml_content = f.read()

        ols = OLSUI()
        ols.open_ols()

        question_with_yaml = (
            "Here is a PersistentVolumeClaim YAML:\n\n```yaml\n"
            f"{pvc_yaml_content}\n```\n\n"
            "What is the storage class name and the requested storage size in this PVC?"
        )
        answer = ols.ask_question(question_with_yaml)

        # Expect response to mention storage class and size from ols_attached_pvc.yaml
        required_terms = [
            "ocs-storagecluster-ceph-rbd",
            "5Gi",
        ]
        accuracy = calculate_accuracy(answer, required_terms)
        assert (
            accuracy >= ACCURACY_THRESHOLD
        ), (
            f"OLS response for attached PVC YAML should contain expected terms "
            f"(storageClassName and storage); accuracy={accuracy}, required_terms={required_terms}"
        )

    @pytest.mark.polarion_id("OCS-7485")
    def test_ols_byok_negative_misconfigured(self):
        """

        Integration with OLS BYOK Tech Preview - negative scenario (intentionally misconfigured).
        Uses valid API token; tests two misconfigurations: invalid URL, then invalid projectID.
        Intended to run after deployment and UI tests (leaves OLS misconfigured).

        Phase 1 - Invalid URL:
        1. Deploy OLS Operator if not already installed.
        2. Remove existing OLSConfig and secret.
        3. Create credential secret with valid API token (from config).
        4. Create ols-config with invalid LLM URL (valid projectID/model).
        5. Verify OLS never reaches Available and pod logs contain
           "ERROR: LLM connection check failed".

        Phase 2 - Invalid projectID:
        6. Remove OLSConfig and secret again.
        7. Create secret with valid API token, ols-config with correct URL but
           invalid projectID (random UUID in format xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
        8. Verify pod logs contain the 404/Not Found project error message
           (code 404, Failed to retrieve project, not_found, Resource requested by the client was not found).

        """
        assert do_deploy_ols(), "Failed to install OLS Operator"

        # ---------- Phase 1: Invalid URL (valid secret / projectID) ----------
        delete_ols_config_and_secret()

        assert create_ols_secret(), "Failed to create credential secret (valid token)"
        assert create_ols_config(
            overrides={"url": "https://invalid-llm.example.com"}
        ), "Failed to create ols-config with invalid URL"

        time.sleep(60)
        assert verify_ols_connection_fails(
            timeout=300
        ), "OLS should not reach Available state when URL is invalid"

        success, message = verify_ols_pod_logs_contain_expected_errors(
            expected_patterns=["ERROR: LLM connection check failed"]
        )
        assert success, (
            f"OLS pod logs must contain 'ERROR: LLM connection check failed' when URL is invalid. {message}"
        )

        # ---------- Phase 2: Invalid projectID (valid URL and secret) ----------
        delete_ols_config_and_secret()

        invalid_project_id = str(uuid.uuid4())
        assert create_ols_secret(), "Failed to create credential secret (valid token)"
        assert create_ols_config(
            overrides={"projectID": invalid_project_id}
        ), "Failed to create ols-config with invalid projectID"

        time.sleep(60)
        assert verify_ols_connection_fails(
            timeout=300
        ), "OLS should not reach Available state when projectID is invalid"

        # Expect 404 / Not Found project error in logs (all key substrings required)
        project_not_found_patterns = [
            "404",
            "Not Found",
            "Failed to retrieve project",
            "not_found",
            "Resource requested by the client was not found",
        ]
        success, message = verify_ols_pod_logs_contain_expected_errors(
            expected_patterns=project_not_found_patterns,
            require_all=True,
        )
        assert success, (
            f"OLS pod logs must contain 404/Not Found project error when projectID is invalid. {message}"
        )
