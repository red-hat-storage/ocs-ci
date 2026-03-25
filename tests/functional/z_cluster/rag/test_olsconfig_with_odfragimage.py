import logging
import pytest
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
    wait_for_ols_config_status_after_apply,
)
from ocs_ci.helpers.ols_qa_answer_validations import (
    load_test_data,
    calculate_accuracy,
    is_uncertain,
)
from ocs_ci.ocs import constants
from ocs_ci.ocs.ui.ols_ui import OLSUI

log = logging.getLogger(__name__)


@cyan_squad
@tier1
class TestRagImageDeploymentAndConfiguration(ManageTest):
    """

    This test case covers the successful deployment of the RAG image and the initial configuration of OLS to use it.
    This will validate the core prerequisites for RAG functionality, and also verifies the response given by OLS.

    UI tests use the ``setup_ui`` fixture from ``tests/conftest.py`` (browser login and teardown).

    """

    def _ensure_ols_operator_installed(self):
        """
        Ensure OLS operator is present before executing each test.
        """
        assert do_deploy_ols(), "Failed to install/verify OLS Operator"

    @pytest.mark.order("first")
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

        # Verify OLS successfully connects to and utilizes the specified IBM watsonx LLM provider
        # Verify all the OLS pods are up and running (polls ApiReady; do not use fixed sleeps)
        verify_ols_connects_to_llm()

    @pytest.mark.polarion_id("OCS-7484")
    def test_data_foundation_answers(self, setup_ui):
        """

        Validate OLS responses against ``qa-expectation.yaml``.

        1. Load sample Q&A from ``qa-expectation.yaml``.
        2. For each case: ask the question, then validate by type (valid / invalid / no_rag_answer).
        3. For ``valid`` cases: ask the same question twice and require both answers to meet the
           keyword accuracy threshold (consistency check).
        4. Collect all failures and report once at the end so every question is exercised.

        """

        self._ensure_ols_operator_installed()
        results = []
        failures = []
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
            accuracy = calculate_accuracy(ans1, keywords)

            entry = {"id": qid, "type": qtype, "accuracy": accuracy}
            if qtype == "valid":
                ans2 = ols.ask_question(question)
                accuracy_repeat = calculate_accuracy(ans2, keywords)
                entry["accuracy_repeat"] = accuracy_repeat
                results.append(entry)
                for label, acc in (
                    ("first", accuracy),
                    ("repeat", accuracy_repeat),
                ):
                    if acc < constants.OLS_QA_ACCURACY_THRESHOLD:
                        failures.append(
                            f"Q{qid} ({label}): accuracy {acc} below threshold "
                            f"{constants.OLS_QA_ACCURACY_THRESHOLD}"
                        )
            else:
                results.append(entry)

            if qtype == "invalid":
                if "data foundation" in ans1.lower():
                    failures.append(
                        f"Q{qid}: Hallucinated answer (unexpected data foundation mention)"
                    )

            elif qtype == "no_rag_answer":
                if not is_uncertain(ans1):
                    failures.append(f"Q{qid}: Should indicate answer not available")

        log.info("OLS Q&A summary: %s", results)
        if failures:
            pytest.fail("One or more OLS Q&A checks failed:\n" + "\n".join(failures))

    @pytest.mark.polarion_id("OCS-7514")
    def test_ols_attach_yaml_and_validate_response(self, setup_ui):
        """

        Attach PVC (or pod) YAML from ocs-ci/conf path, ask a question related to the
        attached YAML, and validate the OLS response meets the expectation.

        1. Load PVC YAML from conf path (conf/ocsci/ols_attached_pvc.yaml).
        2. Attach the YAML as context in the chat (include it in the question).
        3. Ask a question about the attached YAML (e.g. storage class, requested size).
        4. Validate the OLS response contains the expected terms from the YAML.

        """
        self._ensure_ols_operator_installed()
        pvc_yaml_path = constants.OLS_ATTACHED_PVC_YAML
        with open(pvc_yaml_path, encoding="utf-8") as f:
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
        assert accuracy >= constants.OLS_QA_ACCURACY_THRESHOLD, (
            f"OLS response for attached PVC YAML should contain expected terms "
            f"(storageClassName and storage); accuracy={accuracy}, required_terms={required_terms}"
        )

    @pytest.mark.order("last")
    @pytest.mark.polarion_id("OCS-7513")
    def test_ols_byok_negative_misconfigured(self):
        """

        Integration with OLS BYOK Tech Preview - negative scenario (intentionally misconfigured).
        Tests two misconfigurations: invalid URL, then invalid projectID.
        Marked ``order(last)`` so it runs after other tests in this class that need a working OLS config;
        cleanup at the end removes OLSConfig and secret.

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
        self._ensure_ols_operator_installed()
        delete_ols_config_and_secret()

        # ---------- Phase 1: Invalid URL (valid secret / projectID) ----------
        assert create_ols_secret(), "Failed to create credential secret (valid token)"
        assert create_ols_config(
            overrides={"url": "https://invalid-llm.example.com"}
        ), "Failed to create ols-config with invalid URL"

        wait_for_ols_config_status_after_apply()
        assert verify_ols_connection_fails(
            timeout=300
        ), "OLS should not reach Available state when URL is invalid"

        success, message = verify_ols_pod_logs_contain_expected_errors(
            expected_patterns=["ERROR: LLM connection check failed"]
        )
        assert (
            success
        ), f"OLS pod logs must contain 'ERROR: LLM connection check failed' when URL is invalid. {message}"

        # ---------- Phase 2: Invalid projectID (valid URL and secret) ----------
        delete_ols_config_and_secret()

        invalid_project_id = str(uuid.uuid4())
        assert create_ols_secret(), "Failed to create credential secret (valid token)"
        assert create_ols_config(
            overrides={"projectID": invalid_project_id}
        ), "Failed to create ols-config with invalid projectID"

        wait_for_ols_config_status_after_apply()
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
        assert (
            success
        ), f"OLS pod logs must contain 404/Not Found project error when projectID is invalid. {message}"

        delete_ols_config_and_secret()
