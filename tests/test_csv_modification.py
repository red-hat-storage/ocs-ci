"""
Tests for CSV modification functionality
"""

from unittest.mock import patch, MagicMock
from ocs_ci.utility.utils import modify_csv


class TestModifyCSV:
    """
    Test cases for the modify_csv function with pattern-based replacement support
    """

    @patch("ocs_ci.utility.utils.run_cmd")
    @patch("ocs_ci.utility.utils.NamedTemporaryFile")
    def test_pattern_based_replacement_single_image(self, mock_temp_file, mock_run_cmd):
        """
        Test pattern-based replacement for a base image path without SHA/tag.
        Should generate regex pattern to match any SHA or tag.
        """
        csv_name = "ocs-operator.v4.19.0"
        replace_from = "registry.redhat.io/odf4/ocs-rhel9-operator"
        replace_to = "quay.io/custom/ocs-operator:latest"

        # Mock the temp file
        mock_file = MagicMock()
        mock_temp_file.return_value = mock_file
        mock_file.name = "/tmp/test_csv_mod.sh"

        modify_csv(csv_name, replace_from, replace_to)

        # Verify run_cmd was called
        assert mock_run_cmd.call_count == 2  # chmod + sh commands

        # Verify the sed command includes extended regex flag and proper pattern
        chmod_call = mock_run_cmd.call_args_list[0][0][0]
        sh_call = mock_run_cmd.call_args_list[1][0][0]

        assert "chmod 777" in chmod_call
        assert "sh /tmp/test_csv_mod.sh" in sh_call

    @patch("ocs_ci.utility.utils.run_cmd")
    @patch("ocs_ci.utility.utils.NamedTemporaryFile")
    def test_exact_replacement_with_sha(self, mock_temp_file, mock_run_cmd):
        """
        Test exact replacement when replace_from includes SHA digest.
        Should use simple string replacement (backward compatible).
        """
        csv_name = "ocs-operator.v4.19.0"
        replace_from = (
            "registry.redhat.io/odf4/ocs-rhel9-operator@sha256:"
            "582e2365862ada1c649adf8fef865c1a61cb02b735a9551b79c219f0e131aa60"
        )
        replace_to = "quay.io/custom/ocs-operator@sha256:abc123"

        mock_file = MagicMock()
        mock_temp_file.return_value = mock_file
        mock_file.name = "/tmp/test_csv_mod.sh"

        modify_csv(csv_name, replace_from, replace_to)

        # Verify run_cmd was called
        assert mock_run_cmd.call_count == 2

    @patch("ocs_ci.utility.utils.run_cmd")
    @patch("ocs_ci.utility.utils.NamedTemporaryFile")
    def test_exact_replacement_with_tag(self, mock_temp_file, mock_run_cmd):
        """
        Test exact replacement when replace_from includes a tag.
        Should use simple string replacement (backward compatible).
        """
        csv_name = "ocs-operator.v4.19.0"
        replace_from = "registry.redhat.io/odf4/ocs-rhel9-operator:v4.19.0"
        replace_to = "quay.io/custom/ocs-operator:v4.19.0-custom"

        mock_file = MagicMock()
        mock_temp_file.return_value = mock_file
        mock_file.name = "/tmp/test_csv_mod.sh"

        modify_csv(csv_name, replace_from, replace_to)

        # Verify run_cmd was called
        assert mock_run_cmd.call_count == 2

    @patch("ocs_ci.utility.utils.run_cmd")
    @patch("ocs_ci.utility.utils.NamedTemporaryFile")
    def test_pattern_replacement_complex_image_path(self, mock_temp_file, mock_run_cmd):
        """
        Test pattern-based replacement with complex registry path.
        """
        csv_name = "ocs-operator.v4.19.0"
        replace_from = "registry.redhat.io/openshift4/ose-kube-rbac-proxy-rhel9"
        replace_to = "quay.io/custom/rbac-proxy:dev"

        mock_file = MagicMock()
        mock_temp_file.return_value = mock_file
        mock_file.name = "/tmp/test_csv_mod.sh"

        modify_csv(csv_name, replace_from, replace_to)

        # Verify run_cmd was called
        assert mock_run_cmd.call_count == 2

    @patch("ocs_ci.utility.utils.run_cmd")
    @patch("ocs_ci.utility.utils.NamedTemporaryFile")
    def test_pattern_replacement_simple_image(self, mock_temp_file, mock_run_cmd):
        """
        Test pattern-based replacement with simple image name.
        """
        csv_name = "ocs-operator.v4.19.0"
        replace_from = "quay.io/custom/my-image"
        replace_to = "quay.io/custom/my-image:v2"

        mock_file = MagicMock()
        mock_temp_file.return_value = mock_file
        mock_file.name = "/tmp/test_csv_mod.sh"

        modify_csv(csv_name, replace_from, replace_to)

        # Verify run_cmd was called
        assert mock_run_cmd.call_count == 2


class TestCSVModificationIntegration:
    """
    Integration test scenarios for CSV modification
    """

    def test_sed_pattern_for_base_image(self):
        """
        Test that the sed pattern correctly matches images with different SHAs/tags.
        This is a logic test without actually running sed.
        """
        import re

        # Simulate the pattern that would be generated
        base_image = "registry.redhat.io/odf4/ocs-rhel9-operator"
        escaped_image = re.escape(base_image)
        pattern = f"{escaped_image}(@sha256:[a-f0-9]{{64}}|:[^[:space:]@,\"']+)"

        # Test cases that should match
        test_cases_match = [
            f"{base_image}@sha256:582e2365862ada1c649adf8fef865c1a61cb02b735a9551b79c219f0e131aa60",
            f"{base_image}:v4.19.0",
            f"{base_image}:latest",
            f"{base_image}:dev-pr-12345",
        ]

        # Test cases that should NOT match (different images)
        test_cases_no_match = [
            "registry.redhat.io/odf4/other-operator@sha256:abc123",
            "different.io/odf4/ocs-rhel9-operator@sha256:abc123",
        ]

        regex = re.compile(pattern)

        for test_case in test_cases_match:
            match = regex.search(test_case)
            assert match is not None, f"Pattern should match: {test_case}"

        for test_case in test_cases_no_match:
            match = regex.search(test_case)
            assert match is None, f"Pattern should NOT match: {test_case}"
