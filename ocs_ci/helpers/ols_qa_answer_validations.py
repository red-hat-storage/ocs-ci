import logging

from ocs_ci.ocs import constants
from ocs_ci.utility import templating

log = logging.getLogger(__name__)

# Phrases indicating the model could not provide an answer
OLS_UNCERTAINTY_PHRASES = [
    "not available",
    "no information",
    "not found",
    "i don't know",
    "i don't have",
    "no data",
]


def load_test_data():
    """

    Load Q&A cases from ``qa-expectations.yaml`` (via ``constants.OLS_QA_EXPECTATIONS``).

    Returns:
        list: Parsed YAML containing a list of question dicts with keys:
            id, type, question, expected_answer, required_terms.

    Raises:
        FileNotFoundError: If the template file is missing.
        yaml.YAMLError: If the rendered YAML is malformed.

    """
    return templating.generate_yaml_from_jinja2_template_with_data(
        constants.OLS_QA_EXPECTATIONS
    )


def calculate_accuracy(answer, keywords):
    """

    Calculates accuracy based on the keywords given in "qa-expectations.yaml".

    Args:
        answer (str): Model response text.
        keywords (list): Required substrings; each match counts toward accuracy.

    Returns:
        float: Ratio of matched keywords to total keywords. Returns 0.0 when
            ``keywords`` is empty (no terms to verify).

    """
    if not keywords:
        return 0.0

    answer_lower = answer.lower()
    matched_keywords = []
    missing_keywords = []

    for k in keywords:
        if k.lower() in answer_lower:
            matched_keywords.append(k)
        else:
            missing_keywords.append(k)

    accuracy = len(matched_keywords) / len(keywords)

    # Log details for debugging when accuracy is low
    if accuracy < constants.OLS_QA_ACCURACY_THRESHOLD:
        log.warning(
            f"Low accuracy ({accuracy:.2f}): "
            f"Matched {len(matched_keywords)}/{len(keywords)} keywords. "
            f"Missing: {missing_keywords}"
        )
        log.debug(f"Answer received (first 500 chars): {answer[:500]}")

    return accuracy


def is_uncertain(answer):
    """

    Return True if the answer indicates the model could not answer (no RAG / unknown).

    Args:
        answer (str): Model response text.

    Returns:
        bool: True if any uncertainty phrase appears (case-insensitive substring match).

    """
    return any(p in answer.lower() for p in OLS_UNCERTAINTY_PHRASES)
