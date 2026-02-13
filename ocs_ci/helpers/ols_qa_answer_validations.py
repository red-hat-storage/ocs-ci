from difflib import SequenceMatcher

from ocs_ci.ocs import constants
from ocs_ci.utility import templating


def load_test_data():
    """

    Loads 20 sample QnA is set "qa-expectation.yaml" and returns dict generated from template file

    """
    return templating.generate_yaml_from_jinja2_template_with_data(
        constants.OLS_QA_EXPECTATIONS
    )


def calculate_accuracy(answer, keywords):
    """

    Calculates accuracy based on the keywords given in "qa-expectation.yaml"

    """
    if not keywords:
        return 1.0
    matched = sum(1 for k in keywords if k.lower() in answer.lower())
    return matched / len(keywords)


def calculate_consistency(ans1, ans2):
    """

    Calculate consistency based on the response given by OLS for 2 same question

    """
    return SequenceMatcher(None, ans1, ans2).ratio()


def is_uncertain(answer):
    """

    Verify that question is not valid based on the phrases

    """

    phrases = [
        "not available",
        "no information",
        "not found",
        "i don't know",
        "i don't have",
        "no data",
    ]
    return any(p in answer.lower() for p in phrases)
