from difflib import SequenceMatcher

from ocs_ci.ocs import constants
from ocs_ci.utility import templating


def load_test_data():
    return templating.generate_yaml_from_jinja2_template_with_data(
        constants.OLS_QA_EXPECTATIONS
    )


def calculate_accuracy(answer, keywords):
    if not keywords:
        return 1.0
    matched = sum(1 for k in keywords if k.lower() in answer.lower())
    return matched / len(keywords)


def calculate_consistency(ans1, ans2):
    return SequenceMatcher(None, ans1, ans2).ratio()


def is_uncertain(answer):
    phrases = [
        "not available",
        "no information",
        "not found",
        "i don't know",
        "no data",
    ]
    return any(p in answer.lower() for p in phrases)


# def test_generate_graph(results=None, accuracy_threshold=0.7, consistency_threshold=0.75):
#     """
#
#     """
#     if not results:
#         pytest.skip("No results to plot")
#
#     ids = [r["id"] for r in results]
#     accuracy = [r["accuracy"] for r in results]
#     consistency = [r["consistency"] for r in results]
#
#     plt.figure(figsize=(10, 5))
#     plt.plot(ids, accuracy, label="Accuracy", marker="o")
#     plt.plot(ids, consistency, label="Consistency", marker="x")
#     plt.axhline(accuracy_threshold, linestyle="--", color="red", label="Accuracy Threshold")
#     plt.axhline(consistency_threshold, linestyle="--", color="orange", label="Consistency Threshold")
#
#     plt.xlabel("Question ID")
#     plt.ylabel("Score")
#     plt.title("OLS Data Foundation Answer Validation")
#     plt.legend()
#     plt.grid(True)
#     plt.show()
