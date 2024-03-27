"""
Defaults module for AWS cleanup
"""
import sys

AWS_REGION = "us-east-2"
CLUSTER_PREFIXES_SPECIAL_RULES = {
    "jnk-pr": 16,  # keep it as first item before jnk prefix for fist match
    "jnk": 60,
    "j\\d\\d\\d": 60,
    "j-\\d\\d\\d": 60,
    "dnd": "never",
    "lr1": 24,
    "lr2": 48,
    "lr3": 72,
    "lr4": 96,
    "lr5": 120,
}
MINIMUM_CLUSTER_RUNNING_TIME = 10
CONFIRMATION_ANSWER = "yes-i-am-sure-i-want-to-proceed"

BUCKET_PREFIXES_SPECIAL_RULES = {
    "dnd": sys.maxsize,
    "acmobservability": sys.maxsize,
    "ocs-ci-data": sys.maxsize,
    "ocs-ci-public": sys.maxsize,
    "ocs-qe-upi": sys.maxsize,
    "ocs-qe-upi-1": sys.maxsize,
    "ocs-qe-upi-us-east-2": sys.maxsize,
    "ocsci-test-files": sys.maxsize,
    "openshift-qe-upi": sys.maxsize,
    "j-": 500,
    "lr1-": 100,
    "lr2-": 150,
    "lr3-": 200,
    "lr4-": 250,
    "lr5-": 300,
}
DEFAULT_BUCKET_RUNNING_TIME = 100
