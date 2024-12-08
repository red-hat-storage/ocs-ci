"""
Defaults module for IBM cleanup
"""

BUCKET_PREFIXES_SPECIAL_RULES = {
    "jnk-pr": 100,  # keep it as first item before jnk prefix for fist match
    "jnk": 200,
    "j\\d\\d\\d": 60,
    "j-\\d\\d\\d": 60,
    "odf-qe": "never",
    "Default": "never",
    "dnd": "never",
    "lr1": 24,
    "lr2": 48,
    "lr3": 72,
    "lr4": 96,
    "promptlab-donotdelete-": "never",
    "ibmcos-uls": 987,
}

DEFAULT_TIME_BUCKETS = 100
IBM_REGION = "us-south"
