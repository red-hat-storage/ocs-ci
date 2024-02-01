"""
Defaults module for IBM cleanup
"""
CLUSTER_PREFIXES_SPECIAL_RULES = {
    "jnk-pr": 16,  # keep it as first item before jnk prefix for fist match
    "jnk": 60,
    "j\\d\\d\\d": 60,
    "j-\\d\\d\\d": 60,
    "odf-qe": "never",
    "Default": "never",
    "dnd": "never",
    "lr1": 24,
    "lr2": 48,
    "lr3": 72,
    "lr4": 96,
    "lr5": 120,
}

DEFAULT_TIME = 12

IBM_REGION = "us-south"
