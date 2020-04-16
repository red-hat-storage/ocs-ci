"""
Defaults module for AWS cleanup
"""
AWS_REGION = 'us-east-2'
CLUSTER_PREFIXES_SPECIAL_RULES = {
    'jnk': 36, 'DND': 'never', 'LR1': 24, 'LR2': 48, 'LR3': 72, 'LR4': 96,
    'LR5': 120
}
MINIMUM_CLUSTER_RUNNING_TIME = 10
AWS_CLOUDFORMATION_TAG = 'aws:cloudformation:stack-name'
CONFIRMATION_ANSWER = 'yes-i-am-sure-i-want-to-proceed'
