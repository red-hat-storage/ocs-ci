"""
Defaults module for AWS cleanup
"""
AWS_REGION = 'us-east-2'
CLUSTER_PREFIXES_TO_EXCLUDE_FROM_DELETION = {'jnk': 36, 'dnd': 36}
MINIMUM_CLUSTER_RUNNING_TIME_FOR_DELETION = 10
AWS_CLOUDFORMATION_TAG = 'aws:cloudformation:stack-id'
CONFIRMATION_ANSWER = 'yes-i-am-sure-i-want-to-proceed'
