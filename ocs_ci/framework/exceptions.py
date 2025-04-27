from ocs_ci.ocs.constants import (
    CLUSTER_NAME_MIN_CHARACTERS,
    CLUSTER_NAME_MAX_CHARACTERS,
)


class ClusterPathNotProvidedError(Exception):
    def __str__(self):
        return "Please provide a --cluster-path that is empty or non-existant."


class ClusterNameLengthError(Exception):
    def __init__(
        self,
        name,
        min_length=CLUSTER_NAME_MIN_CHARACTERS,
        max_length=CLUSTER_NAME_MAX_CHARACTERS,
    ):
        self.name = name
        self.min_length = min_length
        self.max_length = max_length

    def __str__(self):
        return (
            f"Cluster Name '{self.name}' is {len(self.name)} characters long "
            f"while it should be {self.min_length}-{self.max_length} characters long"
        )


class ClusterNameNotProvidedError(Exception):
    def __str__(self):
        return "Please provide a valid --cluster-name."


class InvalidDeploymentType(Exception):
    pass


class ClusterNotAccessibleError(Exception):
    pass


class ClusterKubeconfigNotFoundError(Exception):
    pass
