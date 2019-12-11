from ocs_ci.ocs.constants import (
    CLUSTER_NAME_MIN_CHARACTERS,
    CLUSTER_NAME_MAX_CHARACTERS
)


class ClusterPathNotProvidedError(Exception):

    def __str__(self):
        return "Please provide a --cluster-path that is empty or non-existant."


class ClusterNameLengthError(Exception):
    def __init__(
        self, name, min=CLUSTER_NAME_MIN_CHARACTERS, max=CLUSTER_NAME_MAX_CHARACTERS
    ):
        self.name = name
        self.min = min
        self.max = max

    def __str__(self):
        return (
            f"Cluster Name '{self.name}' is {len(self.name)} characters long "
            f"while it should be {self.min}-{self.max} characters long"
        )

    
class ClusterNameNotProvidedError(Exception):

    def __str__(self):
        return "Please provide a valid --cluster-name."
    
