
class ClusterPathNotProvidedError(Exception):

    def __str__(self):
        return "Please provide a --cluster-path that is empty or non-existant."


class ClusterNameNotProvidedError(Exception):

    def __str__(self):
        return "Please provide a valid --cluster-name."
