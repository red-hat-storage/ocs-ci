from abc import ABC, abstractmethod
import uuid


class McgReplicationPolicy:
    """
    A class to handle the MCG bucket replication policy JSON structure.

    """

    def __init__(self, destination_bucket, prefix=""):
        self.rule_id = f"replication-rule-{uuid.uuid4().hex}"
        self.destination_bucket = destination_bucket
        self.prefix = prefix

    def to_dict(self):
        return {
            "rules": [
                {
                    "rule_id": self.rule_id,
                    "destination_bucket": self.destination_bucket,
                    "filter": {"prefix": self.prefix},
                }
            ]
        }

    def __str__(self) -> str:
        return str(self.to_dict())


class LogBasedReplicationPolicy(McgReplicationPolicy, ABC):
    """
    An abstract subclass of ReplicationPolicy that includes log-based replication information.

    """

    def __init__(
        self,
        destination_bucket,
        sync_deletions=False,
        prefix="",
    ):
        super().__init__(destination_bucket, prefix)
        self.sync_deletions = sync_deletions

    @abstractmethod
    def to_dict(self):
        dict = super().to_dict()
        dict["rules"][0]["sync_deletions"] = self.sync_deletions
        dict["log_replication_info"] = {}

        return dict


class AwsLogBasedReplicationPolicy(LogBasedReplicationPolicy):
    """
    A class to handle the AWS log-based bucket replication policy JSON structure.

    """

    def __init__(
        self,
        destination_bucket,
        sync_deletions=False,
        logs_bucket="",
        prefix="",
        logs_location_prefix="",
    ):
        super().__init__(destination_bucket, sync_deletions, prefix)
        self.logs_bucket = logs_bucket
        self.logs_location_prefix = logs_location_prefix

    def to_dict(self):
        dict = super().to_dict()
        dict["log_replication_info"]["logs_location"] = {
            "logs_bucket": self.logs_bucket,
            "prefix": self.logs_location_prefix,
        }

        return dict


class AzureLogBasedReplicationPolicy(LogBasedReplicationPolicy):
    """
    A class to handle the Azure log-based bucket replication policy JSON structure.

    """

    def __init__(
        self,
        destination_bucket,
        sync_deletions=False,
        prefix="",
    ):
        super().__init__(destination_bucket, sync_deletions, prefix)

    def to_dict(self):
        dict = super().to_dict()
        dict["log_replication_info"]["endpoint_type"] = "AZURE"

        return dict
