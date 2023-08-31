from abc import ABC
import uuid


class LifecycleConfig:
    """
    A class for handling MCG lifecycle config parsing

    """

    def __init__(self, rules=None):
        """
        Constructor method for the class

        Args:
            rules (list): List of LifecycleRule objects

        """
        self._rules = rules or []

    def __str__(self):
        return {"Rules": [str(rule) for rule in self._rules]}


class LifecycleRule(ABC):
    """
    An abstract class for handling MCG lifecycle rule parsing

    """

    def __init__(self, prefix="", status="Enabled"):
        """
        Constructor method for the class

        Args:
            prefix (str): Prefix of the object
            status (str): Status of the expiration policy - Enabled/Disabled

        """
        self._id = f"rule-{uuid.uuid4().hex[:8]}"
        self._policy_dict = {
            "Filter": {"Prefix": prefix},
            "ID": self._id,
            "Status": status,
        }

    def __str__(self):
        return self._policy_dict.__str__()

    @property
    def id(self):
        return self._id

    @property
    def prefix(self):
        return self._policy_dict["Filter"]["Prefix"]

    @prefix.setter
    def prefix(self, prefix):
        self._policy_dict["Filter"]["Prefix"] = prefix

    @property
    def status(self):
        return self._policy_dict["Status"]


class ExpirationRule(LifecycleRule):
    """
    A class for handling the parsing of an MCG object expiration rule

    """

    def __init__(
        self, days, prefix="", status="Enabled", expire_solo_delete_markers=False
    ):
        """
        Constructor method for the class

        Args:
            days (int): Number of days after which the object will expire
            prefix (str): Prefix of the object
            status (str): Status of the expiration policy - Enabled/Disabled
            expire_solo_delete_markers (bool): Only relevant for versioned buckets.
                                               If set to True, a delete marker of an object
                                               will expire if no other versions of the object
                                               exist. This also means that an expired object
                                               without any other versions will be deleted
                                               along with its delete marker.

        """
        super().__init__(prefix=prefix, status=status)

        self._policy_dict["Expiration"] = {
            "Days": days,
            "ExpiredObjectDeleteMarker": expire_solo_delete_markers,
        }

    @property
    def days(self):
        return self._policy_dict["Expiration"]["Days"]

    @days.setter
    def days(self, days):
        self._policy_dict["Expiration"]["Days"] = days

    @property
    def expire_solo_delete_markers(self):
        return self._policy_dict["Expiration"]["ExpiredObjectDeleteMarker"]

    @expire_solo_delete_markers.setter
    def expire_solo_delete_markers(self, expire_solo_delete_markers):
        self._policy_dict["Expiration"][
            "ExpiredObjectDeleteMarker"
        ] = expire_solo_delete_markers
