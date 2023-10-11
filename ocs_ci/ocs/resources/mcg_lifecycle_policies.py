from abc import ABC
import datetime
import uuid


class LifecyclePolicy:
    """
    A class for handling MCG lifecycle policy config parsing

    """

    def __init__(self, *args):
        """
        Constructor method for the class

        Args:
            *args: One or more LifecycleRule instances or a list of LifecycleRule instances

        """
        if len(args) == 1 and isinstance(args[0], list):
            self.rules = args[0]
        else:
            self.rules = args

        for rule in self.rules:
            if not isinstance(rule, LifecycleRule):
                raise TypeError(f"Rule {rule} is not of type LifecycleRule")

    def as_dict(self):
        return {"Rules": [rule.as_dict() for rule in self.rules]}

    def __str__(self):
        return self.as_dict().__str__()

    def __repr__(self):
        return self.__str__()


class LifecycleFilter:
    """
    A class for handling S3 lifecycle filter parsing

    """

    def __init__(self, prefix=None, tags=None, minBytes=None, maxBytes=None):
        """
        Constructor method for the class

        Args:
            prefix (str): Prefix of the target objects
            tags (list): A list of dictionaries specifying the tags of the target objects
            minBytes (int): Minimum size of the objects
            maxBytes (int): Maximum size of the objects

        """
        self.prefix = prefix
        self.tag = tags
        self.minBytes = minBytes
        self.maxBytes = maxBytes

    def as_dict(self):
        filter_dict = {"And": {}}
        if self.prefix:
            filter_dict["And"]["Prefix"] = self.prefix
        if self.tag:
            filter_dict["And"]["Tag"] = self.tag
        if self.minBytes:
            filter_dict["And"]["MinBytes"] = self.minBytes
        if self.maxBytes:
            filter_dict["And"]["MaxBytes"] = self.maxBytes

        if len(filter_dict["And"]) == 0:
            filter_dict = {}
        elif len(filter_dict["And"]) == 1:
            key, val = next(iter(filter_dict["And"].items()))
            filter_dict = {key: val}

        return filter_dict

    def __str__(self):
        return self.as_dict().__str__()

    def __repr__(self):
        return self.__str__()


class LifecycleRule(ABC):
    """
    An abstract class for handling MCG lifecycle rule parsing

    """

    def __init__(self, filter=LifecycleFilter(), is_enabled=True):
        """
        Constructor method for the class

        Args:
            filter (LifecycleFilter): Optional object filter
            is_enabled (bool): Whether the rule is enabled or not

        """
        self.filter = filter
        self.is_enabled = is_enabled
        self._id = f"rule-{uuid.uuid4().hex[:8]}"

    def as_dict(self):
        rule_dict = {
            "Filter": self.filter.as_dict(),
            "ID": self._id,
            "Status": "Enabled" if self.is_enabled else "Disabled",
        }

        return rule_dict

    def __str__(self):
        return self.as_dict().__str__()

    def __repr__(self):
        return self.__str__()

    @property
    def id(self):
        return self._id


class ExpirationRule(LifecycleRule):
    """
    A class for handling the parsing of an MCG object expiration rule

    """

    def __init__(
        self,
        days,
        filter=LifecycleFilter(),
        use_date=False,
        is_enabled=True,
        expire_solo_delete_markers=False,
    ):
        """
        Constructor method for the class

        Args:
            days (int): Number of days after which the objects will expire
            filter (LifecycleFilter): Optional object filter
            use_date (bool): Whether to set a a date instead of the number of days
            is_enabled (bool): Whether the rule is enabled or not
            expire_solo_delete_markers (bool): Only relevant for versioned buckets.
                                               If set to True, a delete marker of an object
                                               will expire if no other versions of the object
                                               exist. This also means that an expired object
                                               without any other versions will be deleted
                                               along with its delete marker.

        """
        super().__init__(filter=filter, is_enabled=is_enabled)
        self.days = days
        self.use_date = use_date
        self.expire_solo_delete_markers = expire_solo_delete_markers

    def as_dict(self):
        rule_dict = super().as_dict()
        if self.use_date:
            expiration_time_key = "Date"
            expiration_time_value = datetime.datetime.now() + datetime.timedelta(
                days=self.days
            ).strftime("%Y-%m-%d")
        else:
            expiration_time_key = "Days"
            expiration_time_value = self.days

        rule_dict["Expiration"] = {
            expiration_time_key: expiration_time_value,
            "ExpiredObjectDeleteMarker": self.expire_solo_delete_markers,
        }
        return rule_dict
