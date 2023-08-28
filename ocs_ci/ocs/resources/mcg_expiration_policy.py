class McgExpirationPolicy:
    """
    A class representing an MCG object expiraiton policy.

    This class handles the parsing of the relevant parameters to a dictionary that matches the expected JSON structure.

    """

    def __init__(self, days, prefix, status="Enabled"):
        """
        Constructor method for the class

        Args:
            days (int): Number of days after which the object will expire
            prefix (str): Prefix of the object
            status (str): Status of the expiration policy

        """
        self._days = days
        self._prefix = prefix
        self._status = status
