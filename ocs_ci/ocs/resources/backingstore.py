class BackingStore():
    """
    A class that represents BackingStore objects

    """
    def __init__(self, name, uls_name, secret_name=None):
        self.name = name
        self.uls_name = uls_name
