import random
import logging


logger = logging.getLogger(__name__)


def get_random_name(obj_type):
    """
    Helper function for generate random name for object

    Returns:
        str: random name composed from {obj_type}_{random}
    """
    rand = random.randint(1000, 9999)
    return f"{obj_type}_{rand}"


def create_pvcs(count, storage_class):
    """
    Helper function for creating PVCs

    Args:
        count (int): number of PVC to create
        storage_class (StorageClass): storage class object reference

    Returns:
        list: PVCs objects
    """
    _pvcs = []
    for number in range(count):
        pvc_name = get_random_name('pvc')
        _pvcs.append(PVC(pvc_name, storage_class))
    return _pvcs


class PVC:
    """
    Example dummy class of PVC
    """
    def __init__(self, name, storage_class):
        """
        Constructor for PVC

        Args:
            name (str): name of pvc
            storage_class (StorageClass): storage class object reference
        """
        self.storage_class = storage_class
        self.name = name
        self.is_deleted = False

    def delete(self):
        logger.info(f"Deleting pvc: {self.name}")
        self.is_deleted = True


class StorageClass:
    """
    Example dummy class of StorageClass
    """
    def __init__(self, name):
        """
        Constructor for StorageClass

        Args:
            name (str): name of storage class
        """
        self.name = name
        self.is_deleted = False

    def delete(self):
        logger.info(f"Deleting storage class: {self.name}")
        self.is_deleted = True
