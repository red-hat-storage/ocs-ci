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


def create_pvc(storage_class, some_parameter):
    """
    Helper function for creating PVC

    Args:
        storage_class (StorageClass): storage class object reference
        some_parameter (str): you can have some parameter here to utilize pvc

    Returns:
        list: PVCs objects
    """
    pvc_name = get_random_name('pvc')
    return PVC(pvc_name, storage_class, some_parameter)


class PVC:
    """
    Example dummy class of PVC
    """
    def __init__(self, name, storage_class, some_parameter):
        """
        Constructor for PVC

        Args:
            name (str): name of pvc
            storage_class (StorageClass): storage class object reference
            some_parameter (str): you can have some parameter here to utilize pvc
        """
        self.storage_class = storage_class
        self.name = name
        self.some_parameter = some_parameter
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
