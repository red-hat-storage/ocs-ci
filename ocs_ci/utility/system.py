import os


def is_path_empty(path):
    """
    Returns True if the given path does not contain any files, False otherwise.

    Args:
        path (str): Path to be checked
    """
    for root, dirs, files in os.walk(path):
        if files:
            return False
        return True
