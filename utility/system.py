import os


def is_path_empty(path):
    for root, dirs, files in os.walk(path):
        if files:
            return False
        return True
