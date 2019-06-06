"""
Constants module.

This module contains any values that are widely used across the framework,
utilities, or tests that will predominantly remain unchanged.

In the event values here have to be changed it should be under careful review
and with consideration of the entire project.

"""
import os

# Directories
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
TOP_DIR = os.path.dirname(THIS_DIR)
TEMPLATES_DIR = os.path.join(TOP_DIR, "templates")

# Statuses
STATUS_PENDING = 'Pending'
STATUS_AVAILABLE = 'Available'
STATUS_RUNNING = 'Running'
STATUS_TERMINATING = 'Terminating'

# Resources / Kinds
CEPHFILESYSTEM = "CephFileSystem"
CEPHBLOCKPOOL = "CephBlockPool"
STORAGECLASS = "StorageClass"
PVC = "PersistentVolumeClaim"
POD = "Pod"

# Other
SECRET = "Secret"
