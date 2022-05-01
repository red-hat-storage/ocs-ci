import itertools
from enum import Enum


# Enum for admin ops
class AdminOpsEnum(Enum):
    NODE_DRAIN = "node_drain"
    NODE_REBOOT = "node_reboot"
    SNAPSHOT_RESTORE = "snapshot_restore"


# Enum for user ops
class UserOpsEnum(Enum):
    PGSQL = "pgsql"
    COUCHBASE = "couchbase"
    COSBENCH = "cosbench"


EXECUTION_TIME_HOURS = 24
END_TIME = None
SLEEP_TIMEOUT = 10  # TBD

# User Ops
USER_OPS = [op.value for op in UserOpsEnum]

# Admin Ops
ADMIN_OPS_ASYNC = False
ADMIN_OPS = [
    AdminOpsEnum.NODE_DRAIN,
    AdminOpsEnum.NODE_REBOOT,
    AdminOpsEnum.SNAPSHOT_RESTORE,
]
CURRENT_ADMIN_OPS_LIST = [None for x in range(len(ADMIN_OPS))]

# Get all admin ops permutations
ADMIN_OPS_MATRIX = list(itertools.permutations(ADMIN_OPS))

# Copy to temporary list (will be shuffled every loop)
TEMP_ADMIN_OPS_MATRIX = ADMIN_OPS_MATRIX.copy()

CONFIG_VARS = {}
FLOWSTESTED = []
