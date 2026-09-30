# NOTE: This file represents ONLY the lines to be appended to the existing
# ocs_ci/ocs/constants.py file. In practice, add these two lines at the end
# of the existing constants.py file.

# CephConnection resource kind (used for ReadAffinity verification)
CEPHCONNECTION_KIND = "CephConnection"

# ceph-csi-configs ConfigMap name (fallback for ReadAffinity verification)
CEPH_CSI_CONFIGS_CONFIGMAP = "ceph-csi-configs"