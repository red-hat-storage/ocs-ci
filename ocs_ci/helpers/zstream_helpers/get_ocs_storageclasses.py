"""
Helper function extracted by z-stream test generator.

Target: ocs_ci/helpers/helpers.py
Insertion point: end of file
Description: Retrieve StorageClasses managed by OCS/ODF operators based on provisioner matching.

Review and merge this into ocs_ci/helpers/helpers.py before running the test.
"""

def get_ocs_storageclasses():
    """
    Retrieve StorageClasses that are managed by OCS/ODF operators.

    Filters StorageClasses by checking if their provisioner contains
    known OCS/ODF provisioner substrings.

    Returns:
        list: List of StorageClass resource dicts managed by OCS.
    """
    from ocs_ci.ocs.ocp import OCP
    from ocs_ci.ocs import constants

    sc_ocp = OCP(kind=constants.STORAGECLASS)
    all_scs = sc_ocp.get().get("items", [])
    ocs_provisioners = [
        "rbd.csi.ceph.com",
        "cephfs.csi.ceph.com",
        "ceph.rook.io",
        "openshift-storage",
    ]
    ocs_scs = []
    for sc in all_scs:
        provisioner = sc.get("provisioner", "")
        if any(p in provisioner for p in ocs_provisioners):
            ocs_scs.append(sc)
    return ocs_scs
