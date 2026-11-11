"""
Validators for the IBM Storage Scale resources that back Fusion Access for SAN
Metro-DR (FA-MDR).

FA-MDR replicates at the storage array, so none of the Ceph shaped helpers in
``dr_helpers`` describe what has to be true on an FA cluster. What Ramen
actually consumes is a chain of Scale CRs plus the labels the Scale operator
stamps on them:

    LocalDisk -> Filesystem -> FilesystemRecoveryData
                            -> StorageClass -> VolumeGroupReplicationClass

The identity model tying that chain together is the reason most of these checks
exist. For a filesystem with ``status.uid`` ``8DA8300A:6A7B4BEF`` on a GPFS
cluster whose id is ``9550151709074606676``::

    ramendr.openshift.io/groupreplicationid   8DA8300A-6A7B4BEF
    scale.spectrum.ibm.com/dr-primary         9550151709074606676
    ramendr.openshift.io/storageid            9550151709074606676-8DA8300A-6A7B4BEF

So ``groupreplicationid`` is the filesystem uid with ``:`` replaced by ``-``,
and ``storageid`` is ``<dr-primary>-<groupreplicationid>``. Across the two
sites the ``groupreplicationid`` is identical (it is the same filesystem) while
the ``storageid`` differs only in its cluster-id prefix. A ``storageid`` whose
prefix is a placeholder such as ``XXX`` means the Scale operator never resolved
the local cluster id, and Ramen will silently fail to pair the two sites.
"""

import logging

from ocs_ci.framework import config
from ocs_ci.ocs import constants, ocp
from ocs_ci.ocs.exceptions import (
    ResourceWrongStatusException,
    TimeoutExpiredError,
    UnexpectedBehaviour,
)
from ocs_ci.ocs.utils import get_non_acm_cluster_config
from ocs_ci.utility.utils import TimeoutSampler

logger = logging.getLogger(__name__)

# Sections of the mmexportfs output carried in FilesystemRecoveryData.spec.mmexportfs.
# An FSRD missing any of these cannot rebuild the filesystem at the recovery site.
MMEXPORTFS_VERSION = "00_VERSION_LINE"
MMEXPORTFS_MMFSCFG = "70_MMFSCFG"
MMEXPORTFS_SG_HEADR = "30_SG_HEADR"
MMEXPORTFS_SG_ETCFS = "40_SG_ETCFS"
MMEXPORTFS_SG_MOUNT = "50_SG_MOUNT"
MMEXPORTFS_SG_DISKS = "60_SG_DISKS"

MMEXPORTFS_REQUIRED_SECTIONS = (
    MMEXPORTFS_VERSION,
    MMEXPORTFS_SG_HEADR,
    MMEXPORTFS_SG_ETCFS,
    MMEXPORTFS_SG_MOUNT,
    MMEXPORTFS_SG_DISKS,
)

# Sections whose third field is the filesystem name.
MMEXPORTFS_FILESYSTEM_SECTIONS = (
    MMEXPORTFS_SG_HEADR,
    MMEXPORTFS_SG_ETCFS,
    MMEXPORTFS_SG_MOUNT,
    MMEXPORTFS_SG_DISKS,
)

# LocalDisk status.conditions[].type == "LocalDiskIO" reports its verdict through
# the reason, not the type, so both values have to be matched on the reason.
LOCALDISK_IO_CONDITION = "LocalDiskIO"
LOCALDISK_IO_WORKS = "LocalDiskIOWorks"
LOCALDISK_IO_FAIL = "LocalDiskIOFail"

# Values of the scale.spectrum.ibm.com/dr-state label on a VolumeGroupReplication.
# Demotion of the old primary walks unmounting -> passive; a VGR parked at
# unmounting is the signature of a filesystem that cannot enter maintenance mode.
VGR_DR_STATE_UNMOUNTING = "unmounting"
VGR_DR_STATE_PASSIVE = "passive"
VGR_DR_STATE_ACTIVE = "active"


def get_scale_resource(kind, resource_name=None, selector=None, namespace=None):
    """
    Return a Scale CR, or the list of CRs matching a label selector.

    Args:
        kind (str): Fully qualified CR kind, for example
            constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND.
        resource_name (str): Name of a single resource to fetch.
        selector (str): Label selector used when resource_name is not given.
        namespace (str): Namespace to read from. Defaults to the Scale namespace.

    Returns:
        dict: The resource, or a dict with an "items" list when a selector is used.

    """
    resource_ocp = ocp.OCP(
        kind=kind,
        namespace=namespace or constants.IBM_STORAGE_SCALE_NAMESPACE,
        selector=selector,
    )
    return resource_ocp.get(resource_name=resource_name)


def get_conditions(resource_data):
    """
    Return a resource's status conditions keyed by condition type.

    Args:
        resource_data (dict): The resource.

    Returns:
        dict: Condition type to the full condition dict.

    """
    return {
        condition["type"]: condition
        for condition in resource_data.get("status", {}).get("conditions", [])
    }


def get_gpfs_cluster_id():
    """
    Return the GPFS cluster id of the cluster in the current context.

    This is the value that is expected to prefix every storageid label and to
    appear verbatim in the dr-primary label.

    Returns:
        str: The GPFS cluster id.

    Raises:
        UnexpectedBehaviour: If the Daemon CR does not report a cluster id.

    """
    daemon = get_scale_resource(
        kind=constants.IBM_STORAGE_SCALE_DAEMON_KIND,
        resource_name=constants.IBM_STORAGE_SCALE_DAEMON_NAME,
    )
    cluster_id = daemon.get("status", {}).get("clusterID")
    if not cluster_id:
        raise UnexpectedBehaviour(
            "Daemon "
            f"{constants.IBM_STORAGE_SCALE_DAEMON_NAME} does not report "
            "status.clusterID, the GPFS cluster is not formed yet"
        )
    logger.info(
        f"GPFS cluster id of cluster {config.ENV_DATA['cluster_name']}: {cluster_id}"
    )
    return cluster_id


def get_expected_group_replication_id(filesystem_name):
    """
    Derive the groupreplicationid a filesystem should be labelled with.

    Ramen expects the filesystem uid with its colon replaced by a hyphen, so the
    label can be checked against the filesystem itself rather than against
    another label that may be wrong in the same way.

    Args:
        filesystem_name (str): Name of the Scale Filesystem CR.

    Returns:
        str: The expected groupreplicationid.

    Raises:
        UnexpectedBehaviour: If the filesystem does not report status.uid.

    """
    filesystem = get_scale_resource(
        kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND,
        resource_name=filesystem_name,
    )
    filesystem_uid = filesystem.get("status", {}).get("uid")
    if not filesystem_uid:
        raise UnexpectedBehaviour(
            f"Filesystem {filesystem_name} does not report status.uid, "
            "it has not been created in GPFS yet"
        )
    return filesystem_uid.replace(":", "-")


def validate_dr_labels(
    resource_data,
    resource_description,
    group_replication_id=None,
    gpfs_cluster_id=None,
    check_dr_primary=True,
):
    """
    Validate the Ramen DR labels on a single Scale resource.

    Args:
        resource_data (dict): The resource to check.
        resource_description (str): Human readable identity used in messages.
        group_replication_id (str): Expected groupreplicationid. When None the
            value found on the resource is accepted and returned.
        gpfs_cluster_id (str): Expected storageid prefix and dr-primary value.
            When None the prefix is only checked for not being a placeholder.
        check_dr_primary (bool): Whether the resource is expected to carry the
            scale.spectrum.ibm.com/dr-primary label.

    Returns:
        dict: The DR labels found on the resource.

    Raises:
        UnexpectedBehaviour: If a label is missing or does not match.

    """
    labels = resource_data.get("metadata", {}).get("labels", {})
    found_group_replication_id = labels.get(constants.RAMEN_GROUP_REPLICATION_ID_LABEL)
    found_storage_id = labels.get(constants.RAMEN_STORAGE_ID_LABEL)

    if not found_group_replication_id:
        raise UnexpectedBehaviour(
            f"{resource_description} is missing label "
            f"{constants.RAMEN_GROUP_REPLICATION_ID_LABEL}"
        )
    if not found_storage_id:
        raise UnexpectedBehaviour(
            f"{resource_description} is missing label "
            f"{constants.RAMEN_STORAGE_ID_LABEL}"
        )

    if group_replication_id and found_group_replication_id != group_replication_id:
        raise UnexpectedBehaviour(
            f"{resource_description} has groupreplicationid "
            f"{found_group_replication_id}, expected {group_replication_id}"
        )
    group_replication_id = group_replication_id or found_group_replication_id

    # storageid is "<gpfs cluster id>-<groupreplicationid>". Splitting on the
    # first hyphen is safe because a GPFS cluster id is decimal.
    storage_id_prefix, _, storage_id_suffix = found_storage_id.partition("-")
    if storage_id_suffix != group_replication_id:
        raise UnexpectedBehaviour(
            f"{resource_description} has storageid {found_storage_id}, whose "
            f"suffix does not match groupreplicationid {group_replication_id}"
        )
    if not storage_id_prefix.isdigit():
        raise UnexpectedBehaviour(
            f"{resource_description} has storageid {found_storage_id}, whose "
            f"prefix {storage_id_prefix} is not a GPFS cluster id. The Scale "
            "operator never resolved the local cluster id, so Ramen cannot "
            "pair this resource with its peer at the other site"
        )
    if gpfs_cluster_id and storage_id_prefix != gpfs_cluster_id:
        raise UnexpectedBehaviour(
            f"{resource_description} has storageid {found_storage_id}, whose "
            f"prefix does not match the local GPFS cluster id {gpfs_cluster_id}"
        )

    if check_dr_primary:
        found_dr_primary = labels.get(constants.IBM_STORAGE_SCALE_DR_PRIMARY_LABEL)
        if not found_dr_primary:
            raise UnexpectedBehaviour(
                f"{resource_description} is missing label "
                f"{constants.IBM_STORAGE_SCALE_DR_PRIMARY_LABEL}"
            )
        if found_dr_primary != storage_id_prefix:
            raise UnexpectedBehaviour(
                f"{resource_description} has dr-primary {found_dr_primary} but "
                f"its storageid prefix is {storage_id_prefix}"
            )

    logger.info(f"{resource_description} carries the expected DR labels: {labels}")
    return labels


def validate_filesystem_dr_labels(filesystem_names, gpfs_cluster_id=None):
    """
    Validate the DR labels on every given Scale Filesystem CR.

    Args:
        filesystem_names (list): Names of the Filesystem CRs to check.
        gpfs_cluster_id (str): Expected GPFS cluster id. Read from the Daemon CR
            when not given.

    Returns:
        dict: Filesystem name to its groupreplicationid.

    Raises:
        UnexpectedBehaviour: If a filesystem is mislabelled.

    """
    gpfs_cluster_id = gpfs_cluster_id or get_gpfs_cluster_id()
    group_replication_ids = {}

    for filesystem_name in filesystem_names:
        filesystem = get_scale_resource(
            kind=constants.IBM_STORAGE_SCALE_FILESYSTEM_KIND,
            resource_name=filesystem_name,
        )
        expected_group_replication_id = get_expected_group_replication_id(
            filesystem_name
        )
        validate_dr_labels(
            resource_data=filesystem,
            resource_description=f"Filesystem {filesystem_name}",
            group_replication_id=expected_group_replication_id,
            gpfs_cluster_id=gpfs_cluster_id,
        )

        # A DR filesystem is driven by the array, not by GPFS replication.
        replication = filesystem.get("spec", {}).get("local", {}).get("replication")
        if replication != "external":
            raise UnexpectedBehaviour(
                f"Filesystem {filesystem_name} has spec.local.replication "
                f"{replication}, expected external. The filesystem was never "
                "converted for DR"
            )
        group_replication_ids[filesystem_name] = expected_group_replication_id

    logger.info(f"Validated DR labels on filesystems: {group_replication_ids}")
    return group_replication_ids


def validate_storageclass_dr_labels(storageclass_names, gpfs_cluster_id=None):
    """
    Validate the DR labels and parameters on Fusion Access StorageClasses.

    Args:
        storageclass_names (list): Names of the StorageClasses to check.
        gpfs_cluster_id (str): Expected GPFS cluster id. Read from the Daemon CR
            when not given.

    Returns:
        dict: StorageClass name to the filesystem it is backed by.

    Raises:
        UnexpectedBehaviour: If a StorageClass is mislabelled or misconfigured.

    """
    gpfs_cluster_id = gpfs_cluster_id or get_gpfs_cluster_id()
    storageclass_ocp = ocp.OCP(kind=constants.STORAGECLASS)
    backing_filesystems = {}

    for storageclass_name in storageclass_names:
        storageclass = storageclass_ocp.get(resource_name=storageclass_name)
        description = f"StorageClass {storageclass_name}"

        provisioner = storageclass.get("provisioner")
        if provisioner != constants.IBM_STORAGE_SCALE_CSI_PROVISIONER:
            raise UnexpectedBehaviour(
                f"{description} has provisioner {provisioner}, expected "
                f"{constants.IBM_STORAGE_SCALE_CSI_PROVISIONER}"
            )

        volume_backend_fs = storageclass.get("parameters", {}).get("volBackendFs")
        if not volume_backend_fs:
            raise UnexpectedBehaviour(
                f"{description} has no volBackendFs parameter, so it is not "
                "bound to a Scale filesystem"
            )

        # StorageClasses are cluster scoped and have no dr-primary label; the
        # cluster identity is carried only in the storageid prefix.
        validate_dr_labels(
            resource_data=storageclass,
            resource_description=description,
            group_replication_id=get_expected_group_replication_id(volume_backend_fs),
            gpfs_cluster_id=gpfs_cluster_id,
            check_dr_primary=False,
        )

        labels = storageclass.get("metadata", {}).get("labels", {})
        offloaded = labels.get(constants.RAMEN_OFFLOADED_LABEL)
        if offloaded != "true":
            raise UnexpectedBehaviour(
                f"{description} has label {constants.RAMEN_OFFLOADED_LABEL} set "
                f"to {offloaded}, expected 'true'. Ramen would try to replicate "
                "these volumes itself instead of leaving it to the array"
            )
        backing_filesystems[storageclass_name] = volume_backend_fs

    logger.info(f"Validated DR labels on StorageClasses: {backing_filesystems}")
    return backing_filesystems


def get_localdisks(filesystem_name):
    """
    Return the LocalDisk CRs belonging to a filesystem.

    Args:
        filesystem_name (str): Name of the Scale Filesystem CR.

    Returns:
        list: The LocalDisk CRs.

    Raises:
        UnexpectedBehaviour: If the filesystem has no LocalDisks.

    """
    localdisks = get_scale_resource(
        kind=constants.IBM_STORAGE_SCALE_LOCALDISK_KIND,
        selector=f"{constants.IBM_STORAGE_SCALE_FILESYSTEM_LABEL}={filesystem_name}",
    ).get("items", [])
    if not localdisks:
        raise UnexpectedBehaviour(
            f"No LocalDisk is labelled for filesystem {filesystem_name}"
        )
    return localdisks


def validate_localdisk_dr_labels(filesystem_name, gpfs_cluster_id=None):
    """
    Validate the DR labels on every LocalDisk backing a filesystem.

    Args:
        filesystem_name (str): Name of the Scale Filesystem CR.
        gpfs_cluster_id (str): Expected GPFS cluster id. Read from the Daemon CR
            when not given.

    Returns:
        list: Names of the validated LocalDisks.

    Raises:
        UnexpectedBehaviour: If a LocalDisk is mislabelled.

    """
    gpfs_cluster_id = gpfs_cluster_id or get_gpfs_cluster_id()
    group_replication_id = get_expected_group_replication_id(filesystem_name)

    localdisk_names = []
    for localdisk in get_localdisks(filesystem_name):
        localdisk_name = localdisk["metadata"]["name"]
        validate_dr_labels(
            resource_data=localdisk,
            resource_description=f"LocalDisk {localdisk_name}",
            group_replication_id=group_replication_id,
            gpfs_cluster_id=gpfs_cluster_id,
        )
        localdisk_names.append(localdisk_name)

    logger.info(
        f"Validated DR labels on {len(localdisk_names)} LocalDisks of "
        f"filesystem {filesystem_name}"
    )
    return localdisk_names


def get_localdisk_io_state(filesystem_name):
    """
    Return the LocalDiskIO condition reason of every LocalDisk of a filesystem.

    The condition type is always LocalDiskIO; the verdict is in the reason,
    which is LocalDiskIOWorks when every node reaches the disk locally and
    LocalDiskIOFail when some node has to read it over the network instead.

    Args:
        filesystem_name (str): Name of the Scale Filesystem CR.

    Returns:
        dict: LocalDisk name to its LocalDiskIO condition reason, or None when
            the condition has not been reported.

    """
    io_states = {}
    for localdisk in get_localdisks(filesystem_name):
        condition = get_conditions(localdisk).get(LOCALDISK_IO_CONDITION, {})
        io_states[localdisk["metadata"]["name"]] = condition.get("reason")
    return io_states


def validate_localdisk_io_conditions(
    filesystem_name, expected_reason=LOCALDISK_IO_WORKS
):
    """
    Validate the LocalDiskIO condition of every LocalDisk of a filesystem.

    LocalDiskIOFail is not fatal on its own, GPFS keeps serving the disk over
    the network, but on an FA-MDR cluster it means a node lost its path to the
    LUN and the site is one failure away from losing the filesystem. It also
    survives the paths coming back, so it has to be asserted rather than
    assumed.

    Args:
        filesystem_name (str): Name of the Scale Filesystem CR.
        expected_reason (str): Condition reason every LocalDisk must report.

    Returns:
        bool: True if every LocalDisk reports the expected reason.

    Raises:
        ResourceWrongStatusException: If any LocalDisk does not.

    """
    io_states = get_localdisk_io_state(filesystem_name)
    unexpected = {
        name: reason for name, reason in io_states.items() if reason != expected_reason
    }
    if unexpected:
        raise ResourceWrongStatusException(
            f"LocalDisks of filesystem {filesystem_name} do not report "
            f"{LOCALDISK_IO_CONDITION} reason {expected_reason}: {unexpected}"
        )

    logger.info(
        f"All {len(io_states)} LocalDisks of filesystem {filesystem_name} "
        f"report {LOCALDISK_IO_CONDITION} reason {expected_reason}"
    )
    return True


def parse_mmexportfs(mmexportfs):
    """
    Parse the mmexportfs output carried by a FilesystemRecoveryData CR.

    Every line is colon separated and looks like
    ``%%home%%:30_SG_HEADR:fs1:...``, where the second field is the section tag
    and, for the SG_ sections, the third field is the filesystem name.

    Args:
        mmexportfs (str): Content of spec.mmexportfs.

    Returns:
        dict: Section tag to the list of split lines in that section.

    """
    sections = {}
    for line in mmexportfs.splitlines():
        fields = line.split(":")
        if len(fields) < 2 or not fields[1]:
            continue
        sections.setdefault(fields[1], []).append(fields)
    return sections


def validate_filesystem_recovery_data(filesystem_names, gpfs_cluster_id=None):
    """
    Validate the FilesystemRecoveryData CR of every given filesystem.

    The FSRD holds the only copy of the configuration needed to rebuild the
    filesystem at the recovery site, so an FSRD that exists but was never
    exported looks healthy and fails at failover. Both the export status and
    the content are checked.

    Args:
        filesystem_names (list): Names of the filesystems whose FSRDs to check.
        gpfs_cluster_id (str): Expected GPFS cluster id. Read from the Daemon CR
            when not given.

    Returns:
        bool: True if every FSRD is exported and complete.

    Raises:
        UnexpectedBehaviour: If an FSRD is mislabelled or describes the wrong
            filesystem.
        ResourceWrongStatusException: If an FSRD was never exported or is
            missing a required section.

    """
    gpfs_cluster_id = gpfs_cluster_id or get_gpfs_cluster_id()

    for filesystem_name in filesystem_names:
        fsrd = get_scale_resource(
            kind=constants.IBM_STORAGE_SCALE_FSRD_KIND,
            resource_name=filesystem_name,
        )
        description = f"FilesystemRecoveryData {filesystem_name}"

        validate_dr_labels(
            resource_data=fsrd,
            resource_description=description,
            group_replication_id=get_expected_group_replication_id(filesystem_name),
            gpfs_cluster_id=gpfs_cluster_id,
        )

        if not fsrd.get("status", {}).get("lastExportTime"):
            condition = get_conditions(fsrd).get("ConfigurationExported", {})
            raise ResourceWrongStatusException(
                f"{description} has no status.lastExportTime, its configuration "
                f"was never exported. ConfigurationExported condition: "
                f"{condition.get('reason')} - {condition.get('message')}"
            )

        mmexportfs = fsrd.get("spec", {}).get("mmexportfs")
        if not mmexportfs:
            raise ResourceWrongStatusException(
                f"{description} reports an export time but spec.mmexportfs is empty"
            )

        sections = parse_mmexportfs(mmexportfs)
        missing_sections = [
            section
            for section in MMEXPORTFS_REQUIRED_SECTIONS
            if section not in sections
        ]
        if missing_sections:
            raise ResourceWrongStatusException(
                f"{description} mmexportfs output is missing sections "
                f"{missing_sections}, the filesystem cannot be rebuilt at the "
                "recovery site"
            )

        # Every SG_ line names the filesystem it describes. An FSRD holding
        # another filesystem's configuration would import the wrong disks.
        described = {
            fields[2]
            for section in MMEXPORTFS_FILESYSTEM_SECTIONS
            for fields in sections[section]
            if len(fields) > 2
        }
        if described != {filesystem_name}:
            raise UnexpectedBehaviour(
                f"{description} mmexportfs output describes filesystems "
                f"{described}, expected only {filesystem_name}"
            )

        logger.info(
            f"{description} is exported and carries all required sections "
            f"{list(MMEXPORTFS_REQUIRED_SECTIONS)}"
        )

    return True


def get_mmexportfs_disks(filesystem_name):
    """
    Return the NSD names listed in a filesystem's exported configuration.

    NSD names use underscores where the LocalDisk CR name uses hyphens, so they
    are normalised to the CR form to be comparable.

    Args:
        filesystem_name (str): Name of the filesystem.

    Returns:
        set: LocalDisk names referenced by the SG_DISKS section.

    """
    fsrd = get_scale_resource(
        kind=constants.IBM_STORAGE_SCALE_FSRD_KIND,
        resource_name=filesystem_name,
    )
    sections = parse_mmexportfs(fsrd.get("spec", {}).get("mmexportfs", ""))
    return {
        fields[4].replace("_", "-")
        for fields in sections.get(MMEXPORTFS_SG_DISKS, [])
        if len(fields) > 4
    }


def validate_recovery_data_disks(filesystem_name):
    """
    Validate that the exported configuration lists exactly the LocalDisks in use.

    Args:
        filesystem_name (str): Name of the filesystem.

    Returns:
        bool: True if the exported disk list matches the LocalDisk CRs.

    Raises:
        UnexpectedBehaviour: If the two disagree.

    """
    exported_disks = get_mmexportfs_disks(filesystem_name)
    localdisks = {
        localdisk["metadata"]["name"] for localdisk in get_localdisks(filesystem_name)
    }
    if exported_disks != localdisks:
        raise UnexpectedBehaviour(
            f"Exported configuration of filesystem {filesystem_name} lists disks "
            f"{exported_disks} but its LocalDisks are {localdisks}. The recovery "
            "site would import the wrong set of disks"
        )

    logger.info(
        f"Exported configuration of filesystem {filesystem_name} matches its "
        f"{len(localdisks)} LocalDisks"
    )
    return True


def validate_volume_group_replication_class(filesystem_names, gpfs_cluster_id=None):
    """
    Validate the VolumeGroupReplicationClass of every given filesystem.

    Args:
        filesystem_names (list): Names of the filesystems. On Fusion Access the
            VGRC is named after the filesystem it serves.
        gpfs_cluster_id (str): Expected GPFS cluster id. Read from the Daemon CR
            when not given.

    Returns:
        dict: VGRC name to its groupreplicationid.

    Raises:
        UnexpectedBehaviour: If a VGRC is missing, mislabelled or misconfigured.

    """
    gpfs_cluster_id = gpfs_cluster_id or get_gpfs_cluster_id()
    vgrc_ocp = ocp.OCP(kind=constants.VOLUME_GROUP_REPLICATION_CLASS)
    group_replication_ids = {}

    for filesystem_name in filesystem_names:
        vgrc = vgrc_ocp.get(resource_name=filesystem_name)
        description = f"VolumeGroupReplicationClass {filesystem_name}"

        provisioner = vgrc.get("spec", {}).get("provisioner")
        if provisioner != constants.IBM_STORAGE_SCALE_CSI_PROVISIONER:
            raise UnexpectedBehaviour(
                f"{description} has provisioner {provisioner}, expected "
                f"{constants.IBM_STORAGE_SCALE_CSI_PROVISIONER}"
            )

        # Replication is synchronous at the array, so there is no schedule to
        # run. A non zero interval would make Ramen wait for a snapshot that
        # the Scale provisioner never takes.
        scheduling_interval = (
            vgrc.get("spec", {}).get("parameters", {}).get("schedulingInterval")
        )
        if scheduling_interval != "0m":
            raise UnexpectedBehaviour(
                f"{description} has schedulingInterval {scheduling_interval}, "
                "expected 0m for synchronous array replication"
            )

        labels = validate_dr_labels(
            resource_data=vgrc,
            resource_description=description,
            group_replication_id=get_expected_group_replication_id(filesystem_name),
            gpfs_cluster_id=gpfs_cluster_id,
            check_dr_primary=False,
        )

        if labels.get(constants.RAMEN_GLOBAL_LABEL) != "true":
            raise UnexpectedBehaviour(
                f"{description} is missing label "
                f"{constants.RAMEN_GLOBAL_LABEL}=true, Ramen will not consider "
                "it when building the DRPolicy peer classes"
            )
        group_replication_ids[filesystem_name] = labels[
            constants.RAMEN_GROUP_REPLICATION_ID_LABEL
        ]

    logger.info(f"Validated VolumeGroupReplicationClasses: {group_replication_ids}")
    return group_replication_ids


def validate_dr_resource_parity(filesystem_names, storageclass_names):
    """
    Validate that both managed clusters describe the same filesystems.

    Pairing is by groupreplicationid, which must be identical at both sites
    because it is derived from the filesystem uid. The storageid must differ,
    because it is prefixed with the local GPFS cluster id. Two sites reporting
    the same storageid means one of them never resolved its own cluster id.

    Args:
        filesystem_names (list): Filesystems expected at both sites.
        storageclass_names (list): StorageClasses expected at both sites.

    Returns:
        bool: True if both sites agree.

    Raises:
        UnexpectedBehaviour: If the sites disagree.

    """
    restore_index = config.cur_index
    per_cluster = {}
    try:
        for cluster in get_non_acm_cluster_config():
            config.switch_ctx(cluster.MULTICLUSTER["multicluster_index"])
            cluster_name = cluster.ENV_DATA["cluster_name"]
            logger.info(f"Collecting Scale DR resources on cluster {cluster_name}")

            gpfs_cluster_id = get_gpfs_cluster_id()
            per_cluster[cluster_name] = {
                "gpfs_cluster_id": gpfs_cluster_id,
                "filesystems": validate_filesystem_dr_labels(
                    filesystem_names, gpfs_cluster_id=gpfs_cluster_id
                ),
                "storageclasses": validate_storageclass_dr_labels(
                    storageclass_names, gpfs_cluster_id=gpfs_cluster_id
                ),
                "vgrcs": validate_volume_group_replication_class(
                    filesystem_names, gpfs_cluster_id=gpfs_cluster_id
                ),
            }
    finally:
        config.switch_ctx(restore_index)

    if len(per_cluster) < 2:
        raise UnexpectedBehaviour(
            f"Parity needs two managed clusters, found {list(per_cluster)}"
        )

    cluster_ids = {name: data["gpfs_cluster_id"] for name, data in per_cluster.items()}
    if len(set(cluster_ids.values())) != len(cluster_ids):
        raise UnexpectedBehaviour(
            f"Managed clusters do not have distinct GPFS cluster ids: {cluster_ids}"
        )

    first_cluster, *other_clusters = list(per_cluster)
    for resource_type in ("filesystems", "vgrcs"):
        reference = per_cluster[first_cluster][resource_type]
        for cluster_name in other_clusters:
            compared = per_cluster[cluster_name][resource_type]
            if reference != compared:
                raise UnexpectedBehaviour(
                    f"{resource_type} groupreplicationids differ between "
                    f"{first_cluster} ({reference}) and {cluster_name} ({compared})"
                )

    logger.info(
        f"Both managed clusters describe the same filesystems with distinct "
        f"GPFS cluster ids: {cluster_ids}"
    )
    return True


def get_volume_group_replications(namespace):
    """
    Return the VolumeGroupReplication CRs in a namespace.

    Args:
        namespace (str): Namespace to read from.

    Returns:
        list: The VolumeGroupReplication CRs.

    """
    vgr_ocp = ocp.OCP(
        kind=constants.VOLUME_GROUP_REPLICATION,
        namespace=namespace,
    )
    return vgr_ocp.get().get("items", [])


def get_vgr_dr_state(namespace):
    """
    Return the Scale DR state label of every VolumeGroupReplication.

    The scale.spectrum.ibm.com/dr-state label is the Scale side of the demotion
    state machine and is the only place the stall is visible. The VGR status
    carries no conditions and the per PVC conditions look identical on a healthy
    and a wedged filesystem, so this label is the diagnostic to read first.

    Args:
        namespace (str): Namespace holding the VolumeGroupReplications.

    Returns:
        dict: VolumeGroupReplication name to its dr-state label value.

    """
    return {
        vgr["metadata"]["name"]: vgr["metadata"]
        .get("labels", {})
        .get(constants.IBM_STORAGE_SCALE_DR_STATE_LABEL)
        for vgr in get_volume_group_replications(namespace)
    }


def validate_vgr_created(namespace, expected_count=None):
    """
    Validate that Ramen created VolumeGroupReplications for a workload.

    Args:
        namespace (str): Namespace holding the workload.
        expected_count (int): Number of VolumeGroupReplications expected. Only
            presence is checked when not given.

    Returns:
        list: Names of the VolumeGroupReplications found.

    Raises:
        UnexpectedBehaviour: If none exist or the count does not match.

    """
    vgr_names = [
        vgr["metadata"]["name"] for vgr in get_volume_group_replications(namespace)
    ]
    if not vgr_names:
        raise UnexpectedBehaviour(
            f"No VolumeGroupReplication was created in namespace {namespace}"
        )
    if expected_count is not None and len(vgr_names) != expected_count:
        raise UnexpectedBehaviour(
            f"Namespace {namespace} has {len(vgr_names)} VolumeGroupReplications "
            f"{vgr_names}, expected {expected_count}"
        )

    logger.info(f"VolumeGroupReplications in namespace {namespace}: {vgr_names}")
    return vgr_names


def wait_for_vgr_dr_state(namespace, state, timeout=600, sleep=15):
    """
    Wait until every VolumeGroupReplication reaches a Scale DR state.

    Args:
        namespace (str): Namespace holding the VolumeGroupReplications.
        state (str): Expected dr-state label value.
        timeout (int): Seconds to wait.
        sleep (int): Seconds between checks.

    Returns:
        bool: True once every VolumeGroupReplication reports the state.

    Raises:
        ResourceWrongStatusException: If they do not within the timeout.

    """
    dr_states = {}
    try:
        for dr_states in TimeoutSampler(
            timeout=timeout,
            sleep=sleep,
            func=get_vgr_dr_state,
            namespace=namespace,
        ):
            if dr_states and all(value == state for value in dr_states.values()):
                logger.info(
                    f"All VolumeGroupReplications in namespace {namespace} "
                    f"reached dr-state {state}"
                )
                return True
            logger.info(
                f"Waiting for dr-state {state} in namespace {namespace}, "
                f"current states: {dr_states}"
            )
    except TimeoutExpiredError:
        raise ResourceWrongStatusException(
            f"VolumeGroupReplications in namespace {namespace} did not reach "
            f"dr-state {state} within {timeout} seconds, current states: "
            f"{dr_states}"
        )
