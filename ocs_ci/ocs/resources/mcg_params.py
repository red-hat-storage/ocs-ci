from dataclasses import dataclass

from ocs_ci.ocs.resources.namespacestore import NamespaceStore
from ocs_ci.ocs.resources.objectbucket import ObjectBucket
from ocs_ci.ocs.resources.pod import Pod


@dataclass
class NSFS:
    """
    An NSFS dataclass to represent NSFS test parametrization and provide a central state store

    Parametrization parameters:
        method (str): The method to use for namespacestore creation. OC | CLI
        pvc_name (str): Name of the PVC that will host the namespace filesystem
        pvc_size (int): Size of the PVC in Gi
        sub_path (str): The path to a sub directory inside the PVC FS which the NSS will use as its root directory
        mount_existing_dir (bool): Whether to mount an existing directory or create a new one
        existing_dir_mode (int): The mode of the existing directory
        fs_backend (str): The file system backend type - CEPH_FS | GPFS | NFSv4. Defaults to None.
        mount_path (str): The path to the mount point of the NSFS
        uid (int): The UID of the user that will be used to create the NSFS
        gid (int): The GID of the user that will be used to create the NSFS
    """

    method: str = "CLI"
    pvc_name: str = None
    pvc_size: int = 20
    sub_path: str = None
    fs_backend: str = None
    mount_existing_dir: bool = False
    existing_dir_mode: int = 777
    mount_path: str = "/nsfs"
    uid: int = 5678
    gid: int = 1234
    """
    State parameters; These should not be modified unless needed, and will be (over/)written
    after the NSFS object will be passed to the bucket factory.

        interface_pod (Pod): The pod that will be used to interact with the NSFS
        bucket_name (str): The name of the NSFS bucket
        mounted_bucket_path (str): The path to where the bucket is "mounted" in the FS
        s3_creds (str): The NSFS S3 credentials
        nss (NamespaceStore): The namespacestore that the NSFS uses
    """
    interface_pod: Pod = None
    bucket_obj: ObjectBucket = None
    bucket_name: str = None
    mounted_bucket_path: str = None
    s3_creds: dict = None
    nss: NamespaceStore = None
