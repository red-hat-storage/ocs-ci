"""
A module for all StorageClient functionalities and abstractions.
"""


class StorageClient:
    """
    Base StorageClient class
    """

    def __init__(self, client_context = None):
        """
        Args:
            client_context (int): index of cluster context. This is needed for
                client operations executed on client
                (e.g. manipulation of heartbeat cronjob)
        """
        self.client_context = client_context
        if self.client_context:
            self.heartbeat_cronjob = get_heartbeat_cronjob()
            self.original_context = ocsci_config.cluster_ctx
        else:
            self.heartbeat_cronjob = None
            self.original_context = None

    def get_ocs_version(self):
        """
        Get ocs version from storageclient resource.

        Returns:
            string: client ocs version

        """
        pass

    def set_ocs_version(self, version):
        """
        Update ocs client version in storageclient resource. This change assumes
        that the hearthbeat is stopped so that the version is not overwritten by it.

        Args:
            version (str): OCS version to be set

        """
        pass

    def stop_heartbeat(self):
        """
        Suspend status reporter cron job.
        """
        self._switch_client_cluster()
        patch_param = f'{{"spec": {{"suspend": "true"}}}}'
        self.heartbeat_cronjob.patch(resource_name=self.heartbeat_cronjo.name, params=patch_param)
        self._switch_original_cluster()

    def resume_heartbeat(self):
        """
        Resume status reporter cron job.
        """
        self._switch_client_cluster()
        patch_param = f'{{"spec": {{"suspend": "false"}}}}'
        self.heartbeat_cronjob.patch(resource_name=self.heartbeat_cronjo.name, params=patch_param)
        self._switch_original_cluster()

    def get_heartbeat_cronjob(self):
        """
        Returns:
            object: status reporter cronjob OCP object

        """
        cronjobs_obj = ocp.OCP(kind=constants.CRONJOB, namespace=config.cluster_ctx.ENV_DATA["cluster_namespace"])
        cronjob = [ocp.OCP(**job) for job in cronjobs_obj.get().get("items") if job.name.endswith("status-reporter")][0]
        return cronjob

    def _switch_original_cluster(self):
        """
        Switch context to original cluster.
        """
        config.switch_ctx(self.original_context)
        log.info(f"Switched to original cluster with index {self.original_context}")

    def _switch_client_cluster(self):
        """
        Switch context to client cluster.
        """
        config.switch_ctx(self.client_context)
        log.info(f"Switched to client cluster with index {self.original_cluster}")
