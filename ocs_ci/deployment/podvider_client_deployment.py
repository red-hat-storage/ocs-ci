class DeployHostedOCP:
    def __init__(
        self,
        cluster_name: str,
    ):
        self.cluster_name = cluster_name


class DeployHostedOCS:
    def __init__(
        self,
        cluster_name: str,
        storageclass_names: set,
        storageprovider_endpoint: str,
        storageclient_name: str,
        onboarding_key: str,
    ):
        self.cluster_name = cluster_name
        self.storageclass_names = storageclass_names
        self.storageprovider_endpoint = storageprovider_endpoint
        self.storageclient_name = storageclient_name
        self.onboarding_key = onboarding_key

    def claim_storageclass(self, storageclass_name):
        pass
