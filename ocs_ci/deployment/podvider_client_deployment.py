class DeployHostedOCP:
    def __init__(self, onboarding_key: str):
        self.onboarding_key = onboarding_key


class DeployHostedOCS:
    def __init__(
        self, storageclass_names, storageprovider_endpoint, storageclient_name
    ):
        self.storageclass_names = storageclass_names
        self.storageprovider_endpoint = storageprovider_endpoint
        self.storageclient_name = storageclient_name

    def claim_storageclass(self, storageclass_name):
        pass
