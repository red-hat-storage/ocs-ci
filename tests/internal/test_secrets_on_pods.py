import logging
import yaml

from ocs_ci.framework.testlib import ManageTest, bugzilla, tier1
from ocs_ci.utility.utils import run_cmd

logger = logging.getLogger(__name__)
# The below expected keys and names are gathered from pods with safe security.
EXPECTED_KEYS = {
    "mon_initial_members",
    "mon_host",
    "fsid",
    "ceph-username",
    "ceph-secret",
    "token",
}
EXPECTED_NAMES = {"rook-ceph-config", "rook-ceph-mon", "ocs-kms-token"}


class TestSecretsinEnvVariables(ManageTest):
    @tier1
    @bugzilla("2171965")
    def test_secrets_in_env_variables(self):
        """
        Testing if secrets are used in env variables of pods, in a
        normal cluster
        """
        logger.info("Checking pods with security refrence in them.")
        cmd = """
        oc get all -o jsonpath='{range .items[?(@..secretKeyRef)]} {.kind} {.metadata.name}{end}' -A
        """
        output = run_cmd(cmd).strip().split()
        logger.info("Checking securityKeyRef in pods")
        for i in range(0, len(output), 2):
            if output[i] == "Pod":
                pod = output[i + 1]
                if "rook-ceph-" in pod:
                    data = run_cmd(
                        f"oc --namespace=openshift-storage get pod {pod} -o yaml"
                    )
                    yaml_data = yaml.safe_load(data)
                    k, n = self.checking_securtiyKeyRef(yaml_data)
                    for value in k:
                        assert (
                            value in EXPECTED_KEYS
                        ), f"Key: {value} is not expected in securityKeyRef, may be secrutiy breach please check"
                    for value in n:
                        assert (
                            value in EXPECTED_NAMES
                        ), f"Name: {value} is not expected in securityKeyRef, may be secrutiy breach please check"
            else:
                break

    def checking_securtiyKeyRef(self, data):
        """
        This function takes the data from describe pod and then checks what is the entry
        in securityKeyRef of pod.

        args:
        data: yaml: describe data of the pod

        returns:
        key, name : list, list : containing list of keys and names in securityKeyRef
        """
        key, name = [], []
        yaml_data = data
        env_data = yaml_data["spec"]["containers"][0]["env"]
        for i in env_data:
            if "valueFrom" in i.keys():
                if "secretKeyRef" in i["valueFrom"].keys():
                    item = i["valueFrom"]["secretKeyRef"]
                    key.append(item["key"])
                    name.append(item["name"])
        return key, name
