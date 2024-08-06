import logging
import yaml

from ocs_ci.framework.testlib import ManageTest, bugzilla, tier1, green_squad
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


class TestSecretsAndSecurityContext(ManageTest):
    @tier1
    @green_squad
    @bugzilla("2171965")
    def test_secrets_in_env_variables(self):
        """
        Testing if secrets are used in env variables of pods
        """
        logger.info("Checking pods with security refrence in them.")
        cmd = "oc get all -o jsonpath='{range .items[?(@..secretKeyRef)]} {.kind} {.metadata.name}{end}' -A"
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

    @tier1
    @green_squad
    @bugzilla("2180732")
    def test_securityContext_in_Crashcollector(self):
        """
        Testing security context of rook-ceph-crash-collector pods, in a
        normal cluster
        """
        logger.info("Checking security context on rook-ceph-crashcollector pods")
        cmd = "oc --namespace=openshift-storage get pods -l app=rook-ceph-crashcollector -o name"
        output = run_cmd(cmd).strip().split("\n")
        logger.info("Checking securityContext in ceph-crash container")
        for pod in output:
            data = run_cmd(f"oc --namespace=openshift-storage get {pod} -o yaml")
            yaml_data = yaml.safe_load(data)
            securityContext = self.checking_securtiyContext_of_cephcrash_container(
                yaml_data
            )

            assert (
                securityContext["runAsGroup"] == 167
            ), f"Security Context key runAsGroup value is not as expected in pod {pod} \
                expected value is 167"
            assert securityContext[
                "runAsNonRoot"
            ], f"Security Context key runAsNonRoot value is not as expected in pod {pod} \
                expected value is True"
            assert (
                securityContext["runAsUser"] == 167
            ), f"Security Context key runAsUser value is not as expected in pod {pod} \
                expected value is 167"

    def checking_securtiyKeyRef(self, yaml_data):
        """
        This function takes the data from describe pod and then checks what is the entry
        in securityKeyRef of pod.

        args:
        yaml_data: yaml: describe data of the pod

        returns:
        key, name : list, list : containing list of keys and names in securityKeyRef
        """
        key, name = [], []
        env_data = yaml_data["spec"]["containers"][0]["env"]
        for i in env_data:
            if "valueFrom" in i.keys():
                if "secretKeyRef" in i["valueFrom"].keys():
                    item = i["valueFrom"]["secretKeyRef"]
                    key.append(item["key"])
                    name.append(item["name"])
        return key, name

    def checking_securtiyContext_of_cephcrash_container(self, yaml_data):
        """
        This function takes the data from describe pod of rook-ceph-crashcollector and
        then checks what is the entry in securityContext of ceph-crash container.

        args:
        yaml_data: yaml: describe data of the pod

        returns:
        securityContext: dict: dictonary data of security context
        """
        logger.info("Checking the security Context of the container ceph-crash")
        container = yaml_data["spec"]["containers"][0]
        logger.info(f"checking security context of container {container}")
        securityContext = container["securityContext"]
        return securityContext
