"""
This module is used for generating custom SSL certificates.
"""

import argparse
import base64
import logging
import os
import requests
import tempfile
import yaml

from OpenSSL import crypto

from ocs_ci.framework import config
from ocs_ci.ocs import constants, defaults, exceptions, ocp
from ocs_ci.utility.utils import (
    download_file,
    exec_cmd,
    TimeoutSampler,
    wait_for_machineconfigpool_status,
)


logger = logging.getLogger(__name__)


class Key(crypto.PKey):
    """
    Wrapper class over crypto.PKey used for customization.
    """

    def __str__(self):
        return crypto.dump_privatekey(crypto.FILETYPE_PEM, self).decode()


class CSR(crypto.X509Req):
    """
    Wrapper class over crypto.X509Req used for customization.
    """

    def __str__(self):
        return crypto.dump_certificate_request(crypto.FILETYPE_PEM, self).decode()


class Certificate:
    """
    Common certificate class.

    Args:
        cn (str): Certificate Common Name
        sans (list): list of Subject Alternative Names (prefixed by the type
            like 'DNS:' or 'IP:')

    """

    def __init__(self, cn, sans=None):
        self._key = None
        self._csr = None
        self._crt = None

        self.cn = cn
        self.sans = sans or []

    def __str__(self):
        """
        Concatenate key and certificate.

        Returns:
             str : key and certificate
        """
        s = str(self.key)
        s += str(self.crt)
        return s

    @property
    def key(self):
        if self._key is None:
            self.generate_key()
        return self._key

    @key.setter
    def key(self, key):
        self._key = key

    @property
    def csr(self):
        if self._csr is None:
            self.generate_csr()
        return self._csr

    @csr.setter
    def csr(self, csr):
        self._csr = csr

    @property
    def crt(self):
        if self._crt is None:
            self.get_crt()
        return self._crt

    @crt.setter
    def crt(self, crt):
        self._crt = crt

    def save_key(self, path):
        """
        Save certificate key to file

        Args:
            path (str): path where to save certificate key

        """
        with open(path, "w") as f:
            f.write(str(self.key))

    def save_csr(self, path):
        """
        Save certificate signing request to file

        Args:
            path (str): path where to save certificate signing request

        """
        with open(path, "w") as f:
            f.write(str(self.csr))

    def save_crt(self, path):
        """
        Save certificate to file

        Args:
            path (str): path where to save certificate

        """
        with open(path, "w") as f:
            f.write(str(self.crt))


class OCSCertificate(Certificate):
    """
    Generate custom certificate signed by the automatic signing certification
    authority

    Args:
        signing_service (str): URL of the automatic signing CA service
        cn (str): Certificate Common Name
        sans (list): list of Subject Alternative Names (prefixed by the type
            like 'DNS:' or 'IP:')

    """

    def __init__(self, signing_service, cn, sans=None):
        super().__init__(cn=cn, sans=sans)
        # url pointing to automatic signing service
        self.signing_service = signing_service

    def generate_key(self):
        """
        Generate private key for the certificate
        """
        self.key = Key()
        self.key.generate_key(crypto.TYPE_RSA, constants.OPENSSL_KEY_SIZE)

    def generate_csr(self):
        """
        Generate Certificate Signing Request for the certificate
        """
        self.csr = CSR()
        subj = self.csr.get_subject()
        subj.CN = self.cn
        subj.countryName = constants.OPENSSL_CERT_COUNTRY_NAME
        subj.stateOrProvinceName = constants.OPENSSL_CERT_STATE_OR_PROVINCE_NAME
        subj.localityName = constants.OPENSSL_CERT_LOCALITY_NAME
        subj.organizationName = constants.OPENSSL_CERT_ORGANIZATION_NAME
        subj.organizationalUnitName = constants.OPENSSL_CERT_ORGANIZATIONAL_UNIT_NAME
        subj.emailAddress = constants.OPENSSL_CERT_EMAIL_ADDRESS

        sans = ", ".join(self.sans)
        self.csr.add_extensions(
            [crypto.X509Extension(b"subjectAltName", False, sans.encode())]
        )

        self.csr.set_pubkey(self.key)
        self.csr.sign(self.key, "sha256")

    def get_crt(self):
        """
        Use automatic signing CA service to sign the CSR
        """
        r = requests.post(
            f"{self.signing_service}/get_cert",
            crypto.dump_certificate_request(crypto.FILETYPE_PEM, self.csr),
            timeout=120,
        )
        self.crt = r.content.decode()


class LetsEncryptCertificate(Certificate):
    """
    Generate custom certificate signed by Let's Encrypt authority

    Args:
        dns_plugin (str): Certbot DNS Plugin name (default: 'dns-route53')
        cn (str): Certificate Common Name
        sans (list): list of Subject Alternative Names (prefixed by the type
            like 'DNS:' or 'IP:')

    """

    def __init__(self, dns_plugin, cn, sans=None):
        super().__init__(cn=cn, sans=sans)
        # url pointing to automatic signing service
        self.dns_plugin = dns_plugin

    def generate_key(self):
        """
        Generate private key for the certificate
        """
        self.key = Key()
        self.key.generate_key(crypto.TYPE_RSA, constants.OPENSSL_KEY_SIZE)

    def generate_csr(self):
        """
        Generate Certificate Signing Request for the certificate
        """
        self.csr = CSR()
        subj = self.csr.get_subject()
        subj.CN = self.cn
        subj.countryName = constants.OPENSSL_CERT_COUNTRY_NAME
        subj.stateOrProvinceName = constants.OPENSSL_CERT_STATE_OR_PROVINCE_NAME
        subj.localityName = constants.OPENSSL_CERT_LOCALITY_NAME
        subj.organizationName = constants.OPENSSL_CERT_ORGANIZATION_NAME
        subj.organizationalUnitName = constants.OPENSSL_CERT_ORGANIZATIONAL_UNIT_NAME
        subj.emailAddress = constants.OPENSSL_CERT_EMAIL_ADDRESS

        sans = ", ".join(self.sans)
        self.csr.add_extensions(
            [crypto.X509Extension(b"subjectAltName", False, sans.encode())]
        )

        self.csr.set_pubkey(self.key)
        self.csr.sign(self.key, "sha256")

    def get_crt(self):
        """
        Get certificate from Let's Encrypt
        """
        cert_file = self.run_certbot()
        if cert_file:
            with open(cert_file, "r") as f:
                self.crt = f.read()

    def run_certbot(self):
        if config.RUN.get("run_id"):
            certbot_dir = os.path.join(
                os.path.expanduser(config.RUN["log_dir"]),
                f"certbot-letsencrypt-{config.RUN['run_id']}",
            )
            os.mkdir(certbot_dir)
        else:
            certbot_dir = tempfile.mkdtemp(prefix="certbot-letsencrypt_")
        logger.info(f"certbot directory: {certbot_dir}")
        key_file = os.path.join(certbot_dir, "key.pem")
        csr_file = os.path.join(certbot_dir, "csr.pem")
        cert_file = os.path.join(certbot_dir, "crt.pem")
        fullchain_file = os.path.join(certbot_dir, "fullchain-crt.pem")
        chain_file = os.path.join(certbot_dir, "chain.pem")
        conf_dir = os.path.join(certbot_dir, "config")
        work_dir = os.path.join(certbot_dir, "work")
        logs_dir = os.path.join(certbot_dir, "logs")
        self.save_key(key_file)
        self.save_csr(csr_file)
        cmd = (
            "certbot certonly --register-unsafely-without-email --agree-tos "
            f"-n --no-autorenew --key-path {key_file} --csr {csr_file} --cert-path {cert_file} "
            f"--fullchain-path {fullchain_file} --chain-path {chain_file} "
            f"--config-dir {conf_dir} --work-dir {work_dir} --logs-dir {logs_dir} "
            f"--{self.dns_plugin} "
        )
        result = exec_cmd(cmd)
        if result.returncode != 0:
            logger.info(f"certbot return code: {result.returncode}")
            logger.info(f"certbot stdout: {result.stdout}")
            logger.info(f"certbot stderr: {result.stderr}")
        return fullchain_file


def get_root_ca_cert():
    """
    If not available, download Root CA Certificate for custom Ingress
    certificate.

    Returns
        str: Path to Root CA Certificate

    """
    signing_service_url = config.DEPLOYMENT.get("cert_signing_service_url")
    ssl_ca_cert = config.DEPLOYMENT.get(
        "ingress_ssl_ca_cert", defaults.INGRESS_SSL_CA_CERT
    )
    if ssl_ca_cert and not os.path.exists(ssl_ca_cert):
        cert_provider = config.DEPLOYMENT.get("custom_ssl_cert_provider")
        if cert_provider == constants.SSL_CERT_PROVIDER_OCS_QE_CA:
            if not signing_service_url:
                msg = (
                    f"CA Certificate file {ssl_ca_cert} doesn't exists and "
                    "`DEPLOYMENT['cert_signing_service_url']` is not defined. "
                    "Unable to download CA Certificate!"
                )
                logger.error(msg)
                raise exceptions.ConfigurationError(msg)
            download_file(f"{signing_service_url}/root-ca.crt", ssl_ca_cert)
            logger.info(f"CA Certificate downloaded and saved to '{ssl_ca_cert}'")
        elif cert_provider == constants.SSL_CERT_PROVIDER_LETS_ENCRYPT:
            return ""
    return ssl_ca_cert


def update_kubeconfig_with_ca_cert(skip_tls_verify=False):
    """
    Update kubeconfig file with the kube-apiserver CA certificate authority data.
    This extracts the CA certificate from the cluster's kube-apiserver-server-ca
    configmap and embeds it in the kubeconfig. This allows users who download
    the kubeconfig from a shared location to automatically trust the API server
    certificate without additional configuration.

    Args:
        skip_tls_verify (bool): True if allow skipping TLS verification

    """
    kubeconfig_path = os.path.join(
        config.ENV_DATA["cluster_path"], config.RUN.get("kubeconfig_location")
    )

    if not os.path.exists(kubeconfig_path):
        logger.warning(
            f"Kubeconfig file '{kubeconfig_path}' does not exist. "
            "Skipping kubeconfig CA update."
        )
        return

    logger.info(
        f"Updating kubeconfig '{kubeconfig_path}' with kube-apiserver CA certificate"
    )

    try:
        ignore_tls = "--insecure-skip-tls-verify " if skip_tls_verify else ""
        cmd = (
            f"oc get configmap -n openshift-kube-apiserver kube-apiserver-server-ca {ignore_tls}"
            "-o jsonpath='{.data.ca-bundle\\.crt}'"
        )
        result = exec_cmd(cmd)
        ca_bundle = result.stdout.decode("utf-8").strip()

        if not ca_bundle or "BEGIN CERTIFICATE" not in ca_bundle:
            logger.warning(
                "Failed to extract CA certificate from cluster. "
                "The configmap may not exist or is empty."
            )
            return

        lines = ca_bundle.split("\n")
        first_cert_lines = []
        cert_count = 0
        in_cert = False

        for line in lines:
            if "BEGIN CERTIFICATE" in line:
                cert_count += 1
                in_cert = True

            if cert_count == 1 and in_cert:
                first_cert_lines.append(line)

            if "END CERTIFICATE" in line and cert_count == 1:
                break

        if not first_cert_lines:
            logger.warning("Failed to extract first certificate from CA bundle")
            return

        first_cert = "\n".join(first_cert_lines)
        ca_cert_base64 = base64.b64encode(first_cert.encode("utf-8")).decode("utf-8")

        with open(kubeconfig_path, "r") as f:
            kubeconfig = yaml.safe_load(f)

        for cluster in kubeconfig.get("clusters", []):
            cluster_data = cluster.get("cluster", {})
            if "certificate-authority" in cluster_data:
                del cluster_data["certificate-authority"]
            cluster_data["certificate-authority-data"] = ca_cert_base64
            logger.info(
                f"Updated certificate-authority-data for cluster '{cluster.get('name', 'unknown')}'"
            )

        with open(kubeconfig_path, "w") as f:
            yaml.dump(kubeconfig, f, default_flow_style=False)

        logger.info(
            "Kubeconfig updated successfully with kube-apiserver CA certificate"
        )

    except Exception as e:
        logger.warning(
            f"Failed to update kubeconfig with CA certificate: {e}. "
            "This is not critical, but users may need to use --insecure-skip-tls-verify "
            "when using this kubeconfig."
        )


def configure_custom_ingress_cert(
    skip_tls_verify=False, wait_for_machineconfigpool=True
):
    """
    Configure custom SSL certificate for ingress. If the certificate doesn't
    exists, generate new one signed by automatic certificate signing service.

    Args:
        skip_tls_verify (bool): True if allow skipping TLS verification
        wait_for_machineconfigpool (bool): True if it should wait for machineConfigPool

    Raises:
        ConfigurationError: when some required parameter is not configured

    """
    ignore_tls = ""
    if skip_tls_verify:
        ignore_tls = "--insecure-skip-tls-verify "
    logger.info("Configure custom ingress certificate")
    base_domain = config.ENV_DATA["base_domain"]
    cluster_name = config.ENV_DATA["cluster_name"]
    apps_domain = f"*.apps.{cluster_name}.{base_domain}"

    ssl_key = config.DEPLOYMENT.get("ingress_ssl_key", defaults.INGRESS_SSL_KEY)
    ssl_cert = config.DEPLOYMENT.get("ingress_ssl_cert", defaults.INGRESS_SSL_CERT)

    signing_service_url = config.DEPLOYMENT.get("cert_signing_service_url")

    if not (os.path.exists(ssl_key) and os.path.exists(ssl_cert)):
        cert_provider = config.DEPLOYMENT.get("custom_ssl_cert_provider")
        if (
            cert_provider == constants.SSL_CERT_PROVIDER_OCS_QE_CA
            and not signing_service_url
        ):
            msg = (
                "Custom certificate files for ingress doesn't exists and "
                "`DEPLOYMENT['cert_signing_service_url']` is not defined. "
                "Unable to generate custom Ingress certificate!"
            )
            logger.error(msg)
            raise exceptions.ConfigurationError(msg)

        logger.debug(
            f"Files '{ssl_key}' and '{ssl_cert}' doesn't exist, generate certificate"
        )
        if cert_provider == constants.SSL_CERT_PROVIDER_OCS_QE_CA:
            cert = OCSCertificate(
                signing_service=signing_service_url,
                cn=apps_domain,
                sans=[f"DNS:{apps_domain}"],
            )
        elif cert_provider == constants.SSL_CERT_PROVIDER_LETS_ENCRYPT:
            cert = LetsEncryptCertificate(
                dns_plugin=config.DEPLOYMENT["certbot_dns_plugin"],
                cn=apps_domain,
                sans=[f"DNS:{apps_domain}"],
            )
        else:
            msg = (
                f"Certbot DNS plugin {config.DEPLOYMENT['certbot_dns_plugin']} not supported. "
                "Supported `DEPLOYMENT['certbot_dns_plugin']` options are: 'dns-route53'. "
                "Unable to generate custom Ingress certificate!"
            )
            logger.error(msg)
            raise exceptions.ConfigurationError(msg)
        logger.debug(f"Certificate key: {cert.key}")
        logger.debug(f"Certificate: {cert.crt}")
        cert.save_key(ssl_key)
        cert.save_crt(ssl_cert)
        logger.info(f"Certificate saved to '{ssl_cert}' and key to '{ssl_key}'")

    ssl_ca_cert = get_root_ca_cert()
    if ssl_ca_cert:
        logger.debug(f"Configure '{ssl_ca_cert}' for proxy configuration object")
        configure_trusted_ca_bundle(ssl_ca_cert, skip_tls_verify=skip_tls_verify)

    logger.debug(f"Configuring '{ssl_key}' and '{ssl_cert}' for ingress")
    # check if ocs-cert secret already exists, if yes, delete the old secret before the new one is created
    secret_obj = ocp.OCP(
        kind=constants.SECRET,
        namespace=constants.OPENSHIFT_INGRESS_NAMESPACE,
        resource_name="ocs-cert",
        skip_tls_verify=skip_tls_verify,
    )
    if secret_obj.is_exist():
        secret_obj.delete(resource_name="ocs-cert", wait=True)
    cmd = (
        f"oc create secret tls ocs-cert -n openshift-ingress {ignore_tls}"
        f"--cert={ssl_cert} --key={ssl_key}"
    )
    exec_cmd(cmd)

    cmd = (
        f"oc patch ingresscontroller.operator default -n openshift-ingress-operator {ignore_tls}"
        '--type=merge -p \'{"spec":{"defaultCertificate": {"name": "ocs-cert"}}}\''
    )
    exec_cmd(cmd)

    if wait_for_machineconfigpool:
        wait_for_machineconfigpool_status(
            "all", timeout=1800, skip_tls_verify=skip_tls_verify
        )

    if ssl_ca_cert:
        update_kubeconfig_with_ca_cert(skip_tls_verify=skip_tls_verify)


def configure_custom_api_cert(skip_tls_verify=False, wait_for_machineconfigpool=True):
    """
    Configure custom SSL certificate for API. If the certificate doesn't
    exists, generate new one signed by automatic certificate signing service.

    Args:
        skip_tls_verify (bool): True if allow skipping TLS verification
        wait_for_machineconfigpool (bool): True if it should wait for machineConfigPool

    Raises:
        ConfigurationError: when some required parameter is not configured

    """
    ignore_tls = ""
    if skip_tls_verify:
        ignore_tls = "--insecure-skip-tls-verify "
    logger.info("Configure custom API certificate")
    base_domain = config.ENV_DATA["base_domain"]
    cluster_name = config.ENV_DATA["cluster_name"]
    api_domain = f"api.{cluster_name}.{base_domain}"

    ssl_key = config.DEPLOYMENT.get("api_ssl_key", defaults.API_SSL_KEY)
    ssl_cert = config.DEPLOYMENT.get("api_ssl_cert", defaults.API_SSL_CERT)

    signing_service_url = config.DEPLOYMENT.get("cert_signing_service_url")

    if not (os.path.exists(ssl_key) and os.path.exists(ssl_cert)):
        cert_provider = config.DEPLOYMENT.get("custom_ssl_cert_provider")
        if (
            cert_provider == constants.SSL_CERT_PROVIDER_OCS_QE_CA
            and not signing_service_url
        ):
            msg = (
                "Custom certificate files for ingress doesn't exists and "
                "`DEPLOYMENT['cert_signing_service_url']` is not defined. "
                "Unable to generate custom API certificate!"
            )
            logger.error(msg)
            raise exceptions.ConfigurationError(msg)

        logger.debug(
            f"Files '{ssl_key}' and '{ssl_cert}' doesn't exist, generate certificate"
        )
        if cert_provider == constants.SSL_CERT_PROVIDER_OCS_QE_CA:
            cert = OCSCertificate(
                signing_service=signing_service_url,
                cn=api_domain,
                sans=[f"DNS:{api_domain}"],
            )
        elif cert_provider == constants.SSL_CERT_PROVIDER_LETS_ENCRYPT:
            cert = LetsEncryptCertificate(
                dns_plugin=config.DEPLOYMENT["certbot_dns_plugin"],
                cn=api_domain,
                sans=[f"DNS:{api_domain}"],
            )
        else:
            msg = (
                f"Certbot DNS plugin {config.DEPLOYMENT['certbot_dns_plugin']} not supported. "
                "Supported `DEPLOYMENT['certbot_dns_plugin']` options are: 'dns-route53'. "
                "Unable to generate custom API certificate!"
            )
            logger.error(msg)
            raise exceptions.ConfigurationError(msg)
        logger.debug(f"Certificate key: {cert.key}")
        logger.debug(f"Certificate: {cert.crt}")
        cert.save_key(ssl_key)
        cert.save_crt(ssl_cert)
        logger.info(f"Certificate saved to '{ssl_cert}' and key to '{ssl_key}'")

    logger.debug(f"Configuring '{ssl_key}' and '{ssl_cert}' for api")
    # check if api-cert secret already exists, if yes, delete the old secret before the new one is created
    secret_obj = ocp.OCP(
        kind=constants.SECRET,
        namespace=constants.OPENSHIFT_CONFIG_NAMESPACE,
        resource_name="api-cert",
        skip_tls_verify=skip_tls_verify,
    )
    if secret_obj.is_exist():
        secret_obj.delete(resource_name="api-cert", wait=True)
    cmd = (
        f"oc create secret tls api-cert -n openshift-config {ignore_tls}"
        f"--cert={ssl_cert} --key={ssl_key}"
    )
    exec_cmd(cmd)

    cmd = (
        f"oc patch apiserver cluster {ignore_tls}"
        '--type=merge -p \'{"spec":{"servingCerts": {"namedCertificates": '
        f'[{{"names": ["{api_domain}"], "servingCertificate": '
        '{"name": "api-cert"}}]}}}\''
    )
    exec_cmd(cmd)
    if wait_for_machineconfigpool:
        wait_for_machineconfigpool_status(
            "all", timeout=1800, skip_tls_verify=skip_tls_verify
        )
    logger.info(
        f"Checking cluster status of {constants.OPENSHIFT_API_CLUSTER_OPERATOR}"
    )
    for sampler in TimeoutSampler(
        timeout=1800,
        sleep=60,
        func=ocp.verify_cluster_operator_status,
        cluster_operator=constants.OPENSHIFT_API_CLUSTER_OPERATOR,
        skip_tls_verify=skip_tls_verify,
    ):
        if sampler:
            logger.info(f"{constants.OPENSHIFT_API_CLUSTER_OPERATOR} status is valid")
            break
        else:
            logger.info(
                f"{constants.OPENSHIFT_API_CLUSTER_OPERATOR} status is not valid"
            )


def configure_ingress_and_api_certificates(skip_tls_verify=False):
    """
    Configure custom SSL certificate for ingress and API.

    Args:
        skip_tls_verify (bool): True if allow skipping TLS verification

    """
    configure_custom_ingress_cert(skip_tls_verify, wait_for_machineconfigpool=False)
    configure_custom_api_cert(skip_tls_verify, wait_for_machineconfigpool=False)
    wait_for_machineconfigpool_status(
        "all", timeout=3600, skip_tls_verify=skip_tls_verify
    )


def create_ocs_ca_bundle(
    ca_cert_path,
    ca_bundle_name="ocs-ca-bundle",
    namespace=constants.OPENSHIFT_CONFIG_NAMESPACE,
    skip_tls_verify=False,
):
    """
    Create or update configmap object with ocs-ca-bundle

    Args:
        ca_cert_path (str): path to CA Certificate(s) bundle file
        ca_bundle_name (str): name of the created or updated configmap object (default: ocs-ca-bundle)
        namespace (str): namespace where to create the configmap (default: openshift-config)
        skip_tls_verify (bool): True if allow skipping TLS verification

    """
    # check if ocs-ca-bundle configmap already exists, if yes, concatenate
    # existing ca-bundle.crt with the new CA bundle (ca_cert_path) and delete
    # the old configmap before the new one is created
    configmap_obj = ocp.OCP(
        kind=constants.CONFIGMAP,
        namespace=namespace,
        resource_name=ca_bundle_name,
        skip_tls_verify=skip_tls_verify,
    )
    if configmap_obj.is_exist():
        existing_ca_bundle = configmap_obj.get()["data"]["ca-bundle.crt"]
        with open(ca_cert_path, "a") as fd:
            fd.write(existing_ca_bundle)
        configmap_obj.delete(resource_name=ca_bundle_name, wait=True)
    ignore_tls = ""
    if skip_tls_verify:
        ignore_tls = "--insecure-skip-tls-verify "
    cmd = (
        f"oc create configmap {ca_bundle_name} -n {namespace} {ignore_tls}"
        f"--from-file=ca-bundle.crt={ca_cert_path}"
    )
    exec_cmd(cmd)


def configure_trusted_ca_bundle(ca_cert_path, skip_tls_verify=False):
    """
    Configure cluster-wide trusted CA bundle in Proxy object

    Args:
        ca_cert_path (str): path to CA Certificate(s) bundle file
        skip_tls_verify (bool): True if allow skipping TLS verification

    """
    ocs_ca_bundle_name = "ocs-ca-bundle"
    create_ocs_ca_bundle(
        ca_cert_path, ocs_ca_bundle_name, skip_tls_verify=skip_tls_verify
    )
    ignore_tls = ""
    if skip_tls_verify:
        ignore_tls = "--insecure-skip-tls-verify "
    cmd = (
        f"oc patch proxy/cluster --type=merge {ignore_tls}"
        f'--patch=\'{{"spec":{{"trustedCA":{{"name":"{ocs_ca_bundle_name}"}}}}}}\''
    )
    exec_cmd(cmd)


def init_arg_parser():
    """
    Init argument parser.

    Returns:
        object: Parsed arguments

    """

    parser = argparse.ArgumentParser(
        description="OCS Automatic Certification Authority client",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-s",
        "--cert-signing-service",
        action="store",
        required=False,
        help="automatic certification signing service URL",
    )
    group.add_argument(
        "-p",
        "--dns-plugin",
        action="store",
        required=False,
        default="dns-route53",
        help="Certbot DNS plugin to use (options: route53, ). NOTE: more to come later",
    )
    parser.add_argument(
        "-f",
        "--file",
        action="store",
        help="certificate, csr and key file name (suffix '.key', '.csr' and '.crt' added automaticaly)",
    )
    parser.add_argument(
        "-n", "--cn", action="store", required=True, help="certificate Common Name"
    )
    parser.add_argument(
        "-a",
        "--san",
        action="append",
        default=[],
        help=(
            "certificate Subject Alternative Names (prefixed by the type like 'DNS:', 'IP:', etc., "
            "see also: https://www.openssl.org/docs/man1.0.2/man5/x509v3_config.html#Subject-Alternative-Name)"
        ),
    )
    args = parser.parse_args()

    return args


def configure_certs_arg_parser():
    """
    Init argument parser for configure-ssl-certs command.

    Returns:
        object: Parsed arguments

    """
    parser = argparse.ArgumentParser(
        description="Configure custom SSL certificates for OpenShift cluster",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Configure both ingress and API with Let's Encrypt (auto-detect cluster info)
  configure-ssl-certs --provider letsencrypt

  # Configure only ingress with OCS QE CA
  configure-ssl-certs --ingress --provider ocs-qe-ca \\
    --signing-service-url https://example.com

  # Configure with explicit cluster info and cluster path
  configure-ssl-certs --provider letsencrypt \\
    --cluster-name my-cluster --base-domain example.com \\
    --cluster-path /path/to/cluster-dir

  # Configure with custom kubeconfig
  configure-ssl-certs --provider ocs-qe-ca \\
    --signing-service-url https://example.com:8443 \\
    --kubeconfig /path/to/kubeconfig
        """,
    )
    parser.add_argument(
        "--ingress",
        action="store_true",
        help="Configure custom SSL certificate for ingress",
    )
    parser.add_argument(
        "--api",
        action="store_true",
        help="Configure custom SSL certificate for API",
    )
    parser.add_argument(
        "--provider",
        choices=["ocs-qe-ca", "letsencrypt"],
        default="ocs-qe-ca",
        help="Certificate provider to use: ocs-qe-ca or letsencrypt (default: ocs-qe-ca)",
    )
    parser.add_argument(
        "--signing-service-url",
        help="URL of the OCS QE automatic certificate signing service (required for ocs-qe-ca provider)",
    )
    parser.add_argument(
        "--dns-plugin",
        default="dns-route53",
        help="Certbot DNS plugin to use (required for letsencrypt provider, default: dns-route53)",
    )
    parser.add_argument(
        "--cluster-name",
        help="OpenShift cluster name (auto-detected if not provided)",
    )
    parser.add_argument(
        "--base-domain",
        help="OpenShift base domain (auto-detected if not provided)",
    )
    parser.add_argument(
        "--skip-tls-verify",
        action="store_true",
        help="Skip TLS verification when communicating with the cluster",
    )
    parser.add_argument(
        "--kubeconfig",
        help="Path to kubeconfig file (uses KUBECONFIG env var or default if not provided)",
    )
    parser.add_argument(
        "--cluster-path",
        help="Path to cluster directory containing auth/kubeconfig (creates temp dir if not provided)",
    )
    args = parser.parse_args()

    # If neither --ingress nor --api is specified, configure both
    if not args.ingress and not args.api:
        args.ingress = True
        args.api = True

    # Validate provider-specific requirements
    if args.provider == "ocs-qe-ca" and not args.signing_service_url:
        parser.error(
            "--signing-service-url is required when using --provider ocs-qe-ca"
        )

    return args


def get_cluster_info_from_cluster(skip_tls_verify=False):
    """
    Auto-detect cluster name and base domain from the running OpenShift cluster.

    Args:
        skip_tls_verify (bool): Skip TLS verification

    Returns:
        tuple: (cluster_name, base_domain)

    Raises:
        Exception: If unable to extract cluster information

    """
    ignore_tls = "--insecure-skip-tls-verify " if skip_tls_verify else ""

    # Get the apps domain from ingress configuration
    # Format: *.apps.cluster-name.base-domain
    cmd = f"oc get ingress.config cluster {ignore_tls}" "-o jsonpath='{.spec.domain}'"
    result = exec_cmd(cmd)
    apps_domain = result.stdout.decode("utf-8").strip().strip("'")

    if not apps_domain or "apps" not in apps_domain:
        raise exceptions.ConfigurationError(
            f"Failed to extract apps domain from cluster. Got: {apps_domain}"
        )

    # Extract cluster_name and base_domain from apps domain
    # Expected format: *.apps.cluster-name.base-domain
    # or: apps.cluster-name.base-domain
    apps_domain = apps_domain.lstrip("*.")
    if apps_domain.startswith("apps."):
        apps_domain = apps_domain[5:]  # Remove 'apps.' prefix

    # Split to get cluster_name and base_domain
    parts = apps_domain.split(".", 1)
    if len(parts) != 2:
        raise exceptions.ConfigurationError(
            f"Unable to parse cluster_name and base_domain from apps domain: {apps_domain}"
        )

    cluster_name = parts[0]
    base_domain = parts[1]

    logger.info(f"Auto-detected cluster_name: {cluster_name}")
    logger.info(f"Auto-detected base_domain: {base_domain}")

    return cluster_name, base_domain


def configure_certs_main():
    """
    Main function for configure-ssl-certs command.
    Configures custom SSL certificates for ingress and/or API.
    """
    args = configure_certs_arg_parser()

    # Initialize logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Handle cluster-path: create temp dir if not provided
    if args.cluster_path:
        cluster_path = os.path.abspath(args.cluster_path)
        if not os.path.exists(cluster_path):
            raise exceptions.ConfigurationError(
                f"Cluster path '{cluster_path}' does not exist"
            )
        logger.info(f"Using cluster path: {cluster_path}")
    else:
        cluster_path = tempfile.mkdtemp(prefix="configure-ssl-certs-", dir="/tmp")
        logger.info(f"Created temporary cluster path: {cluster_path}")

    # Handle kubeconfig
    kubeconfig_path = None
    if args.kubeconfig:
        kubeconfig_path = os.path.abspath(args.kubeconfig)
        if not os.path.exists(kubeconfig_path):
            raise exceptions.ConfigurationError(
                f"Kubeconfig file '{kubeconfig_path}' does not exist"
            )
        os.environ["KUBECONFIG"] = kubeconfig_path
        logger.info(f"Using kubeconfig: {kubeconfig_path}")
    elif args.cluster_path:
        # Check for auth/kubeconfig in cluster_path
        default_kubeconfig = os.path.join(cluster_path, "auth", "kubeconfig")
        if os.path.exists(default_kubeconfig):
            kubeconfig_path = default_kubeconfig
            os.environ["KUBECONFIG"] = kubeconfig_path
            logger.info(f"Found and using kubeconfig: {kubeconfig_path}")
        else:
            logger.info(
                f"No kubeconfig found at {default_kubeconfig}, using default from KUBECONFIG env var"
            )

    # Get or detect cluster information
    if args.cluster_name and args.base_domain:
        cluster_name = args.cluster_name
        base_domain = args.base_domain
        logger.info(f"Using provided cluster_name: {cluster_name}")
        logger.info(f"Using provided base_domain: {base_domain}")
    else:
        logger.info("Auto-detecting cluster name and base domain from cluster...")
        try:
            cluster_name, base_domain = get_cluster_info_from_cluster(
                skip_tls_verify=args.skip_tls_verify
            )
        except Exception as e:
            logger.error(
                f"Failed to auto-detect cluster information: {e}\n"
                "Please provide --cluster-name and --base-domain explicitly."
            )
            raise

    # Initialize config object
    if not hasattr(config, "ENV_DATA"):
        config.ENV_DATA = {}
    if not hasattr(config, "DEPLOYMENT"):
        config.DEPLOYMENT = {}
    if not hasattr(config, "RUN"):
        config.RUN = {}

    # Set cluster information in config
    config.ENV_DATA["cluster_name"] = cluster_name
    config.ENV_DATA["base_domain"] = base_domain
    config.ENV_DATA["cluster_path"] = cluster_path

    # Set kubeconfig location (relative to cluster_path)
    if kubeconfig_path and kubeconfig_path.startswith(cluster_path):
        # Make it relative to cluster_path
        config.RUN["kubeconfig_location"] = os.path.relpath(
            kubeconfig_path, cluster_path
        )
    else:
        # Use default location
        config.RUN["kubeconfig_location"] = defaults.KUBECONFIG_LOCATION

    # Set certificate provider and related configuration
    config.DEPLOYMENT["custom_ssl_cert_provider"] = args.provider

    if args.provider == constants.SSL_CERT_PROVIDER_OCS_QE_CA:
        config.DEPLOYMENT["cert_signing_service_url"] = args.signing_service_url
        logger.info(f"Using OCS QE CA signing service: {args.signing_service_url}")
    elif args.provider == constants.SSL_CERT_PROVIDER_LETS_ENCRYPT:
        config.DEPLOYMENT["certbot_dns_plugin"] = args.dns_plugin
        logger.info(f"Using Let's Encrypt with DNS plugin: {args.dns_plugin}")

    # Set default certificate paths in cluster_path if not already configured
    config.DEPLOYMENT.setdefault(
        "ingress_ssl_key", os.path.join(cluster_path, "ingress-cert.key")
    )
    config.DEPLOYMENT.setdefault(
        "ingress_ssl_cert", os.path.join(cluster_path, "ingress-cert.crt")
    )
    config.DEPLOYMENT.setdefault(
        "ingress_ssl_ca_cert", os.path.join(cluster_path, "ca.crt")
    )
    config.DEPLOYMENT.setdefault(
        "api_ssl_key", os.path.join(cluster_path, "api-cert.key")
    )
    config.DEPLOYMENT.setdefault(
        "api_ssl_cert", os.path.join(cluster_path, "api-cert.crt")
    )
    config.DEPLOYMENT.setdefault(
        "api_ssl_ca_cert", os.path.join(cluster_path, "ca.crt")
    )

    # Configure certificates based on user selection
    if args.ingress and args.api:
        logger.info("Configuring custom SSL certificates for both ingress and API")
        configure_ingress_and_api_certificates(skip_tls_verify=args.skip_tls_verify)
    elif args.ingress:
        logger.info("Configuring custom SSL certificate for ingress only")
        configure_custom_ingress_cert(
            skip_tls_verify=args.skip_tls_verify, wait_for_machineconfigpool=True
        )
    elif args.api:
        logger.info("Configuring custom SSL certificate for API only")
        configure_custom_api_cert(
            skip_tls_verify=args.skip_tls_verify, wait_for_machineconfigpool=True
        )

    logger.info("Custom SSL certificate configuration completed successfully")


def main():
    """
    Main function for get-ssl-cert command
    """
    args = init_arg_parser()

    if args.cert_signing_service:
        # initialize OCSCertificate object
        cert = OCSCertificate(
            signing_service=args.cert_signing_service,
            cn=args.cn,
            sans=args.san,
        )
    elif args.dns_plugin:
        # initialize LetsEncryptCertificate object
        cert = LetsEncryptCertificate(
            dns_plugin=args.dns_plugin,
            cn=args.cn,
            sans=args.san,
        )

    # print everything to console or save key and crt to respective files
    if not args.file or args.file == "-":
        print(cert.key)
        print(cert.csr)
        print(cert.crt)
    else:
        cert.save_key(f"{args.file}.key")
        cert.save_crt(f"{args.file}.crt")


if __name__ == "__main__":
    main()
