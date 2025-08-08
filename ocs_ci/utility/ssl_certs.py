"""
This module is used for generating custom SSL certificates.
"""

import argparse
import logging
import os
import requests
import tempfile

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
