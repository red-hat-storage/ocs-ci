"""
This module is used for generating custom SSL certificates.
"""

import argparse
import logging
import os
import requests

from OpenSSL import crypto

from ocs_ci.framework import config
from ocs_ci.ocs import constants, exceptions, ocp
from ocs_ci.utility.utils import (
    download_file,
    exec_cmd,
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


class OCSCertificate:
    """
    Generate custom certificate signed by the automatic siging certification
    authority

    Args:
        signing_service (str): URL of the automatic signing CA service
        cn (str): Certificate Common Name
        sans (list): list of Subject Alternative Names (prefixed by the type
            like 'DNS:' or 'IP:')

    """

    def __init__(self, signing_service, cn, sans=None):
        self._key = None
        self._csr = None
        self._crt = None

        # url pointing to automatic signing service
        self.signing_service = signing_service
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


def get_root_ca_cert():
    """
    If not available, download Root CA Certificate for custom Ingress
    certificate.

    Returns
        str: Path to Root CA Certificate

    """
    signing_service_url = config.DEPLOYMENT.get("cert_signing_service_url")
    ssl_ca_cert = config.DEPLOYMENT.get("ingress_ssl_ca_cert", "")
    if ssl_ca_cert and not os.path.exists(ssl_ca_cert):
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
    return ssl_ca_cert


def configure_custom_ingress_cert():
    """
    Configure custom SSL certificate for ingress. If the certificate doesn't
    exists, generate new one signed by automatic certificate signing service.

    Raises:
        ConfigurationError: when some required parameter is not configured

    """
    logger.info("Configure custom ingress certificate")
    base_domain = config.ENV_DATA["base_domain"]
    cluster_name = config.ENV_DATA["cluster_name"]
    apps_domain = f"*.apps.{cluster_name}.{base_domain}"

    ssl_key = config.DEPLOYMENT.get("ingress_ssl_key")
    ssl_cert = config.DEPLOYMENT.get("ingress_ssl_cert")

    signing_service_url = config.DEPLOYMENT.get("cert_signing_service_url")

    if not (os.path.exists(ssl_key) and os.path.exists(ssl_cert)):
        if not signing_service_url:
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
        cert = OCSCertificate(
            signing_service=signing_service_url,
            cn=apps_domain,
            sans=[f"DNS:{apps_domain}"],
        )
        logger.debug(f"Certificate key: {cert.key}")
        logger.debug(f"Certificate: {cert.crt}")
        cert.save_key(ssl_key)
        cert.save_crt(ssl_cert)
        logger.info(f"Certificate saved to '{ssl_cert}' and key to '{ssl_key}'")

    ssl_ca_cert = get_root_ca_cert()
    if ssl_ca_cert:
        logger.debug(f"Configure '{ssl_ca_cert}' for proxy configuration object")
        configure_trusted_ca_bundle(ssl_ca_cert)

    logger.debug(f"Configuring '{ssl_key}' and '{ssl_cert}' for ingress")
    cmd = (
        "oc create secret tls ocs-cert -n openshift-ingress "
        f"--cert={ssl_cert} --key={ssl_key}"
    )
    exec_cmd(cmd)

    cmd = (
        "oc patch ingresscontroller.operator default -n openshift-ingress-operator "
        '--type=merge -p \'{"spec":{"defaultCertificate": {"name": "ocs-cert"}}}\''
    )
    exec_cmd(cmd)
    wait_for_machineconfigpool_status("all", timeout=1800)


def configure_trusted_ca_bundle(ca_cert_path):
    """
    Configure cluster-wide trusted CA bundle in Proxy object

    Args:
        ca_cert_path (str): path to CA Certificate(s) bundle file

    """
    ocs_ca_bundle_name = "ocs-ca-bundle"
    # check if ocs-ca-bundle configmap already exists, if yes, concatenate
    # existing ca-bundle.crt with the new CA bundle (ca_cert_path) and delete
    # the old configmap before the new one is created
    configmap_obj = ocp.OCP(
        kind=constants.CONFIGMAP,
        namespace=constants.OPENSHIFT_CONFIG_NAMESPACE,
        resource_name=ocs_ca_bundle_name,
    )
    if configmap_obj.is_exist():
        existing_ca_bundle = configmap_obj.get()["data"]["ca-bundle.crt"]
        with open(ca_cert_path, "a") as fd:
            fd.write(existing_ca_bundle)
        configmap_obj.delete(resource_name=ocs_ca_bundle_name, wait=True)

    cmd = (
        f"oc create configmap {ocs_ca_bundle_name} -n openshift-config "
        f"--from-file=ca-bundle.crt={ca_cert_path}"
    )
    exec_cmd(cmd)
    cmd = (
        "oc patch proxy/cluster --type=merge "
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
    parser.add_argument(
        "-s",
        "--cert-signing-service",
        action="store",
        required=True,
        help="automatic certification signing service URL",
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

    # initialize OCSCertificate object
    cert = OCSCertificate(
        signing_service=args.cert_signing_service,
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
