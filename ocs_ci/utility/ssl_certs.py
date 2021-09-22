"""
This module is used for generating custom SSL certificates.
"""

import argparse
import requests

from OpenSSL import crypto

from ocs_ci.ocs import constants


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

        sans = ", ".join([f"DNS: {san}" for san in self.sans])
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
            self.signing_service,
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
