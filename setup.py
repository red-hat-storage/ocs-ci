# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools

    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name="ocs-ci",
    version="4.18.0",
    description="OCS CI tests that run in jenkins and standalone mode using aws provider",
    author="OCS QE",
    author_email="ocs-ci@redhat.com",
    license="MIT",
    python_requires=">=3.9.18",
    install_requires=[
        "apache-libcloud==3.1.0",
        "docopt==0.6.2",
        "gevent==23.9.1",
        "ibm-cos-sdk==2.13.5",
        "reportportal-client==3.2.3",
        "requests==2.32.3",
        "paramiko==3.5.0",
        "pyyaml>=4.2b1",
        "jinja2==3.1.5",
        "openshift==0.13.2",
        "boto3==1.24.96",
        "munch==2.5.0",
        "pytest-progress==1.3.0",
        "pytest==8.3.3",
        "pytest-logger==1.1.1",
        "pytest-html==4.1.1",
        "pytest-metadata==3.1.1",
        "bs4==0.0.1",
        "gspread==3.6.0",
        "google-auth-oauthlib==0.7.1",
        "oauth2client==4.1.3",
        "pytest_marker_bugzilla>=0.9.3",
        "pyvmomi==7.0",
        "python-hcl2==3.0.1",
        "python-dateutil==2.9.0",
        "pytest-order==1.3.0",
        "funcy==1.14",
        "semantic-version==2.8.5",
        "jsonschema>=3.2.0",
        "google-cloud-storage==2.6.0",
        "google-auth==2.14.1",
        "elasticsearch==8.11.1",
        "numpy==1.23.2",
        "pandas==1.5.2",
        "tabulate==0.9.0",
        "python-ipmi==0.4.2",
        "scipy==1.12.0",
        "PrettyTable==0.7.2",
        "azure-common==1.1.28",
        "azure-mgmt-compute==33.0.0",
        "azure-mgmt-network==28.0.0",
        "azure-mgmt-resource==23.2.0",
        "azure-storage-blob==12.23.1",
        "msrestazure==0.6.3",
        "python-novaclient==17.1.0",
        "python-cinderclient==7.1.0",
        "keystoneauth1==4.2.0",
        "range-key-dict==1.1.0",
        "GitPython==3.1.41",
        "selenium==3.141.0",
        "webdriver-manager==4.0.2",
        # greenlet 1.0.0 is broken on ppc64le
        # https://github.com/python-greenlet/greenlet/issues/230
        # by default program attempting to load an x86_64-only library from a native arm64 process
        # Beginning with gevent 20.12.0, 64-bit ARM binaries are distributed on PyPI for aarch64 manylinux2014
        # compatible systems. Resolves problem for m1 Mac chips
        "greenlet==3.0rc3",
        "ovirt-engine-sdk-python==4.4.11",
        "junitparser==3.1.0",
        "flaky==3.8.1",
        "ocp-network-split",
        "pyopenssl==24.2.1",
        "pyparsing==2.4.7",
        "mysql-connector-python==9.1.0",
        "pytest-repeat==0.9.3",
        "pexpect>=4.8.0",
        # googleapis-common-protos 1.56.2 needs to have protobuf<4.0.0>=3.15.0
        "protobuf==4.21.7",
        "ping3==4.0.3",
        "psutil==5.9.0",
        "azure-identity==1.16.1",
        "azure-mgmt-storage==21.0.0",
        "fauxfactory==3.1.0",
        "google-api-core==2.11.0",
        "google-api-python-client==2.105.0",
        "google-auth-httplib2==0.1.1",
        "google-cloud-core==2.3.2",
        "google-crc32c==1.5.0",
        "google-resumable-media==2.4.1",
        "googleapis-common-protos==1.59.0",
        "urllib3==1.26.19",
        "psycopg2-binary==2.9.9",
        "azure-keyvault-secrets==4.8.0",
        "pytest-jira==0.3.21",
        "certbot==3.0.0",
        "certbot-dns-route53==3.0.0",
        "openshift-python-wrapper==11.0.36",
    ],
    entry_points={
        "console_scripts": [
            "run-ci=ocs_ci.framework.main:main",
            "run-ci-deploy=ocs_ci.framework.deploy:main",
            "report-version=ocs_ci.ocs.version:main",
            "ci-cleanup=ocs_ci.cleanup.aws.cleanup:cluster_cleanup",
            "ci-pause=ocs_ci.pause.pause:cluster_pause",
            "aws-cleanup=ocs_ci.cleanup.aws.cleanup:aws_cleanup",
            "vsphere-cleanup=ocs_ci.cleanup.vsphere.cleanup:vsphere_cleanup",
            "ocs-build=ocs_ci.utility.ocs_build:main",
            "get-ssl-cert=ocs_ci.utility.ssl_certs:main",
            "rosa-ocp-version=ocs_ci.utility.rosa:rosa_ocp_version_endpoint",
        ],
    },
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=["ez_setup"]),
)
