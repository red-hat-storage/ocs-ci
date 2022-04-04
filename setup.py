# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools

    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name="ocs-ci",
    version="4.6.0",
    description="OCS CI tests that run in jenkins and standalone mode using aws provider",
    author="OCS QE",
    author_email="ocs-ci@redhat.com",
    license="MIT",
    install_requires=[
        "apache-libcloud==3.1.0",
        "docopt==0.6.2",
        "gevent==20.9.0",
        "reportportal-client==3.2.3",
        "requests==2.23.0",
        "paramiko==2.10.1",
        "pyyaml>=4.2b1",
        "jinja2==3.0.3",
        "openshift==0.11.2",
        "boto3==1.14.7",
        "munch==2.5.0",
        "pytest==6.2.5",
        "pytest-logger==0.5.1",
        'dataclasses==0.7;python_version < "3.7"',
        "pytest-html==3.1.1",
        "pytest-metadata==1.11.0",
        "bs4==0.0.1",
        "gspread==3.6.0",
        "oauth2client==4.1.3",
        "pytest_marker_bugzilla>=0.9.3",
        "pyvmomi==7.0",
        "python-hcl2==3.0.1",
        # issue opened for botocore
        # https://github.com/boto/botocore/issues/1872
        # till above issue fixed, manually pointing python-dateutil to 2.8.0
        "python-dateutil==2.8.0",
        "pytest-ordering==0.6",
        "funcy==1.14",
        "semantic-version==2.8.5",
        "jsonschema>=3.2.0",
        "google-cloud-storage==1.29.0",
        "google-auth==1.35.0",
        "elasticsearch==7.14.0",
        "numpy==1.21.0",
        "python-ipmi==0.4.2",
        "scipy==1.6.0",
        "PrettyTable==0.7.2",
        "azure-common==1.1.25",
        "azure-mgmt-compute==12.0.0",
        "azure-mgmt-network==10.2.0",
        "azure-mgmt-resource==10.0.0",
        "azure-storage-blob==12.5.0",
        "msrestazure==0.6.3",
        "python-novaclient==17.1.0",
        "python-cinderclient==7.1.0",
        "keystoneauth1==4.2.0",
        "range-key-dict==1.1.0",
        "GitPython==3.1.7",
        "selenium==3.141.0",
        "webdriver-manager==3.2.2",
        # greenlet 1.0.0 is broken on ppc64le
        # https://github.com/python-greenlet/greenlet/issues/230
        "greenlet<1.0.0",
        "ovirt-engine-sdk-python==4.4.11",
        "junitparser",
        "flaky==3.7.0",
        "ocp-network-split",
        "pyopenssl",
        "pyparsing ==2.4.7",
        "mysql-connector-python==8.0.27",
        "pytest-repeat",
        "pexpect>=4.8.0",
        "yq==2.14.0",
    ],
    entry_points={
        "console_scripts": [
            "run-ci=ocs_ci.framework.main:main",
            "report-version=ocs_ci.ocs.version:main",
            "ci-cleanup=ocs_ci.cleanup.aws.cleanup:cluster_cleanup",
            "ci-pause=ocs_ci.pause.pause:cluster_pause",
            "aws-cleanup=ocs_ci.cleanup.aws.cleanup:aws_cleanup",
            "vsphere-cleanup=ocs_ci.cleanup.vsphere.cleanup:vsphere_cleanup",
            "ocs-build=ocs_ci.utility.ocs_build:main",
            "get-ssl-cert=ocs_ci.utility.ssl_certs:main",
        ],
    },
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=["ez_setup"]),
)
