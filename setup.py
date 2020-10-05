# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='ocs-ci',
    version='0.1',
    description='OCS CI tests that run in jenkins and standalone mode using aws provider',
    author='OCS QE',
    author_email='ocs-ci@redhat.com',
    license='MIT',
    install_requires=[
        'apache-libcloud==3.1.0',
        'docopt==0.6.2',
        'gevent==20.6.2',
        'reportportal-client==3.2.3',
        'requests==2.23.0',
        'paramiko==2.4.2',
        'pyyaml>=4.2b1',
        'jinja2==2.10.1',
        'openshift==0.11.2',
        'boto3==1.14.7',
        'munch==2.5.0',
        'pytest==5.3.5',
        'pytest-reportportal==1.10.0',
        'pytest-logger==0.5.1',
        'dataclasses==0.7;python_version < "3.7"',
        'pytest-html==2.1.1',
        'bs4==0.0.1',
        'gspread==3.6.0',
        'oauth2client==4.1.3',
        'pytest_marker_bugzilla>=0.9.3',
        'pyvmomi==7.0',
        'pyhcl==0.4.4',
        # issue opened for botocore
        # https://github.com/boto/botocore/issues/1872
        # till above issue fixed, manually pointing python-dateutil to 2.8.0
        'python-dateutil==2.8.0',
        'pytest-ordering==0.6',
        'funcy==1.14',
        'semantic-version==2.8.5',
        'jsonschema>=3.2.0',
        'google-cloud-storage==1.29.0',
        'elasticsearch==7.8.0',
        'numpy==1.18.5',
        'python-ipmi==0.4.2',
        'scipy==1.4.1',
        'PrettyTable==0.7.2',
        'azure-common==1.1.25',
        'azure-mgmt-compute==12.0.0',
        'azure-mgmt-network==10.2.0',
        'azure-mgmt-resource==10.0.0',
        'msrestazure==0.6.3',
        'python-novaclient==17.1.0',
        'python-cinderclient==7.1.0',
        'keystoneauth1==4.2.0',
        'range-key-dict==1.1.0',
        'GitPython==3.1.7',
    ],
    entry_points={
        'console_scripts': [
            'run-ci=ocs_ci.framework.main:main',
            'report-version=ocs_ci.ocs.version:main',
            'ci-cleanup=ocs_ci.cleanup.aws.cleanup:cluster_cleanup',
            'ci-pause=ocs_ci.pause.pause:cluster_pause',
            'aws-cleanup=ocs_ci.cleanup.aws.cleanup:aws_cleanup'
        ],
    },
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=['ez_setup']),
)
