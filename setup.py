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
        'apache-libcloud',
        'docopt==0.6.2',
        'gevent==1.4.0',
        'reportportal-client @ git+https://github.com/reportportal/client-Python.git@master',
        'requests==2.21.0',
        'paramiko==2.4.2',
        'pyyaml>=4.2b1',
        'jinja2==2.10.1',
        'kubernetes==9.0.0',
        'openshift',
        'boto3',
        'munch',
        'pytest',
        'pytest-reportportal==1.0.5',
        'DeepDiff',
        'dataclasses',  # For compatibility with python 3.6
        'pytest-html',
        'bs4',
    ],
    entry_points={
        'console_scripts': [
            'run-ci=run_ocsci:main',
        ],
    },
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=['ez_setup']),
)
