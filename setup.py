# -*- coding: utf-8 -*-
try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='cephci',
    version='0.1',
    description='Ceph CI tests that run in jenkins using openstack provider',
    author='Vasu Kulkarni',
    author_email='vasu@redhat.com',
    install_requires=[
        'apache-libcloud',
        'requests',
        'paramiko',
        'pyyaml',
    ],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=['ez_setup']),
)
