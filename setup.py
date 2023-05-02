"""
Setup module for YarnCleaner
"""
from setuptools import setup

setup(
    name='yarncleaner',
    version='0.1',
    description='A utility for cleaning up YARN applications',
    author='stv',
    packages=['yarncleaner'],
    install_requires=[
       "paramiko" # list any dependencies here
    ],
    entry_points={
        'console_scripts': ['yarncleaner=yarncleaner_package.yarncleaner:main']
    },
    test_suite='tests',
    tests_require=[
        # list any testing dependencies here
    ],
)
