'''
Created on 25/03/2009

@author: jose blanca
'''

from setuptools import setup
setup(
    # basic package data
    name = "subprocess",
    version = "0.0.1",
    author='Jose Blanca, Peio Ziarsolo',
    author_email='jblanca@btc.upv.es',
    description='runs commands in parallel environments',
    # package structure
    packages=['psubprocess'],
    package_dir={'':'.'},
    requires=[],
    scripts=['scripts/run_in_parallel.py']
)
