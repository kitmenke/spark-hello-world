#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import sparkhelloworld


if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as f:
    readme = f.read()

packages = [
    'sparkhelloworld',
]

package_data = {
}

requires = [
]

classifiers = [
]

setup(
    name='sparkhelloworld',
    version=sparkhelloworld.__version__,
    description='A pyspark example application',
    long_description=readme,
    packages=packages,
    package_data=package_data,
    install_requires=requires,
    author=sparkhelloworld.__author__,
    url='https://github.com/kitmenke/spark-hello-world',
    license='MIT',
    classifiers=classifiers,
)
