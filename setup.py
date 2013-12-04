#!/usr/bin/env python
# http://docs.python.org/distutils/setupscript.html
# http://docs.python.org/2/distutils/examples.html

import sys
from setuptools import setup
import ast
import os

name = 'caches'
version = ''
with open('{}{}__init__.py'.format(name, os.sep), 'rU') as f:
    for node in (n for n in ast.parse(f.read()).body if isinstance(n, ast.Assign)):
        node_name = node.targets[0]
        if isinstance(node_name, ast.Name) and node_name.id.startswith('__version__'):
            version = node.value.s
            break

if not version:
    raise RuntimeError('Unable to find version number')

setup(
    name=name,
    version=version,
    description='Python caching backed by Redis',
    author='Jay Marcyes',
    author_email='jay@marcyes.com',
    url='http://github.com/firstopinion/{}'.format(name),
    packages=[name],
    license="MIT",
    install_requires=['dsnparse', 'redis', 'redis-collections', 'decorators'],
    classifiers=[ # https://pypi.python.org/pypi?:action=list_classifiers
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        'Programming Language :: Python :: 2.7',
    ],
    #test_suite = "{}_test".format(name),
)
