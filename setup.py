#!/usr/bin/env python3

from setuptools import setup
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Get the version from file
with open(path.join(here, 'dwq/version.py'), encoding='utf-8') as f:
    _version = f.read()

version = _version.split("=")[1].lstrip().rstrip().lstrip('"').rstrip('"')

setup(
    name='dwq',
    version=version,

    description='dwq: a Disque based work queue',
    long_description=long_description,

    url='https://github.com/kaspar030/dwq',

    author='Kaspar Schleiser',
    author_email='kaspar@schleiser.de',

    license='GPLv3',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],

    keywords='distributed queue',
    packages=['dwq'],
    install_requires=['pydisque_dwq', "prometheus-client"],
    entry_points={
        'console_scripts': [
            'dwqc=dwq.dwqc:main',
            'dwqm=dwq.dwqm:main',
            'dwqw=dwq.dwqw:main',
            'dwq-localjob=dwq.dwq_localjob:main',
        ],
    },
)
