#!/usr/bin/python3 -S
import os
import uuid
from setuptools import setup
from pkgutil import walk_packages


PKG = 'cargo'
PKG_NAME = 'cargo-orm'
PKG_VERSION = '0.2.0'

pathname = os.path.dirname(os.path.realpath(__file__))

def parse_requirements(filename):
    """ load requirements from a pip requirements file """
    lineiter = (line.strip() for line in open(filename))
    return (line for line in lineiter if line and not line.startswith("#"))

install_reqs = parse_requirements(pathname + "/requirements.txt")


def find_packages(prefix=""):
    path = [prefix]
    yield prefix
    prefix = prefix + "."
    for _, name, ispkg in walk_packages(path, prefix):
        if ispkg:
            yield name

# print(list(find_packages(PKG)))
setup(
    name=PKG_NAME,
    version=PKG_VERSION,
    description='A friendly, rich Postgres ORM for Python 3.5+.',
    author='Jared Lunde',
    author_email='jared@tessellate.io',
    url='https://github.com/jaredlunde/cargo-orm',
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: Implementation :: CPython",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent"
    ],
    install_requires=list(install_reqs),
    packages=[
        'cargo',
        'cargo.builder',
        'cargo.etc',
        'cargo.etc.translator',
        'cargo.fields',
        'cargo.logic'
    ]
)
