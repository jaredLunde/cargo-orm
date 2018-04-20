#!/usr/bin/python3 -S
import os
import uuid
from setuptools import setup
from pip.req import parse_requirements
from pkgutil import walk_packages


PKG = 'cargo'
PKG_NAME = 'cargo-orm'
PKG_VERSION = '0.1.10'

pathname = os.path.dirname(os.path.realpath(__file__))


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements(pathname + "/requirements.txt",
                                  session=uuid.uuid1())


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
    install_requires=[str(ir.req) for ir in install_reqs],
    packages=[
        'cargo',
        'cargo.builder',
        'cargo.etc',
        'cargo.etc.translator',
        'cargo.fields',
        'cargo.logic'
    ]
)
