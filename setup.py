#!/usr/bin/python3 -S
import os
import uuid
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
from pip.req import parse_requirements
from pkgutil import walk_packages


pathname = os.path.dirname(os.path.realpath(__file__))


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements(
    pathname + "/requirements.txt",
    session=uuid.uuid1())


pkg = 'cargo'
pkg_dir = "{}/{}".format(pathname, pkg)


def find_packages(prefix=""):
    path = [pkg_dir]
    yield prefix
    prefix = prefix + "."
    for _, name, ispkg in walk_packages(path, prefix):
        if ispkg:
            yield name


setup(
    name='cargo-orm',
    version='0.0.1b4',
    description='A friendly, feature-rich Postgres ORM for Python.',
    author='Jared Lunde',
    author_email='jared.lunde@gmail.com',
    url='https://github.com/jaredlunde/cargo-orm',
    license="MIT",
    install_requires=[str(ir.req) for ir in install_reqs],
    package_dir={pkg: pkg_dir},
    include_package_data=True,
    packages=list(find_packages(pkg))
)
