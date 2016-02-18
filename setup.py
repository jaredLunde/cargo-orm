#!/usr/bin/python3 -S
import os
import uuid
try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup
from pip.req import parse_requirements
from pkgutil import walk_packages
from Cython.Build import cythonize
from distutils.command.build_ext import build_ext


pathname = os.path.dirname(os.path.realpath(__file__))


extensions = [
    Extension(
        'vital.structures.multidict',
        [pathname + '/vital/structures/multidict.pyx'])
]

extensions = cythonize(extensions)


# parse_requirements() returns generator of pip.req.InstallRequirement objects
install_reqs = parse_requirements(
    pathname+\
    "/requirements.txt",
    session=uuid.uuid1())


pkg = 'vital'
pkg_dir = "{}/{}".format(pathname, 'vital')


def find_packages(prefix=""):
    path = [pkg_dir]
    yield prefix
    prefix = prefix + "."
    for _, name, ispkg in walk_packages(path, prefix):
        if ispkg:
            yield name


class ve_build_ext(build_ext):
    def run(self):
        try:
            build_ext.run(self)
        except (DistutilsPlatformError, FileNotFoundError):
            raise BuildFailed()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except (CCompilerError, DistutilsExecError,
                DistutilsPlatformError, ValueError):
            raise BuildFailed()


print(list(find_packages('vital')))
setup(
    name='vital',
    version='0.0.1b4',
    description='Fast, efficient, web framework written for Python 3.',
    author='Jared Lunde',
    author_email='jared.lunde@gmail.com',
    url='https://github.com/jaredlunde/vital',
    license="MIT",
    install_requires=[str(ir.req) for ir in install_reqs],
    package_dir={pkg: pkg_dir},
    ext_modules=extensions,
    include_package_data=True,
    packages=list(find_packages(pkg)),
    cmdclass=dict(build_ext=ve_build_ext)
)
