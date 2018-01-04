#!/usr/bin/env python
try:
    from setuptools import setup
    HAVE_SETUPTOOLS = True
except ImportError:
    from distutils.core import setup
    HAVE_SETUPTOOLS = False


VERSION = '0.0.0'

setup_kwargs = {
    "version": VERSION,
    "description": 'A Cyclus batch execution service',
    "license": 'BSD 3-clause',
    "author": 'The ERGS developers',
    "author_email": 'ergsonomic@googlegroups.com',
    "url": 'https://github.com/ergs/fixie-batch',
    "download_url": "https://github.com/ergs/fixie-batch/zipball/" + VERSION,
    "classifiers": [
        "License :: OSI Approved",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Topic :: Utilities",
        ],
    "zip_safe": False,
    "data_files": [("", ['LICENSE', 'README.rst']),],
    }

if HAVE_SETUPTOOLS:
    setup_kwargs['install_requires'] = ['fixie', 'pprintpp']


if __name__ == '__main__':
    setup(
        name='fixie-batch',
        packages=['fixie_batch'],
        long_description=open('README.rst').read(),
        **setup_kwargs
        )
