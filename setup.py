#!/usr/bin/env python

from setuptools import setup, find_packages
import subprocess

setup(name="singer-encodings",
      version='0.0.2',
      description="Singer.io encodings library",
      author="Stitch",
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      url="http://singer.io",
      install_requires=[
      ],
      packages=find_packages(),
)
