#!/usr/bin/env python

from setuptools import setup, find_packages
import subprocess

setup(name="singer-encodings",
      version='0.0.8',
      description="Singer.io encodings library",
      author="Stitch",
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      url="http://singer.io",
      install_requires=[
      ],
      extras_require={
          "dev": ["nose"]
      },
      packages=find_packages(),
)
