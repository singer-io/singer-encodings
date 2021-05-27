#!/usr/bin/env python

from setuptools import setup, find_packages
import subprocess

setup(name="singer-encodings",
      version='0.1.0',
      description="Singer.io encodings library",
      author="Stitch",
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      url="http://singer.io",
      install_requires=[
          "singer-python"
      ],
      extras_require={
          "dev": ["nose"]
      },
      packages=find_packages(),
)
