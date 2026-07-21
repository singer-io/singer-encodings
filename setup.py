#!/usr/bin/env python

from setuptools import setup, find_packages
import subprocess

setup(name="singer-encodings",
      version='0.6.1',
      description="Singer.io encodings library",
      author="Stitch",
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      url="http://singer.io",
      install_requires=[
          "singer-python",
          "pyarrow==23.0.1",
          "fastavro==1.12.0",
          "openpyxl==3.1.5"
      ],
      extras_require={
          "dev": ["pytest"]
      },
      packages=find_packages(),
)
