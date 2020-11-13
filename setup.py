#! /usr/bin/env python

from setuptools import setup, find_packages
import os


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


github = "@https://github.com/"


setup(
    name="berlin_hp",
    version="0.0.1",
    author="Uwe Krien",
    author_email="krien@uni-bremen.de",
    description="A reegis heat and power model of Berlin.",
    long_description=read("README.rst"),
    long_description_content_type="text/x-rst",
    packages=find_packages(),
    package_dir={"berlin_hp": "berlin_hp"},
    url="https://github.com/reegis/deflex",
    license="MIT",
    install_requires=[
        "oemof.solph == 0.4.1",
        "pandas == 1.1.4",
        "reegis@https://github.com/reegis/reegis/archive/phd.zip",
        "owslib == 0.20.0",
        "geopandas == 0.8.1",
        "workalendar == 13.0.0",
        "demandlib{0}oemof/demandlib/archive/v0.1.7b1.zip".format(github),
        "requests == 2.25.0",
    ],
)
