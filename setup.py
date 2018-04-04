#! /usr/bin/env python

from setuptools import setup

setup(name='berlin_hp',
      version='0.0.1',
      author='Uwe Krien',
      author_email='uwe.krien@rl-institut.de',
      description='A reegis heat and power model of Berlin.',
      package_dir={'berlin_hp': 'berlin_hp'},
      install_requires=['oemof >= 0.1.0',
                        'pandas >= 0.17.0',
                        'reegis_tools',
                        'owslib',
                        'geopandas',
                        'workalendar',
                        'demandlib',
                        'requests']
      )
