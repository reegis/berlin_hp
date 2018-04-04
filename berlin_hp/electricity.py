# -*- coding: utf-8 -*-

"""Adapting the general reegis power plants to the de21 model.

Copyright (c) 2016-2018 Uwe Krien <uwe.krien@rl-institut.de>

SPDX-License-Identifier: GPL-3.0-or-later
"""
__copyright__ = "Uwe Krien <uwe.krien@rl-institut.de>"
__license__ = "GPLv3"


import pandas as pd
import os
import logging
from xml.etree import ElementTree

import oemof.tools.logger as logger

import reegis_tools.config as cfg

import berlin_hp.download


def fill_data_gaps(df):
    logging.info('Fill the gaps and resample to hourly values.')
    df.index = pd.to_datetime(df.index)
    df = df.apply(pd.to_numeric)
    df = df.replace(0, float('nan'))
    for col in df.columns:
        df[col] = df[col].fillna(df[col].shift(7 * 4 * 24))
        df[col] = df[col].interpolate()
    return df


def convert_net_xml2df(year, hourly=True):
    filename = os.path.join(
        cfg.get('paths', 'electricity'),
        cfg.get('electricity', 'file_xml').format(year=year))
    if not os.path.isfile(filename):
        logging.info("Download Berlin grid data for {0} as xml.".format(year))
        berlin_hp.download.get_berlin_net_data(year)
    tree = ElementTree.parse(filename)
    elem = tree.getroot()
    logging.info("Convert xml-file to csv-file for {0}".format(year))
    n = 0
    attributes = ['usage', 'generation', 'feed', 'key-acount-usage']
    df = pd.DataFrame(columns=attributes)
    df_temp = pd.DataFrame(columns=attributes)
    logging.info('start')
    for distr_ele in elem.find('district'):
        for f in distr_ele.getchildren():
            value_list = []
            for atr in attributes:
                value_list.append(float(f.find(atr).text))
            df_temp.loc[f.attrib['value'], attributes] = value_list
            if n % 100 == 0:
                df = pd.concat([df, df_temp])
                df_temp = pd.DataFrame(columns=attributes)
            n += 1
    df = pd.concat([df, df_temp])

    # fill the data gaps
    df = fill_data_gaps(df)

    # cut the time series to the given year
    start_date = '{0}-1-1'.format(year)
    end_date = '{0}-1-1'.format(year + 1)
    df = df.loc[(df.index >= start_date) & (df.index < end_date)]

    # resample to hourly values if hourly is set to True
    if hourly is True:
        df = df.resample('H').mean()

    # dump the data as csv-file
    outfile = os.path.join(
        cfg.get('paths', 'electricity'),
        cfg.get('electricity', 'file_csv')).format(year=year)
    df.to_csv(outfile)


if __name__ == "__main__":
    logger.define_logging(file_level=logging.INFO)
    for y in [2012, 2013, 2014, 2015, 2016]:
        convert_net_xml2df(y)
