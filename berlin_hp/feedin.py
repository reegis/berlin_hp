# -*- coding: utf-8 -*-

"""Aggregating feed-in time series for the model regions.

Copyright (c) 2016-2018 Uwe Krien <uwe.krien@rl-institut.de>

SPDX-License-Identifier: GPL-3.0-or-later
"""
__copyright__ = "Uwe Krien <uwe.krien@rl-institut.de>"
__license__ = "GPLv3"


# Python libraries
import logging

# External libraries
import pandas as pd

# oemof packages
from oemof.tools import logger

# internal modules
import reegis_tools.coastdat


def get_berlin_feedin(year, feedin_type):
    f = reegis_tools.coastdat.get_feedin_by_state(year, feedin_type, 'BE')
    return f




if __name__ == "__main__":
    logger.define_logging(screen_level=logging.DEBUG)
    logging.info("Aggregating regions.")
    # aggregate_by_region(2014)
    wind = get_berlin_feedin(2014, 'wind')
    solar = get_berlin_feedin(2014, 'solar')
    from matplotlib import pyplot as plt
    wind.plot()
    solar.plot()
    plt.show()
