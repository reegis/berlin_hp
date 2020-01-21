# -*- coding: utf-8 -*-

"""Aggregating feed-in time series for the model regions.

SPDX-FileCopyrightText: 2016-2019 Uwe Krien <krien@uni-bremen.de>

SPDX-License-Identifier: MIT
"""
__copyright__ = "Uwe Krien <krien@uni-bremen.de>"
__license__ = "MIT"


# Python libraries
import logging

# External libraries

# oemof packages
from oemof.tools import logger

# internal modules
import reegis.coastdat


def get_berlin_feedin(year, feedin_type):
    f = reegis.coastdat.get_feedin_by_state(year, feedin_type, 'BE')
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
