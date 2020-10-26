# -*- coding: utf-8 -*-

"""Main script.

SPDX-FileCopyrightText: 2016-2019 Uwe Krien <krien@uni-bremen.de>

SPDX-License-Identifier: MIT
"""
__copyright__ = "Uwe Krien <krien@uni-bremen.de>"
__license__ = "MIT"


import logging
import os
from datetime import datetime

import berlin_hp
from berlin_hp import config as cfg
from oemof.tools import logger


def stopwatch():
    if not hasattr(stopwatch, "start"):
        stopwatch.start = datetime.now()
    return str(datetime.now() - stopwatch.start)[:-7]


def model_scenarios(scenarios):
    for scenario in scenarios:
        y = int([x for x in scenario.split("_") if x.isnumeric()][0])
        path = os.path.dirname(scenario)
        file = os.path.basename(scenario)
        main(y, path=path, file=file)


def main(
    year, path=None, file=None, resultpath=None, solver="cbc", graph=False
):
    stopwatch()

    sc = berlin_hp.BerlinScenario(year=year, name="berlin_hp", debug=False)

    if path is None:
        path = os.path.join(
            cfg.get("paths", "scenario"), "berlin_hp", str(year)
        )

    logging.info("Read scenario from excel-sheet: {0}".format(stopwatch()))
    if file is None:
        excel_fn = os.path.join(
            path, "_".join(["berlin_hp", str(year), "single"]) + ".xls"
        )
    else:
        excel_fn = os.path.join(path, file)

    if not os.path.isfile(excel_fn):
        berlin_hp.basic_scenario.create_basic_scenario(year)

    sc.load_excel(excel_fn)
    sc.check_table("time_series")

    logging.info("Add nodes to the EnergySystem: {0}".format(stopwatch()))
    nodes = sc.create_nodes()
    sc.add_nodes(nodes)

    # Save energySystem to '.graphml' file.
    if graph is True:
        sc.plot_nodes(
            filename=os.path.join(path, "berlin_hp"),
            remove_nodes_with_substrings=["bus_cs"],
        )

    logging.info("Create the concrete model: {0}".format(stopwatch()))
    sc.create_model()

    logging.info("Solve the optimisation model: {0}".format(stopwatch()))
    sc.solve(solver=solver)

    logging.info("Solved. Dump results: {0}".format(stopwatch()))
    if resultpath is None:
        resultpath = os.path.join(path, "results_{0}".format(solver))

    sc.dump_es(
        os.path.join(resultpath, "berlin_hp_{0}_single.esys".format(str(year)))
    )

    logging.info(
        "All done. berlin_hp finished without errors: {0}".format(stopwatch())
    )


if __name__ == "__main__":
    pass
