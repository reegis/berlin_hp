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
from oemof import solph
from .scenario_tools import Label


def stopwatch():
    if not hasattr(stopwatch, "start"):
        stopwatch.start = datetime.now()
    return str(datetime.now() - stopwatch.start)[:-7]


def model_scenarios(scenarios):
    for scenario in scenarios:
        y = int([x for x in scenario.split("_") if x.isnumeric()][0])
        main(y, scenario)


def add_upstream_import_export_nodes(nodes, bus, costs):
    logging.info("Add upstream prices from {0}".format(costs["name"]))
    exp_label = Label("export", "electricity", "all", bus.label.region)
    nodes[exp_label] = solph.Sink(
        label=exp_label,
        inputs={bus: solph.Flow(variable_costs=costs["export"])},
    )

    imp_label = Label("import", "electricity", "all", bus.label.region)
    nodes[imp_label] = solph.Source(
        label=imp_label,
        outputs={bus: solph.Flow(variable_costs=costs["import"])},
    )
    return nodes


def main(
    year,
    file,
    resultpath=None,
    solver="cbc",
    graph=False,
    upstream_prices=None,
):
    stopwatch()

    sc = berlin_hp.BerlinScenario(year=year, name="berlin_hp", debug=False)
    sc.name = os.path.basename(file).split(".")[0]
    path = os.path.dirname(file)
    logging.info("Read scenario from excel-sheet: {0}".format(stopwatch()))
    sc.load_excel(file)
    sc.check_table("time_series")

    logging.info("Add nodes to the EnergySystem: {0}".format(stopwatch()))
    nodes = sc.create_nodes()
    if upstream_prices is not None:
        bus = [
            v
            for k, v in nodes.items()
            if k.tag == "electricity" and isinstance(v, solph.Bus)
        ][0]
        nodes = add_upstream_import_export_nodes(nodes, bus, upstream_prices)
        sc.name = "{0}_UP_{1}".format(sc.name, upstream_prices["name"])
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
        os.path.join(resultpath, "{0}.esys".format(sc.name))
    )

    logging.info(
        "All done. berlin_hp finished without errors: {0}".format(stopwatch())
    )


if __name__ == "__main__":
    pass
