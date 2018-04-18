# -*- coding: utf-8 -*-

"""Main script.

Copyright (c) 2016-2018 Uwe Krien <uwe.krien@rl-institut.de>

SPDX-License-Identifier: GPL-3.0-or-later
"""
__copyright__ = "Uwe Krien <uwe.krien@rl-institut.de>"
__license__ = "GPLv3"


# Python libraries
import os
import logging
from datetime import datetime

import pandas as pd

# oemof packages
from oemof.tools import logger
import oemof.solph as solph

# internal modules
import reegis_tools.config as cfg
import reegis_tools.scenario_tools
import berlin_hp
import de21


def stopwatch():
    if not hasattr(stopwatch, 'start'):
        stopwatch.start = datetime.now()
    return str(datetime.now() - stopwatch.start)[:-7]


def create_reduced_de21_scenario(year):
    stopwatch()

    logging.info("Read scenarios from excel-sheet: {0}".format(stopwatch()))

    # Berlin
    berlin = berlin_hp.Scenario(name='berlin_basic', year=year)
    berlin_fn = os.path.join(cfg.get('paths', 'scenario'), 'berlin_basic',
                             '{year}', 'berlin_basic_{year}.xls')
    berlin.load_excel(berlin_fn.format(year=year))
    berlin.check_table('time_series')

    # de21
    de = de21.Scenario(name='basic', year=2014)
    de_path = os.path.join(cfg.get('paths', 'scenario'), 'basic', '{year}',
                           'csv')
    de.load_csv(de_path.format(year=year))
    # de.check_table('time_series')

    # control table
    ct = pd.DataFrame(
        columns=['DE_orig', 'DE01_orig', 'BE', 'DE01_new', 'DE_new'])

    region = 'DE01'

    de_be = {
        'natural gas': 'gas',
        'hard coal': 'coal',
        'wind': 'Wind',
        'solar': 'Solar',
        'oil': 'oil',
        'geothermal': 'Geothermal',
        'hydro': 'Hydro',
    }

    # TODO Check units (s. below)

    # Demand of all district heating systems
    berlin_district_heating = (
        berlin.table_collection['time_series']['district_heating_demand'])
    berlin_district_heating = berlin_district_heating.sum(axis=1)

    ct.loc['district heating', 'DE01_orig'] = round(
        de.table_collection['time_series'][region, 'district_heating'].sum())
    ct.loc['district heating', 'BE'] = round(
        berlin_district_heating.sum())

    de.table_collection['time_series'][region, 'district_heating'] -= (
        berlin_district_heating)

    ct.loc['district heating', 'DE01_new'] = round(
        de.table_collection['time_series'][region, 'district_heating'].sum())

    # Electricity demand
    berlin_elec_demand = (
        berlin.table_collection['time_series']['electricity', 'demand'])

    ct.loc['electricity demand', 'DE01_orig'] = round(
        de.table_collection['time_series'][region, 'electrical_load'].sum())
    ct.loc['electricity demand', 'BE'] = round(berlin_elec_demand.sum())

    de.table_collection['time_series'][region, 'electrical_load'] -= (
        berlin_elec_demand)

    ct.loc['electricity demand', 'DE01_new'] = round(
        de.table_collection['time_series'][region, 'electrical_load'].sum())

    # Decentralised heating
    # TODO: electricity ????
    # gas und natural_gas bei DE????
    # Einheiten kontrollieren!

    dch_berlin = list(
        berlin.table_collection['time_series']['decentralised_demand'].columns)

    for col in de.table_collection['time_series']['DE_demand'].columns:
        ct.loc['decentralised_' + col, 'DE_orig'] = round(
            de.table_collection['time_series']['DE_demand', col].sum())
        ct.loc['decentralised_' + col, 'BE'] = round((
            berlin.table_collection['time_series'].get(
                ('decentralised_demand', de_be.get(col)),
                pd.Series([0, 0]))).sum())

        de.table_collection['time_series']['DE_demand', col] -= (
                berlin.table_collection['time_series'].get((
                    'decentralised_demand', de_be.get(col)), 0))

        ct.loc['decentralised_' + col, 'DE_new'] = round(
            de.table_collection['time_series']['DE_demand', col].sum())

        if de_be.get(col) in dch_berlin:
            dch_berlin.remove(de_be.get(col))

    for col in dch_berlin:
        ct.loc['decentralised_' + col, 'BE'] = round((
                berlin.table_collection['time_series'][
                    'decentralised_demand', col]).sum())

    # Volatile Sources
    vs_berlin = list(berlin.table_collection['volatile_source']['BE'].columns)
    for col in de.table_collection['volatile_source'][region].columns:
        ct.loc['re_' + col, 'DE01_orig'] = round(float(
            de.table_collection['volatile_source'][region, col]))
        # if de_be.get(col) in vs_berlin:
        de.table_collection['volatile_source'][region, col] -= (
            berlin.table_collection['volatile_source'].get(
                ('BE', de_be[col]), 0))
        ct.loc['re_' + col, 'BE'] = round(float(
            berlin.table_collection['volatile_source'].get(
                ('BE', de_be[col]), 0)))

        if de_be.get(col) in vs_berlin:
            vs_berlin.remove(de_be.get(col))

        ct.loc['re_' + col, 'DE01_new'] = round(float(
            de.table_collection['volatile_source'][region, col]))

    for col in vs_berlin:
        ct.loc['re_' + col, 'BE'] = round(float(
                berlin.table_collection['volatile_source']['BE', col]))

    # Elec. Storages
    pass

    # Power Plants
    sub = pd.DataFrame(
        columns=['DE_orig', 'DE01_orig', 'BE', 'DE01_new', 'DE_new'])

    y = 2014
    import reegis_tools.powerplants
    pwp = reegis_tools.powerplants.get_pp_by_year(
        y, overwrite_capacity=True, capacity_in=True)
    table_collect = de21.basic_scenario.powerplants(
        pwp, {}, y, region_column='federal_states')
    heat_b = reegis_tools.powerplants.get_chp_share_and_efficiency_states(y)

    heat_b['BE']['fuel_share'].rename(columns={'re': 'bioenergy'},
                                      inplace=True)

    heat_demand = pd.DataFrame(berlin_district_heating,
                               columns=['district_heating'])
    heat_demand = (
        pd.concat([heat_demand], axis=1, keys=['BE']).sort_index(1))

    table_collect = de21.basic_scenario.chp_table(
        heat_b, heat_demand, table_collect, regions=['BE'])

    rows = [r for r in de.table_collection['transformer'].index
            if 'efficiency' not in r]

    sub['BE'] = (table_collect['transformer'].loc[rows, 'BE']).sum(axis=1)

    sub['DE01_orig'] = de.table_collection['transformer'].loc[
        rows, region].sum(axis=1)

    asd = de.table_collection['transformer'].loc[
        rows, region]
    bsd = table_collect['transformer'].loc[rows, 'BE']

    for col in de.table_collection['transformer'][region].columns:
        de.table_collection['transformer'].loc[rows, (region, col)] -= (
                table_collect['transformer'].loc[rows, ('BE', col)])
        de.table_collection['transformer'].loc[rows, (region, col)].fillna(
            float('inf'), inplace=True)

    sub['DE01_new'] = de.table_collection['transformer'].loc[rows, region].sum(
        axis=1)
    csd = de.table_collection['transformer'].loc[
        rows, region]
    pd.concat([asd, bsd, csd]).to_excel(os.path.join(
        cfg.get('paths', 'messages'), 'summery_embedded_powerplants.xls'))
    ct = pd.concat([ct, sub])

    ct.to_excel(os.path.join(
        cfg.get('paths', 'messages'), 'summery_embedded_model.xls'))

    sce = reegis_tools.scenario_tools.Scenario(
        table_collection=de.table_collection, name='de_without_BE', year=2014)
    path = os.path.join(cfg.get('paths', 'scenario'), 'basic', str(year))
    sce.to_excel(os.path.join(path, '_'.join([sce.name, str(year)]) + '.xls'))
    csv_path = os.path.join(path, 'csv_without_BE')
    sce.to_csv(csv_path)
    return csv_path


def connect_electricity_buses(bus1, bus2, nodes):
    lines = [(bus1, bus2), (bus2, bus1)]
    for line in lines:
        line_label = 'power_line_{0}_{1}'.format(line[0], line[1])
        bus_label_in = 'bus_elec_{0}'.format(line[0])
        bus_label_out = 'bus_elec_{0}'.format(line[1])
        if bus_label_in not in nodes:
            raise ValueError(
                "Bus {0} missing for power line from {0} to {1}".format(
                    bus_label_in, bus_label_out))
        if bus_label_out not in nodes:
            raise ValueError(
                "Bus {0} missing for power line from {0} to {1}".format(
                    bus_label_out, bus_label_in))
        nodes[line_label] = solph.Transformer(
            label=line_label,
            inputs={nodes[bus_label_in]: solph.Flow()},
            outputs={nodes[bus_label_out]: solph.Flow()})
    return nodes


def main(year, de21_csv_path):
    stopwatch()

    # Load data of the de21 model
    logging.info("Read de21 scenario from csv collection: {0}".format(
        stopwatch()))
    sc_de = de21.Scenario(name='basic', year=year)
    sc_de.load_csv(de21_csv_path)
    # sc_de.check_table('time_series')

    # Create nodes for the de21 model
    nodes_de21 = sc_de.create_nodes()

    sc_be = berlin_hp.Scenario(name='berlin_basic', year=year)

    scpath = os.path.join(cfg.get('paths', 'scenario'), 'berlin_basic',
                          str(year))

    # Load data of the berlin_hp Model
    logging.info("Read scenario from excel-sheet: {0}".format(stopwatch()))
    excel_fn = os.path.join(scpath, '_'.join([sc_be.name, str(year)]) + '.xls')
    sc_be.load_excel(excel_fn)
    sc_be.check_table('time_series')

    # Create nodes for the berlin_hp model
    nodes = sc_be.create_nodes(nodes_de21)

    # Connect de21 and berlin_hp with a transmission line
    nodes = connect_electricity_buses('DE01', 'BE', nodes)

    # Add nodes to the energy system
    sc_be.add_nodes2solph(nodes=nodes)

    # Create model (all constraints)
    logging.info("Create the concrete model: {0}".format(stopwatch()))
    sc_be.create_model()

    # Pass the model to the solver and fetch the results afterwards..
    logging.info("Solve the optimisation model: {0}".format(stopwatch()))
    sc_be.solve()

    # Dump the energy system with the results to disc
    logging.info("Solved. Dump results: {0}".format(stopwatch()))
    sc_be.dump_es(os.path.join(scpath, 'berlin_hp_de21.esys'))

    logging.info("All done. de21 finished without errors: {0}".format(
        stopwatch()))


if __name__ == "__main__":
    logger.define_logging(file_level=logging.INFO)
    yr = 2014
    berlin_hp.main(yr)
    de21.main(yr)
    sc_path = create_reduced_de21_scenario(yr)
    main(yr, sc_path)
