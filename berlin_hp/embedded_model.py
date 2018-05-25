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
import time
import traceback

import pandas as pd

# oemof packages
from oemof.tools import logger
import oemof.solph as solph

# internal modules
import reegis_tools.config as cfg
import reegis_tools.scenario_tools
import berlin_hp
import deflex


def stopwatch():
    if not hasattr(stopwatch, 'start'):
        stopwatch.start = datetime.now()
    return str(datetime.now() - stopwatch.start)[:-7]


def create_reduced_de22_scenario(year):
    de = deflex.Scenario(name='basic', year=2014)
    de_path = os.path.join(cfg.get('paths', 'scenario'), '{year}',
                           'csv', 'basic_de22')
    de.load_csv(de_path.format(year=year))
    de.check_table('time_series')
    logging.info('Remove region DE22....')
    for sheet in de.table_collection.values():
        if 'DE22' in sheet.columns:
            del sheet['DE22']

    for i in de.table_collection['transmission'].index:
        if 'DE22' in i:
            de.table_collection['transmission'].drop(i, inplace=True)

    name = 'de22_without_BE'

    sce = reegis_tools.scenario_tools.Scenario(
        table_collection=de.table_collection, name=name, year=year)
    path = os.path.join(cfg.get('paths', 'scenario'), str(year))
    sce.to_excel(os.path.join(path, '_'.join([sce.name, str(year)]) + '.xls'))
    csv_path = os.path.join(path, 'csv', name)
    sce.to_csv(csv_path)


def create_reduced_de21_scenario(year):
    stopwatch()

    logging.info("Read scenarios from excel-sheet: {0}".format(stopwatch()))

    # Berlin
    name = 'berlin_basic'
    berlin = berlin_hp.Scenario(name=name, year=year)
    berlin_fn = os.path.join(cfg.get('paths', 'scenario'),
                             '{year}', 'berlin_basic_{year}.xls')
    berlin.load_excel(berlin_fn.format(year=year))
    berlin.check_table('time_series')

    # de21
    de = deflex.Scenario(name='basic', year=2014)
    de_path = os.path.join(cfg.get('paths', 'scenario'), '{year}',
                           'csv', 'basic_de21')
    de.load_csv(de_path.format(year=year))
    de.check_table('time_series')

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

    import reegis_tools.powerplants
    pwp = reegis_tools.powerplants.get_pp_by_year(
        year, overwrite_capacity=True, capacity_in=True)

    table_collect = deflex.basic_scenario.powerplants(
        pwp, {}, year, region_column='federal_states')

    heat_b = reegis_tools.powerplants.get_chp_share_and_efficiency_states(year)

    heat_b['BE']['fuel_share'].rename(columns={'re': 'bioenergy'},
                                      inplace=True)
    print(berlin_district_heating)
    print(type(berlin_district_heating))
    heat_demand = pd.DataFrame(berlin_district_heating,
                               columns=['district_heating'])

    heat_demand = (
        pd.concat([heat_demand], axis=1, keys=['BE']).sort_index(1))

    print(heat_demand)
    print(type(heat_demand))

    table_collect = deflex.basic_scenario.chp_table(
        heat_b, heat_demand, table_collect, regions=['BE'])

    rows = [r for r in de.table_collection['transformer'].index
            if 'efficiency' not in r]
    print(de.table_collection['transformer'].loc[rows, region])
    print(table_collect['transformer'].loc[rows, 'BE'])

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
        table_collection=de.table_collection,
        name='de21_without_BE',
        year=year)
    path = os.path.join(cfg.get('paths', 'scenario'), str(year))
    sce.to_excel(os.path.join(path, '_'.join([sce.name, str(year)]) + '.xls'))
    csv_path = os.path.join(path, 'csv', 'de21_without_BE')
    sce.to_csv(csv_path)


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


def main(year, rmap):
    stopwatch()

    scenario_path = os.path.join(cfg.get('paths', 'scenario'), str(year))
    de21_scenario_csv = os.path.join(scenario_path, 'csv', '{0}_without_BE')

    # Load data of the de21 model
    logging.info("Read de21 scenario from csv collection: {0}".format(
        stopwatch()))
    sc_de = deflex.Scenario(name='basic', year=year)
    sc_de.load_csv(de21_scenario_csv.format(rmap))
    sc_de.check_table('time_series')

    # Create nodes for the de21 model
    nodes_de21 = sc_de.create_nodes()

    # Load data of the berlin_hp Model
    logging.info("Read scenario from excel-sheet: {0}".format(stopwatch()))
    excel_fn = os.path.join(scenario_path, 'berlin_basic_{0}.xls'.format(year))
    sc_be = berlin_hp.Scenario(name='berlin_basic', year=year)
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
    res_path = os.path.join(scenario_path, 'results')
    if not os.path.isdir(res_path):
        os.mkdir(res_path)
    sc_be.dump_es(os.path.join(res_path, 'berlin_hp_de21.esys'))

    logging.info("All done. de21 finished without errors: {0}".format(
        stopwatch()))


if __name__ == "__main__":
    logger.define_logging(file_level=logging.INFO)
    # yr = 2014
    # rmap = de21
    # berlin_hp.main(yr)
    # de21.main(yr)
    # create_reduced_de22_scenario(yr)
    for y in [2014, 2013, 2012]:
        create_reduced_de21_scenario(y)
        create_reduced_de22_scenario(y)
        for my_rmap in ['de21', 'de22']:
            try:
                main(y, my_rmap)
            except Exception as e:
                logging.error(traceback.format_exc())
                time.sleep(0.5)
                logging.error(e)
                time.sleep(0.5)
    # main(yr, rmap)
