# -*- coding: utf-8 -*-

"""Work with the scenario data.

Copyright (c) 2016-2018 Uwe Krien <uwe.krien@rl-institut.de>

SPDX-License-Identifier: GPL-3.0-or-later
"""
__copyright__ = "Uwe Krien <uwe.krien@rl-institut.de>"
__license__ = "GPLv3"

from collections import namedtuple

# oemof libraries
import oemof.tools.logger as logger
import oemof.solph as solph

# internal modules
import reegis_tools.config as cfg
import reegis_tools.scenario_tools


class Label(namedtuple('solph_label', ['cat', 'tag', 'subtag', 'region'])):
    __slots__ = ()

    def __str__(self):
        return '_'.join(map(str, self._asdict().values()))


class Scenario(reegis_tools.scenario_tools.Scenario):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_nodes(self, nodes=None, region='BE'):
        return nodes_from_table_collection(
            self.table_collection, nodes, region=region)


def nodes_from_table_collection(table_collection, nodes=None, region='BE'):
    # Create  a special dictionary that will raise an error if a key is
    # updated. This avoids the
    if nodes is None:
        nodes = reegis_tools.scenario_tools.NodeDict()

    # Global commodity sources
    cs = table_collection['commodity_sources']['DE']
    for fuel in cs.columns:
        bus_label = Label('bus', 'commodity', fuel.replace(' ', '_'), 'DE')
        if bus_label not in nodes:
            nodes[bus_label] = solph.Bus(label=bus_label)

        cs_label = Label('source', 'commodity', fuel.replace(' ', '_'), 'DE')
        if cs_label not in nodes:
            nodes[cs_label] = solph.Source(
                label=cs_label, outputs={nodes[bus_label]: solph.Flow(
                    variable_costs=cs.loc['costs', fuel],
                    emission=cs.loc['emission', fuel])})

    # Create additional bus for electricity as source. This bus can be
    # connected to the electricity bus for future scenarios.
    # nodes['bus_cs_electricity'] = solph.Bus(label='bus_cs_electricity')

    # Create electricity Bus
    elec_bus_label = Label('bus', 'electricity', 'all', region)
    nodes[elec_bus_label] = solph.Bus(label=elec_bus_label)

    # Local volatile electricity sources
    vs = table_collection['volatile_source']
    ts = table_collection['time_series']
    feedin = None
    for vs_type in vs[region].columns:
        vs_label = Label('source', 'ee', vs_type, region)
        capacity = vs.loc['capacity', (region, vs_type)]
        try:
            feedin = ts[region, vs_type.lower()]
        except KeyError:
            if capacity > 0:
                msg = "Missing time series for {0} (capacity: {1}) in {2}."
                raise ValueError(msg.format(vs_type, capacity, region))
        if capacity * sum(feedin) > 0:
            print(capacity * sum(feedin))
            nodes[vs_label] = solph.Source(
                label=vs_label,
                outputs={nodes[elec_bus_label]: solph.Flow(
                    actual_value=feedin, nominal_value=capacity,
                    fixed=True, emission=0)})

    # Decentralised heating systems
    dh = table_collection['decentralised_heating']
    for fuel in ts['decentralised_demand'].columns:
        src = dh.loc['source', ('BE_demand', fuel)]
        if src == 'elec':
            bus_label = elec_bus_label
        else:
            bus_label = Label('bus', 'commodity', src.replace(' ', '_'), 'DE')

        # Check if source bus exists
        if bus_label not in nodes:
            msg = "Bus '{0}' not found for source '{1}'. Node without Bus!"
            raise ValueError(msg.format(bus_label, src))

        # Create heating bus as Bus
        heat_bus_label = Label('bus', 'heat', fuel.replace(' ', '_'),
                               'decentralised_BE')
        nodes[heat_bus_label] = solph.Bus(label=heat_bus_label)

        # Create heating system as Transformer
        trsf_label = Label('trsf', 'heat', fuel.replace(' ', '_'),
                           'decentralised_BE')
        efficiency = float(dh.loc['efficiency', ('BE_demand', fuel)])
        nodes[trsf_label] = solph.Transformer(
            label=trsf_label,
            inputs={nodes[bus_label]: solph.Flow()},
            outputs={nodes[heat_bus_label]: solph.Flow()},
            conversion_factors={nodes[heat_bus_label]: efficiency})

        # Create demand as Sink
        d_heat_demand_label = Label('demand', 'heat',
                                    fuel.replace(' ', '_'), 'decentralised_BE')
        nodes[d_heat_demand_label] = solph.Sink(
                label=d_heat_demand_label,
                inputs={nodes[heat_bus_label]: solph.Flow(
                    actual_value=ts['decentralised_demand', fuel],
                    nominal_value=1, fixed=True)})

    # Electricity demand
    elec_demand_label = Label('demand', 'electricity', 'all', region)
    nodes[elec_demand_label] = solph.Sink(
        label=elec_demand_label,
        inputs={nodes[elec_bus_label]: solph.Flow(
            actual_value=ts['electricity', 'demand'],
            nominal_value=1, fixed=True)})

    # District heating demand
    for system in ts['district_heating_demand'].columns:
        if ts['district_heating_demand'][system].sum() > 0:
            bus_label = Label('bus', 'heat', 'district', system)
            if bus_label not in nodes:
                nodes[bus_label] = solph.Bus(label=bus_label)
            dh_demand_label = Label('demand', 'heat', 'district', system)
            nodes[dh_demand_label] = solph.Sink(
                label=dh_demand_label,
                inputs={nodes[bus_label]: solph.Flow(
                    actual_value=ts['district_heating_demand', system],
                    nominal_value=1, fixed=True)})

    # Prepare the input table for power plants
    pp = table_collection['powerplants'][region].copy()

    pp = pp.fillna(0)
    pp['capacity_in'] = (pp.capacity_elec + pp.capacity_heat) / pp.efficiency
    pp['eff_cond_elec'] = pp.capacity_elec / pp.capacity_in
    pp['eff_chp_heat'] = pp.capacity_heat / pp.capacity_in
    pp['capacity_elec_chp'] = (pp.eff_cond_elec - pp.elec_loss_factor *
                               pp.eff_chp_heat) * pp.capacity_in
    pp['eff_chp_elec'] = pp.capacity_elec_chp / pp.capacity_in
    pp = pp.groupby(['type', 'network', 'fuel']).sum()
    pp['eff_cond_elec'] = pp.capacity_elec / pp.capacity_in
    pp['eff_chp_heat'] = pp.capacity_heat / pp.capacity_in
    pp['eff_chp_elec'] = pp.capacity_elec_chp / pp.capacity_in
    district_heating_systems = cfg.get_dict('district_heating_systems')
    fuel_dict = cfg.get_dict('fuel_dict')

    # Create chp plants with extraction turbine
    if 'EXT' in pp.index:
        for ext in pp.loc['EXT'].iterrows():
            heat_sys = district_heating_systems[ext[0][0]]
            src = ext[0][1].replace(' ', '_')
            if src in fuel_dict:
                src = fuel_dict[src]
            src_bus_label = Label('bus', 'commodity', src, 'DE')
            heat_bus_label = Label('bus', 'heat', 'district', heat_sys)
            chp_label = Label('chp', 'ext', src, heat_sys)

            bel = nodes[elec_bus_label]
            bth = nodes[heat_bus_label]

            nodes[chp_label] = solph.components.ExtractionTurbineCHP(
                label=chp_label,
                inputs={nodes[src_bus_label]: solph.Flow(
                    nominal_value=ext[1].capacity_in)},
                outputs={bel: solph.Flow(),
                         bth: solph.Flow()},
                conversion_factors={bel: ext[1].eff_chp_elec,
                                    bth: ext[1].eff_chp_heat},
                conversion_factor_full_condensation={
                    bel: ext[1].eff_cond_elec})

    # Create chp plants with fixed heat ratio (e.g. backpressure)
    if 'FIX' in pp.index:
        for fix in pp.loc['FIX'].iterrows():
            heat_sys = district_heating_systems[fix[0][0]]
            src = fix[0][1].replace(' ', '_')
            if src in fuel_dict:
                src = fuel_dict[src]
            src_bus_label = Label('bus', 'commodity', src, 'DE')
            heat_bus_label = Label('bus', 'heat', 'district', heat_sys)
            chp_label = Label('chp', 'fix', src, heat_sys)

            bel = nodes[elec_bus_label]
            bth = nodes[heat_bus_label]

            nodes[chp_label] = solph.Transformer(
                label=chp_label,
                inputs={nodes[src_bus_label]: solph.Flow(
                    nominal_value=fix[1].capacity_in)},
                outputs={bel: solph.Flow(),
                         bth: solph.Flow()},
                conversion_factors={bel: fix[1].eff_chp_elec,
                                    bth: fix[1].eff_chp_heat})

    # Create heat plants (hp) without power production
    if 'HP' in pp.index:
        for hp in pp.loc['HP'].iterrows():
            heat_sys = district_heating_systems[hp[0][0]]
            src = hp[0][1].replace(' ', '_')
            if src in fuel_dict:
                src = fuel_dict[src]

            if src == 'electricity':
                src_bus_label = Label('bus', 'electricity', 'all', 'BE')
            else:
                src_bus_label = Label('bus', 'commodity', src, 'DE')
            heat_bus_label = Label('bus', 'heat', 'district', heat_sys)
            hp_label = Label('hp', 'heat', src, heat_sys)

            bth = nodes[heat_bus_label]

            nodes[hp_label] = solph.Transformer(
                label=hp_label,
                inputs={nodes[src_bus_label]: solph.Flow(
                    nominal_value=hp[1].capacity_in)},
                outputs={bth: solph.Flow()},
                conversion_factors={bth: hp[1].eff_chp_heat})

    # Create power plants without heat extraction
    if 'PP' in pp.index:
        for pp in pp.loc['PP'].iterrows():
            heat_sys = district_heating_systems[pp[0][0]]
            src = pp[0][1].replace(' ', '_')
            if src in fuel_dict:
                src = fuel_dict[src]
            src_bus_label = Label('bus', 'commodity', src, 'DE')
            pp_label = Label('pp', 'electricity', src, heat_sys)

            bel = nodes[elec_bus_label]

            nodes[pp_label] = solph.Transformer(
                label=pp_label,
                inputs={nodes[src_bus_label]: solph.Flow()},
                outputs={bel: solph.Flow(
                    nominal_value=pp[1].capacity_elec)},
                conversion_factors={bel: pp[1].efficiency})

    # # Storages
    # storages = table_collection['storages']
    # storages.columns = storages.columns.swaplevel()
    # for region in storages['phes'].columns:
    #     storage_label = 'phe_storage_{0}'.format(region)
    #     bus_label = 'bus_elec_{0}'.format(region)
    #     params = storages['phes'][region]
    #     nodes[storage_label] = solph.components.GenericStorage(
    #         label=storage_label,
    #         inputs={nodes[bus_label]: solph.Flow(
    #             nominal_value=params.pump)},
    #         outputs={nodes[bus_label]: solph.Flow(
    #             nominal_value=params.turbine)},
    #         nominal_capacity=params.energy,
    #         capacity_loss=0,
    #         initial_capacity=None,
    #         inflow_conversion_factor=params.pump_eff,
    #         outflow_conversion_factor=params.turbine_eff)

    # Add shortage excess to every electricity bus
    elec_bus_keys = [key for key in nodes.keys()
                     if 'bus_electricity' in str(key)]
    bus_keys = [key for key in nodes.keys() if 'bus' in str(key)]

    for key in elec_bus_keys:
        excess_label = Label('excess', key.tag, key.subtag, key.region)
        if excess_label not in nodes:
            nodes[excess_label] = solph.Sink(
                label=excess_label,
                inputs={nodes[key]: solph.Flow()})

    for key in bus_keys:
        shortage_label = Label('shortage', key.tag, key.subtag, key.region)
        if shortage_label not in nodes:
            nodes[shortage_label] = solph.Source(
                label=shortage_label,
                outputs={nodes[key]: solph.Flow(variable_costs=9000)})
    return nodes


if __name__ == "__main__":
    # import pandas as pd
    logger.define_logging()
