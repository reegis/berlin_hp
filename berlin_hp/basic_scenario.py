import pandas as pd
import datetime
import logging
import os

import reegis_tools.config as cfg
import reegis_tools.commodity_sources
import reegis_tools.powerplants
import reegis_tools.coastdat as coastdat

# import reegis_tools.demand as de21_demand

import oemof.tools.logger as logger

from berlin_hp import feedin
import berlin_hp.heat as heat
import berlin_hp.electricity
import berlin_hp.scenario_tools as scenario_tools


def create_scenario(year):
    table_collection = {}

    logging.info('BASIC SCENARIO - FEED-IN TIME SERIES')
    table_collection['time_series'] = scenario_feedin(year)

    logging.info('BASIC SCENARIO - HEAT DEMAND TIME SERIES')
    table_collection['time_series'] = scenario_heat_profiles(
        year, table_collection['time_series'])

    logging.info('BASIC SCENARIO - DEMAND')
    table_collection['time_series'] = scenario_elec_demand(
        year, table_collection['time_series'])

    # logging.info('BASIC SCENARIO - STORAGES')
    # table_collection['storages'] = scenario_storages()
    #
    logging.info('BASIC SCENARIO - POWER PLANTS')
    table_collection['powerplants'] = scenario_powerplants(
        year, table_collection['time_series'])

    logging.info('BASIC SCENARIO - DECENTRALISED HEAT')
    table_collection['decentralised_heating'] = decentralised_heating()

    logging.info('BASIC SCENARIO - SOURCES')
    table_collection['commodity_sources'] = commodity_sources(year)

    logging.info('BASIC SCENARIO - VOLATILE SOURCES')
    table_collection['volatile_source'] = scenario_volatile_sources(year)

    return table_collection


def time_logger(txt, ref):
    msg = "{0}.Elapsed time: {1}".format(txt, datetime.datetime.now() - ref)
    logging.info(msg)


def scenario_powerplants(year, ts):
    pp = pd.read_csv(os.path.join(cfg.get('paths', 'data_berlin'),
                                  cfg.get('powerplants', 'main_powerplants')),
                     index_col=[0])
    pp = pp.loc[(pp.commission < year) & (pp.decommission >= year)]
    pp.columns = pd.MultiIndex.from_product([['BE'], pp.columns])

    dec_dh = ts['district_heating_demand', 'decentralised_dh']

    # Fetch config values from scenario.ini
    share_hp_chp = cfg.get('decentralised_chp', 'share_hp_chp')
    over_cap = cfg.get('decentralised_chp', 'overcapacity_factor')
    eff_chp_heat = cfg.get('decentralised_chp', 'efficiency_chp_heat')
    eff_chp_elec = cfg.get('decentralised_chp', 'efficiency_chp_elec')
    eff_heat = cfg.get('decentralised_chp', 'efficiency_heat')
    heat_capacity = dec_dh.max() * over_cap

    # decentralised CHP blocks
    pp.loc['decentralised CHP-blocks', ('BE', 'fuel')] = 'natural_gas'
    pp.loc['decentralised CHP-blocks', ('BE', 'capacity_elec')] = round(
        heat_capacity * (1 - share_hp_chp) / eff_chp_heat * eff_chp_elec)
    pp.loc['decentralised CHP-blocks', ('BE', 'capacity_heat')] = round(
        heat_capacity * (1 - share_hp_chp))
    pp.loc['decentralised CHP-blocks', ('BE', 'efficiency')] = (
        eff_chp_elec + eff_chp_heat)
    pp.loc['decentralised CHP-blocks', ('BE', 'type')] = 'FIX'
    pp.loc['decentralised CHP-blocks', ('BE', 'network')] = 'decentralised_dh'

    # decentralised heat devices
    pp.loc['decentralised heat-blocks', ('BE', 'fuel')] = 'natural_gas'
    pp.loc['decentralised heat-blocks', ('BE', 'capacity_heat')] = round(
        heat_capacity * share_hp_chp)
    pp.loc['decentralised heat-blocks', ('BE', 'efficiency')] = eff_heat
    pp.loc['decentralised heat-blocks', ('BE', 'type')] = 'HP'
    pp.loc['decentralised heat-blocks', ('BE', 'network')] = 'decentralised_dh'

    return pp


def scenario_volatile_sources(year):
    pp = reegis_tools.powerplants.get_pp_by_year(year, overwrite_capacity=True)
    re = pd.DataFrame()
    for pp_type in ['Wind', 'Solar']:
        re.loc['capacity', pp_type] = round(pp.loc[
            (pp.federal_states == 'BE') &
            (pp.energy_source_level_2 == pp_type)].sum().capacity, 1)
    re.columns = pd.MultiIndex.from_product([['BE'], re.columns])
    return re


def scenario_feedin(year):
    return coastdat.scenario_feedin(year, 'BE')


def commodity_sources(year):
    commodity_src = scenario_commodity_sources(year)
    commodity_src = commodity_src.swaplevel().unstack()

    msg = ("The unit for {0} of the source is '{1}'. "
           "Will multiply it with {2} to get '{3}'.")

    converter = {'costs': ['costs', 'EUR/J', 1e+9 * 3.6, 'EUR/MWh'],
                 'emission': ['emission', 'g/J', 1e+6 * 3.6, 'kg/MWh']}

    # convert units
    for key in converter.keys():
        commodity_src.loc[key] = commodity_src.loc[key].multiply(
            converter[key][2])
        logging.warning(msg.format(*converter[key]))

    # Add region level to be consistent to other tables
    commodity_src.columns = pd.MultiIndex.from_product(
        [['BE'], commodity_src.columns])

    return commodity_src


def scenario_commodity_sources(year, use_znes_2014=True):
    cs = reegis_tools.commodity_sources.get_commodity_sources()
    rename_cols = {key.lower(): value for key, value in
                   cfg.get_dict('source_names').items()}
    cs = cs.rename(columns=rename_cols)
    cs_year = cs.loc[year]
    if use_znes_2014:
        before = len(cs_year[cs_year.isnull()])
        cs_year = cs_year.fillna(cs.loc[2014])
        after = len(cs_year[cs_year.isnull()])
        if before - after > 0:
            logging.warning("Values were replaced with znes2014 data.")
    cs_year.sort_index(inplace=True)
    return cs_year


def decentralised_heating():
    filename = os.path.join(cfg.get('paths', 'data_berlin'),
                            cfg.get('heating', 'table'))
    return pd.read_csv(filename, header=[0, 1], index_col=[0])


def scenario_heat_profiles(year, ts, basic_scenario=True):
    df = heat.create_heat_profiles(year)

    dc_name = 'decentralised_demand'
    dh_name = 'district_heating_demand'

    df.columns = pd.MultiIndex.from_product([[dc_name], df.columns])
    for col in df[dc_name].columns:
        if '_' in col:
            df[(dh_name, col)] = df[(dc_name, col)]
            del df[(dc_name, col)]

    df = pd.concat([ts, df], axis=1)
    if basic_scenario is True:
        df['decentralised_demand', 'elec'] = 0
    return df


def scenario_elec_demand(year, time_series):
    elec_demand = berlin_hp.electricity.get_electricity_demand(year)
    time_series['electricity', 'demand'] = elec_demand.usage.values * 1000
    return time_series


def create_basic_scenario(year):
    table_collection = create_scenario(year)
    name = '{0}_{1}_{2}'.format('berlin_hp', year, 'single')
    sce = scenario_tools.Scenario(table_collection=table_collection,
                                  name=name, year=year)
    path = os.path.join(cfg.get('paths', 'scenario'), 'berlin_hp', str(year))
    sce.to_excel(os.path.join(path, name + '.xls'))
    sce.to_csv(os.path.join(path, '{0}_csv'.format(name)))


if __name__ == "__main__":
    logger.define_logging()
    start = datetime.datetime.now()
    for y in [2014, 2013, 2012]:
        create_basic_scenario(y)
        mesg = "Basic scenario for {0} created: {1}"
        logging.info(mesg.format(y, datetime.datetime.now() - start))
    logging.info("Done: {0}".format(datetime.datetime.now() - start))
