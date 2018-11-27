import os
import logging
from datetime import datetime

import pandas as pd
import geopandas as gpd

import reegis_tools.config as cfg
import reegis_tools.commodity_sources
import reegis_tools.powerplants
import reegis_tools.coastdat as coastdat
from reegis_tools import geometries

import oemof.tools.logger as logger

import berlin_hp.heat as heat
import berlin_hp.electricity as electricity
import berlin_hp.download as download
import berlin_hp.scenario_tools as scenario_tools
from berlin_hp.scenario_tools import Scenario


def stopwatch():
    if not hasattr(stopwatch, 'start'):
        stopwatch.start = datetime.now()
    return str(datetime.now() - stopwatch.start)[:-7]


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

    logging.info('BASIC SCENARIO - POWER PLANTS')
    table_collection['powerplants'] = scenario_powerplants(
        year, table_collection['time_series'])

    logging.info('BASIC SCENARIO - DECENTRALISED HEAT')
    table_collection['decentralised_heating'] = decentralised_heating()

    logging.info('BASIC SCENARIO - SOURCES')
    table_collection['commodity_sources'] = commodity_sources(year)

    logging.info('BASIC SCENARIO - VOLATILE SOURCES')
    table_collection['volatile_source'] = scenario_volatile_sources()

    return table_collection


def time_logger(txt, ref):
    msg = "{0}.Elapsed time: {1}".format(txt, datetime.now() - ref)
    logging.info(msg)


def scenario_powerplants(year, ts):
    pp = pd.read_csv(os.path.join(
            cfg.get('paths', 'data_berlin'),
            cfg.get('powerplants', 'powerplants_friedrichshagen')),
        index_col=[0])
    pp = pp.loc[(pp.commission < year) & (pp.decommission >= year)]
    pp.columns = pd.MultiIndex.from_product([['FHG'], pp.columns])

    dec_dh = ts['district_heating_demand', 'decentralised_dh']

    # # Fetch config values from scenario.ini
    # share_hp_chp = cfg.get('decentralised_chp', 'share_hp_chp')
    # over_cap = cfg.get('decentralised_chp', 'overcapacity_factor')
    # eff_chp_heat = cfg.get('decentralised_chp', 'efficiency_chp_heat')
    # eff_chp_elec = cfg.get('decentralised_chp', 'efficiency_chp_elec')
    # eff_heat = cfg.get('decentralised_chp', 'efficiency_heat')
    # heat_capacity = dec_dh.max() * over_cap
    #
    # # decentralised CHP blocks
    # pp.loc['decentralised CHP-blocks', ('BE', 'fuel')] = 'natural_gas'
    # pp.loc['decentralised CHP-blocks', ('BE', 'capacity_elec')] = round(
    #     heat_capacity * (1 - share_hp_chp) / eff_chp_heat * eff_chp_elec)
    # pp.loc['decentralised CHP-blocks', ('BE', 'capacity_heat')] = round(
    #     heat_capacity * (1 - share_hp_chp))
    # pp.loc['decentralised CHP-blocks', ('BE', 'efficiency')] = (
    #     eff_chp_elec + eff_chp_heat)
    # pp.loc['decentralised CHP-blocks', ('BE', 'type')] = 'FIX'
    # pp.loc['decentralised CHP-blocks', ('BE', 'network')] = 'decentralised_dh'
    #
    # # decentralised heat devices
    # pp.loc['decentralised heat-blocks', ('BE', 'fuel')] = 'natural_gas'
    # pp.loc['decentralised heat-blocks', ('BE', 'capacity_heat')] = round(
    #     heat_capacity * share_hp_chp)
    # pp.loc['decentralised heat-blocks', ('BE', 'efficiency')] = eff_heat
    # pp.loc['decentralised heat-blocks', ('BE', 'type')] = 'HP'
    # pp.loc['decentralised heat-blocks', ('BE', 'network')] = 'decentralised_dh'

    return pp


def scenario_volatile_sources():
    re = pd.DataFrame()
    re.loc['capacity', 'Wind'] = 0.001
    re.loc['capacity', 'Solar'] = installed_pv_capacity()
    re.columns = pd.MultiIndex.from_product([['FHG'], re.columns])
    return re


def scenario_feedin(year):
    df = coastdat.scenario_feedin(year, 'BE')
    df.columns = df.columns.set_levels(['FHG'], level=0)
    return df


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
        [['DE'], commodity_src.columns])

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
    # First calculate the heat demand for Berlin to get the scaling factor
    # heat.create_heat_profiles(year)
    logging.debug("Scaling factor of the heat demand: {0}".format(
        cfg.get('oeq', 'oeq_balance_factor')))
    df = heat.create_heat_profiles(year, region=90517)

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
    elec_demand = calculate_elec_demand_friedrichshagen(year)
    time_series['electricity', 'demand'] = elec_demand.values * 1000
    return time_series


# **********************************************************


def get_inhabitants(polygon, name):
    """The inhabitants of 2016 are used."""
    table = 'ew'
    cfg_data = cfg.get_dict(table)

    ew_fn = os.path.join(cfg.get('paths', 'fis_broker'),
                         cfg_data['table'],
                         'shp',
                         cfg_data['table'] + '.shp')

    logging.debug("Reading {0}".format(ew_fn))

    if not os.path.isfile(ew_fn):
        ew_fn = download.download_maps(single=table)
    ew = geometries.load(fullname=ew_fn)
    ew['centroid_column'] = ew.representative_point()
    ew = ew.set_geometry('centroid_column')

    neu = geometries.spatial_join_with_buffer(ew, polygon, name=name, limit=0,
                                              )
    grp = neu.groupby(name).sum()
    grp['frac'] = grp['EW'].div(grp.sum()['EW']).multiply(100).round(1)
    return grp


def calculate_inhabitants_friedrichshagen(year, geo=None):
    if geo is None:
        fhg_fn = os.path.join(
            cfg.get('paths', 'geo_berlin'),
            cfg.get('geometry', 'friedrichshagen_block'))
        geo_fhg = geometries.load(fullname=fhg_fn, index_col='BZR_NAME')
    else:
        geo_fhg = geo

    return get_inhabitants(geo_fhg, 'brz_name').loc[geo_fhg.index[0], 'EW']


def calculate_inhabitants_districts(year, geo=None):
    if geo is None:
        berlin_district_fn = os.path.join(
            cfg.get('paths', 'geo_berlin'),
            cfg.get('geometry', 'berlin_bezirke'))
        geo_bln = geometries.load(
            fullname=berlin_district_fn, index_col='BEZIRK')
    else:
        geo_bln = geo

    return get_inhabitants(geo_bln, 'bezirk')


def calculate_elec_demand_friedrichshagen(year):
    fhg_ew = calculate_inhabitants_friedrichshagen(year)
    trp_koep_ew = calculate_inhabitants_districts(year).loc[
        '09_TREP/KOEP', 'EW']

    elec_demand_trp_koep = electricity.get_electricity_demand(
        year, district='Treptow-Koepenick')['usage']

    elec_demand_fhg = fhg_ew / trp_koep_ew * elec_demand_trp_koep
    return elec_demand_fhg


def installed_pv_capacity():
    table = 're_08_09_1pv_bzr2013'
    path = os.path.join(cfg.get('paths', 'fis_broker'), table, 'shp')
    shapefile = os.path.join(path, table + '.shp')
    pv_cap = gpd.read_file(shapefile)
    pv_cap.set_index('spatial_na', inplace=True)
    return round(pv_cap.loc['090517'].BZR_GLEIST / 1000, 3)


def solar_potential():
    bp = '/home/uwe/chiba/Promotion/reegis_geometries/'
    # full = 'solar_berlin/solar_teildach_TH_WGS84.shp'
    fhg = 'friedrichshagen/solardach_friedrichshagen.shp'
    gdf = gpd.read_file(bp + fhg)

    gdf['STRMIT_TOT'] = gdf['AREA_KOR'] * gdf['STRMIT_KOR']
    gdf_sum = gdf.sum()
    str_max = 1132
    perform_factor = gdf_sum.STRMIT_TOT / (gdf_sum.AREA_KOR * str_max)

    print('Area:', int(gdf_sum.AREA_KOR), 'm²')
    print('Power:', int(gdf_sum.AREA_KOR) / 6, 'kWp')
    print('Overall performance:', round(perform_factor, 2))


def main(year, overwrite=False):
    stopwatch()
    name = '{0}_{1}_{2}'.format('friedrichshagen', year, 'single')
    sc = Scenario(name=name, year=year, debug=False)

    path = os.path.join(cfg.get('paths', 'scenario'), 'friedrichshagen')

    logging.info("Read scenario from excel-sheet: {0}".format(stopwatch()))
    excel_fn = os.path.join(path, name + '.xls')

    if not os.path.isfile(excel_fn) or overwrite:
        create_basic_scenario(year)

    sc.load_excel(excel_fn)
    sc.check_table('time_series')

    logging.info("Add nodes to the EnergySystem: {0}".format(stopwatch()))
    sc.es = sc.initialise_energy_system()
    nodes = sc.create_nodes(region='FHG')
    sc.es.add(*nodes.values())

    # Save energySystem to '.graphml' file.
    sc.plot_nodes(filename=os.path.join(path, 'friedrichshagen'),
                  remove_nodes_with_substrings=['bus_cs'])

    logging.info("Create the concrete model: {0}".format(stopwatch()))
    sc.create_model()

    logging.info("Solve the optimisation model: {0}".format(stopwatch()))
    sc.solve()

    logging.info("Solved. Dump results: {0}".format(stopwatch()))
    results_path = os.path.join(path, 'results_{0}'.format(
        cfg.get('general', 'solver')))
    if not os.path.isdir(results_path):
        os.mkdir(results_path)
    sc.dump_es(os.path.join(results_path,
                            'friedrichshagen_{0}_single.esys'.format(str(year))
                            ))

    logging.info(
        "All done. friedrichshagen finished without errors: {0}".format(
            stopwatch()))


def create_basic_scenario(year, excel=None):
    table_collection = create_scenario(year)

    name = '{0}_{1}_{2}'.format('friedrichshagen', year, 'single')

    sce = scenario_tools.Scenario(table_collection=table_collection,
                                  name=name, year=year)
    path = os.path.join(cfg.get('paths', 'scenario'), 'friedrichshagen')
    if excel is None:
        excel = os.path.join(path, name + '.xls')
        csv_path = os.path.join(path, '{0}_csv'.format(name))
    else:
        csv_path = excel[:-4] + '_csv'
    os.makedirs(csv_path, exist_ok=True)

    sce.to_excel(excel)
    sce.to_csv(csv_path)


if __name__ == "__main__":
    # logger.define_logging(file_level=logging.INFO)
    # installed_pv_capacity()

    # print(electricity.get_electricity_demand(2014, district='berlin').sum())
    # exit(0)
    # print(calculate_elec_demand_friedrichshagen(2014).sum())
    # print(get_heat_profiles(2014).sum())
    # exit(0)
    # friedrichshagen = os.path.join(
    #     cfg.get('paths', 'geo_berlin'),
    #     cfg.get('geometry', 'friedrichshagen_block'))
    # geo_fhg = geometries.load(fullname=friedrichshagen)
    # get_inhabitants(geo_fhg, 'brz')
    #
    # berlin_bezirke = os.path.join(
    #     cfg.get('paths', 'geo_berlin'),
    #     cfg.get('geometry', 'berlin_bezirke'))
    # geo_bln = geometries.load(fullname=berlin_bezirke)
    # geo_bln = geo_bln.set_index('BEZIRK')
    # get_inhabitants(geo_bln, 'bezirk')

    logger.define_logging()
    start = datetime.now()
    # solar_potential()
    # exit(0)
    overwrite = False
    for y in [2014, 2013, 2012]:
        main(y, overwrite=overwrite)
        mesg = "Basic scenario for {0} created: {1}"
        logging.info(mesg.format(y, datetime.now() - start))
    logging.info("Done: {0}".format(datetime.now() - start))
