# -*- coding: utf-8 -*-

"""Aggregating feed-in time series for the model regions.

SPDX-FileCopyrightText: 2016-2019 Uwe Krien <krien@uni-bremen.de>

SPDX-License-Identifier: MIT
"""
__copyright__ = "Uwe Krien <krien@uni-bremen.de>"
__license__ = "MIT"

# Python libraries
import logging
import datetime
import os
import warnings

# External libraries
import pandas as pd
import geopandas as gpd

# oemof packages
import oemof.tools.logger as logger

# internal modules
import Open_eQuarterPy.building_evaluation as be
from reegis import config as cfg
import reegis.geometries
import berlin_hp.download as download


def process_alkis_buildings(shapefile_out, table, remove_non_heated=True):
    """

    Parameters
    ----------
    shapefile_out
    table
    remove_non_heated

    Returns
    -------

    """
    path = os.path.join(cfg.get('paths', 'fis_broker'), table, 'shp')
    shapefile_in = os.path.join(path, table + '.shp')

    # Download shp_file if it does not exist
    if not os.path.isfile(shapefile_in):
        shapefile_in = download.download_maps(single='alkis')

    geo_table = gpd.read_file(shapefile_in)

    # Removing parts of the Alkis table:
    # Bauart_sch == 0 : Data sets with Bauart_sch > 0 are building parts
    # LageZurErd != 1200 : Remove underground buildings
    logging.info("Length of data set before removing parts: {0}".format(
        len(geo_table)))
    geo_table = geo_table[geo_table["BAT"].isnull()]
    geo_table = geo_table[geo_table["OFL"] != 1200]

    # Remove all data sets that are marked es non-heated in the alkis heat
    # factor table if remove_non_heated is set to True.
    if remove_non_heated is True:
        filename_heat_factor = os.path.join(
            cfg.get('paths', 'data_berlin'),
            cfg.get('oeq', 'alkis_heat_factor_table'))
        heat_factor = pd.read_csv(filename_heat_factor, index_col=[0])
        non_heated = list(heat_factor.loc[heat_factor.heat_factor == 0].index)
        geo_table = geo_table[~geo_table['BEZGFK'].isin(non_heated)]

    logging.info("Length of data set after removing parts: {0}".format(
        len(geo_table)))

    # Calculate the perimeter and area of the polygons and add it as columns
    logging.info("Calculate perimeter and area of each polygon...")
    geo_table = geo_table.to_crs({'init': 'epsg:3035'})
    geo_table['area'] = geo_table['geometry'].area
    geo_table['perimeter'] = geo_table['geometry'].length
    geo_table = geo_table.to_crs({'init': 'epsg:4326'})

    # Dump table as new shape_file
    logging.info("Dump new table to shp-file.")
    geo_table.to_file(shapefile_out)
    return shapefile_out


def merge_maps():
    gdf = {}

    table = 's_wfs_alkis_gebaeudeflaechen'
    path = os.path.join(cfg.get('paths', 'fis_broker'), table, 'shp')
    shapefile_alkis = os.path.join(path, table + '_prepared' + '.shp')
    if not os.path.isfile(shapefile_alkis):
        shapefile_alkis = process_alkis_buildings(shapefile_alkis, table)

    tables = download.get_map_config()

    # Filename and path for output files
    filename_poly_layer = os.path.join(
        cfg.get('paths', 'fis_broker'),
        cfg.get('fis_broker', 'merged_blocks_polygon'))

    # Columns to use
    cols = {
        'block': ['gml_id', 'PLR', 'STAT', 'STR_FLGES'],
        'nutz': ['STSTRNAME', 'TYPKLAR', 'WOZ_NAME'],
        'ew': ['EW_HA']}

    logging.info("Read tables to be joined: {0}.".format(tuple(cols.keys())))
    for t in ['block', 'nutz', 'ew']:
        tables[t]['path'] = os.path.join(cfg.get('paths', 'fis_broker'),
                                         tables[t]['table'], 'shp',
                                         tables[t]['table'] + '.shp')
        logging.debug("Reading {0}".format(tables[t]['path']))

        if not os.path.isfile(tables[t]['path']):
            tables[t]['path'] = download.download_maps(single=t)
        gdf[t] = gpd.read_file(tables[t]['path'])[cols[t] + ['geometry']]

    logging.info("Spatial join of all tables...")

    gdf['block'].rename(columns={'gml_id': 'SCHL5'}, inplace=True)
    # Convert geometry to representative points to simplify the join
    gdf['block']['geometry'] = gdf['block'].representative_point()
    gdf['block'] = gpd.sjoin(gdf['block'], gdf['nutz'], how='inner',
                             op='within')
    del gdf['block']['index_right']
    gdf['block'] = gpd.sjoin(gdf['block'], gdf['ew'], how='left',
                             op='within')
    del gdf['block']['index_right']
    del gdf['block']['geometry']

    # Merge with polygon layer to dump polygons instead of points.
    gdf['block'] = pd.DataFrame(gdf['block'])
    polygons = gpd.read_file(tables['block']['path'])[['gml_id', 'geometry']]
    polygons.rename(columns={'gml_id': 'SCHL5'}, inplace=True)
    polygons = polygons.merge(gdf['block'], on='SCHL5')
    polygons = polygons.set_geometry('geometry')

    logging.info("Dump polygon layer to {0}...".format(filename_poly_layer))
    polygons.to_file(filename_poly_layer)

    logging.info("Read alkis table...")
    alkis = gpd.read_file(shapefile_alkis)

    logging.info("Join alkis buildings with block data...")
    alkis = alkis[['AnzahlDerO', 'area', 'perimeter', 'Gebaeudefu', 'gml_id',
                   'geometry']]
    block_j = polygons[
        ['SCHL5', 'PLR', 'STAT', 'TYPKLAR', 'EW_HA', 'geometry']]
    alkis['geometry'] = alkis.representative_point()

    alkis = gpd.sjoin(alkis, block_j, how='left', op='within')
    del alkis['index_right']

    # Join the alkis data with the map of the heating system fraction
    logging.info("Join alkis buildings with heiz data...")

    geoheiz_obj = reegis.geometries.Geometry()
    geoheiz_obj.load_csv(cfg.get('paths', 'data_berlin'),
                         cfg.get('fis_broker', 'heating_systems_csv'))
    geoheiz_obj.df = geoheiz_obj.df.loc[geoheiz_obj.df['geometry'].notnull()]
    geoheiz_obj.df = geoheiz_obj.df.rename(columns={'block': 'heiz_block'})
    geoheiz_obj.create_geo_df()
    geoheiz = geoheiz_obj.gdf

    geoheiz = geoheiz[geoheiz.geometry.is_valid]

    alkis = gpd.sjoin(alkis, geoheiz, how='left', op='within')
    del alkis['index_right']

    logging.info("Add block data for non-matching points using buffers.")
    remain = len(alkis.loc[alkis['PLR'].isnull()])
    logging.info("This will take some time. Number of points: {0}".format(
        remain))

    # I think it is possible to make this faster and more elegant but I do not
    # not have the time to think about it. As it has to be done only once it
    # is not really time-sensitive.
    for row in alkis.loc[alkis['PLR'].isnull()].iterrows():
        idx = int(row[0])
        point = row[1].geometry
        intersec = False
        n = 0
        block_id = 0
        while not intersec and n < 500:
            bi = block_j.loc[block_j.intersects(point.buffer(n / 100000))]
            if len(bi) > 0:
                intersec = True
                bi = bi.iloc[0]
                block_id = bi['SCHL5']
                del bi['geometry']
                alkis.loc[idx, bi.index] = bi
            n += 1
        remain -= 1

        if intersec:
            logging.info(
                "Block found for {0}: {1}, Buffer: {2}. Remains: {3}".format(
                    alkis.loc[idx, 'gml_id'][-12:], block_id[-16:], n, remain))
        else:
            warnings.warn(
                "{0} does not intersect with any region. Please check".format(
                    row[1]))

    logging.info(
        "Check: Number of buildings without PLR attribute: {0}".format(
            len(alkis.loc[alkis['PLR'].isnull()])))

    # Merge with polygon layer to dump polygons instead of points.
    logging.info("Merge new alkis layer with alkis polygon layer.")
    alkis = pd.DataFrame(alkis)
    del alkis['geometry']
    alkis_poly = gpd.read_file(shapefile_alkis)[['gml_id', 'geometry']]
    alkis_poly = alkis_poly.merge(alkis, on='gml_id')
    alkis_poly = alkis_poly.set_geometry('geometry')
    logging.info("Dump new alkis layer with additional block data.")

    filename_shp = os.path.join(cfg.get('paths', 'fis_broker'),
                                cfg.get('fis_broker', 'alkis_joined_shp'))
    alkis_poly.to_file(filename_shp)

    return filename_shp


def convert_shp2table():
    filename = {
        'hdf': os.path.join(cfg.get('paths', 'fis_broker'),
                            cfg.get('fis_broker', 'alkis_joined_hdf')),
        'csv': os.path.join(cfg.get('paths', 'fis_broker'),
                            cfg.get('fis_broker', 'alkis_joined_csv')),
        'geo': os.path.join(cfg.get('paths', 'fis_broker'),
                            cfg.get('fis_broker', 'alkis_geometry_csv')),
        'shp': os.path.join(cfg.get('paths', 'fis_broker'),
                            cfg.get('fis_broker', 'alkis_joined_shp'))}

    if not os.path.isfile(filename['shp']):
        filename['shp'] = merge_maps()
    alkis = gpd.read_file(filename['shp'])

    alkis.to_csv(filename['csv'])
    data = pd.read_csv(filename['csv'], index_col=[0])
    data['gml_id'] = data['gml_id'].str.replace(
        's_wfs_alkis_gebaeudeflaechen.', '')
    data['SCHL5'] = data['SCHL5'].str.replace('s_ISU5_2015_UA.', '')
    data.set_index('gml_id', drop=True, inplace=True)
    data['geometry'].to_csv(filename['geo'])
    del data['geometry']

    data.to_csv(filename['csv'])
    data.to_hdf(filename['hdf'], 'alkis')
    return filename


def get_alkis_with_additional_data():
    filename_alkis = os.path.join(cfg.get('paths', 'fis_broker'),
                                  cfg.get('fis_broker', 'alkis_joined_hdf'))

    if not os.path.isfile(filename_alkis):
        filename_alkis = convert_shp2table()['hdf']

    return pd.read_hdf(filename_alkis, 'alkis')


def oeq(bzr=None):
    start = datetime.datetime.now()

    if bzr is None:
        reg = 'berlin'
    else:
        reg = bzr

    filename_oeq_results = os.path.join(
        cfg.get('paths', 'oeq'), cfg.get('oeq', 'results').format(region=reg))

    data = get_alkis_with_additional_data()

    if bzr is not None:
        data['BZR'] = data.PLR.div(100).astype(int)
        data = data.loc[data.BZR == bzr]

    data['alkis_id'] = data.index

    sn_data = pd.read_csv(os.path.join(cfg.get('paths', 'data_berlin'),
                                       'data_by_blocktype.csv'), ';')

    data = data.merge(sn_data, on='TYPKLAR', how='left')
    data.set_index('alkis_id', drop=True, inplace=True)

    rename_alkis = {
        'AnzahlDerO': 'floors',
        'Gebaeudefu': 'building_function',
        'SCHL5': 'block',
        'PLR': 'lor',
        'STAT': 'statistical_region',
        'TYPKLAR': 'block_type_name',
        'EW_HA': 'population_density',
        'PRZ_FERN': 'frac_district_heating',
        'PRZ_GAS': 'frac_gas',
        'PRZ_KOHLE': 'frac_coal',
        'PRZ_NASTRO': 'frac_elec',
        'PRZ_OEL': 'frac_oil',
        'type': 'block_type',
        }

    data = data.rename(columns=rename_alkis)

    data['all_systems'] = 1

    # *** Year of construction ***
    # Replace ranges with one year.
    year_of_construction = {
        '1950-1979': 1964,
        'ab 1945': 1970,
        '1920-1939': 1929,
        '1920-1949': 1934,
        '1870-1918': 1894,
        'bis 1945': 1920,
        '1870-1945': 1908,
        '1890-1930': 1910,
        '1960-1989': 1975,
        'ab 1990': 2003,
        '1870-1899': 1885,
        'bis 1869': 1860,
        '1900-1918': 1909,
        '1975-1992': 1984,
        '1962-1974': 1968,
        '1946-1961': 1954,
        '1919-1932': 1926,
        '1933-1945': 1939,
        'None': None,
        'NaN': None,
        'nan': None
        }
    # data['age_scan'].replace(year_of_construction, inplace=True)
    data['building_age'] = data['building_age'].replace(year_of_construction)

    # Fill all remaining nan values with a default value of 1960
    data['year_of_construction'] = data['building_age'].fillna(1960)

    # Calculate the heat demand of the building
    logging.debug("Data types of the DataFrame: {0}".format(data.dtypes))
    logging.info("Calculate the heat demand of the buildings...")

    parameter = {'fraction_living_area': 0.8}

    result = be.evaluate_building(data, **parameter)

    result['my_total'] = result.total_loss_pres

    str_cols = ['block', 'block_type_name', 'share_non_tilted_roof']
    result.loc[:, str_cols] = result[str_cols].applymap(str)

    # Store results to hdf5 file
    logging.info("Store results to {0}".format(filename_oeq_results))
    store = pd.HDFStore(filename_oeq_results)
    store['oeq'] = result
    store['year_of_construction'] = pd.Series(year_of_construction)
    store['parameter'] = pd.Series(parameter)
    store.close()
    logging.warning('No date saved! Please add date to hdf5-file.')
    logging.info("Elapsed time: {0}".format(datetime.datetime.now() - start))


if __name__ == "__main__":
    logger.define_logging(file_level=logging.INFO)
    # oeq()
    oeq(bzr=90517)
