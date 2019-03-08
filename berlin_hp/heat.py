# -*- coding: utf-8 -*-

"""Aggregating feed-in time series for the model regions.

Copyright (c) 2016-2018 Uwe Krien <uwe.krien@rl-institut.de>

SPDX-License-Identifier: GPL-3.0-or-later
"""
__copyright__ = "Uwe Krien <uwe.krien@rl-institut.de>"
__license__ = "GPLv3"

# Python libraries
import logging
import os
import datetime

# External libraries
import pandas as pd
from workalendar.europe import Germany

# oemof packages
import oemof.tools.logger as logger
import demandlib.bdew as bdew

# internal modules
from reegis import config as cfg
import reegis.energy_balance
import reegis.coastdat
import reegis.heat_demand
import reegis.energy_balance
import reegis.geometries
import reegis.bmwi
import berlin_hp.my_open_e_quarter


def load_heat_data(filename=None, method='oeq', fill_frac_column=True,
                   region='berlin'):
    if method == 'oeq':
        if filename is None:
            filename = cfg.get('oeq', 'results').format(region=region)
        fn = os.path.join(cfg.get('paths', 'oeq'), filename)
        if not os.path.isfile(fn):
            berlin_hp.my_open_e_quarter.oeq()
        data = pd.read_hdf(fn, method)

    elif method == 'wt':
        if filename is None:
            filename = 'waermetool_berlin.hdf'
        data = pd.HDFStore(os.path.join(cfg.get('paths', 'wt'), filename))
    else:
        logging.warning('No data file found.')
        data = None

    if fill_frac_column:
        data = fill_fraction_column(data)

    return data
        

def fill_fraction_column(data):

    # Get the columns with the fraction of the fuel
    frac_cols = [x for x in data.columns if 'frac_' in x]

    # Divide columns with 100 to get the fraction instead of percentage
    data[frac_cols] = data[frac_cols].div(100)

    # Sum up columns to check if the sum is 1.
    data['check'] = data[frac_cols].sum(axis=1)

    # Level columns if sum is between 0.95 and 1.
    data.loc[data['check'] > 0.95, frac_cols] = (
        data.loc[data['check'] > 0.95, frac_cols].multiply(
            (1 / data.loc[data['check'] > 0.95, 'check']), axis=0))

    # Update the check column.
    data['check'] = data[frac_cols].sum(axis=1)

    # Get the average values for each fraction
    length = len(data.loc[round(data['check']) == 1, frac_cols])
    s = data.loc[data['check'] > 0.95, frac_cols].sum() / length

    # add average values if fraction is missing
    data.loc[data['check'] < 0.1, frac_cols] = (
            data.loc[data['check'] < 0.1, frac_cols] + s)

    # Update the check column.
    data['check'] = data[frac_cols].sum(axis=1)
    length = float(len(data['check']))
    check_sum = data['check'].sum()
    if check_sum > length + 1 or check_sum < length - 1:
        logging.warning("The fraction columns do not equalise 1.")

    return data


def demand_by(data, demand_column, heating_systems=None,
              building_types=None, remove_string='', percentage=False):
        """
        Adds a new table to the hdf-file where the demand is divided by
        building types and/or heating systems.

        Parameters
        ----------
        data : pandas.DataFrame
        demand_column : string
            Name of the column with the overall demand
        heating_systems : list of strings
            List of column names. The columns should contain the
             fraction of each heating system. The sum of all these
             columns should be 1 (or 100) for each row. If the sum is
             100 the percentage parameter (prz) must be set to True
        building_types : dictionary or None
            All building types with their condition. If None no distinction
            between building types.
        remove_string : string
            Part of the column names of the heating systems that
             should be removed to name the results. If the column is
             name "fraction_of_district_heating" the string could be
             "fraction_of_" to use just "district_heating" for the name
             of the result column.
        percentage : boolean
            True if the fraction of the heating system columns sums up
            to hundred instead of one.
        Returns
        -------

        """
        if percentage:
            prz = 100
        else:
            prz = 1

        # if building_types is None all building will be fetched
        if building_types is None:
            building_types = {'all': '{0} == {0}'.format(demand_column)}

        # Create an empty DataFrame with the same index as the data DataFrame
        demand_by_building = pd.DataFrame(
            index=data.index)

        # Loop over the building types and use the condition to filter.
        # The value from demand column is written into the new condition
        # column if the condition  is true
        for btype, condition in building_types.items():
            demand_by_building.loc[data.query(
                condition).index, btype] = (
                data[demand_column][data.query(condition).index])

        # Create an empty DataFrame with the same index as the data DataFrame
        demand = pd.DataFrame(index=data.index)

        # Get the columns from the buildings condition
        loop_list = demand_by_building.keys()

        # If heating system is None do not filter.
        if heating_systems is None:
            heating_systems = []
            loop_list = []
            logging.error(
                "Demand_by without heating systems is not implemented")

        blist = list()
        for btype in loop_list:
            # Create renaming dictionary
            rename_dict = {
                col: 'demand_' + btype + '_' + col.replace(
                    remove_string, '')
                for col in heating_systems}

            # Multiply each buildings column with the heating system fraction
            demand = demand.combine_first(
                data[heating_systems].multiply(
                    demand_by_building[btype], axis='index').div(prz))

            # Rename the columns
            demand = demand.rename(columns=rename_dict)

            # Create a list with name of building columns with
            blist.extend(list((btype, )) * len(heating_systems))
        hlist = heating_systems * len(set(blist))
        multindex = pd.MultiIndex.from_tuples(list(zip(blist, hlist)),
                                              names=['first', 'second'])

        return pd.DataFrame(data=demand.as_matrix(), columns=multindex,
                            index=data.index)


def dissolve(data, level, columns=None):
    """

    Parameters
    ----------
    data : pandas.DataFrame
    level : integer or string
        1 = district, 2 = prognoseraum, 3 = bezirksregion, 4 = planungsraum
    columns : string or list
        Name of the column in the given table.

    Returns
    -------
    pandas.Series
        Dissolved Column.

    """
    if columns is None:
        columns = list(data.columns)

    data['lor'] = data['lor'].astype(str).str.zfill(8)

    error_level = level
    if isinstance(level, str):
        trans_dict = {'bezirk': 1,
                      'prognoseraum': 2,
                      'bezirksregion': 3,
                      'planungsraum': 4}
        level = trans_dict.get(level)

    if level is None:
        logging.error("Wrong level: {0}".format(error_level))

    level *= 2
    results = data.groupby(data['lor'].str[:level])[columns].sum()

    # self.annual_demand = results
    return results


def get_end_energy_data(year, state='BE'):
    """End energy demand from energy balance (reegis)
    """
    filename_heat_reference = os.path.join(
        cfg.get('paths', 'oeq'), 'heat_reference_TJ_{0}_{1}.csv'.format(
            year, state))

    if not os.path.isfile(filename_heat_reference):
        eb = reegis.energy_balance.get_states_balance(
            year=year, grouped=True)
        end_energy_table = eb.loc[state]
        end_energy_table.to_csv(filename_heat_reference)
    else:
        end_energy_table = pd.read_csv(filename_heat_reference, index_col=[0])

    return end_energy_table


def get_district_heating_areas():
    """TODO: Die Erstellung der Karte ist derzeit 'nur' beschrieben. Die
    Erstellung fehlt noch.

    Read map with areas of district heating systems in Berlin
    This map is the results of the intersection of the block map with the
    district heat systems map.
    """

    dh_filename = os.path.join(
        cfg.get('paths', 'data_berlin'),
        cfg.get('district_heating', 'map_district_heating_areas'))
    distr_heat_areas = pd.read_csv(dh_filename, index_col=[0])

    # Replace alphanumeric code from block id
    distr_heat_areas['gml_id'] = distr_heat_areas['gml_id'].str.replace(
        's_ISU5_2015_UA.', '')

    return distr_heat_areas


def create_standardised_heat_load_profile(shlp, year):
    """

    Parameters
    ----------
    shlp : dict
    year : int

    Returns
    -------
    pandas.DataFrame

    """
    avg_temp_berlin = (reegis.coastdat.federal_state_average_weather(
        year, 'temp_air')['BE'])

    # Calculate the average temperature in degree Celsius
    temperature = avg_temp_berlin - 272.15

    # Fetch the holidays of Germany from the workalendar package
    cal = Germany()
    holidays = dict(cal.holidays(year))

    profile_type = pd.DataFrame()
    for shlp_type in shlp.keys():
        shlp_name = str(shlp_type)
        profile_type[shlp_name] = bdew.HeatBuilding(
            temperature.index, holidays=holidays, temperature=temperature,
            shlp_type=shlp_type, wind_class=0,
            building_class=shlp[shlp_type]['build_class'],
            annual_heat_demand=1000,
            name=shlp_name, ww_incl=True).get_bdew_profile()
    return profile_type


def create_heat_profiles(year, region='berlin'):
    """Create heat_profiles for the basic scenario as time series in MW.

    - district heating time series for the different district heating systems
    - decentralised heating demand time series for different fuels

    Parameters
    ----------
    year : int
        The year of the basic scenario.
    region : str or int
        Region or LOR to load the heat data from.

    Returns
    -------
    pandas.DataFrame

    """
    logging.info("Creating heat profiles...")

    # allocation of district heating systems (map) to groups (model)
    district_heating_groups = cfg.get_dict('district_heating_systems')

    # A file with a heat factor for each building type of the alkis
    # classification. Buildings like garages etc get the heat-factor 0. It is
    # possible to define building factors between 0 and 1.
    filename_heat_factor = os.path.join(
        cfg.get('paths', 'data_berlin'),
        cfg.get('oeq', 'alkis_heat_factor_table'))
    heat_factor = pd.read_csv(filename_heat_factor, index_col=[0])
    del heat_factor['gebaeude_1']

    # heat demand for each building from open_e_quarter
    data = load_heat_data()

    # Every building has a block id from the block the building is located.
    # Every block that touches a district heating area has the STIFT (number)
    # of this district heating system. By merging this information every
    # building gets the STIFT (number) of the district heating area.
    # areas of district heating systems in Berlin
    distr_heat_areas = get_district_heating_areas()
    data = data.merge(distr_heat_areas[['gml_id', 'STIFT']],
                          left_on='block', right_on='gml_id', how='left')

    # Merge the heat-factor for each building type to the alkis types
    data = data.merge(heat_factor, left_on='building_function',
                      right_index=True)

    data = data[['block', 'lor', 'frac_elec', 'frac_district_heating',
                 'frac_gas', 'frac_oil', 'frac_coal', 'HLAC', 'HLAP', 'AHDC',
                 'AHDP', 'my_total', 'check', 'gml_id', 'STIFT',
                 'heat_factor', 'ghd', 'mfh']]

    # Multiply the heat demand of the buildings with the heat factor
    data['total'] = data['my_total'] * data['heat_factor']
    data['lor'] = data.lor.apply(str)

    # if region != 'berlin':
    #     berlin_total = data.total.sum()
    #     data['lor'] = data.lor.apply(str)
    #     data = data.loc[data.lor.str.startswith(str(region))]
    #     region_factor = data.total.sum() / berlin_total
    # else:
    #     region_factor = 1
 
    # Level the overall heat demand with the heat demand from the energy
    # balance. Get energy balance first.
    end_energy_table = reegis.heat_demand.heat_demand(year).loc['BE']

    # bmwi_table = reegis.bmwi.read_bmwi_sheet_7()
    tab_a = reegis.bmwi.read_bmwi_sheet_7('a')
    tab_b = reegis.bmwi.read_bmwi_sheet_7('b')

    # calculate the fraction of process energy and building heat (with dhw)
    heat_process = {}
    p = 'sonstige Prozesswärme'
    r = 'Raumwärme'
    w = 'Warmwasser'

    s = 'private Haushalte'
    heat_process['domestic'] = (tab_b.loc[(s, p, p), year] / (
            tab_b.loc[(s, p, p), 2014] +
            tab_b.loc[(s, r, r), 2014] +
            tab_b.loc[(s, w, w), 2014]))

    s = 'Gewerbe, Handel, Dienstleistungen '
    heat_process['retail'] = (tab_b.loc[(s, p, p), year] / (
            tab_b.loc[(s, p, p), 2014] +
            tab_b.loc[(s, r, r), 2014] +
            tab_b.loc[(s, w, w), 2014]))
    s = 'Industrie'
    heat_process['industrial'] = (tab_a.loc[(s, p, p), year] / (
        tab_a.loc[(s, p, p), 2014] +
        tab_a.loc[(s, r, r), 2014] +
        tab_a.loc[(s, w, w), 2014]))

    # multiply the energy balance with the building/process fraction
    profile_type = pd.DataFrame(columns=end_energy_table.columns)
    profile_type.loc['process'] = 0
    profile_type.loc['building'] = 0
    for key in end_energy_table.index:
        profile_type.loc['process'] += (
                end_energy_table.loc[key] * heat_process[key] / 3.6 * 1000000)
        profile_type.loc['building'] += (
                end_energy_table.loc[key] * (1 - heat_process[key]) /
                3.6 * 1000000)

    # remove values below 1,5% and add it to the other values proportionally
    for pt in profile_type.index:
        s = profile_type.loc[pt].sum()
        r = 0
        for col in profile_type.columns:
            if profile_type.loc[pt, col] / s < 0.015:
                r += profile_type.loc[pt, col]
                profile_type.loc[pt, col] = 0
        s = profile_type.loc[pt].sum()
        profile_type.loc[pt] = (
                profile_type.loc[pt] + profile_type.loc[pt].div(s).multiply(r))

    # Create a dictionary for each demand profile group
    shlp = {'ghd': {'build_class': 0},
            'mfh': {'build_class': 1}}
    fuels = []

    # Create a table with absolute heat demand for each fuel and each sector
    # Industrial building heat will be treated as retail
    frac_cols = [x for x in data.columns if 'frac_' in x]
    two_level_columns = pd.MultiIndex(levels=[[], []], codes=[[], []])
    abs_data = pd.DataFrame(index=data.index, columns=two_level_columns)
    for col in frac_cols:
        for t in ['ghd', 'mfh']:
            c = col.replace('frac_', '').replace('_', ' ').replace(
                'gas', 'natural gas')
            abs_data[c, t] = data[col].multiply(data['total'] *
                                                data[t], axis=0)

    # Calculate a reduction factor for each fuel type. As there is no data
    # of electricity used for heating purpose an overall factor is used for
    # electricity.
    factor = {'elec': (profile_type.loc['building'].sum() /
                       abs_data.sum().sum())}
    for fuel in abs_data.columns.get_level_values(0).unique():
        if fuel not in factor and fuel in profile_type.columns:
            factor[fuel] = (profile_type.loc['building', fuel] /
                            abs_data[fuel].sum().sum())
        elif fuel not in factor and fuel not in profile_type.columns:
            factor[fuel] = 0

        abs_data[fuel] *= factor[fuel]
        if abs_data[fuel].sum().sum() > 0:
            fuels.append(fuel)

    # Create normalised heat load profiles (shlp) for each sector
    norm_heat_profiles = create_standardised_heat_load_profile(shlp, year)
    norm_heat_profiles['proc'] = 1000 / len(norm_heat_profiles)

    heat_profiles = pd.DataFrame(index=norm_heat_profiles.index,
                                 columns=two_level_columns)

    # Create a summable column for each demand group for district heating
    for fuel in fuels:
        if fuel in profile_type.columns:
            abs_data[fuel, 'proc'] = (
                profile_type.loc['process', fuel] / len(abs_data))

    if region != 'berlin':
        abs_data = abs_data.loc[data.lor.str.startswith(str(region))]

    for fuel in fuels:
        for sector in abs_data[fuel].columns:
            heat_profiles[fuel, sector] = norm_heat_profiles[sector].multiply(
                abs_data[fuel, sector].sum())

    # ********* Multiplication with the region_factor !!!
    # heat_profiles *= region_factor
    # print('Region factor:', region_factor)

    # Group district heating by district heating systems (STIFT = id)
    district_by_stift = data.loc[abs_data.index].groupby('STIFT').sum()

    # Create translation Series with STIFT (numeric) and KLASSENNAM (text)
    stift2name = distr_heat_areas.groupby(
        ['STIFT', 'KLASSENNAM']).size().reset_index(
            level='KLASSENNAM')['KLASSENNAM']
    stift2name[0] = 'unknown'  # add description 'unknown' to STIFT 0

    # Group district heating by own definition (ini) of district heating
    # systems.

    district_groups = pd.DataFrame(
        pd.concat([district_by_stift, stift2name], axis=1)).set_index(
            'KLASSENNAM').groupby(by=district_heating_groups).sum()

    # Calculate the fraction of each distric heating group.
    frac_district_groups = district_groups.div(district_groups.sum())

    # Create standardised heat load profile for each group
    for nr in frac_district_groups.index:
        for sector in heat_profiles['district heating'].columns:
            heat_profiles[nr, sector] = (
                heat_profiles['district heating', sector] *
                frac_district_groups.loc[nr, 'frac_district_heating'])

    heat_profiles = heat_profiles.groupby(level=0, axis=1).sum()
    del heat_profiles['district heating']

    for c in heat_profiles.columns:
        if heat_profiles[c].sum() == 0:
            del heat_profiles[c]

    return heat_profiles.div(1000000)


if __name__ == "__main__":
    logger.define_logging()
    start = datetime.datetime.now()
    # my_data = load_heat_data()
    #
    # bt_dict1 = {
    #     'efh': 'floors < 2',
    #     'mfh': 'floors > 1',
    # }
    #
    # heating_systems1 = [s for s in my_data.columns if "frac_" in s]
    # remove_string1 = 'frac_'
    # print(demand_by(my_data, 'total_loss_pres', heating_systems1, bt_dict1,
    #                 remove_string1))
    # print(dissolve(my_data, 'bezirk', ['my_total']))
    bln = create_heat_profiles(2014)
    print(bln.sum())
    print(bln.sum().sum())
    # print(create_heat_profiles(2014, use_factor=False).sum())
    # print(cfg.get('oeq', 'oeq_balance_factor'))
    df = create_heat_profiles(2014, region=90517)
    df.to_csv('/home/uwe/tmp_heat90517.csv')
    # df = pd.read_csv('/home/uwe/tmp_heat90517.csv')
    print(df.sum())
    print(df.sum().sum())
