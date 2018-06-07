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
from reegis_tools import config as cfg
import reegis_tools.energy_balance
import reegis_tools.coastdat
import reegis_tools.energy_balance
import reegis_tools.geometries
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
    """End energy demand from energy balance (reegis_tools)
    """
    filename_heat_reference = os.path.join(
        cfg.get('paths', 'oeq'), 'heat_reference_TJ_{0}.csv'.format(year))

    if not os.path.isfile(filename_heat_reference):
        eb = reegis_tools.energy_balance.get_states_balance(
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
    distr_heat_areas = pd.read_csv(dh_filename)

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
    avg_temp_berlin = (reegis_tools.coastdat.federal_state_average_weather(
        year, 'temp_air')['BE'])

    # Calculate the average temperature in degree Celsius
    temperature = avg_temp_berlin - 272.15

    # Fetch the holidays of Germany from the workalendar package
    cal = Germany()
    holidays = dict(cal.holidays(year))

    fuel_list = shlp[list(shlp.keys())[0]]['demand'].index

    profile_fuel = pd.DataFrame()
    for fuel in fuel_list:
        fuel_name = fuel.replace('frac_', '')
        profile_type = pd.DataFrame()
        for shlp_type in shlp.keys():
            shlp_name = str(shlp_type)
            profile_type[fuel_name + '_' + shlp_name] = bdew.HeatBuilding(
                temperature.index, holidays=holidays, temperature=temperature,
                shlp_type=shlp_type, wind_class=0,
                building_class=shlp[shlp_type]['build_class'],
                annual_heat_demand=shlp[shlp_type]['demand'][fuel],
                name=fuel_name + shlp_name, ww_incl=True).get_bdew_profile()

        # for district heating the systems the profile will not be summed up
        # but kept as different profiles ('district_heating_' + shlp_name).
        if fuel_name == 'district_heating':
            for n in profile_type.columns:
                profile_fuel[n] = profile_type[n]
        else:
            profile_fuel[fuel_name] = profile_type.sum(axis=1)
    return profile_fuel


def create_heat_profiles(year, region='berlin'):
    """Create heat_profiles for the basic scenario as time series in MW.

    - district heating time series for the different district heating systems
    - decentralised heating demand time series for different fuels

    Parameters
    ----------
    year : int
        The year of the basic scenario.
    region : str or int
        Region to load the heat data from.

    Returns
    -------
    pandas.DataFrame

    """
    logging.info("Starting...")

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
    data_oeq = load_heat_data(region=region)

    # areas of district heating systems in Berlin
    distr_heat_areas = get_district_heating_areas()

    # Every building has a block id from the block the building is located.
    # Every block that touches a district heating area has the STIFT (number)
    # of this district heating system. By merging this information every
    # building gets the STIFT (number) of the district heating area.
    data = data_oeq.merge(distr_heat_areas[['gml_id', 'STIFT']],
                          left_on='block', right_on='gml_id', how='left')

    # Merge the heat-factor for each building type to the alkis types
    data = data.merge(heat_factor, left_on='building_function',
                      right_index=True)

    data['total'] = data['my_total']
    # Multiply the heat demand of the buildings with the heat factor
    data['total'] *= data['heat_factor']

    # Level the overall heat demand with the heat demand from the energy
    # balance
    end_energy_table = get_end_energy_data(year)

    factor = (end_energy_table.loc['total', 'district heating'] /
              (data['total'].sum() / 1000 / 1000))

    if region == 'berlin':
        cfg.tmp_set('oeq', 'oeq_balance_factor', str(factor))
    else:
        factor = cfg.get('oeq', 'oeq_balance_factor')

    data['total'] = data['total'] * factor

    # Todo: Prozesswärme

    # Create a dictionary for each demand profile group
    shlp = {'ghd': {'build_class': 0},
            'mfh': {'build_class': 1}}

    # Add the annual demand to the profile dictionary.
    frac_cols = [x for x in data.columns if 'frac_' in x]
    for t in shlp.keys():
        # Multiply fraction columns with total heat demand to get the total
        # demand for each fuel type
        shlp[t]['demand'] = data[frac_cols].multiply(data['total'] * data[t],
                                                     axis=0).sum()

    # Create the standardised heat load profiles (shlp) for each group
    heat_profiles = create_standardised_heat_load_profile(shlp, year)

    # Create a summable column for each demand group for district heating
    cols = []
    for shlp_type in shlp.keys():
        name = 'district_' + shlp_type
        data[name] = (data['frac_district_heating'] *
                      data[shlp_type] * data['total'])
        cols.append(name)

    # Group district heating by district heating systems (STIFT = id)
    district_by_stift = data.groupby('STIFT').sum()[cols]

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
        heat_profiles[nr] = (
            (heat_profiles['district_heating_mfh'] *
             frac_district_groups.loc[nr, 'district_mfh']) +
            (heat_profiles['district_heating_ghd'] *
             frac_district_groups.loc[nr, 'district_ghd']))
    del heat_profiles['district_heating_ghd']
    del heat_profiles['district_heating_mfh']

    # Returns MW

    return heat_profiles.div(1000)


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
    print(create_heat_profiles(2014).sum())
    print(cfg.get('oeq', 'oeq_balance_factor'))
    print(create_heat_profiles(2014, region=90517).sum())
