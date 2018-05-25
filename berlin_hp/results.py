import berlin_hp
import os
import logging
import reegis_tools.gui as gui
import reegis_tools.geometries
import pprint as pp
from oemof.tools import logger
import reegis_tools.config as cfg
from datetime import datetime
import oemof.outputlib as outputlib
from matplotlib import pyplot as plt
import oemof_visio as oev
import pandas as pd
import matplotlib.patheffects as path_effects
import matplotlib.patches as patches
import math
from matplotlib.colors import LinearSegmentedColormap


def shape_legend(node, reverse=False, **kwargs):
    handels = kwargs['handles']
    labels = kwargs['labels']
    axes = kwargs['ax']
    parameter = {}

    new_labels = []
    for label in labels:
        label = label.replace('(', '')
        label = label.replace('), flow)', '')
        label = label.replace(node, '')
        label = label.replace(',', '')
        label = label.replace(' ', '')
        new_labels.append(label)
    labels = new_labels

    parameter['bbox_to_anchor'] = kwargs.get('bbox_to_anchor', (1, 0.5))
    parameter['loc'] = kwargs.get('loc', 'center left')
    parameter['ncol'] = kwargs.get('ncol', 1)
    plotshare = kwargs.get('plotshare', 0.9)

    if reverse:
        handels = handels.reverse()
        labels = labels.reverse()

    box = axes.get_position()
    axes.set_position([box.x0, box.y0, box.width * plotshare, box.height])

    parameter['handles'] = handels
    parameter['labels'] = labels
    axes.legend(**parameter)
    return axes


def stopwatch():
    if not hasattr(stopwatch, 'start'):
        stopwatch.start = datetime.now()
    return str(datetime.now() - stopwatch.start)[:-7]


def plot_power_lines(data, key, cmap_lines=None, cmap_bg=None,
                     vmax=None, label_max=None):
    lines = reegis_tools.geometries.Geometry()
    lines.load(cfg.get('paths', 'geometry'),
               cfg.get('geometry', 'de21_power_lines'))
    polygons = reegis_tools.geometries.Geometry()
    polygons.load(cfg.get('paths', 'geometry'),
                  cfg.get('geometry', 'de21_polygons_simple'))

    lines.gdf = lines.gdf.merge(data, left_index=True, right_index=True)

    lines.gdf['centroid'] = lines.gdf.centroid

    if cmap_bg is None:
        cmap_bg = LinearSegmentedColormap.from_list(
            'mycmap', [(0, '#aed8b4'), (1, '#bddce5')])

    if cmap_lines is None:
        cmap_lines = LinearSegmentedColormap.from_list(
            'mycmap', [
                (0, '#aaaaaa'),
                (0.0001, 'green'),
                (0.5, 'yellow'),
                (1, 'red')])

    for i, p in polygons.gdf.iterrows():
        if 'see' in p['name'].lower():
            polygons.gdf.loc[i, 'color'] = 1
        else:
            polygons.gdf.loc[i, 'color'] = 0

    lines.gdf['reverse'] = lines.gdf[key] < 0
    lines.gdf.loc[lines.gdf['reverse'], key] = (
        lines.gdf.loc[lines.gdf['reverse'], key] * -1)

    if vmax is None:
        vmax = lines.gdf[key].max()

    if label_max is None:
        label_max = vmax * 0.5

    ax = polygons.gdf.plot(edgecolor='#9aa1a9', cmap=cmap_bg,
                           column='color')
    ax = lines.gdf.plot(cmap=cmap_lines, legend=True, ax=ax, column=key,
                        vmin=0, vmax=vmax)
    for i, v in lines.gdf.iterrows():
        x1 = v['geometry'].coords[0][0]
        y1 = v['geometry'].coords[0][1]
        x2 = v['geometry'].coords[1][0]
        y2 = v['geometry'].coords[1][1]

        value = v[key] / vmax
        mc = cmap_lines(value)

        orient = math.atan(abs(x1-x2)/abs(y1-y2))

        if (y1 > y2) & (x1 > x2):
            orient *= -1

        if v['reverse']:
            orient += math.pi

        if round(v[key]) == 0:
            pass
            polygon = patches.RegularPolygon(
                (v['centroid'].x, v['centroid'].y),
                4,
                0.15,
                orientation=orient,
                color=(0, 0, 0, 0),
                zorder=10)
        else:
            polygon = patches.RegularPolygon(
                (v['centroid'].x, v['centroid'].y),
                3,
                0.15,
                orientation=orient,
                color=mc,
                zorder=10)
        ax.add_patch(polygon)

        if v[key] > label_max:
            ax.text(
                v['centroid'].x, v['centroid'].y,
                '{0} GWh'.format(round(v[key])),
                color='#000000',
                fontsize=9.5,
                zorder=15,
                path_effects=[
                    path_effects.withStroke(linewidth=3, foreground="w")])

    polygons.gdf.apply(lambda x: ax.annotate(
        s=x.name, xy=x.geometry.centroid.coords[0], ha='center'), axis=1)

    plt.show()


def compare_transmission(year):
    # DE21
    sc_de21_results = load_de_results(year)

    # DE21 + BE
    sc_debe_results = load_de_be_results(year)

    lines = [x for x in sc_de21_results.keys() if 'power_line' in x[0]]

    # Calculate results
    transmission = pd.DataFrame()
    for line in lines:
        line_name = line[0].replace('power_line_', '').replace('_', '-')
        regions = line_name.split('-')
        name1 = 'power_line_{0}_{1}'.format(regions[0], regions[1])
        name2 = 'power_line_{0}_{1}'.format(regions[1], regions[0])
        # sc_de21.results
        de21a = outputlib.views.node(sc_de21_results, name1)['sequences'].sum()
        de21b = outputlib.views.node(sc_de21_results, name2)['sequences'].sum()
        debea = outputlib.views.node(sc_debe_results, name1)['sequences'].sum()
        debeb = outputlib.views.node(sc_debe_results, name2)['sequences'].sum()
        transmission.loc[line_name, 'DE'] = de21a.iloc[0] - de21b.iloc[0]
        transmission.loc[line_name, 'BE'] = debea.iloc[0] - debeb.iloc[0]

    # PLOTS
    transmission.plot(kind='bar')

    transmission['diff'] = transmission['BE'] - transmission['DE']

    key = gui.get_choice(list(transmission.columns),
                         "Plot transmission lines", "Choose data column.")
    transmission[key] = transmission[key] / 1000
    vmax = max([abs(transmission[key].max()), abs(transmission[key].min())])
    plot_power_lines(transmission, key, vmax=vmax/2)

    return transmission


def plot_regions(data, column):
    polygons = reegis_tools.geometries.Geometry()
    polygons.load(cfg.get('paths', 'geometry'),
                  cfg.get('geometry', 'de21_polygons_simple'))
    polygons.gdf = polygons.gdf.merge(data, left_index=True, right_index=True)

    print(polygons.gdf)

    cmap = LinearSegmentedColormap.from_list(
            'mycmap', [
                # (0, '#aaaaaa'),
                (0.000000000, 'green'),
                (0.5, 'yellow'),
                (1, 'red')])

    ax = polygons.gdf.plot(edgecolor='#9aa1a9', cmap=cmap, vmin=0,
                           column=column, legend=True)

    polygons.gdf.apply(lambda x: ax.annotate(
        s=x[column], xy=x.geometry.centroid.coords[0], ha='center'), axis=1)

    plt.show()


def reshape_results(results, data, region, node=None):
    if node is None:
        node = 'bus_elec_{0}'.format(region)
    tmp = outputlib.views.node(results, node)['sequences']

    # aggregate powerline columns to import and export
    exp = [x for x in tmp.columns if 'power_line' in x[0][1]]
    imp = [x for x in tmp.columns if 'power_line' in x[0][0]]
    tmp[((node, 'export'), 'flow')] = tmp[exp].sum(axis=1)
    tmp[(('import', node), 'flow')] = tmp[imp].sum(axis=1)
    tmp.drop(exp + imp, axis=1, inplace=True)

    in_c = [x for x in tmp.columns if x[0][1] == node]
    out_c = [x for x in tmp.columns if x[0][0] == node]

    data[region] = pd.concat({'in': tmp[in_c], 'out': tmp[out_c]}, axis=1)
    dc = {}
    for c in data[region]['in'].columns:
        dc[c] = c[0][0].replace('_{0}'.format(region), '')
    for c in data[region]['out'].columns:
        dc[c] = c[0][1].replace('_{0}'.format(region), '')
    data[region] = data[region].rename(columns=dc, level=1)
    return data


def get_multiregion_results(year):
    de_results = load_de_results(year)

    regions = [x[0].replace('shortage_bus_elec_', '')
               for x in de_results.keys()
               if 'shortage_bus_elec' in x[0]]

    data = {}
    for region in sorted(regions):
        data = reshape_results(de_results, data, region)
    return pd.concat(data, axis=1)


def show_region_values_gui(year):
    data = get_multiregion_results(year)
    data = data.agg(['sum', 'min', 'max', 'mean'])
    data = data.reorder_levels([1, 2, 0], axis=1)

    data.sort_index(1, inplace=True)

    key1 = gui.get_choice(data.columns.get_level_values(0).unique(),
                          "Plot transmission lines", "Choose data column.")
    key2 = gui.get_choice(data[key1].columns.get_level_values(0).unique(),
                          "Plot transmission lines", "Choose data column.")
    key3 = gui.get_choice(data.index.unique(),
                          "Plot transmission lines", "Choose data column.")

    plot_data = pd.DataFrame(data.loc[key3, (key1, key2)], columns=[key3])

    plot_data = plot_data.div(1000).round().astype(int)

    plot_regions(plot_data, key3)


def load_param_results():
    path = os.path.join(
        cfg.get('paths', 'scenario'), 'berlin_basic', str(2014))
    file = gui.select_filename(work_dir=path,
                               title='Berlin results!',
                               extension='esys')
    sc = berlin_hp.Scenario()
    sc.restore_es(os.path.join(path, file))
    return outputlib.processing.convert_keys_to_strings(sc.es.results['param'])
    # return sc.es.results['param']


def load_de_results(year):
    path = os.path.join(cfg.get('paths', 'scenario'), 'basic', '{year}')
    file = 'de21_basic_{year}.esys'
    return load_results(path.format(year=year), file.format(year=year))


def load_de_be_results(year):
    path = os.path.join(cfg.get('paths', 'scenario'), 'berlin_basic', '{year}')
    file = 'berlin_hp_de21_11.esys'
    return load_results(path.format(year=year), file)


def load_berlin_results(year):
    path = os.path.join(cfg.get('paths', 'scenario'), 'berlin_basic', '{year}')
    file = 'berlin_hp.esys'
    return load_results(path.format(year=year), file)


def load_results(path, file):
    sc = berlin_hp.Scenario()
    sc.restore_es(os.path.join(path, file))
    return outputlib.processing.convert_keys_to_strings(sc.results)


def check_excess_shortage(year):
    results = load_berlin_results(year)
    ex_nodes = [x for x in results.keys() if 'excess' in x[1]]
    sh_nodes = [x for x in results.keys() if 'shortage' in x[0]]
    for node in ex_nodes:
        f = outputlib.views.node(results, node[1])
        s = int(round(f['sequences'].sum()))
        if s > 0:
            print(node[1], ':', s)

    for node in sh_nodes:
        f = outputlib.views.node(results, node[0])
        s = int(round(f['sequences'].sum()))
        if s > 0:
            print(node[0], ':', s)


def find_input_flow(out_flow, nodes):
    return [x for x in list(nodes.keys()) if x[1] == out_flow[0][0]]


def get_full_load_hours(year):
    bus_label = 'bus_elec_BE'
    results = load_berlin_results(year)
    params = load_param_results()
    my_node = outputlib.views.node(results, bus_label)['sequences']
    sums = my_node.sum()
    max_vals = my_node.max()

    flh = pd.DataFrame()

    input_flows = [c for c in my_node.columns if bus_label == c[0][1]]
    for col in input_flows:
        inflow = [x for x in list(params.keys()) if x[1] == col[0][0]]
        node_label = col[0][0]
        if 'nominal_value' in params[(node_label, bus_label)]['scalars']:
            flh.loc[node_label, 'nominal_value'] = (
                params[(col[0][0], bus_label)]['scalars']['nominal_value'])
        else:
            if len(inflow) > 0:
                inflow = inflow[0]
                if 'nominal_value' in params[inflow]['scalars']:
                    try:
                        cf = (
                            params[(inflow[1], 'None')]['scalars']
                            ['conversion_factor_full_condensation_{0}'.format(
                                bus_label)])
                    except KeyError:
                        cf = params[(inflow[1], 'None')]['scalars'][
                            'conversion_factors_{0}'.format(bus_label)]

                    flh.loc[node_label, 'nominal_value'] = (
                            params[inflow]['scalars']['nominal_value'] * cf)
                else:
                    flh.loc[node_label, 'nominal_value'] = 0
            else:
                flh.loc[node_label, 'nominal_value'] = 0
        if len(inflow) > 0:
            if isinstance(inflow, list):
                inflow = inflow[0]

            flh.loc[node_label, 'average_efficiency'] = (
                sums[col] / results[inflow]['sequences']['flow'].sum())

        flh.loc[node_label, 'energy'] = sums[col]
        flh.loc[node_label, 'max'] = max_vals[col]
        if flh.loc[node_label, 'nominal_value'] > 0:
            flh.loc[node_label, 'full_load_hours'] = (
                    sums[col] / flh.loc[node_label, 'nominal_value'])
            flh.loc[node_label, 'average_power'] = sums[col] / 8760
    flh['check'] = flh['max'] > flh['nominal_value']
    flh['full_load_hours'].plot(kind='bar')

    print(flh)
    plt.show()


def plot_bus(node_label, year):
    results = load_berlin_results(year)

    fig = plt.figure(figsize=(10, 5))

    my_node = outputlib.views.node(results, node_label)['sequences']

    plot_slice = oev.plot.slice_df(my_node)

    # pprint.pprint(get_cdict(my_node))

    pp.pprint(my_node.columns)
    pp.pprint(get_orderlist(my_node))
    # exit(0)
    my_plot = oev.plot.io_plot(node_label, plot_slice,
                               cdict=get_cdict(my_node),
                               inorder=get_orderlist(my_node),
                               ax=fig.add_subplot(1, 1, 1),
                               smooth=True)
    ax = shape_legend(node_label, **my_plot)
    ax = oev.plot.set_datetime_ticks(ax, plot_slice.index, tick_distance=48,
                                     date_format='%d-%m-%H', offset=12)

    # ax.set_ylabel('Power in MW')
    ax.set_xlabel(str(year))
    # ax.set_title("Electricity bus")
    plt.show()


def get_orderlist(my_node, inflow=True):
    my_order = ['source_solar', 'source_wind', 'chp', 'hp', 'pp', 'shortage']
    cols = list(my_node.columns)
    if inflow is True:
        f = 0
    else:
        f = 1
    order = []

    for element in my_order:
        tmp = [x for x in cols if element in x[0][f].lower()]
        for t in tmp:
            cols.remove(t)
        order.extend(tmp)
    return order


def get_cdict(my_node):
    my_colors = cfg.get_dict_list('plot_colors', string=True)
    color_dict = {}
    for col in my_node.columns:
        n = 0
        color_keys = list(my_colors.keys())
        try:
            while color_keys[n] not in col[0][0].lower():
                n += 1
            if len(my_colors[color_keys[n]]) > 1:
                color = '#{0}'.format(my_colors[color_keys[n]].pop(0))
            else:
                color = '#{0}'.format(my_colors[color_keys[n]][0])
            color_dict[col] = color
        except IndexError:
            n = 0
            try:
                while color_keys[n] not in col[0][1].lower():
                    n += 1
                if len(my_colors[color_keys[n]]) > 1:
                    color = '#{0}'.format(my_colors[color_keys[n]].pop(0))
                else:
                    color = '#{0}'.format(my_colors[color_keys[n]][0])
                color_dict[col] = color
            except IndexError:
                color_dict[col] = '#ff00f0'

    return color_dict


if __name__ == "__main__":
    logger.define_logging()
    stopwatch()
    # show_region_values_gui(2014)
    compare_transmission(2014)
    exit(0)
    # get_full_load_hours(2014)
    # check_excess_shortage(2014)
    # plot_bus('bus_distr_heat_vattenfall_mv')
    # plot_bus('bus_elec_BE')
    compare_transmission(2014)
    exit(0)
    'bus_elec_BE'
