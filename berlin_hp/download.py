# -*- coding: utf-8 -*-
"""
Created on Tue Mar 15 16:33:38 2016

@author: uwe
"""

import requests
import os
from owslib.wfs import WebFeatureService
from reegis_tools import config as cfg
import subprocess as sub
import geopandas as gpd
import logging
import oemof.tools.logger as logger
from shutil import copyfile


def feature2gml(bbox, file, table, wfs11):
    response = wfs11.getfeature(typename='fis:' + table,
                                bbox=bbox, srsname='EPSG:25833')
    out = open(file, 'wb')
    try:
        out.write(bytes(response.read(), 'UTF-8'))
    except TypeError:
        out.write(response.read())
    out.close()


def dump_from_wfs(table, server, version='1.1.0'):

    wfs11 = WebFeatureService(url=server + table, version=version, timeout=300)

    logging.info("Download {0} from {1}".format(table, server))
    logging.info(wfs11.identification.title)
    logging.info(list(wfs11.contents))

    x_min = 369097
    y_min = 5799298
    x_max = 416866
    y_max = 5838237

    # number of tiles to split the query in parts
    number_of_tiles_x = 12
    number_of_tiles_y = 10

    # calculate steps
    steps_x = (x_max - x_min) / number_of_tiles_x
    steps_y = (y_max - y_min) / number_of_tiles_y

    path = os.path.join(cfg.get('paths', 'fis_broker'), table)

    if not os.path.isdir(path):
        os.mkdir(path)

    for x_tile in range(number_of_tiles_x):
        for y_tile in range(number_of_tiles_y):
            my_box = (x_min + (x_tile * steps_x),
                      y_max - (y_tile * steps_y),
                      x_min + ((x_tile + 1) * steps_x),
                      y_max - ((y_tile + 1) * steps_y))
            filename = "{0}_{1}_{2}.gml".format(table, x_tile, y_tile)
            fullpath = os.path.join(path, filename)
            if not os.path.isfile(fullpath):
                logging.info("Processing tile {0}-{1}".format(x_tile, y_tile))
                feature2gml(my_box, fullpath, table, wfs11)
    logging.info("Download completed.")


def convert_gml2shp(table):
    logging.info("Convert gml-files to shp-files for {0}".format(table))
    basic_call = 'ogr2ogr -f "ESRI Shapefile" {0} {1}'
    src_path = os.path.join(cfg.get('paths', 'fis_broker'), table)
    trg_path = os.path.join(src_path, 'shp')
    if not os.path.isdir(trg_path):
        os.mkdir(trg_path)
    for f in sorted(os.listdir(src_path)):
        if '.gml' in f:
            logging.debug("Convert {0} to shp".format(f))
            src_file = os.path.join(src_path, f)
            trg_file = os.path.join(trg_path, f[:-4] + '.shp')
            ogr_call = basic_call.format(trg_file, src_file)
            sub.Popen(
                ogr_call, stderr=open(os.devnull, 'w'), shell=True).wait()
    logging.info("Shp-files created.")


def merge_shapefiles(path, table):
    logging.info("Merge shp-files into {0}".format(table + '.shp'))
    mergefile = os.path.join(path, 'merge.shp')
    fileset = '{0} {1}'.format(mergefile, '{0}')
    basic_call = (
        'ogr2ogr -f "ESRI Shapefile" -t_srs EPSG:4326 -update -append'
        ' {0} -nln merge')
    basic_call = basic_call.format(fileset)
    n = 0
    for f in sorted(os.listdir(path)):
        if '.shp' in f and 'merge' not in f:
            f = os.path.join(path, f)
            if os.path.isfile(f[:-4] + '.prj'):
                logging.info("Merge {0}".format(f))
                cmd = basic_call.format(f)
                sub.Popen(cmd, stdout=open(os.devnull, 'w'), shell=True).wait()
            f = f[:-4]
            for s in ['.shx', '.shp', '.prj', '.dbf']:
                if os.path.isfile(f + s):
                    os.remove(f + s)
            n += 1
    # rename
    newfile = os.path.join(path, table)
    for s in ['.shx', '.shp', '.prj', '.dbf']:
        if os.path.isfile(mergefile[:-4] + s):
            copyfile(mergefile[:-4] + s, newfile + s)
            os.rename(mergefile[:-4] + s, newfile + '_orig' + s)
    logging.info("Merge completed.")


def remove_duplicates(shp_file, id_col):
    logging.info("Removing duplicates in {0}".format(shp_file))
    geo_table = gpd.read_file(shp_file)
    orig_crs = geo_table.crs
    geo_table = geo_table.drop_duplicates(id_col)
    geo_table = geo_table.to_crs(orig_crs)
    geo_table.to_file(shp_file)
    logging.info("Duplicates removed.")


def shapefile_from_wfs(table, server, id_col='gml_id', keep_orig=False):
    path = os.path.join(cfg.get('paths', 'fis_broker'), table, 'shp')
    shp_file = os.path.join(path, table + '.shp')
    if not os.path.isfile(shp_file):
        logging.info("Dump table {0} from {1}".format(table, server))
        dump_from_wfs(table=table, server=server)
        convert_gml2shp(table)
        merge_shapefiles(path, table)
        remove_duplicates(shp_file, id_col)
    else:
        logging.info("Table {0} exist. Download not necessary.".format(table))
    if not keep_orig:
        orig_file = os.path.join(path, table + '_orig')
        for s in ['.shx', '.shp', '.prj', '.dbf']:
                if os.path.isfile(orig_file + s):
                    os.remove(orig_file + s)
    return shp_file


def shapefile_from_fisbroker(table, senstadt_server=None):
    if senstadt_server == 'data':
        server = 'http://fbinter.stadt-berlin.de/fb/wfs/data/senstadt/'
    elif senstadt_server == 'geometry':
        server = 'http://fbinter.stadt-berlin.de/fb/wfs/geometry/senstadt/'
    else:
        server = None
    return shapefile_from_wfs(table=table, server=server)


def get_map_config():
    maps = {}
    keys = cfg.get_list('fis_broker', 'maps')
    for key in keys:
        maps[key] = cfg.get_dict(key)
    return maps


def download_maps(single=None):
    maps = get_map_config()
    filename = {}

    if single is None:
        for key in maps.keys():
            filename[key] = shapefile_from_fisbroker(**maps[key])
    else:
        filename = shapefile_from_fisbroker(**maps[single])

    return filename


def get_xml_from_server(url, xml, filename):
    headers = {'Content-Type': 'application/xml'}
    response = requests.post(url, data=xml, headers=headers)
    open(filename, 'wb').write(response.content)


def get_berlin_net_data(year, district=None):
    """ Fetch the electricity grid data of Berlin or its districts.
    See:
    https://www.stromnetz.berlin/de/file/Erlaeuterungen-
    Livedaten-Zugriff-Stromnetz-Berlin-SMeter_Engine_93664683.pdf

    The result is stored as csv-file.

    Parameters
    ----------
    year : int
        Year of the data set.
    district :
        District of Berlin. If no district is given the whole city is chosen.
        Possible values are: Pankow, Lichtenberg, Marzahn-Hellersdorf,
        Treptow-Koepenick, Neukoelln, Friedrichshain-Kreuzberg, Mitte,
        Tempelhof-Schöneberg, Steglitz-Zehlendorf, Charlottenburg-Wilmersdorf,
        Reinickendorf, Spandau

    """
    # If district is None the data of the city of Berlin is fetched.
    #
    if district is None:
        xml_district = '<district>'
    else:
        xml_district = "<district name='{0}'>".format(district)

    xml = """
    <smeterengine>
               <scale>DAY</scale>
               <city>BERLIN</city>
               {0}
      <time_period begin="{1}-12-31 23:00:00" end="{2}-01-01 01:00:00"
       time_zone='CET'/>
    </district>
    </smeterengine> """.format(xml_district, year - 1, year + 1)
    url = cfg.get('electricity', 'url')
    filename = os.path.join(
        cfg.get('paths', 'electricity'),
        cfg.get('electricity', 'file_xml').format(year=year))
    get_xml_from_server(url, xml, filename)


if __name__ == "__main__":
    logger.define_logging(file_level=logging.INFO)
    # mp = download_maps()
