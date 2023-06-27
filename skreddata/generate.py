
import itertools
import numpy as np
import datetime
import shapely
from dateutil import parser
import os
import dask
import json
import hashlib
import argparse
import geopandas as gpd
import dask
from dask import distributed
import logging

from gdar import rastertools, meta, fileformats, raster, coordinates
from gtile.core import shapetools, decorators, tileset
from gtile.core.rastertools import mosaic_tiles
from gtile.sat import asf, elevation, sentinel1

from skreddata import database


logger = logging.getLogger(__name__)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
#  WRITE INPUT GEOJSON
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #


@dask.delayed
def add_to_database(item):
    db = database.Database()
    logger.info(f'Inserting item in database:\ndatabase: {db}\nitem: {item}')
    db.insert(item)
    return item


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
#  WRITE INPUT GEOJSON
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #


@dask.delayed
def write_input_geojson(uuid, folder, aoi, t_0, t_1): 
    
    if not isinstance(t_0, datetime.datetime):
        t_0 = parser.parse(t_0)
    if not isinstance(t_1, datetime.datetime):
        t_1 = parser.parse(t_1)
        
    poly = shapely.wkt.loads(shapetools.misc_to_wkt(aoi))
    
    df = gpd.GeoDataFrame(
        data={'uuid':[uuid], 't_0':[t_0], 't_1':[t_1]}, 
        geometry=[poly], 
        crs='epsg:4326'
        )
    
    fn = os.path.join(folder, uuid, f'{uuid}_input.geojson')
    df.to_file(fn, driver='GeoJSON')
    
    return fn


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
#  WRITE PRODUCT JSON
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #


@dask.delayed
def write_pair_json(uuid, folder, pair): 

    fn = os.path.join(folder, uuid, f'{uuid}_products.json')
    os.makedirs(os.path.dirname(fn), exist_ok=True)
    with open(fn, 'w') as fp:
        json.dump(pair, fp)
    
    return fn


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
#  WRITE RADAR CORSS SECTION AND DEM
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #


def write_rcs_and_dem(uuid, folder, pair, grid, tile_set):
    
    output = []
    
    # Prepare DEM: 
    dem_source = elevation.MergedDEM(tile_set=tile_set)
    dem_tiles = dem_source.get_tiles(aoi=grid)
    
    # Download S1-scenes (delayed): 
    downloader = dask.delayed(asf.cached_downloader())
    pair = [[downloader(itm) for itm in group] for group in pair]
    
    # S1-mosaic: 
    gec_source = sentinel1.GeocodedS1Grd(tile_set=tile_set)    
    kws = {}
    kws['dem'] = {'files': list(dem_tiles.values())}
    
    fn = os.path.join(folder, uuid, f'{uuid}_crs.tif')
    os.makedirs(os.path.dirname(fn), exist_ok=True)
    tile_pair = [gec_source.get_tiles(aoi=grid, files=files, **kws) for files in pair]
    output.append(_pair_to_file(tile_pair, fn, grid))

    # DEM-mosaic: 
    fn = os.path.join(folder, uuid, f'{uuid}_dem.tif')
    output.append(_dem_to_file(dem_tiles, fn, grid))
    
    return output


@dask.delayed
def _pair_to_file(tile_pair, fn, grid): 
    mosaic_pair = [mosaic_tiles(tiles, mosaicing_kws={'grid':grid}) for tiles in tile_pair]
    col = {}
    for i, dr in enumerate(mosaic_pair): 
        for j, n in enumerate(dr.dtype.names):
            key = f'{n}_{i}' 
            col[key] = rastertools.select(dr, j)
    combined = rastertools.merge(raster.Collection(col))
    fileformats.write_crs(combined, fn)
    return fn


@dask.delayed
def _dem_to_file(tiles, fn, grid): 
    fileformats.write_crs(mosaic_tiles(tiles, mosaicing_kws={'grid':grid}), fn)
    return fn


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
#  SEARCH S1-PAIRS
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #


@decorators.input_as_copy
def search_grd_pairs(aoi, t_0, t_1, space_buffer=7_500, exact_times=False): 

    if not isinstance(t_0, datetime.datetime):
        t_0 = parser.parse(t_0)
    if not isinstance(t_1, datetime.datetime):
        t_1 = parser.parse(t_1)

    poly = shapely.wkt.loads(shapetools.misc_to_wkt(aoi))
    poly = shapetools.buffer_in_closest_utm(poly, space_buffer)
    poly = shapely.geometry.polygon.orient(poly)
    wkt = shapely.wkt.dumps(poly, rounding_precision=12)

    query = {
        "processingLevel": "GRD_HD",
        "intersectsWith": wkt,
        "start": t_0 - datetime.timedelta(days=12),
        "end": t_1 + datetime.timedelta(days=12)
        }

    res = asf.search(query)

    prods = sorted([r.properties for r in res], key=lambda x: x['pathNumber'])
    pairs = []
    for _, _prods in itertools.groupby(prods, lambda x: x['pathNumber']):
        _prods = sorted(_prods, key=lambda x: x['startTime'])
        adj_prods = rastertools.group_adjacent(list(_prods), 
            duration_getter=lambda x: [np.datetime64(x['startTime']), np.datetime64(x['stopTime'])])
        for a, b in zip(adj_prods[:-1], adj_prods[1:]): 
            _t0 = np.min([parser.parse(p['startTime'].rstrip('Z')) for p in a])
            _t1 = np.min([parser.parse(p['startTime'].rstrip('Z')) for p in b])
            if exact_times:
                if np.abs(_t0 - t_0) < datetime.timedelta(minutes=10) and \
                    np.abs(_t1 - t_1) < datetime.timedelta(minutes=10): 
                    pairs.append([a,b])
            else: 
                if _t1 < t_0: 
                    pass
                elif _t0 > t_1:
                    pass
                else:  
                    pairs.append([a,b])

    return pairs


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #
#  MAIN 
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - #


@decorators.input_as_copy
def main(aoi, toi, folder, refsys=None, shape=None, uuid=None, comment=None, label=None): 
    
    # AOI: 
    aoi_wkt = shapetools.misc_to_wkt(aoi)
    aoi_shp = shapely.wkt.loads(aoi_wkt)
    
    # Time interval: 
    if np.isscalar(toi): 
        t_0 = toi
        t_1 = toi
    elif len(toi) == 1: 
        t_0 = toi[0]
        t_1 = toi[0]
    elif len(toi) == 2: 
        t_0 = toi[0]
        t_1 = toi[1]
    else: 
        raise Exception('<toi> must be scalar or list of length 1 or 2')
    
    # Target refsys: 
    if refsys is None: 
        refsys_wkt = shapetools.utm_from_shape(aoi_shp)[0]
        refsys = meta.Meta_refsys({'type':'crs', 'wkt':refsys_wkt})
    
    # Image shape: 
    if shape is None: 
        shape = (1024, 1024)
    
    # Make UUID: 
    if uuid is None: 
        ser = json.dumps((aoi_wkt, str(t_0), str(t_1), refsys[...], shape)).encode("utf-8")
        uuid = hashlib.sha256(ser).hexdigest()[:10]

    # Target grid: 
    centroid_geod = np.array(aoi_shp.centroid.coords[0])[::-1]  # GDAR-convention (lat, lon)
    centroid_coord = refsys.get_functionality('coord_from_geod')(centroid_geod)
    grid_shape = np.array(shape)
    grid_ssp = np.array([10, 10])
    grid = meta.Meta_grid({
        'origin': centroid_coord - grid_ssp*grid_shape//2,
        'shape': grid_shape,
        'offset': np.array([0, 0]),
        'samplespacing': grid_ssp,
        'refsys': refsys
    })
    
    # Tile-set: 
    tile_set = tileset.QuadTileSet(
        tile_shape=(2048, 2048), 
        refsys=refsys, 
        origin='centered-y', 
        sample_spacing=grid_ssp
    )
    
    # Database: 
    db = database.Database()
    
    # Search pairs: 
    pairs = search_grd_pairs(aoi_wkt, t_0, t_1)
    
    # Process each pair: 
    output = {}
    for n, pair in enumerate(pairs): 
        
        _uuid = str(uuid) + f'_{n:02d}' if len(pairs) > 1 else str(uuid)
        
        # Check database for UUID: 
        if db.get_by_uuid(_uuid) is not None:
            logger.info(f'UUID exists in database, skipping UUID={_uuid}')
            continue  
        
        output_files = []
        
        # Write products JSON: 
        output_files += [write_pair_json(_uuid, folder, pair)]
        
        # Write input GEOJSON
        output_files += [write_input_geojson(_uuid, folder, aoi, t_0, t_1)]
        
        # Write RCS and DEM: 
        output_files += write_rcs_and_dem(_uuid, folder, pair, grid, tile_set)
        
        # Make database item: 
        itm = database.Item(
            uuid=_uuid, 
            geometry=aoi_wkt, 
            t_0=t_0, 
            t_1=t_1, 
            label=label, 
            comment=comment
        )
        
        # Add to outputs: 
        output[_uuid] = {
            'files': output_files, 
            'database_item': add_to_database(itm)
        }
        
    return output


if __name__ == '__main__':

    p = argparse.ArgumentParser(description='Generate samples')
    p.add_argument('-aoi','--area-of-interest', type=str, help='Area of interest as geoJSON file or WKT-string. ', required=True)
    p.add_argument('-toi','--time-of-interest', nargs='+', type=str, help='Time of interest as one or two strings. ', required=True)
    p.add_argument('-t','--target', type=str, help='Target folder. ', required=True)
    p.add_argument('--shape', nargs='+', type=int, help='Shape as two integers. ', required=False)
    p.add_argument('--epsg', type=int, help='Target EPSG.', required=False)
    p.add_argument('--uuid', type=str, help='Universal unique identifier. If not set, it will be derived from the input arguments.', required=False)
    args = p.parse_args()
    
    if args.epsg is not None: 
        wkt = coordinates.wkt_from_epsg(args.epsg)
        projname = coordinates.projname_from_wkt(wkt)
        refsys = meta.Meta_refsys({
            'type': 'crs',
            'wkt': wkt,
            'name': projname
        })
    else: 
        refsys = None
        
    outputs = main(args.area_of_interest, args.time_of_interest, args.target, refsys=refsys, shape=args.shape, uuid=args.uuid)
 
    """
    with dask.config.set(scheduler='synchronous'):
        outputs = dask.compute(outputs)[0]
    """
    
    cluster = distributed.LocalCluster()
    with distributed.Client(cluster):
        outputs = dask.compute(outputs)[0] 
    
    print(outputs)
    
    
    
    