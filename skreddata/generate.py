
import itertools
import numpy as np
import datetime
import shapely
from dateutil import parser
import os
import dask
import json
import hashlib

from gdar import rastertools, meta, fileformats, raster
from gtile.core import shapetools, decorators, tileset
from gtile.core.rastertools import mosaic_tiles
from gtile.sat import asf, elevation, sentinel1


def rcs_and_dem(aoi, toi, folder, uuid=None, refsys=None, shape=(512, 512)): 
    
    aoi_wkt = shapetools.misc_to_wkt(aoi)
    aoi_shp = shapely.wkt.loads(aoi_wkt)
    
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
    
    if refsys is None: 
        refsys_wkt = shapetools.utm_from_shape(aoi_shp)[0]
        refsys = meta.Meta_refsys({'type':'crs', 'wkt':refsys_wkt})
        
    if uuid is None: 
        ser = json.dumps((aoi_wkt, t_0, t_1)).encode("utf-8")
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
    ts = tileset.QuadTileSet(
        tile_shape=(2048, 2048), 
        refsys=refsys, 
        origin='centered-y', 
        sample_spacing=grid_ssp
    )
    
    # Prepare DEM: 
    dem_source = elevation.MergedDEM(tile_set=ts)
    dem_tiles = dem_source.get_tiles(aoi=aoi_wkt)
    
    # S1-pairs: 
    pairs = search_grd_pairs(aoi, t_0, t_1)
        
    # Download S1-scenes (delayed): 
    downloader = dask.delayed(asf.cached_downloader())
    pairs = [[[downloader(itm) for itm in group] for group in pair] for pair in pairs]

    # Geocode and mosaic: 
    gec_source = sentinel1.GeocodedS1Grd(tile_set=ts)    
    kws = {}
    kws['dem'] = {'files': list(dem_tiles.values())}
    
    out = []
    for n, pair in enumerate(pairs): 
        
        files = []
        
        if len(pairs) == 1: 
            _uuid = f'{uuid}'
        else: 
            _uuid = f'{uuid}_{n:02d}'
        fn = os.path.join(folder, _uuid, f'{_uuid}_crs.tif')
        os.makedirs(os.path.dirname(fn), exist_ok=True)
        tile_pair = [gec_source.get_tiles(aoi=aoi, files=files, **kws) for files in pair]
        files.append(dask.delayed(_pair_to_file)(tile_pair, fn, grid))
    
        # Prepare DEM: 
        fn = os.path.join(folder, _uuid, f'{_uuid}_dem.tif')
        files.append(dask.delayed(_dem_to_file)(dem_tiles, fn, grid))
        
        out.append(files)
    
    return out


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


def _dem_to_file(tiles, fn, grid): 
    fileformats.write_crs(mosaic_tiles(tiles, mosaicing_kws={'grid':grid}), fn)
    return fn


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

