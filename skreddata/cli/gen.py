import typer
from typing import Optional
from pathlib import Path
import logging


app = typer.Typer(no_args_is_help=True, name=__name__.split('.')[-1], help='SKREDDATA sample generation')
logger = logging.getLogger(__name__)


@app.command()
def from_geojson( 
          file: Path = typer.Argument(..., help='Input GeoJSON-file. '), 
          target: Path = typer.Argument(..., help='Target folder. '),  
          t0: Optional[str] = typer.Option('t_0', help='Column name representing start times. '), 
          t1: Optional[str] = typer.Option('t_1', help='Column name representing end times. '), 
          uuid: Optional[str] = typer.Option(None, help='Column name representing UUIDs. '), 
          label: Optional[str] = typer.Option(None, help='Column name representing labels. '), 
          comment: Optional[str] = typer.Option(None, help='Column name representing comments. '), 
          epsg: Optional[int] = typer.Option(None, help='EPSG code. Taken as the UTM zone closest to the AOI if not given. '), 
          shape: Optional[int] = typer.Option(1024, help='Image shape (assumed same in x and y). '),
          print: Optional[bool] = typer.Option(True, help='Print output. ')
     ):
          
     import os
     from gdar import coordinates, meta
     from skreddata import generate, database
     import geopandas as gpd
     import dask
     from dask import distributed
     from rich.pretty import pprint
     
     if epsg is not None: 
          wkt = coordinates.wkt_from_epsg(epsg)
          projname = coordinates.projname_from_wkt(wkt)
          refsys = meta.Meta_refsys({
               'type': 'crs',
               'wkt': wkt,
               'name': projname
          })
     else: 
          refsys = None
     
     df = gpd.read_file(file)
     db = database.Database()
     os.makedirs(target, exist_ok=True)
     output = {}
     for i in range(len(df)): 
          
          row = df.iloc[i]
          
          # Check database for UUID if given: 
          if uuid is not None and db.get_by_uuid(row[uuid]):
               logger.info(f'UUID exists in database, skipping UUID={row[uuid]}')
               continue 
          
          # Generate data: 
          res = generate.main(
               aoi=row.geometry, 
               toi=[row[t0], row[t1]], 
               folder=target, 
               refsys=refsys, 
               shape=(shape, shape), 
               uuid=str(row[uuid]) if uuid is not None else None, 
               label=row[label] if label is not None else None, 
               comment=row[comment] if comment is not None else None
          )
          
          output.update(res)
     
     #cluster = distributed.LocalCluster()
     #with distributed.Client(cluster):
     #with dask.config.set(scheduler='threads'):
     
     output = dask.compute(output)[0] 
     
     if print: 
          pprint(output)

     return output


if __name__ == "__main__":
    app()
    
    