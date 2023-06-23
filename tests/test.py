import unittest
from unittest import mock

import sys
sys.path.append('../')

from skreddata import generate
import dask
from dask import distributed


class TestRcsAndDem(unittest.TestCase):
    def test_rcs_and_dem(self):
        aoi = 'aoi_lavangsdalen.geojson'
        toi = ['2020-01-01', '2020-01-02']
        folder = 'test_output'

        # Mock the necessary dependencies for the function
        mock_shapetools = mock.MagicMock()
        mock_sentinel1 = mock.MagicMock()
        mock_asf = mock.MagicMock()
        mock_dask = mock.MagicMock()

        with mock.patch.dict('sys.modules', 
            {'shapely': mock.MagicMock(wkt=mock_shapetools), 
             'gtile.sat.sentinel1': mock_sentinel1, 
             'gtile.sat.asf': mock_asf, 
             'dask': mock_dask}):

            # Mock the return values of the mocked functions/classes as needed
            mock_shapetools.misc_to_wkt.return_value = 'mock_wkt'
            mock_sentinel1.GeocodedS1Grd.return_value = mock.MagicMock()
            mock_asf.search.return_value = [mock.MagicMock(properties={'pathNumber': 1})]
            mock_dask.delayed.return_value = mock.MagicMock()
            
            # Call the function under test
            result = generate.rcs_and_dem(aoi, toi, folder)
            
            cluster = distributed.LocalCluster()
            with distributed.Client(cluster):
                result = dask.compute(result)[0]
                
            print(result)

            # Assert the expected results or behaviors
            self.assertEqual(len(result), 2)  # Assuming two pairs of scenes

            # Additional assertions based on the specific behavior of the function

if __name__ == '__main__':
    unittest.main()