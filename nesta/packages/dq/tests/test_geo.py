import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
import pytest

from nesta.packages.dq.geo import latlon_distribution

@pytest.fixture
def data():
    lat = [0.5, 1.5, 5.5]
    lon = [1.0, 2.5, 2.5]
    return lat, lon

class TestGeoDataQuality():

    def test_latlon_distribution(self, data):
        lat_bins = [0, 1, 2, 3, 4, 5, 6]
        lon_bins = [0, 1, 2, 3]
        lat, lon = data
        result = latlon_distribution(lat, lon, 
                lat_bins=lat_bins,
                lon_bins=lon_bins)

        expected_result = pd.DataFrame(
                index=lat_bins[:-1],
                columns=lon_bins[:-1],
                data=np.array([
                        [0., 1., 0.],
			[0., 0., 1.],
			[0., 0., 0.],
			[0., 0., 0.],
			[0., 0., 0.],
			[0., 0., 1.]]
                            ))
        expected_result.index.name = 'lat'
        expected_result.columns.name = 'lon'
        assert_frame_equal(result, expected_result)
