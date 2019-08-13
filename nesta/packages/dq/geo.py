import numpy as np
import pandas as pd
from ast import literal_eval


def latlon_distribution(lat, lon, lat_bins=180, lon_bins=360, **kwargs):
    '''latlon_distribution
    Generates a 2d histogram of lat lon coordinate pairs.

    Args:
        lat (:obj:`iter` of :obj:`float`): Array of latitude values.
        lon (:obj:`iter` of :obj:`float`): Array of longitude values.
        lat_bins (:obj:`int` or :obj:`array-like`): The bin specification
            for latitude. If int, the number of bins. If array, the bin
            edges.
        lon_bins (:obj:`int` or :obj:`array-like`): The bin specification
            for longitude. If int, the number of bins. If array, the bin
            edges.
        kwargs: Any other keyword arguments are passed to numpy.histogram2d
    Returns:
        df_geo (:obj:`pandas.DataFrame`): DataFrame with latitude (index), longitude (columns)
            and histogram frequency (values).
    '''
    bins = [lat_bins, lon_bins]
    hist = np.histogram2d(lat, lon, bins=bins, **kwargs)
    df_geo = pd.DataFrame(index=hist[1][:-1], columns=hist[2][:-1], data=hist[0])
    df_geo.index.name = 'lat'
    df_geo.columns.name = 'lon'
    print(df_geo)
    return df_geo
    

def _geojson_to_columns(s, format='str', lat_header='lat', lon_header='lon'):
    '''_geojson_to_columns
    Converts a single column of json strings to lat lon columns

    Args:
        s (:obj:`iter`): An iterable of json-like geo coordinates in either
            dict or string form. e.g. {'lat': 0.00, 'lon': 0.00}
        format (:obj:`str`): 

    Returns:
        df_geo (:obj:`pandas.DataFrame`):
    '''
    s = pd.Series(s)
    if format == 'str':
        df_geo = (s.fillna('{}')
                 .apply(literal_eval)
                 .pipe(pd.io.json.json_normalize)
                )
    elif format == 'dict':
        df_geo = (s.fillna(dict)
                 .pipe(pd.io.json.json_normalize)
                 )
    else:
        raise Exception('Must provide format. Either "str" or "dict".')
    
    df_geo = df_geo[[lat_header, lon_header]]

    return df_geo

