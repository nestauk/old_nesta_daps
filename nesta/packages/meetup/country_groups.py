import logging
import os
import requests
import time
import pandas as pd
import geopy.distance                                  
import shapefile            
import numpy as np
from . import meetup_utils


def generate_coords(x0, y0, x1, y1, n):
    '''Generate :math:`\mathcal{O}(\\frac{n}{2}^2)` coordinates in the bounding box
    :math:`(x0, y0), (x1, y1)`, such that overlapping circles of equal
    radii (situated at each coordinate) entirely cover the area of
    the bounding box. The longitude and latitude are treated as
    euclidean variables, although the radius (calculated from the
    smallest side of the bounding box divided by :math:`n`) is calculated
    correctly. In order for the circles to fully cover the region,
    an unjustified factor of 10% is included in the radius. Feel free
    to do the maths and work out a better strategy for covering a
    geographical area with circles.

    The circles (centred on each X) are staggered as so (single vertical 
    lines or four underscores correspond to a circle radius):

    ____X____ ____X____

    \|

    X________X________X

    \|

    ____X____ ____X____

    This configuration corresponds to :math:`n=4`.

    Args:
        float x0, y0, x1, y1: Bounding box coordinates (lat/lon)
        n (int): The fraction by which to calculate the Meetup
                 API radius parameter, with respect to the
                 smallest side of the country's shape bbox.
                 This will generate :math:`\mathcal{O}(\\frac{n}{2}^2)`
                 separate Meetup API radius searches. The total
                 number of searches scales with the ratio
                 of the bbox sides.

    Returns:
        float, :obj:`list` of :obj:`tuple`: The radius and coordinates for the Meetup API request
    '''
    fudge = 1.1

    # Work out the number of coordinates required
    dx = np.fabs(x0-x1)
    dy = np.fabs(y0-y1)
    r = fudge*min(dx, dy)/n  # Compensate for non-Euclidean geometry
    nx = int(np.ceil(dx/r))
    ny = int(np.ceil(dy/r))

    # Convert the radius to miles (unit required for Meetup API)
    radius = geopy.distance.distance((y0, x0), (y0+r, x0+r)).miles
    coords = []  #  The output
    # Loop through y until the end is found
    y = 0
    while ny >= y:
        # x starts with an offset every other iteration
        x = 0
        if y % 2 == 0:
            x += 1                    
        # Loop through x until the end is found
        while nx >= x:
            coords.append((x0 + x*r, y0 + y*r))
            x += 2
        y += 1
    return radius, coords


def get_coordinate_data(n):
    '''Generate the radius and coordinate data 
    (see :code:`generate_coords`) for
    each shape (country) in the shapefile pointed to by
    the environmental variable WORLD_BORDERS.

    Args:
        n (int): The fraction by which to calculate the Meetup
                 API radius parameter, with respect to the
                 smallest side of the country's shape bbox.
                 This will generate :math:`\mathcal{O}(\\frac{n}{2}^2)`
                 separate Meetup API radius searches. The total
                 number of searches scales with the ratio
                 of the bbox sides.

    Returns:
        :obj:`pd.DataFrame`: containing coordinate and radius
                             for each country.
    '''
    
    sf = shapefile.Reader(os.environ["WORLD_BORDERS"], encodingErrors='ignore')
    output = []
    for shape_info in sf.shapeRecords():
        # Zip together the field names and record values
        data = {field_info[0]: value 
                for field_info, value 
                in zip(sf.fields[1:], shape_info.record)}
        # Get the radius and coordinate data for this country
        radius, coords = generate_coords(n=n, *shape_info.shape.bbox)
        data["radius"] = radius
        data["coords"] = coords
        output.append(data)
        
    # Tidy up
    # TODO: Put in a pull request to do a better job of this in shapefile
    sf.shp.close()
    sf.shx.close()
    sf.dbf.close()
    return pd.DataFrame(output)


def assert_iso2_key(df, iso2):    
    condition = df.ISO2 == iso2
    if condition.sum() != 1:
        raise KeyError("%s retrieved %s entries from %s" 
                       % (iso2, condition.sum(), os.environ["WORLD_BORDERS"]))
    return condition


class MeetupCountryGroups:
    '''Extract all meetup groups for a given country.

    Attributes:
        country_code (str): ISO2 code
        params (:obj:'dict'): GET request parameters, including lat/lon.
        groups (:obj:`list` of :obj:`str`): List of meetup groups in this country, assigned
            assigned after calling `get_groups`.
    '''

    def __init__(self, country_code, coords, radius, category, n=10):
        '''Set meetup search parameters.

        Args:
            country_code (str): A country ISO2
            coords (:obj:`list` of :obj:`tuple`): (lat, lon) coordinates
                from which to perform the Meetup API calls, with radius
                :obj:`radius`.
            radius (float): Meetup API radius parameter.
            category (int): A Meetup category
            n (int): The fraction by which to calculate the Meetup
                 API radius parameter, with respect to the
                 smallest side of the country's shape bbox.
                 This will generate :math:`\mathcal{O}(\\frac{n}{2}^2)`
                 separate Meetup API radius searches. The total
                 number of searches scales with the ratio
                 of the bbox sides.
            
        '''

        self.ids = set()
        self.country_code = country_code
        self.coords = coords

        # Set up the static Meetup API parameters
        self.params = dict(country=country_code,
                           page=200, 
                           category=str(category),
                           radius=radius)
        logging.info("Generated parameters %s" % self.params)
        self.groups = []


    def get_groups(self, lon, lat, offset=0, max_pages=None):
        '''Recursively get all groups for the given parameters.
        It is assumed that you will run with the default arguments,
        since they are set automatically in the recursing procedure.
        '''
        
        # Check if we're in too deep
        if max_pages is not None and offset >= max_pages:
            return
        # Set the offset parameter and make the request
        self.params["offset"] = offset
        self.params['lat'] = lat
        self.params['lon'] = lon
        self.params['key'] = meetup_utils.get_api_key()
        
        # Work out whether the task has failed or not
        failed = False
        try:
            r = requests.get("https://api.meetup.com/2/groups",
                             params=self.params)
            r.raise_for_status()
        except Exception as err:
            failed = True
            if type(err) not in (requests.exceptions.HTTPError, 
                                 requests.exceptions.ChunkedEncodingError,
                                 ConnectionResetError):
                if "reset by peer" in str(err):
                    logging.info("Reset by peer error")
                else:
                    raise err
        if not failed:
            failed = len(r.text) == 0

        # If no response is found
        if failed:
            time.sleep(10)
            logging.info("Got a bad response, so retrying page %s" % offset)
            return self.get_groups(lon, lat, offset=offset, max_pages=max_pages)

        # Extract results in the country of interest (bonus countries
        # can enter the fold because of the radius parameter)
        data = r.json()
        for row in data["results"]:
            if row['id'] in self.ids:
                continue
            if row["country"].lower() != self.country_code.lower():
                continue
            if 'category' not in row:
                continue
            if str(row['category']['id']) != self.params['category']:
                continue
            self.ids.add(row['id'])
            self.groups.append(row)
        # Check if a "next" url is specified
        next_url = data["meta"]["next"]
        if next_url != "":
            # If so, increment offset and get the groups
            self.get_groups(lon, lat, offset=offset+1, max_pages=max_pages)


    def get_groups_recursive(self):
        '''Call :code:`get_groups` for each lat,lon coordinate'''
        for i, (lon, lat) in enumerate(self.coords):
            logging.info("--> %s / %s ==> %s" % 
                         (i+1, len(self.coords), len(self.groups)))
            self.get_groups(lon, lat)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    iso2 = "MX"
    category = 34

    # Get all country data and generate the lat/lon and radius
    # parameter for this country
    df = get_coordinate_data(n=10)
    condition = assert_iso2_key(df, iso2)
    
    # Get parameters for this country
    name = df.loc[condition, "NAME"].values[0]
    coords = df.loc[condition, "coords"].values[0]
    radius = df.loc[condition, "radius"].values[0]
    
    # Get groups for the first 10 coords in this country
    mcg = MeetupCountryGroups(country_code=iso2, coords=coords[0:10],
                              radius=radius, category=category)
    mcg.get_groups_recursive()
    logging.info("Got %s groups", len(mcg.groups))

    # Flatten the json data
    output = meetup_utils.flatten_data(mcg.groups, 
                                       country_name=name,
                                       country_code=iso2,
                                       keys=[('category', 'name'),
                                             ('category', 'shortname'),
                                             ('category', 'id'), 
                                             'description', 
                                             'created',
                                             'country',
                                             'city',
                                             'id',
                                             'lat',
                                             'lon',
                                             'members',
                                             'name',
                                             'topics',
                                             'urlname'])
    
    # Write the output
    meetup_utils.save_sample(output, 'data/country_groups.json', 20)
