'''
geocode
=======

Tools for geocoding.
'''

import requests

def geocode(**request_kwargs):
    '''                                                                                     
    Geocoder using the Open Street Map Nominatim API.                                       
                                                                                            
    If there are multiple results the first one is returned (they are ranked by importance).
    The API usage policy allows maximum 1 request per second and no multithreading:         
    https://operations.osmfoundation.org/policies/nominatim/                                
    
    Args:
        request_kwargs (dict): Parameters for OSM API.
    Returns:
        JSON from API response.
    '''
    # Explictly require json for ease of use
    request_kwargs["format"] = "json"
    response = requests.get("https://nominatim.openstreetmap.org/search",
                            params=request_kwargs,
                            headers={'User-Agent': 'Nesta health data geocode'})
    response.raise_for_status()
    geo_data = response.json()
    if len(geo_data) == 0:
        raise ValueError(f"No geocode match for {request_kwargs}")
    return geo_data
