'''
country iso codes
=======

tools for lookup of iso codes for countries
'''

import pycountry

def country_iso_code(country):
    '''
    Look up the ISO 3166 codes for countries.
    https://www.iso.org/glossary-for-iso-3166.html

    Wraps the pycountry module to attempt lookup with all name options.

    Args:
        country (str): name of the country to lookup
    Returns:
        Country object from the pycountry module
    '''
    country = str(country).title()
    for name_type in ['name', 'common_name', 'official_name']:
        query = {name_type: country}
        try:
            return pycountry.countries.get(**query)
        except KeyError:
            pass

    raise KeyError(f"{country} not found")
