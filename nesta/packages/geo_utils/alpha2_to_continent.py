import pycountry
from pycountry_convert import country_alpha2_to_continent_code
def alpha2_to_continent_mapping():
    '''Wrapper around :obj:`pycountry-convert`'s :obj:`country_alpha2_to_continent_code`
    function to generate a dictionary mapping ISO2 to continent codes, accounting
    where :obj:`pycountry-convert` has no mapping (e.g. for Vatican).

    Returns:
        :obj`dict`
    '''
    continents = {}
    for c in pycountry.countries:
        continents[c.alpha_2] = None
        try:
            continents[c.alpha_2] = country_alpha2_to_continent_code(c.alpha_2)
        except KeyError:
            pass
    return continents
