'''
country iso codes
=================

tools for lookup of iso codes for countries
'''

import pycountry

from nesta.packages.geo_utils.alpha2_to_continent import alpha2_to_continent_mapping


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


def country_iso_code_dataframe(df, country='country'):
    '''
    A wrapper for the country_iso_code function to apply it to a whole dataframe,
    using the country name. Also appends the continent code based on the country.

    Args:
        df (:obj:`pandas.DataFrame`): a dataframe containing a country field.
        country (str): field in df containing the country name
    Returns:
        a dataframe with country_alpha_2, country_alpha_3, country_numeric, and
        continent columns appended.
    '''
    df['country_alpha_2'], df['country_alpha_3'], df['country_numeric'] = None, None, None
    df['continent'] = None

    continents = alpha2_to_continent_mapping()

    for idx, row in df.iterrows():
        try:
            country_codes = country_iso_code(row[country])
        except KeyError:
            # some fallback method could go here
            pass
        else:
            df.at[idx, 'country_alpha_2'] = country_codes.alpha_2
            df.at[idx, 'country_alpha_3'] = country_codes.alpha_3
            df.at[idx, 'country_numeric'] = country_codes.numeric
            df.at[idx, 'continent'] = continents.get(country_codes.alpha_2)

    return df


def country_iso_code_to_name(code, iso2=False):
    """Converts country alpha_3 into name and catches error so this can be used with
       pd.apply.

    Args:
        code (str): iso alpha 3 code
        iso2 (bool): use alpha 2 code instead
    Returns:
        str: name of the country or None if not valid
    """
    try:        
        if iso2:
            return pycountry.countries.get(alpha_2=code).name
        else:
            return pycountry.countries.get(alpha_3=code).name
    except (KeyError, AttributeError):
        return None
