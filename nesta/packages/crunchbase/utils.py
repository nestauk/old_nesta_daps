import re

def split_str(text):
    """Split a string on comma. Required for the pickling of the vectoriser."""
    return text.split(',')

def parse_investor_names(investor_names):  
    '''Parse investor names into a list. NB: this field is badly formatted in Crunchbase.

    Args:
        investor_names (str): Raw investor_names fields from crunchbase
    Returns:
        A list of investor names.
    '''
    # First extract anything in quotations
    result = re.findall(r'"(.*?)"', investor_names)  
    for item in result:  
        investor_names = investor_names.replace(f'"{item}"','')  
    # Next extract everything else, assumed to be comma sep'd
    investor_names = investor_names.replace("{","").replace("}","")
    investor_names = result + investor_names.split(",")  
    # Return whatever you found
    return [item for item in investor_names if len(item) > 0]  
