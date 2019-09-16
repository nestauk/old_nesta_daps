import re

def camel_to_snake(name):
    '''Convert lowerCamelCase or upperCamelCase to snake_case

    Args:
        name (str): lowerCamelCase or upperCamelCase word.
    Returns:
        _name (str): snake_case word.
    '''
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
