import pandas as pd

def guess_sql_type(df_col, text_len=30, lookup = {int:'INTEGER',
                                                  float:'FLOAT',
                                                  bool:'BOOLEAN'}):
    '''Guess the SQL type of a pandas DataFrame column.

    Args:
        df_col (pd.Series): A single column of a pandas DataFrame.
        text_len (int): The maximum VARCHAR size, before TEXT is inferred.
        lookup (dict): Mapping of non-str python types to SQL type names.
    Returns:
        sql_type (str): SQL type name
    '''
    _types = [type(row) for row in df_col
             if not pd.isnull(row)]
    # Assign type by str --> float --> int --> bool hierarchy
    _type = (str if str in _types 
             else (float if float in _types 
                   else (int if int in _types
                         else (bool if bool in _types else None))))
    if _type is str:
        _len = max(len(str(row)) for row in df_col if not pd.isnull(row))
        _type = 'TEXT' if _len > text_len else f'VARCHAR({_len})'
    else:
        _type = f'{lookup[_type]}'
    return _type
