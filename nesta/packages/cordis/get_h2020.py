import pandas as pd
import re                                                   
from itertools.chain import from_iterable as chain_iter
                                
TOP_URL = 'http://cordis.europa.eu/data/cordis-h2020{}.csv'

def camel_to_snake(name):                                   
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)         
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def guess_type(df_col, text_len=30, lookup = {int:'INTEGER',
                                              float:'FLOAT',
                                              bool:'BOOL'}):
    _type = [type(row) for row in df[col]
             if not pd.isnull(row)][0]
    if _type is str:
        _len = max(len(row) for row in df_col
                   if type(row) is str)
        if _len > text_len:
            _type = 'TEXT'
        else:
            _type = f'VARCHAR({_len})'
    else:
        _type = f'{lookup[_type]}'
    return _type


if __name__ == "__main__":
    entities = ['organizations', 'projectPublications',
                'projects', 'reports',                 
                'projectDeliverables']                 
    
    for entity_name in entities:                               
        class_name = entity_name[0].upper() + entity_name[1:]  
        table_name = f'cordisH2020_{camel_to_snake(class_name)}'
        
        df = pd.read_csv(TOP_URL.format(entity_name),           
                         decimal=',', sep=';',                  
                         error_bad_lines=False,                 
                         warn_bad_lines=True)                   
        
        df = df.dropna(axis=1, how='all')                       
        df.columns = [camel_to_snake(col) for col in df.columns]

        if entity_name == 'projects':
            fp = df.pop('framework_programme')[0]        
            df['programmes'] = [progs.split(";") for progs in df.pop('programme')]
            unique_progs = set(chain_iter(df['programmes']))
            data['programmes'] = pd.DataFrame([{'id':prog, 
                                                'framework_programme':fp}
                                               for prog in unique_progs])

        data[entity_name] = df                                  
        # _class = get_class_by_tablename(table_name)           
        # for row in df: _row = _class(**row); insert_row(engine, _row);
        
