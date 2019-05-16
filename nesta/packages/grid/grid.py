import numpy as np
import pandas as pd


from nesta.production.orms.orm_utils import get_mysql_engine


def read_institutes(filepath):
    df = pd.read_csv(f"{filepath}/grid.csv", low_memory=False)
    addresses = pd.read_csv(f"{filepath}/full_tables/addresses.csv", low_memory=False)

    addresses = addresses.set_index(keys=['grid_id'])
    df = df.join(addresses, on='ID')

    columns_to_delete = ['City', 'State', 'Country', 'primary']
    df = df.drop(columns_to_delete, axis=1)

    columns_to_rename = {'ID': 'id',
                         'lat': 'latitude',
                         'lng': 'longitude',
                         'Name': 'name',
                         'line_1': 'address_line_1',
                         'line_2': 'address_line_2',
                         'line_3': 'address_line_3'}
    df = df.rename(columns=columns_to_rename)

    return df


def read_aliases(filepath):
    aliases = pd.read_csv(f"{filepath}/full_tables/aliases.csv", low_memory=False)

    return aliases


class ComboFuzzer:
    def __init__(self, fuzzers):
        self.fuzzers = fuzzers
        # Define the normalisation variable in advance
        self.norm = 1 / np.sqrt(len(fuzzers))

    def combo_fuzz(self, target, candidate):
        _score = 0
        for _fuzz in self.fuzzers:
            _raw_score = (_fuzz(target, candidate) / 100)
            _score += _raw_score ** 2
        return np.sqrt(_score) * self.norm


if __name__ == '__main__':
    database = 'dev'
    engine = get_mysql_engine('MYSQLDB', 'mysqldb', database)

    filepath = "~/Downloads/grid-2019-02-17"

    institutes = read_institutes(filepath)
    institutes.to_sql('grid_institutes', engine, if_exists='append', index=False)

    aliases = read_aliases(filepath)
    aliases.to_sql('grid_aliases', engine, if_exists='append', index=False)
