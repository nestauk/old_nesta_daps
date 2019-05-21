from collections import defaultdict
from fuzzywuzzy import process as fuzzy_proc
import logging
import numpy as np
import pandas as pd
import re

from nesta.production.orms.grid_orm import Institute, Alias
from nesta.production.orms.orm_utils import db_session


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
    def __init__(self, fuzzers, store_history=False):
        self.fuzzers = fuzzers
        # Define the normalisation variable in advance
        self.norm = 1 / np.sqrt(len(fuzzers))

        self.successful_fuzzy_matches = {}
        self.failed_fuzzy_matches = set()
        self.store_history = store_history

    def combo_fuzz(self, target, candidate):
        _score = 0
        for _fuzz in self.fuzzers:
            _raw_score = (_fuzz(target, candidate) / 100)
            _score += _raw_score ** 2
        return np.sqrt(_score) * self.norm

    def fuzzy_match_one(self, query, choices):
        if query in self.failed_fuzzy_matches:
            raise KeyError(f"Fuzzy match failed previously: {query}")

        try:
            match, score = self.successful_fuzzy_matches[query]
        except KeyError:
            # attempt a new fuzzy match
            match, score = fuzzy_proc.extractOne(query=query,
                                                 choices=choices,
                                                 scorer=self.combo_fuzz)

        if score < 0.85:  # <0.85 is definitely a bad match
            if self.store_history:
                self.failed_fuzzy_matches.add(query)
            raise KeyError(f"Failed to fuzzy match: {query}")

        if self.store_history:
            self.successful_fuzzy_matches.update({query: (match, score)})
        return match, score


def create_article_institute_links(article, institute_ids, score):
    # add an entry to the link table for each grid id (there will be multiple if the org is multinational)
    return [{'article_id': article.id,
             'institute_id': institute_id,
             'is_multinational': len(institute_ids) > 1,
             'matching_score': float(score)}
            for institute_id in institute_ids]


def grid_name_lookup(engine):
    """Constructs a lookup table of Institute names to ids by combining names with
    aliases and cleaned names containing country names in brackets. Multinationals are
    detected.

    Args:
        engine (:obj:`sqlalchemy.engine.base.Engine`): connection to the database

    Returns:
        (:obj:`list` of :obj:`dict`): lookup table [{name: [id1, id2, id3]}]
                Where ids are different country entities for multinational institutes.
                Most entities just have a singe [id1]
    """
    with db_session(engine) as session:
        institute_name_id_lookup = {institute.name.lower(): [institute.id]
                                    for institute in session.query(Institute).all()}
        logging.info(f"{len(institute_name_id_lookup)} institutes in GRID")

        for alias in session.query(Alias).all():
            institute_name_id_lookup.update({alias.alias.lower(): [alias.grid_id]})
        logging.info(f"{len(institute_name_id_lookup)} institutes after adding aliases")

        # look for institute names containing brackets: IBM (United Kingdom)
        with_country = defaultdict(list)
        for bracketed in (session
                          .query(Institute)
                          .filter(Institute.name.contains('(') & Institute.name.contains(')'))
                          .all()):
            found = re.match(r'(.*) \((.*)\)', bracketed.name)
            if found:
                # combine all matches to a cleaned country name {IBM : [grid_id1, grid_id2]}
                with_country[found.groups()[0]].append(bracketed.id)
        logging.info(f"{len(with_country)} institutes with country name in the title")

        # append cleaned names to the lookup table
        institute_name_id_lookup.update(with_country)
        logging.info(f"{len(institute_name_id_lookup)} total institute names in lookup")

    return institute_name_id_lookup


if __name__ == '__main__':
    from nesta.production.orms.orm_utils import get_mysql_engine

    database = 'dev'
    engine = get_mysql_engine('MYSQLDB', 'mysqldb', database)

    filepath = "~/Downloads/grid-2019-02-17"

    # load institute file to sql
    institutes = read_institutes(filepath)
    institutes.to_sql('grid_institutes', engine, if_exists='append', index=False)

    # load aliases file to sql
    aliases = read_aliases(filepath)
    aliases.to_sql('grid_aliases', engine, if_exists='append', index=False)
