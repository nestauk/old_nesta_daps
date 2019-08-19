from collections import defaultdict
from fuzzywuzzy import process as fuzzy_proc
import logging
import numpy as np
import pandas as pd
import re

from nesta.core.orms.grid_orm import Institute, Alias
from nesta.core.orms.orm_utils import db_session


def read_institutes(filepath):
    """Reads the grid.csv and addresses.csv flatfiles, loads it into a dataframe and
    combines them, dropping duplicate and redundant data.

    Not currently implemented into any pipeline, but should be. Scraping may be required
    to identify the latest file.

    Args:
        filepath (str): location of the folder containg the extracted files

    Returns
        (:obj:`pandas.DataFrame`): all data from the file
    """
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
    """Reads the aliases.csv flatfile and loads it into a dataframe.

    Not currently implemented into any pipeline, but should be. Scraping may be required
    to identify the latest file.

    Args:
        filepath (str): location of the folder containg the extracted files

    Returns
        (:obj:`pandas.DataFrame`): all data from the file
    """
    aliases = pd.read_csv(f"{filepath}/full_tables/aliases.csv", low_memory=False)

    return aliases


class ComboFuzzer:
    """Combines multiple fuzzy matching methods and provides a wrapper
    around fuzzy_proc.extractOne to enable keeping previous successful and failed
    matches in memory for faster checks; use if data is likely to contain duplicates.
    This can consume a lot of memory if ids are large or if there is a lot of data.
    """
    def __init__(self, fuzzers, store_history=False):
        """
        Args:
            fuzzers (function): fuzzy matching function returning a number between 0:100
            store_history (bool): keep previous attempts in memory for faster checking
        """
        self.fuzzers = fuzzers
        # Define the normalisation variable in advance
        self.norm = 1 / np.sqrt(len(fuzzers))

        self.store_history = store_history
        self.successful_fuzzy_matches = {}
        self.failed_fuzzy_matches = set()

    def combo_fuzz(self, target, candidate):
        """Scorer to be applied during a fuzzy match.

        Args:
            target (str): string to find
            candidate (str): string to score against target

        Returns:
            (numpy.float64): score between 0 and 1
        """
        _score = 0
        for _fuzz in self.fuzzers:
            _raw_score = (_fuzz(target, candidate) / 100)
            _score += _raw_score ** 2
        return np.sqrt(_score) * self.norm

    def fuzzy_match_one(self, query, choices, lowest_match_score=0.85):
        """Find the best fuzzy match from provided choices. If storing_history is set to
        True, previous failed and successful fuzzy matched are stored in memory for
        faster checking. KeyError is raised for failed queries.

        Args:
            query (str): target string
            choices (list): items to fuzzy match against
            lowest_match_score (float): a score below this value is considered a fail

        Returns:
            (str): the closest match
            (float): score between 0 and 1
        """
        if query in self.failed_fuzzy_matches:
            raise KeyError(f"Fuzzy match failed previously: {query}")

        try:
            match, score = self.successful_fuzzy_matches[query]
        except KeyError:
            # attempt a new fuzzy match
            match, score = fuzzy_proc.extractOne(query=query,
                                                 choices=choices,
                                                 scorer=self.combo_fuzz)
        if score < lowest_match_score:
            if self.store_history:
                self.failed_fuzzy_matches.add(query)
            raise KeyError(f"Failed to fuzzy match: {query}")

        if self.store_history:
            self.successful_fuzzy_matches.update({query: (match, score)})
        return match, score


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
                # combine all matches to a cleaned and lowered country name {IBM : [grid_id1, grid_id2]}
                with_country[found.groups()[0].lower()].append(bracketed.id)
        logging.info(f"{len(with_country)} institutes with country name in the title")

        # append cleaned names to the lookup table
        institute_name_id_lookup.update(with_country)
        logging.info(f"{len(institute_name_id_lookup)} total institute names in lookup")

    return institute_name_id_lookup


if __name__ == '__main__':
    from nesta.core.orms.orm_utils import get_mysql_engine

    database = 'dev'
    engine = get_mysql_engine('MYSQLDB', 'mysqldb', database)

    filepath = "~/Downloads/grid-2019-02-17"

    # load institute file to sql
    institutes = read_institutes(filepath)
    institutes.to_sql('grid_institutes', engine, if_exists='append', index=False)

    # load aliases file to sql
    aliases = read_aliases(filepath)
    aliases.to_sql('grid_aliases', engine, if_exists='append', index=False)
