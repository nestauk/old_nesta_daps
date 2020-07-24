from contextlib import contextmanager
import logging
import pandas as pd
import re
import requests
import tarfile
from tempfile import NamedTemporaryFile
from collections import defaultdict

from nesta.packages.crunchbase.utils import split_str  # required for unpickling of split_health_flag: vectoriser
from nesta.packages.geo_utils.country_iso_code import country_iso_code_to_name
from nesta.packages.geo_utils.geocode import generate_composite_key
from nesta.core.luigihacks import misctools
from nesta.core.orms.orm_utils import db_session, insert_data
from nesta.core.orms.crunchbase_orm import Organization


@contextmanager
def crunchbase_tar():
    """Downloads the tar archive of Crunchbase data.

    Returns:
        :code:`tarfile.TarFile`: opened tar archive
    """
    crunchbase_config = misctools.get_config('crunchbase.config', 'crunchbase')
    user_key = crunchbase_config['user_key']
    url = 'https://api.crunchbase.com/bulk/v4/bulk_export.tar.gz?user_key='
    with NamedTemporaryFile() as tmp_file:
        r = requests.get(''.join([url, user_key]))
        tmp_file.write(r.content)
        tmp_tar = tarfile.open(tmp_file.name, mode='r:gz')
    try:
        yield tmp_tar
    finally:
        tmp_tar.close()


def get_csv_list():
    """Gets a list of csv files within the Crunchbase tar archive.

    Returns:
        list: all .csv files in the archive
    """
    csvs = []
    csv_pattern = re.compile(r'^(.*)\.csv$')
    with crunchbase_tar() as tar:
        names = tar.getnames()
    for name in names:
        tablename = csv_pattern.match(name)
        if tablename is not None:
            csvs.append(tablename.group(1))
    return csvs


def get_files_from_tar(files, nrows=None):
    """Converts csv files in the crunchbase tar into dataframes and returns them.

    Args:
        files (list): names of the files to extract (without .csv suffix)
        nrows (int): limit the number of rows extracted from each file, for testing.
                     Default of None will return all rows

    Returns:
        (:obj:`list` of :obj:`pandas.Dataframe`): the extracted files as dataframes
    """
    if type(files) != list:
        raise TypeError("Files must be provided as a list")

    dfs = []
    with crunchbase_tar() as tar:
        for filename in files:
            df = pd.read_csv(tar.extractfile(f'{filename}.csv'),
                             low_memory=False, nrows=nrows)
            df = df.where(pd.notnull(df), None)  # Fill NaN as null for MySQL
            dfs.append(df)
            logging.info(f"Collected {filename} from crunchbase tarfile")
    return dfs


def rename_uuid_columns(data):
    """Renames any columns called or containing `uuid`, to the convention of `id`.

    Args:
        data (:obj:`pandas.Dataframe`): dataframe with column names to amend

    Returns:
        (:obj:`pandas.Dataframe`): the original dataframe with amended column names
    """
    renames = {col: col.replace('uuid', 'id') for col in data}
    return data.rename(columns=renames)


def bool_convert(value):
    """Converter from string representations of bool to python bools.

    Args:
        value (str): t or f

    Returns:
        (bool): boolean representation of the field, or None on failure so pd.apply can
                be used and unconvertable fields will be empty
    """
    lookup = {'t': True, 'f': False}
    try:
        return lookup[value]
    except KeyError:
        logging.info(f"Not able to convert to a boolean: {value}")


def _generate_composite_key(**kwargs):
    """Wrapper around generate_composite_key to treat errors and therefore allow it to
    be used with pd.apply

    Returns:
        (str): composite key if able to generate, otherwise None
    """
    try:
        return generate_composite_key(**kwargs)
    except ValueError:
        return None


def process_orgs(orgs, existing_orgs, cat_groups, org_descriptions):
    """Processes the organizations data.

    Args:
        orgs (:obj:`pandas.Dataframe`): organizations data
        existing_orgs (set): ids of orgs already in the database
        cat_groups (:obj:`pandas.Dataframe`): category groups data
        org_descriptions (:obj:`pandas.Dataframe`): long organization descriptions

    Returns:
        (:obj:`list` of :obj:`dict`): processed organization data
        (:obj:`list` of :obj:`dict`): generated organization_category data
        (:obj:`list` of :obj:`dict`): names of missing categories from category_groups
    """
    # fix uuid column names
    orgs = rename_uuid_columns(orgs)

    # lookup country name and add as a column
    orgs['country'] = orgs['country_code'].apply(country_iso_code_to_name)

    orgs['long_description'] = None
    org_cats = []
    cat_groups = cat_groups.set_index(['name'])
    missing_cat_groups = set()
    org_descriptions = org_descriptions.set_index(['uuid'])

    # generate composite key for location lookup
    logging.info("Generating composite keys for location")
    orgs['location_id'] = orgs[['city', 'country']].apply(lambda row: _generate_composite_key(**row), axis=1)

    for idx, row in orgs.iterrows():
        # generate link table data for organization categories
        try:
            for cat in row.category_list.split(','):
                org_cats.append({'organization_id': row.id,
                                 'category_name': cat.lower()})
        except AttributeError:
            # ignore NaNs
            pass

        # append long descriptions to organization
        try:
            orgs.at[idx, 'long_description'] = org_descriptions.loc[row.id].description
        except KeyError:
            # many of these are missing
            pass

        if not (idx + 1) % 25000:
            logging.info(f"Processed {idx + 1} organizations")
    logging.info(f"Processed {idx + 1} organizations")

    # identify missing category_groups
    for row in org_cats:
        category_name = row['category_name']
        try:
            cat_groups.loc[category_name]
        except KeyError:
            logging.debug(f"Category '{category_name}' not found in categories table")
            missing_cat_groups.add(category_name)
    logging.info(f"{len(missing_cat_groups)} missing category groups to add")
    missing_cat_groups = [{'name': cat} for cat in missing_cat_groups]

    # remove redundant category columns
    orgs = orgs.drop(['category_list', 'category_groups_list'], axis=1)

    # remove existing orgs
    drop_mask = orgs['id'].apply(lambda x: x in existing_orgs)
    orgs = orgs.loc[~drop_mask]
    orgs = orgs.to_dict(orient='records')

    return orgs, org_cats, missing_cat_groups


def process_non_orgs(df, existing, pks):
    """Processes any crunchbase table, other than organizations, which are already
    handled by the process_orgs function.

    Args:
        df (:obj: `pd.DataFrame`): data from one table
        existing (:obj: `set` of :obj: `tuple`): tuples of primary keys that are already in the database
        pks (list): names of primary key columns in the dataframe

    Returns:
        (:obj:`list` of :obj:`dict`): processed table with existing records removed
    """
    # fix uuid column names
    df = rename_uuid_columns(df)

    # drop any rows already existing in the database
    total_rows = len(df)
    drop_mask = df[pks].apply(lambda row: tuple(row[pks]) in existing, axis=1)
    df = df.loc[~drop_mask]
    logging.info(f"Dropped {total_rows - len(df)} rows already existing in database")

    # convert country name and add composite key if locations in table
    if {'city', 'country_code'}.issubset(df.columns):
        logging.info("Locations found in table. Generating composite keys.")
        df['country'] = df['country_code'].apply(country_iso_code_to_name)
        df['location_id'] = df[['city', 'country']].apply(lambda row: _generate_composite_key(**row), axis=1)

    # convert any boolean columns to correct values
    for column in (col for col in df.columns if re.match(r'^is_.+', col)):
        logging.info(f"Converting boolean field {column}")
        df[column] = df[column].apply(bool_convert)

    df = df.to_dict(orient='records')
    return df


def all_org_ids(engine, limit=None):
    """Retrieve the id of every organization in the crunchbase data in MYSQL.

    Args:
        engine(:obj:`sqlalchemy.engine.Base.Engine`): engine to use with the session
            when connecting to the database
        limit(int): row limit to apply to query (for testing)

    Returns:
        (set): all organisation ids
    """
    with db_session(engine) as session:
        orgs = session.query(Organization.id)
        if limit is not None:
            orgs = orgs.limit(limit)
        return {org.id for org in orgs}


def predict_health_flag(data, vectoriser, classifier):
    """Predict health labels for crunchbase organisations using the list of categories.

    Args:
        data (:obj:`list` of :obj:`dict`): Crunchbase IDs and list of categories.
        vectoriser: vectoriser model
        classifier: classifier model

    Return:
        (:obj:`list` of :obj:`dict`): Crunchbase ids and bool health flag

    """
    # remove and store index (cannot be passed to predict)
    ids = [row['id'] for row in data]
    categories = [row['categories'] for row in data]

    labels = classifier.predict(vectoriser.transform(categories))

    # rejoin ids to the output labels
    return [{'id': id_, 'is_health': bool(pred)}
            for id_, pred in zip(ids, labels)]


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    log_file_handler = logging.FileHandler('logs.log')
    logging.basicConfig(handlers=(log_stream_handler, log_file_handler),
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")
