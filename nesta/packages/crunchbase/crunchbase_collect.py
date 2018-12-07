from contextlib import contextmanager
import logging
import pandas as pd
import re
import requests
import tarfile
from tempfile import NamedTemporaryFile

from nesta.production.luigihacks import misctools
from nesta.packages.geo_utils.country_iso_code import country_iso_code_to_name


@contextmanager
def crunchbase_tar():
    """Downloads the tar archive of Crunchbase data.

    Returns:
        :code:`tarfile.Tarfile`: opened tar archive
    """
    crunchbase_config = misctools.get_config('crunchbase.config', 'crunchbase')
    user_key = crunchbase_config['user_key']
    url = 'https://api.crunchbase.com/v3.1/csv_export/csv_export.tar.gz?user_key='
    with NamedTemporaryFile() as tmp_file:
        r = requests.get(''.join([url, user_key]))
        tmp_file.write(r.content)
        try:
            tmp_tar = tarfile.open(tmp_file.name)
            yield tmp_tar
        finally:
            tmp_tar.close()


def get_csvs():
    """Gets a list of csv files within the Crunchbase tar archive.

    Returns:
        list: all .csv files int the archive
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


def rename_uuid_columns(data):
    """Renames any columns called or containing `uuid`, to the convention of `id`.

    Args:
        data (:obj:`pandas.Dataframe`): dataframe with column names to amend

    Returns:
        (:obj:`pandas.Dataframe`): the original dataframe with amended column names
    """
    renames = {col: col.replace('uuid', 'id') for col in data}
    return data.rename(columns=renames)


def generate_composite_key(city=None, country=None):
    """Generates a composite key to use as the primary key for the geographic data.

    Args:
        city (str): name of the city
        country (str): name of the country

    Returns:
        (str): composite key
    """
    try:
        city = city.replace(' ', '-').lower()
        country = country.replace(' ', '-').lower()
    except AttributeError:
        raise ValueError(f"Invalid city or country name. City: {city} | Country: {country}")
    return '_'.join([city, country])


def process_orgs(orgs, cat_groups, org_descriptions):
    """Processes the organizations data.

    Args:
        orgs (:obj:`pandas.Dataframe`): organizations data
        cat_groups (:obj:`pandas.Dataframe`): category groups data
        org_descriptions (:obj:`pandas.Dataframe`): long organization descriptions

    Returns:
        (:obj:`pandas.Dataframe`): processed organization data
        (:obj:`pandas.Dataframe`): generated organization_category data
    """
    orgs['country'] = orgs['country_code'].apply(country_iso_code_to_name)
    orgs = orgs.drop('country_code', axis=1)  # now redundant with country_alpha_3 appended

    orgs['location_id'] = generate_composite_key(orgs.city, orgs.country)
    # split categories_list and lookup the cat_groups table to generate the link table
    # remove category_list and category_group_list columns
    # process org descriptions and add to long_description column


def get_files_from_tar(files):
    """Converts csv files in the crunchbase tar into dataframes and returns them.

    Args:
        files (list): names of the files to extract (without .csv suffix)

    Returns:
        (:obj:`list` of :obj:`pandas.Dataframe`): the extracted files as dataframes
    """
    dfs = []
    with crunchbase_tar() as tar:
        for filename in files:
            dfs.append(pd.read_csv(tar.extractfile(''.join([filename, '.csv'])),
                                   low_memory=False))
    return dfs


if __name__ == '__main__':
    log_stream_handler = logging.StreamHandler()
    log_file_handler = logging.FileHandler('logs.log')
    logging.basicConfig(handlers=(log_stream_handler, log_file_handler),
                        level=logging.INFO,
                        format="%(asctime)s:%(levelname)s:%(message)s")

    # with crunchbase_tar() as tar:
    #     names = tar.getnames()
    # print(names)
    # assert 'category_groups.csv' in names

    # with crunchbase_tar() as tar:
    #     cg_df = pd.read_csv(tar.extractfile('category_groups.csv'))
    # print(cg_df.columns)

    csvs = get_csvs()
    print(csvs)




