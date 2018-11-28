from contextlib import contextmanager
import logging
import re
import requests
import tarfile
from tempfile import NamedTemporaryFile

from nesta.production.luigihacks import misctools


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
    """Gets a list of csv files withing the Crunchbase tar archive.

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




