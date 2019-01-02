import os
import pandas as pd
import s3fs

from nesta.production.orms.geographic_orm import Geographic
from nesta.production.orms.orm_utils import get_mysql_engine, db_session
from nesta.packages.geo_utils.geocode import geocode_batch_dataframe
from nesta.packages.geo_utils.country_iso_code import country_iso_code_dataframe


def run():
    batch_file = os.environ['BATCHPAR_batch_file']
    db = os.environ['BATCHPAR_db_name']
    bucket = os.environ['BATCHPAR_bucket']

    # database setup
    engine = get_mysql_engine('BATCHPAR_config', 'mysqldb', db)

    # collect data
    target = f"s3://{bucket}/{batch_file}"
    df = pd.read_json(target, orient='records')

    # append country iso codes and continent
    df = country_iso_code_dataframe(df)

    # geocode, appending latitude and longitude columns
    df = geocode_batch_dataframe(df)

    # remove city and country columns and append done column
    df = df.drop(['city', 'country'], axis=1)
    df['done'] = True

    # convert to list of dict and output to database
    rows = df.to_dict(orient='records')
    with db_session(engine) as session:
        session.bulk_update_mappings(Geographic, rows)


if __name__ == '__main__':
    run()
