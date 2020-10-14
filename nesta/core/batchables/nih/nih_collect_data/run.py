"""
run.py (nih_collect_data)
-------------------------

Collect NiH table from the official data dump,
based on the name of the table. The data
is piped into the MySQL database.
"""

from nesta.core.orms.orm_utils import get_class_by_tablename, insert_data
from nesta.core.orms.nih_orm import Base
from nesta.packages.nih.collect_nih import iterrows
from nesta.packages.nih.preprocess_nih import preprocess_row
from nesta.core.luigihacks.s3 import parse_s3_path

import os
import boto3


def run():
    table_name = os.environ["BATCHPAR_table_name"]
    url = os.environ["BATCHPAR_url"]
    db_name = os.environ["BATCHPAR_db_name"]
    s3_path = os.environ["BATCHPAR_outinfo"]

    # Get the data
    _class = get_class_by_tablename(Base, table_name)
    data = [preprocess_row(row, _class) 
            for row in iterrows(url) if len(row) > 0]
    insert_data("BATCHPAR_config", "mysqldb", db_name,
                _class, data, low_memory=True)

    # Mark the task as done
    s3 = boto3.resource('s3')
    s3_obj = s3.Object(*parse_s3_path(s3_path))
    s3_obj.put(Body="")


if __name__ == "__main__":
    run()
