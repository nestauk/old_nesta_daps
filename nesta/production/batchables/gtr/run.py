import os
import boto3
from collections import defaultdict
from urllib.parse import urlsplit

from nesta.packages.gtr.get_gtr_data import read_xml_from_url
from nesta.packages.gtr.get_gtr_data import extract_data
from nesta.packages.gtr.get_gtr_data import extract_data_recursive
from nesta.packages.gtr.get_gtr_data import unpack_list_data
from nesta.packages.gtr.get_gtr_data import deduplicate_participants
from nesta.packages.gtr.get_gtr_data import extract_link_table
from nesta.packages.gtr.get_gtr_data import TOP_URL

from nesta.production.orms.orm_utils import insert_data
from nesta.production.orms.orm_utils import get_class_by_tablename
from nesta.production.orms.gtr_orm import Base


def parse_s3_path(path):
    '''For a given S3 path, return the bucket and key values'''
    parsed_path = urlsplit(path)
    s3_bucket = parsed_path.netloc
    s3_key = parsed_path.path.lstrip('/')
    return (s3_bucket, s3_key)


def run():
    PAGE_SIZE = int(os.environ['BATCHPAR_PAGESIZE'])
    page = int(os.environ['BATCHPAR_page'])
    db = os.environ["BATCHPAR_db"]
    s3_path = os.environ["BATCHPAR_outinfo"]

    data = defaultdict(list)

    # Get all projects on this page
    projects = read_xml_from_url(TOP_URL, p=page, s=PAGE_SIZE)
    for project in projects.getchildren():
        # Extract the data for the project into 'row'
        # Then recursively extract data from nested rows into the parent 'row'
        _, row = extract_data(project)
        extract_data_recursive(project, row)
        # Flatten out any list data directly into 'data'
        unpack_list_data(row, data)
        # Append the row
        data[row.pop('entity')].append(row)
    # Much of the participant data is repeated so remove overlaps
    if 'participant' in data:
        deduplicate_participants(data)
    # Finally, extract links between entities and the core projects
    extract_link_table(data)
    
    objs = []
    for table_name, rows in data.items():
        _class = get_class_by_tablename(Base, f"gtr_{table_name}")
        # Remove any fields that aren't in the ORM
        cleaned_rows = [{k:v for k, v in row.items() if k in _class.__dict__}
                        for row in rows]
        objs += insert_data("BATCHPAR_config", "mysqldb", db,
                            Base, _class, cleaned_rows)

    # Mark the task as done
    if s3_path != "":
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path))
        s3_obj.put(Body="")

    return len(objs)

    

if __name__ == "__main__":
    # Local testing
    if "BATCHPAR_outinfo" not in os.environ:
        os.environ['BATCHPAR_TOPURL'] = "https://gtr.ukri.org/gtr/api/projects"
        os.environ['BATCHPAR_PAGESIZE'] = "100"
        os.environ['BATCHPAR_page'] = "1"
        os.environ["BATCHPAR_db"] = "dev"
        os.environ["BATCHPAR_outinfo"] = ""
        os.environ["BATCHPAR_config"] = os.environ["MYSQLDBCONF"]
    run()
