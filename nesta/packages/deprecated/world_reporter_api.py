'''
World Reporter (API) [deprecated]
=================================

Extract all of the World Reporter abstract data (one per project ID)
via the hidden World Reporter API. This method is much quicker than
one presented in `world_reporter.py`.

For colleagues at Nesta: the CSV can be accessed in the AWS
bucket `nesta-inputs` at key `world_reporter_inputs.csv`.
'''

import boto3
import pandas as pd
from io import BytesIO
import re
import requests

s3 = boto3.resource('s3')

RE_COMP = re.compile(("https://worldreport.nih.gov:443/app/#!/"
                      "researchOrgId=(\w+)&programId=(\w+)"))

COOKIES = {
    '_ga': 'GA1.2.1522992933.1537784434',
    '_gid': 'GA1.2.697490070.1537969087',
    '_gat': '1',
}

API_URL = 'https://worldreport.nih.gov/worldreport/rest/abstract'

HEADERS = {
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X'
                   ' 10_13_6) AppleWebKit/537.36 (KHTML, '
                   'like Gecko) Chrome/69.0.3497.100 Safari/537.36'),
    'Accept': '*/*',
    'Referer': 'https://worldreport.nih.gov/app/',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
}

def get_project_id(url):
    '''Create a composite key based on the abstract url'''
    _, program_id = [int(i) for i in RE_COMP.findall(url)[0]]
    return program_id


def get_abstract(program_id):
    response = requests.get(API_URL, headers=HEADERS, cookies=COOKIES,
                            params=(('programId', program_id),))
    data = response.json()
    return data[0]["abstract"]


def get_csv_data():
    '''Retrieve the CSV data from AWS and tidy the columns'''
    obj = s3.Object("nesta-inputs", "world_reporter_inputs.csv")
    bio = BytesIO(obj.get()['Body'].read())
    df = pd.read_csv(bio)
    df.columns = [col.replace(" ","_").lower() for col in df.columns]
    # Create a field ready for the abstract text
    df["abstract_text"] = None
    return df
    
    
if __name__ == "__main__":
    df = get_csv_data()

    from datetime import datetime as dt
    t0 = dt.now()

    # Get the data for each abstract link
    for idx, row in df.iterrows():
        program_id = get_project_id(row["abstract_link"])
        df.at[idx, "abstract_text"] = get_abstract(program_id)
        # Only do the first few for this test
        condition = ~pd.isnull(df["abstract_text"])
        if condition.sum() >= 10:
            break
            
    # Show off the results
    print(df.loc[condition])
