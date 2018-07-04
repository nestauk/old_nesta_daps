import os
import boto3
import sys
import requests
import json
import random


s3 = boto3.resource('s3')
os.environ["LD_LIBRARY_PATH"] = os.getcwd()
desired_keys = [('category', 'name'), ('category', 'shortname'),
                ('category', 'id'),
                'description', 'created', 'country', 'city', 'id',
                'lat', 'lon', 'members', 'name', 'topics']


def run(event, context):
    # Set path
    sys.path.append(os.getcwd())
    # Read the input data. Note: the S3 file is empty,
    # but the file has been used to trigger this function
    # and the file 'key' corresponds to the MeetUp member ID
    trigger_file = event["Records"][0]["s3"]["object"]
    key = trigger_file["key"]
    bucket = "tier-0-inputs"
    print("Got file", key)
    obj = s3.Bucket(bucket).Object(key)
    obj.delete()
    # Get the MeetUp member ID, and extract groups for the ID
    prefix = "_".join(key.split("_")[0:-1])+"_"
    urlname = key.split("_")[-1]
    print(key, prefix, urlname)

    api_keys = ["497355443664b516a1659a6a9c48",
                "6d265b6478231312541560545821f25",
                "6579334475358f3f5615584c1e1247",
                "713f1174501b26786b527e62464f3864",
                "201ae3319585f363b3536542864446e",
                "271c51246e4d5a5b5349a12166a3431",
                "1104d42668166a5833295e785d2b75",
                "481a7f197f75f2a2533516f5e1c64a"]
    api_key = random.choice(api_keys)

    params = dict(sign='true', fields='topics',
                  key=api_key)
    r = requests.get('https://api.meetup.com/{}'.format(urlname),
                     params=params)
    info = r.json()

    #
    row = dict(urlname=urlname)
    # Generate the field names and values, if they exist
    for key in desired_keys:
        field_name = key
        try:
            # If the key is just a string
            if type(key) == str:
                value = info[key]
            # Otherwise, assume its a list of keys
            else:
                field_name = "_".join(key)
                # Recursively assign the list of keys
                value = info
                for k in key:
                    value = value[k]
        # Ignore fields which aren't found (these will appear
        # as NULL in the database anyway)                                                       
        except KeyError:
            continue
        row[field_name] = value
    print(row)

    # Copy output to S3
    bucket = s3.Bucket('tier-0')
    fname = "/tmp/"+prefix+urlname+".json"
    with open(fname, 'w') as f:
        json.dump(row, f, sort_keys=True, indent=4)
    bucket.upload_file(fname, prefix+urlname)
    print("Done", urlname)

    return {
        'message': "Done "+urlname
    }


if __name__ == "__main__":
    run(None, None)
