from nesta.production.luigihacks.s3 import parse_s3_path

import numpy as np
from itertools import cycle

import os
import boto3
import json
import numpy as np
import json


def run():
    # Get variables out
    s3_path_in = os.environ['BATCHPAR_s3_path_in']
    s3_path_out = os.environ["BATCHPAR_outinfo"]
    first_index = int(os.environ['BATCHPAR_first_index'])
    last_index = int(os.environ['BATCHPAR_last_index'])

    # Load the data
    s3 = boto3.resource('s3')
    s3_obj_in = s3.Object(*parse_s3_path(s3_path_in))
    data = json.load(s3_obj_in.get()['Body'])

    # Create a "corpus" by joining together text fields
    # which have been analysed by the ngrammer already
    n_topics = 10
    n_topics_per_doc = 3

    topic_loop = cycle(range(0, n_topics))
    topic_nums = list(range(0, n_topics))
    all_topics = []
    for _ in range(0, len(data)):
        idx = np.random.choice(topic_nums)
        topics = []
        counter = 0
        for i, jdx in enumerate(topic_loop):
            if idx == jdx:
                counter += 1                    
            if counter > 0:
                topics.append(f"FAKE_TOPIC_{i}")
                counter += 1
            if counter > n_topics_per_doc:
                break
        all_topics.append(topics)

    # Mark the task as done
    if s3_path_out != "":
        s3 = boto3.resource('s3')
        s3_obj = s3.Object(*parse_s3_path(s3_path_out))
        s3_obj.put(Body=json.dumps(all_topics))


if __name__ == "__main__":
    run()

