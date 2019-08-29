#!/bin/bash

# Install any other python packages which aren't picked up
# in the requirements
sudo ls /usr/bin/pip*
sudo /home/ubuntu/.local/bin/pip3.5 install awscli --upgrade --user

# Pull the batchable from S3
echo "Getting file" ${BATCHPAR_S3FILE_TIMESTAMP}
~/.local/bin/aws s3 cp s3://nesta-batch/${BATCHPAR_S3FILE_TIMESTAMP} run.zip
/usr/bin/unzip run.zip
cd run

# Install dependencies from the requirements file
sudo pip3 install -r requirements.txt

# Check the file exists and run it
echo "Starting..."
cat run.py &> /dev/null
time /usr/bin/python3 run.py
