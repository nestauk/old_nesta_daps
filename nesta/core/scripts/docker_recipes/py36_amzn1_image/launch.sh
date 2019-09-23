#!/bin/bash

# Install any other python packages which aren't picked up
# in the requirements
source activate py36
which pip
which python
pip install awscli --upgrade --user
pip install lxml
conda clean -afy

# Pull the batchable from S3
echo "Getting file" ${BATCHPAR_S3FILE_TIMESTAMP}
aws s3 cp s3://nesta-batch/${BATCHPAR_S3FILE_TIMESTAMP} run.zip
/usr/bin/unzip run.zip
rm run.zip  # clear up some space
cd run
ls

# Print out the caller id
#aws sts get-caller-identity
#aws iam list-roles
sed -i '/tensorflow/d' requirements.txt  # rm TF from reqs since it's huge
pip install -r requirements.txt
#pip freeze
conda clean -afy

# Check the file exists and run it
echo "Starting..."
cat run.py &> /dev/null
#time /usr/bin/python3.6 run.py
time python run.py
