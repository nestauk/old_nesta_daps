#!/bin/sh                                            

# Install any other python packages which aren't picked up
# in the requirements
sudo /usr/bin/pip-3.6 install awscli --upgrade --user
# sudo /usr/bin/pip-3.6 install pyvirtualdisplay

# Pull the batchable from S3
echo "Getting file" ${FILE_TIMESTAMP}
aws s3 cp s3://nesta-batch/${FILE_TIMESTAMP} run.zip
/usr/bin/unzip run.zip
cd run

# You could install anything else here as you wish, 
# but you should really do this in the Dockerfile
# sudo yum -y install wget
# sudo yum -y install findutils

# Install dependencies from the requirements file
sudo /usr/bin/pip-3.6 install -r requirements.txt

# Check the file exists and run it
echo "Starting..."
cat run.py &> /dev/null
time /usr/bin/python3.6 run.py
