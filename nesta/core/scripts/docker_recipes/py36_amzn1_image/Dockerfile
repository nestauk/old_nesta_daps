FROM amazonlinux:1

# Install common dependencies
RUN rm -rf /var/cache/yum/x86_64/latest
RUN yum update -y
RUN yum install sudo -y
RUN yum install aws-cli -y
RUN yum install zip -y
RUN yum install unzip -y
RUN yum install which -y
RUN yum install findutils -y
RUN yum install wget -y
RUN yum install gcc -y

# Install base conda env
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && /bin/bash ~/miniconda.sh -b -p /opt/conda && rm ~/miniconda.sh && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && echo "conda activate base" >> ~/.bashrc && /bin/bash ~/.bashrc
ENV PATH /opt/conda/bin:$PATH
RUN conda create -y -n py36 python=3.6 cython
RUN conda init bash

# Set global env variables
ARG MEETUP_API_KEYS
ENV MEETUP_API_KEYS=$MEETUP_API_KEYS
ENV WORLD_BORDERS="meetup/data/TM_WORLD_BORDERS_SIMPL-0.3.shp"
ENV PYTHONIOENCODING=utf8

# Prepare for launch
ADD launch.sh /usr/local/bin/launch.sh
WORKDIR /tmp
USER root
ENTRYPOINT ["/usr/local/bin/launch.sh"]