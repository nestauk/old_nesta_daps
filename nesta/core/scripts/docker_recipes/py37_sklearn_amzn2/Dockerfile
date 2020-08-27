FROM amazonlinux:2

# For debugging (leaving this here since we're still near the docker diskspace thresholds)
RUN du -cxh --threshold=5M --max-depth=3 /

RUN mkdir /root/.conda
ENV PYTHONDONTWRITEBYTECODE=true
ENV PYTHONIOENCODING=utf8
ENV LANG=en_US.UTF-8

# Install common dependencies
RUN rm -rf /var/cache/yum/x86_64/latest
RUN yum update -y
RUN yum install aws-cli -y
RUN yum install unzip -y
RUN yum install which -y
RUN yum install findutils -y
RUN yum install wget -y
RUN yum install gcc -y
RUN yum clean all -y

# Install base conda env
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && /bin/bash ~/miniconda.sh -b -p /opt/conda && rm ~/miniconda.sh && ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && echo "conda activate base" >> ~/.bashrc && /bin/bash ~/.bashrc
ENV PATH /opt/conda/bin:$PATH
RUN conda create -y -n py37 \
    nomkl \
    python=3.7 \
    cython \
    geos \
    scikit-learn \
    && conda clean -afy \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

# Remove packages that you won't need
RUN yum remove gcc -y
RUN rm -rf /var/lib/rpm
RUN rm -rf /usr/share/doc/*
RUN rm -rf /opt/conda/envs/py37/share/*
RUN rm -rf /opt/conda/share/*
RUN rm -rf /var/cache/yum

# For debugging (leaving this here since we're still near the docker diskspace thresholds)
RUN du -cxh --threshold=5M --max-depth=3 /
RUN du -cxh --threshold=5M --max-depth=3 /opt/conda/
RUN conda init bash

# Prepare for launch
ADD launch.sh /usr/local/bin/launch.sh
WORKDIR /tmp
USER root
ENTRYPOINT ["/usr/local/bin/launch.sh"]
