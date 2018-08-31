FROM ubuntu:16.04

RUN apt-get update && apt-get install -y --no-install-recommends apt-utils
RUN apt-get install sudo  -y
RUN sudo apt-get install wget -y
RUN sudo apt-get install curl -y
RUN wget https://gist.githubusercontent.com/ziadoz/3e8ab7e944d02fe872c3454d17af31a5/raw/ff10e54f562c83672f0b1958a144c4b72c070158/install.sh
RUN /bin/bash -c "source install.sh"
RUN sudo apt-get install chromium-browser -y
RUN sudo apt-get install python3-pip -y
RUN pip3 install selenium
RUN pip3 install pyvirtualdisplay
RUN pip3 install awscli --upgrade --user
RUN ~/.local/bin/aws --version

ADD launch.sh /usr/local/bin/launch.sh
WORKDIR /tmp   
USER root      

ENTRYPOINT ["/usr/local/bin/launch.sh"]
