FROM ubuntu:latest

RUN wget https://gist.githubusercontent.com/ziadoz/3e8ab7e944d02fe872c3454d17af31a5/raw/ff10e54f562c83672f0b1958a144c4b72c070158/install.sh
RUN source install.sh
RUN sudo apt-get install chromium-browser
RUN sudo apt-get install python3-pip
RUN pip install pyvirtualdisplay
RUN pip install awscli --upgrade --user

ADD launch.sh /usr/local/bin/launch.sh
WORKDIR /tmp
USER root

ENTRYPOINT ["/usr/local/bin/launch.sh"]
