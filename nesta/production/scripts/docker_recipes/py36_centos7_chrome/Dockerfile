FROM centos:centos7

# Install huge list of dependencies
RUN rm -rf /var/cache/yum/x86_64/latest
RUN yum update -y
RUN yum install sudo -y
RUN yum install python -y
RUN yum install https://centos7.iuscommunity.org/ius-release.rpm -y
RUN yum install python36u -y
RUN yum install python36u-pip -y
RUN yum install python36u-devel -y
RUN pip3.6 install awscli
RUN yum install zip -y
RUN yum install unzip -y
RUN yum -y install findutils
RUN yum -y install rpm
RUN yum -y install wget
RUN yum -y install Xvfb
RUN yum -y install binutils
RUN yum -y install gawk
RUN yum -y install coreutils
RUN yum -y install sed
RUN yum -y install redhat-lsb-core

RUN wget https://dl.google.com/linux/linux_signing_key.pub
RUN rpm --import linux_signing_key.pub
RUN yum -y localinstall https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm
RUN ls /usr/bin/google-chrome

RUN curl https://intoli.com/install-google-chrome.sh | bash
RUN sudo /usr/bin/pip3.6 install pyvirtualdisplay

ADD launch.sh /usr/local/bin/launch.sh
WORKDIR /tmp
USER root

ENTRYPOINT ["/usr/local/bin/launch.sh"]