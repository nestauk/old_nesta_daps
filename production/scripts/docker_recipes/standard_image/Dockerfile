FROM amazonlinux:latest

# Install common dependencies
RUN rm -rf /var/cache/yum/x86_64/latest
RUN yum update -y
RUN yum install sudo -y
RUN yum install aws-cli -y
RUN yum install zip -y
RUN yum install unzip -y
RUN yum -y install findutils

RUN yum install python -y
RUN yum install python3 -y

ADD launch.sh /usr/local/bin/launch.sh
WORKDIR /tmp
USER root

ENTRYPOINT ["/usr/local/bin/launch.sh"]
