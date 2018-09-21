FROM amazonlinux:1

# Install common dependencies
RUN rm -rf /var/cache/yum/x86_64/latest
RUN yum update -y
RUN yum install sudo -y
RUN yum install aws-cli -y
RUN yum install zip -y
RUN yum install unzip -y
RUN yum -y install findutils

RUN yum install python -y
RUN yum install python36 -y

ARG MEETUP_API_KEYS
ENV MEETUP_API_KEYS=$MEETUP_API_KEYS
RUN echo $MEETUP_API_KEYS
ENV WORLD_BORDERS="meetup/data/TM_WORLD_BORDERS_SIMPL-0.3.shp"

ADD launch.sh /usr/local/bin/launch.sh
WORKDIR /tmp
USER root

ENTRYPOINT ["/usr/local/bin/launch.sh"]
