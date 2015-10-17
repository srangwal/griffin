FROM srangwal/baseimage:0.9.17
MAINTAINER srangwal@gmail.com

RUN mkdir /etc/service/griffin
COPY run /etc/service/griffin/run

RUN groupadd -r griffin && useradd -r -m -g griffin griffin
RUN mkdir /opt/griffin && chown -R griffin.griffin /opt/griffin
USER griffin
COPY griffin-0.1.0.jar /opt/griffin/griffin-0.1.0.jar


USER root
# Clean up APT when done.
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# vim: set nospell:
