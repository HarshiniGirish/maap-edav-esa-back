FROM ubuntu:20.04
LABEL mantainer="info@meeo.it"


USER root

RUN chmod 777 /tmp
RUN apt-get update
RUN apt-get -y upgrade
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install wget

#RUN wget http://www.meeo.it/pub/deb/pool/mea-mwcs/mea-mwcs-2.1-49_amd64.deb -P /tmp
RUN wget http://www.meeo.it/pub/deb/pool/mea-mwcs/mea-mwcs-2.3-6_amd64.deb -P /tmp
RUN touch    /bin/systemctl
RUN chmod +x /bin/systemctl
#RUN DEBIAN_FRONTEND=noninteractive apt -y install /tmp/mea-mwcs-2.1-49_amd64.deb
RUN DEBIAN_FRONTEND=noninteractive apt -y install /tmp/mea-mwcs-2.3-6_amd64.deb
RUN DEBIAN_FRONTEND=noninteractive apt-get install -f 

COPY ./MWCS.conf /opt/mea/dar/mwcs/MWCS.conf
COPY ./start.sh /opt/mea/dar/mwcs/start.sh

RUN mkdir -p /mnt/storage
RUN a2dismod mpm_event
RUN a2enmod  mpm_prefork

RUN adduser -q --disabled-password --no-create-home --gecos "" meeo
RUN sed -i /etc/apache2/envvars -e 's/www-data/meeo/g'

ENV APACHE_RUN_USER www-data
ENV APACHE_RUN_GROUP www-data
ENV APACHE_LOG_DIR /var/log/apache2
ENV APACHE_LOCK_DIR /var/lock/apache2
ENV APACHE_PID_FILE /var/run/apache2.pid


EXPOSE 80

CMD [ "/opt/mea/dar/mwcs/start.sh" ]
