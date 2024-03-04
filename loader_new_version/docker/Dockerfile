FROM ubuntu:23.10
LABEL maintainer="info@meeo.it"

#config envars
ARG USERNAME="maap"

USER root

#install dependencies and configure os
RUN apt-get update && apt-get -y upgrade && DEBIAN_FRONTEND=noninteractive apt-get -y install  python3-pip gdal-bin python3-gdal python3-venv vim nfs-common nginx curl wget gnupg supervisor

RUN curl -fsSL https://pgp.mongodb.com/server-6.0.asc | gpg -o /usr/share/keyrings/mongodb-server-6.0.gpg --dearmor

RUN echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-6.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/6.0 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-6.0.list
RUN apt-get update && apt-get install -y mongodb-org-tools mongodb-mongosh

#create default user
RUN adduser -q --disabled-password --gecos "" ${USERNAME}

RUN mkdir -p /opt/meeo-das/loader/log
RUN mkdir -p /opt/meeo-das/loader/run


COPY . /opt/meeo-das/loader/.
RUN python3 -m venv "/opt/meeo-das/loader/venv"
RUN /bin/bash -c "source /opt/meeo-das/loader/venv/bin/activate && pip install -r /opt/meeo-das/loader/requirements.txt"
RUN ln -s "/usr/lib/python3/dist-packages/osgeo" "/opt/meeo-das/loader/venv/lib/python3.11/site-packages/osgeo"

RUN chown -R ${USERNAME}:${USERNAME} /opt/meeo-das

USER root

COPY conf/loaderweb.conf /etc/nginx/sites-enabled/loaderweb.conf

COPY docker/supervisord/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY docker/supervisord/supervisord-nginx.sh /supervisord-nginx.sh
RUN chmod 755 /*.sh

EXPOSE 80
CMD ["/usr/bin/supervisord"]