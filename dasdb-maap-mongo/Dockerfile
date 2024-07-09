FROM mongo:4.4

USER root

COPY schema.js /docker-entrypoint-initdb.d/schema.js

RUN chmod 755 /docker-entrypoint-initdb.d/*.js

EXPOSE 27017
