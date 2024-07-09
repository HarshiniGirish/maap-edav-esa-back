#!/bin/bash
sed "s#REFERENCE_HOST#$REFERENCE_HOST#g" -i /etc/apache2/sites-enabled/MWCS.conf
sed "s#MWCSMAXMEMORY#$MWCS_MAX_MEMORY#g" -i /etc/apache2/sites-enabled/MWCS.conf
sed "s#AWS_SECRET_ID#$AWS_SECRET_ID#g" -i /etc/apache2/sites-enabled/MWCS.conf
sed "s#AWS_SECRET_KEY#$AWS_SECRET_KEY#g" -i /etc/apache2/sites-enabled/MWCS.conf
sed "s#AWS_ENDPOINT#$AWS_ENDPOINT#g" -i /etc/apache2/sites-enabled/MWCS.conf


source /etc/apache2/envvars
exec /usr/sbin/apache2ctl -D FOREGROUND
