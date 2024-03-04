#!/bin/bash

if [ -f /etc/nginx/sites-enabled/default ]; then
        rm "/etc/nginx/sites-enabled/default"
fi
exec /usr/sbin/nginx -g "daemon off;"