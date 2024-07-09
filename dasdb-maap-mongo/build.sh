#!/bin/bash

module="maap-mongo"
version="0.0.3"


docker build -t "edav/dasdb-$module:$version" . 

image_id="$( docker image ls -f "reference=edav/dasdb-$module:$version" -q )"

docker tag "$image_id" "registry.eu-west-0.prod-cloud-ocb.orange-business.com/cloud-biomass-maap/edav/dasdb-$module:$version"

docker push "registry.eu-west-0.prod-cloud-ocb.orange-business.com/cloud-biomass-maap/edav/dasdb-$module:$version"
