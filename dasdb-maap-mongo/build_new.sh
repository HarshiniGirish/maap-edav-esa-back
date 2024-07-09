#!/bin/bash

module="maap-mongo-new"
version="0.0.2"


docker build -t "edav/dasdb-$module:$version" -f "Dockerfile_new" .

image_id="$( docker image ls -f "reference=edav/dasdb-$module:$version" -q )"

docker tag "$image_id" "registry.eu-west-0.prod-cloud-ocb.orange-business.com/cloud-biomass-maap/edav/dasdb-$module:$version"

docker push "registry.eu-west-0.prod-cloud-ocb.orange-business.com/cloud-biomass-maap/edav/dasdb-$module:$version"

