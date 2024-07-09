#BUOLDING
```bash
# edit build updating version
$ cat build | bash

$ docker tag 50e8f38d462b registry.eu-west-0.prod-cloud-ocb.orange-business.com/cloud-biomass-maap/edav/mwcs:${version}

$ docker push registry.eu-west-0.prod-cloud-ocb.orange-business.com/cloud-biomass-maap/edav/mwcs:${version}
```
