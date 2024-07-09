echo "Creating namespace $NAMESPACE"
kubectl create namespace $EDAV_NAMESPACE
#echo "Creating docker registry secret"
#kubectl create secret docker-registry docker-registry-cred --docker-server=$DOCKER_REGISTRY --docker-username=$DOCKER_REGISTRY_USER --docker-password=$DOCKER_REGISTRY_PASSWORD -n $EDAV_NAMESPACE
#echo "Creating ingress tls secret"
#kubectl create secret tls biomass-ssl-key --key ~/esa-maap.org/$MAAP_ENV_TYPE/privkey.pem --cert ~/esa-maap.org/$MAAP_ENV_TYPE/fullchain.pem -n $EDAV_NAMESPACE
echo "Installing helm chart"
helm install $EDAV_BACKEND_HELM_RELEASE_NAME ./edav --namespace $EDAV_NAMESPACE -f edav/values-DEV.yaml
