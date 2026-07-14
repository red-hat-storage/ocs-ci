#!/bin/bash
#pass project name argument while running this shell script
killall "kubectl proxy"
kubectl proxy &
echo "Deleting the namespace $1"
oc project $1
oc delete --all all,secret,pvc > /dev/null
oc get ns $1 -o json > tempfile
sed -i 's/"kubernetes"//g' tempfile
curl --silent -H "Content-Type: application/json" -X PUT --data-binary @tempfile http://127.0.0.1:8001/api/v1/namespaces/"$1"/finalize
