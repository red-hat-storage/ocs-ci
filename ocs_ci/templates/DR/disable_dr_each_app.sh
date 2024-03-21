#!/bin/bash

surviving_cluster=$1
namespaces=$(oc get placement -A | awk '{print $1}' | tail -n +2)
names=$(oc get placement -A | awk '{print $2}' | tail -n +2)
echo $names
echo $namespaces

for ns in $namespaces
do
    names=$(oc get placement -n $ns | awk '{print $1}' | tail -n +2)
    echo $names
    if [$names != 'all-openshift-clusters']
    then
	oc patch placement $names --type=json -p='[{ "op": "replace", "path": "/spec/predicates/0/requiredClusterSelector/labelSelector/matchExpressions/0/values/0", "value": $surviving _cluster }]' -n $namespaces -o yaml
    fi
done

#Delete drpc
oc delete drpc $names-drpc -n $namespaces

#Verify drpc are deleted
if $(oc get drpc -A )== Null:
do
	echo "All drpc are deleted
done"


#Remove annotations
for ns in $namespaces
do
    names=$(oc get placement -n $ns | awk '{print $1}' | tail -n +2)
    echo $names
    if [$names != 'all-openshift-clusters']
    then
	oc patch placement $names --type=json -p='[{ "op": "remove", "path": "/metadata/annotations/cluster.open-cluster-management.io~1experimental-scheduling-disable" }]' -n $namespaces -o yaml
    fi
done
