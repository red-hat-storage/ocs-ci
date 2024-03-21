#!/bin/bash
secrets=$(oc get secrets -n openshift-operators | grep Opaque | cut -d" " -f1)
echo $secrets
for secret in $secrets
do
    oc patch -n openshift-operators secret/$secret -p '{"metadata":{"finalizers":null}}' --type=merge
done
mirrorpeers=$(oc get mirrorpeer -o name)
echo $mirrorpeers
for mp in $mirrorpeers
do
    oc patch $mp -p '{"metadata":{"finalizers":null}}' --type=merge
    oc delete $mp
done
drpolicies=$(oc get drpolicy -o name)
echo $drpolicies
for drp in $drpolicies
do
    oc patch $drp -p '{"metadata":{"finalizers":null}}' --type=merge
    oc delete $drp
done
drclusters=$(oc get drcluster -o name)
echo $drclusters
for drp in $drclusters
do
    oc patch $drp -p '{"metadata":{"finalizers":null}}' --type=merge
    oc delete $drp
done
oc delete project openshift-operators
managedclusters=$(oc get managedclusters -o name | cut -d"/" -f2)
echo $managedclusters
for mc in $managedclusters
do
    secrets=$(oc get secrets -n $mc | grep multicluster.odf.openshift.io/secret-type | cut -d" " -f1)
    echo $secrets
    for secret in $secrets
    do
        set -x
        oc patch -n $mc secret/$secret -p '{"metadata":{"finalizers":null}}' --type=merge
        oc delete -n $mc secret/$secret
    done
done

oc delete clusterrolebinding spoke-clusterrole-bindings
