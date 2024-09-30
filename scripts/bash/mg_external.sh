#!/usr/bin/env bash

set -x

dbglog() {
    # Allow the input to be piped
    declare msg=${1:-$(</dev/stdin)}

    echo -e "${msg}" | tee -a "${BASE_COLLECTION_PATH}"/ceph/gather-debug.log
}


# Expect base collection path as an exported variable
# If it is not defined, use PWD instead
BASE_COLLECTION_PATH=${1:-"$(pwd)"}
echo $BASE_COLLECTION_PATH

KUBECONFIG=${2:-"~/.kube/config"}
ns=${3:-"openshift-storage"}

TOOL_POD_NAME=$(oc get pods --no-headers -n ${ns} -l app='rook-ceph-tools' | awk '{print $1}')
if [ -z "$TOOL_POD_NAME" ]; then
    dbglog "No tool pod found"
    echo "No tool pod found"
    exit 2
fi


gather_common_ceph_resources "${BASE_COLLECTION_PATH}"
CEPH_GATHER_DBGLOG="${BASE_COLLECTION_PATH}"/gather-ceph-debug.log
CEPH_COLLECTION_PATH="${BASE_COLLECTION_PATH}/ceph"
COMMAND_OUTPUT_DIR=${CEPH_COLLECTION_PATH}/must_gather_commands
COMMAND_JSON_OUTPUT_DIR=${CEPH_COLLECTION_PATH}/must_gather_commands_json_output
COMMAND_ERR_OUTPUT_DIR=${CEPH_COLLECTION_PATH}/logs
mkdir -p "${COMMAND_OUTPUT_DIR}"
mkdir -p "${COMMAND_JSON_OUTPUT_DIR}"
mkdir -p "${COMMAND_ERR_OUTPUT_DIR}"


pids_ceph=()

# Ceph commands1
ceph_commands1=()
ceph_commands1+=("ceph auth list")
ceph_commands1+=("ceph balancer pool ls")
ceph_commands1+=("ceph balancer status")
ceph_commands1+=("ceph config dump")
ceph_commands1+=("ceph config-key ls")
ceph_commands1+=("ceph crash ls")
ceph_commands1+=("ceph crash stat")
ceph_commands1+=("ceph device ls")
ceph_commands1+=("ceph df detail")
ceph_commands1+=("ceph fs dump")
ceph_commands1+=("ceph fs ls")
ceph_commands1+=("ceph fs status")
ceph_commands1+=("ceph health detail")
ceph_commands1+=("ceph healthcheck history ls")
ceph_commands1+=("ceph mds stat")
ceph_commands1+=("ceph mgr dump")
ceph_commands1+=("ceph mgr module ls")
ceph_commands1+=("ceph mgr services")
ceph_commands1+=("ceph mon stat")
ceph_commands1+=("ceph mon dump")
ceph_commands1+=("ceph osd df tree")
ceph_commands1+=("ceph osd tree")
ceph_commands1+=("ceph osd stat")
ceph_commands1+=("ceph osd dump")
ceph_commands1+=("ceph osd utilization")
ceph_commands1+=("ceph osd crush show-tunables")
ceph_commands1+=("ceph osd crush dump")
ceph_commands1+=("ceph osd crush weight-set ls")

# Ceph commands2
ceph_commands2=()
ceph_commands2+=("ceph osd crush weight-set dump")
ceph_commands2+=("ceph osd crush rule dump")
ceph_commands2+=("ceph osd crush rule ls")
ceph_commands2+=("ceph osd crush class ls")
ceph_commands2+=("ceph osd perf")
ceph_commands2+=("ceph osd numa-status")
ceph_commands2+=("ceph osd getmaxosd")
ceph_commands2+=("ceph osd pool ls detail")
ceph_commands2+=("ceph osd lspools")
ceph_commands2+=("ceph osd df")
ceph_commands2+=("ceph osd blocked-by")
ceph_commands2+=("ceph osd blacklist ls")
ceph_commands2+=("ceph osd pool autoscale-status")
ceph_commands2+=("ceph pg dump")
ceph_commands2+=("ceph pg stat")
ceph_commands2+=("ceph progress")
ceph_commands2+=("ceph progress json")
ceph_commands2+=("ceph quorum_status")
ceph_commands2+=("ceph rbd task list")
ceph_commands2+=("ceph report")
ceph_commands2+=("ceph service dump")
ceph_commands2+=("ceph status")
ceph_commands2+=("ceph time-sync-status")
ceph_commands2+=("ceph versions")
ceph_commands2+=("ceph log last 10000 debug cluster")
ceph_commands2+=("ceph log last 10000 debug audit")
ceph_commands2+=("rados lspools")
ceph_commands2+=("rados ls --pool=ocs-storagecluster-cephblockpool")
ceph_commands2+=("rados ls --pool=ocs-storagecluster-cephfilesystem-metadata --namespace=csi")



# Collecting output of ceph osd config
for i in $(timeout 120 oc -n "${ns}" exec "${TOOL_POD_NAME}" -- bash -c "ceph osd tree --connect-timeout=15 |  grep up " | awk '{print $4}'); do
    { timeout 120 oc -n "${ns}" exec "${TOOL_POD_NAME}" -- bash -c "ceph config show $i" >>"${COMMAND_OUTPUT_DIR}/config_$i"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-config-"$i"-debug.log 2>&1 &
    pids_ceph+=($!)
done
# Check if PID array has any values, if so, wait for them to finish
if [ ${#pids[@]} -ne 0 ]; then
    echo "Waiting on subprocesses to finish execution."
    wait "${pids[@]}"
fi


# Collecting output of ceph commands
for ((i = 0; i < ${#ceph_commands1[@]}; i++)); do
    dbglog "collecting command output for: ${ceph_commands1[$i]}"
    COMMAND_OUTPUT_FILE=${COMMAND_OUTPUT_DIR}/${ceph_commands1[$i]// /_}
    JSON_COMMAND_OUTPUT_FILE=${COMMAND_JSON_OUTPUT_DIR}/${ceph_commands1[$i]// /_}_--format_json-pretty
    { timeout 120 oc --kubeconfig="${KUBECONFIG}" -n "${ns}" exec "${TOOL_POD_NAME}" -- bash -c "${ceph_commands1[$i]} --connect-timeout=15" >>"${COMMAND_OUTPUT_FILE}"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-"${ceph_commands1[$i]}"-debug.log 2>&1 &
    pids_ceph+=($!)
    { timeout 120 oc --kubeconfig="${KUBECONFIG}" -n "${ns}" exec "${TOOL_POD_NAME}" -- bash -c "${ceph_commands1[$i]} --connect-timeout=15 --format json-pretty" >>"${JSON_COMMAND_OUTPUT_FILE}"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-"${ceph_commands1[$i]}"-json-debug.log 2>&1 &
    pids_ceph+=($!)
done
# Check if PID array has any values, if so, wait for them to finish
if [ ${#pids[@]} -ne 0 ]; then
    echo "Waiting on subprocesses to finish execution."
    wait "${pids[@]}"
fi


# Collecting output of ceph commands
for ((i = 0; i < ${#ceph_commands2[@]}; i++)); do
    dbglog "collecting command output for: ${ceph_commands2[$i]}"
    COMMAND_OUTPUT_FILE=${COMMAND_OUTPUT_DIR}/${ceph_commands2[$i]// /_}
    JSON_COMMAND_OUTPUT_FILE=${COMMAND_JSON_OUTPUT_DIR}/${ceph_commands2[$i]// /_}_--format_json-pretty
    { timeout 120 oc --kubeconfig="${KUBECONFIG}" -n "${ns}" exec "${TOOL_POD_NAME}" -- bash -c "${ceph_commands2[$i]} --connect-timeout=15" >>"${COMMAND_OUTPUT_FILE}"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-"${ceph_commands2[$i]}"-debug.log 2>&1 &
    pids_ceph+=($!)
    { timeout 120 oc --kubeconfig="${KUBECONFIG}" -n "${ns}" exec "${TOOL_POD_NAME}" -- bash -c "${ceph_commands2[$i]} --connect-timeout=15 --format json-pretty" >>"${JSON_COMMAND_OUTPUT_FILE}"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-"${ceph_commands2[$i]}"-json-debug.log 2>&1 &
    pids_ceph+=($!)
done

# Check if PID array has any values, if so, wait for them to finish
if [ ${#pids[@]} -ne 0 ]; then
    echo "Waiting on subprocesses to finish execution."
    wait "${pids[@]}"
fi
