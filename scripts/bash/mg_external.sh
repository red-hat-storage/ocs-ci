#!/usr/bin/env bash

# redirect the debugging output to stdout
exec 2>&1
set -x

# Function to print usage information
usage() {
    echo "Usage: $0 [BASE_COLLECTION_PATH [KUBECONFIG [NAMESPACE]]]"
    echo
    echo "Parameters:"
    echo "  BASE_COLLECTION_PATH   Optional. Path where debug logs will be stored. Default is the current directory."
    echo "  KUBECONFIG            Optional. Path to the kubeconfig file. Default is '~/.kube/config'."
    echo "  NAMESPACE             Optional. OpenShift namespace. Default is 'openshift-storage'."
    exit 0
}

# Check for help flag
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
fi

dbglog() {
    # Allow the input to be piped
    declare msg=${1:-$(</dev/stdin)}

    echo -e "${msg}" | tee -a "${BASE_COLLECTION_PATH}/external-ceph-gather.log"
}


# Expect base collection path as an exported variable
# If it is not defined, use PWD instead
BASE_COLLECTION_PATH=${1:-"$(pwd)"}
mkdir -p $BASE_COLLECTION_PATH
dbglog  "ceph commands for external cluster will be collected at ${BASE_COLLECTION_PATH}"

KUBECONFIG=${2:-"~/.kube/config"}
NS=${3:-"openshift-storage"}

TOOL_POD_NAME=$(oc get pods --no-headers -n "${NS}" -l app='rook-ceph-tools' | awk '{print $1}')
if [ -z "$TOOL_POD_NAME" ]; then
    dbglog "No tool pod found"
    exit 2
fi


CEPH_COLLECTION_PATH="${BASE_COLLECTION_PATH}/ceph"
COMMAND_OUTPUT_DIR=${CEPH_COLLECTION_PATH}/must_gather_commands
COMMAND_JSON_OUTPUT_DIR=${CEPH_COLLECTION_PATH}/must_gather_commands_json_output
COMMAND_ERR_OUTPUT_DIR=${CEPH_COLLECTION_PATH}/logs
mkdir -p "${COMMAND_OUTPUT_DIR}"
mkdir -p "${COMMAND_JSON_OUTPUT_DIR}"
mkdir -p "${COMMAND_ERR_OUTPUT_DIR}"


pids_ceph=()

# Ceph commands
ceph_commands=()
ceph_commands+=("ceph auth list")
ceph_commands+=("ceph balancer pool ls")
ceph_commands+=("ceph balancer status")
ceph_commands+=("ceph config dump")
ceph_commands+=("ceph config-key ls")
ceph_commands+=("ceph crash ls")
ceph_commands+=("ceph crash stat")
ceph_commands+=("ceph device ls")
ceph_commands+=("ceph df detail")
ceph_commands+=("ceph fs dump")
ceph_commands+=("ceph fs ls")
ceph_commands+=("ceph fs status")
ceph_commands+=("ceph health detail")
ceph_commands+=("ceph healthcheck history ls")
ceph_commands+=("ceph mds stat")
ceph_commands+=("ceph mgr dump")
ceph_commands+=("ceph mgr module ls")
ceph_commands+=("ceph mgr services")
ceph_commands+=("ceph mon stat")
ceph_commands+=("ceph mon dump")
ceph_commands+=("ceph osd df tree")
ceph_commands+=("ceph osd tree")
ceph_commands+=("ceph osd stat")
ceph_commands+=("ceph osd dump")
ceph_commands+=("ceph osd utilization")
ceph_commands+=("ceph osd crush show-tunables")
ceph_commands+=("ceph osd crush dump")
ceph_commands+=("ceph osd crush weight-set ls")
ceph_commands+=("ceph osd crush weight-set dump")
ceph_commands+=("ceph osd crush rule dump")
ceph_commands+=("ceph osd crush rule ls")
ceph_commands+=("ceph osd crush class ls")
ceph_commands+=("ceph osd perf")
ceph_commands+=("ceph osd numa-status")
ceph_commands+=("ceph osd getmaxosd")
ceph_commands+=("ceph osd pool ls detail")
ceph_commands+=("ceph osd lspools")
ceph_commands+=("ceph osd df")
ceph_commands+=("ceph osd blocked-by")
ceph_commands+=("ceph osd blacklist ls")
ceph_commands+=("ceph osd pool autoscale-status")
ceph_commands+=("ceph pg dump")
ceph_commands+=("ceph pg stat")
ceph_commands+=("ceph progress")
ceph_commands+=("ceph progress json")
ceph_commands+=("ceph quorum_status")
ceph_commands+=("ceph rbd task list")
ceph_commands+=("ceph report")
ceph_commands+=("ceph service dump")
ceph_commands+=("ceph status")
ceph_commands+=("ceph time-sync-status")
ceph_commands+=("ceph versions")
ceph_commands+=("ceph log last 10000 debug cluster")
ceph_commands+=("ceph log last 10000 debug audit")
ceph_commands+=("rados lspools")
ceph_commands+=("rados ls --pool=ocs-storagecluster-cephblockpool")
ceph_commands+=("rados ls --pool=ocs-storagecluster-cephfilesystem-metadata --namespace=csi")

# Counter for batching the commands
batch_size=10

# Collecting output of ceph osd config
for i in $(timeout 120 oc -n "${NS}" exec "${TOOL_POD_NAME}" -- bash -c "ceph osd tree --connect-timeout=15 |  grep up " | awk '{print $4}'); do
    { timeout 120 oc -n "${NS}" exec "${TOOL_POD_NAME}" -- bash -c "ceph config show $i" >>"${COMMAND_OUTPUT_DIR}/config_$i"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-config-"$i"-debug.log 2>&1 &
    pids_ceph+=($!)
done
# Check if PID array has any values, if so, wait for them to finish
if [ ${#pids_ceph[@]} -ne 0 ]; then
    dbglog "Waiting on subprocesses to finish execution."
    wait "${pids_ceph[@]}"
fi


# Collecting output of ceph commands
for ((i = 0; i < ${#ceph_commands[@]}; i++)); do
    dbglog "collecting command output for: ${ceph_commands[$i]}"
    COMMAND_OUTPUT_FILE=${COMMAND_OUTPUT_DIR}/${ceph_commands[$i]// /_}
    JSON_COMMAND_OUTPUT_FILE=${COMMAND_JSON_OUTPUT_DIR}/${ceph_commands[$i]// /_}_--format_json-pretty
    { timeout 120 oc --kubeconfig="${KUBECONFIG}" -n "${NS}" exec "${TOOL_POD_NAME}" -- bash -c "${ceph_commands[$i]} --connect-timeout=15" >>"${COMMAND_OUTPUT_FILE}"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-"${ceph_commands[$i]}"-debug.log 2>&1 &
    pids_ceph+=($!)
    { timeout 120 oc --kubeconfig="${KUBECONFIG}" -n "${NS}" exec "${TOOL_POD_NAME}" -- bash -c "${ceph_commands[$i]} --connect-timeout=15 --format json-pretty" >>"${JSON_COMMAND_OUTPUT_FILE}"; } >>"${COMMAND_ERR_OUTPUT_DIR}"/gather-"${ceph_commands[$i]}"-json-debug.log 2>&1 &
    pids_ceph+=($!)
    # If batch_size is reached or last command, wait for processes to finish
    if (( (i + 1) % batch_size == 0 || i + 1 == ${#ceph_commands[@]} )); then
        dbglog "Waiting on subprocesses to finish execution for batch."
        wait "${pids_ceph[@]}"
        # Reset pids array after waiting
        pids_ceph=()
    fi
done
