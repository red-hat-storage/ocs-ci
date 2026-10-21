#!/usr/bin/env bash
# Probes pod IP:port endpoints from /tmp/endpoints.txt with openssl s_client; emits CSV.
# Invoked from in-cluster scanner pods by ocs_ci.helpers.tlsprofile_helper.scan_cluster.

set -uo pipefail

TIMEOUT="${TIMEOUT:-5}"
SKIP_PORTS="${SKIP_PORTS:-22,53}"
TLS_VERSIONS="${TLS_VERSIONS:-tls1.2,tls1.3}"
TLS12_CIPHERS="${TLS12_CIPHERS:-ECDHE-ECDSA-AES128-GCM-SHA256,ECDHE-ECDSA-AES256-GCM-SHA384,ECDHE-ECDSA-CHACHA20-POLY1305,ECDHE-RSA-AES128-GCM-SHA256,ECDHE-RSA-AES256-GCM-SHA384,ECDHE-RSA-CHACHA20-POLY1305}"
TLS12_GROUPS="${TLS12_GROUPS:-prime256v1,secp384r1,secp521r1,X25519}"
TLS13_CIPHERS="${TLS13_CIPHERS:-TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384,TLS_CHACHA20_POLY1305_SHA256}"
TLS13_GROUPS="${TLS13_GROUPS:-prime256v1,secp384r1,secp521r1,X25519,X25519MLKEM768,SecP256r1MLKEM768,SecP384r1MLKEM1024}"

IFS=',' read -ra TLS_VERSIONS_ARRAY <<< "$TLS_VERSIONS"
IFS=',' read -ra TLS12_CIPHERS_ARRAY <<< "$TLS12_CIPHERS"
IFS=',' read -ra TLS12_GROUPS_ARRAY <<< "$TLS12_GROUPS"
IFS=',' read -ra TLS13_CIPHERS_ARRAY <<< "$TLS13_CIPHERS"
IFS=',' read -ra TLS13_GROUPS_ARRAY <<< "$TLS13_GROUPS"
IFS=',' read -ra SKIP_PORTS_ARRAY <<< "$SKIP_PORTS"

test_tls_handshake() {
    local ip=$1 port=$2
    local result
    result=$(echo | timeout 2 openssl s_client -connect "$ip:$port" 2>/dev/null)
    echo "$result" | grep -q "^CONNECTED"
}

test_tls_version() {
    local ip=$1 port=$2 version=$3
    local flag
    case "$version" in
        tls1)   flag="-tls1" ;;
        tls1.1) flag="-tls1_1" ;;
        tls1.2) flag="-tls1_2" ;;
        tls1.3) flag="-tls1_3" ;;
        *)      return 1 ;;
    esac
    local result
    result=$(echo | timeout "$TIMEOUT" openssl s_client -connect "$ip:$port" "$flag" 2>/dev/null)
    echo "$result" | grep -q "^New, TLSv"
}

test_tls12_cipher() {
    local ip=$1 port=$2 cipher=$3
    local result
    result=$(echo | timeout "$TIMEOUT" openssl s_client -connect "$ip:$port" -tls1_2 -cipher "$cipher" 2>/dev/null)
    echo "$result" | grep -q "Cipher is" && ! echo "$result" | grep -q "Cipher is (NONE)"
}

test_tls13_cipher() {
    local ip=$1 port=$2 cipher=$3
    local result
    result=$(echo | timeout "$TIMEOUT" openssl s_client -connect "$ip:$port" -tls1_3 -ciphersuites "$cipher" 2>/dev/null)
    echo "$result" | grep -q "Cipher is" && ! echo "$result" | grep -q "Cipher is (NONE)"
}

test_group() {
    local ip=$1 port=$2 version=$3 group=$4
    local flag
    case "$version" in
        tls1.2) flag="-tls1_2" ;;
        tls1.3) flag="-tls1_3" ;;
        *)      return 1 ;;
    esac
    local result
    result=$(echo | timeout "$TIMEOUT" openssl s_client -connect "$ip:$port" "$flag" -groups "$group" 2>/dev/null)
    echo "$result" | grep -qE "(Server|Peer) Temp Key" || echo "$result" | grep -qE "Negotiated.*: [^<]"
}

scan_endpoint() {
    local pod_ns=$1 pod_name=$2 pod_ip=$3 container_name=$4 port=$5 process=$6

    for skip in "${SKIP_PORTS_ARRAY[@]}"; do
        if [[ "$port" == "$skip" ]]; then
            echo "$pod_ns,$pod_name,$pod_ip,$container_name,$port,$process,SKIPPED,NA,NA,NA,NA,NA,Port in skip list"
            return
        fi
    done

    if ! test_tls_handshake "$pod_ip" "$port"; then
        echo "$pod_ns,$pod_name,$pod_ip,$container_name,$port,$process,NO_TLS,NA,NA,NA,NA,NA,No TLS handshake"
        return
    fi

    local supported_versions=""
    local supported_tls12_ciphers=""
    local supported_tls12_groups=""
    local supported_tls13_ciphers=""
    local supported_tls13_groups=""

    for version in "${TLS_VERSIONS_ARRAY[@]}"; do
        if ! test_tls_version "$pod_ip" "$port" "$version"; then
            continue
        fi
        supported_versions="${supported_versions:+$supported_versions }$version"

        if [[ "$version" == "tls1.2" ]]; then
            for cipher in "${TLS12_CIPHERS_ARRAY[@]}"; do
                if test_tls12_cipher "$pod_ip" "$port" "$cipher"; then
                    supported_tls12_ciphers="${supported_tls12_ciphers:+$supported_tls12_ciphers }$cipher"
                fi
            done
            for group in "${TLS12_GROUPS_ARRAY[@]}"; do
                if test_group "$pod_ip" "$port" "$version" "$group"; then
                    supported_tls12_groups="${supported_tls12_groups:+$supported_tls12_groups }$group"
                fi
            done
        elif [[ "$version" == "tls1.3" ]]; then
            for cipher in "${TLS13_CIPHERS_ARRAY[@]}"; do
                if test_tls13_cipher "$pod_ip" "$port" "$cipher"; then
                    supported_tls13_ciphers="${supported_tls13_ciphers:+$supported_tls13_ciphers }$cipher"
                fi
            done
            for group in "${TLS13_GROUPS_ARRAY[@]}"; do
                if test_group "$pod_ip" "$port" "$version" "$group"; then
                    supported_tls13_groups="${supported_tls13_groups:+$supported_tls13_groups }$group"
                fi
            done
        fi
    done

    [[ -z "$supported_versions" ]] && supported_versions="NA"
    [[ -z "$supported_tls12_ciphers" ]] && supported_tls12_ciphers="NA"
    [[ -z "$supported_tls12_groups" ]] && supported_tls12_groups="NA"
    [[ -z "$supported_tls13_ciphers" ]] && supported_tls13_ciphers="NA"
    [[ -z "$supported_tls13_groups" ]] && supported_tls13_groups="NA"

    if [[ "$supported_versions" != "NA" ]]; then
        echo "$pod_ns,$pod_name,$pod_ip,$container_name,$port,$process,OK,$supported_versions,$supported_tls12_ciphers,$supported_tls12_groups,$supported_tls13_ciphers,$supported_tls13_groups,Supports: $supported_versions"
    else
        echo "$pod_ns,$pod_name,$pod_ip,$container_name,$port,$process,NO_TLS,NA,NA,NA,NA,NA,No TLS version accepted"
    fi
}

echo "pod_namespace,pod_name,pod_ip,container_name,port,process,status,tlsversions,tls12ciphers,tls12groups,tls13ciphers,tls13groups,reason"

while IFS='|' read -r pod_ns pod_name pod_ip container_name port process; do
    [[ -z "$port" ]] && continue
    scan_endpoint "$pod_ns" "$pod_name" "$pod_ip" "$container_name" "$port" "$process" &
done < /tmp/endpoints.txt

wait
