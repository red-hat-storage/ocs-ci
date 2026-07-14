#!/usr/bin/env bash

usage() {
    cat << EOF
Generate an OCS Provider/Consumer onboarding ticket to STDOUT

USAGE: $0 [-h] [-r subjectRole] [-c storageCluster] <private_key_file>

OPTIONS:
  -h                    Show this help message
  -r subjectRole        Specify the subject role (e.g., 'ocs-client')
  -c storageCluster     Specify the storage cluster ID (UUID format)
  -q storageQuotaInGiB  Specify storage quota (int)

ARGUMENTS:
  private_key_file    A file containing a valid RSA private key.

EXAMPLES:
  Generate a ticket with default values:
    $0 my_private_key.pem

  Generate a ticket with specified subjectRole and storageCluster:
    $0 -r ocs-client -c 2918bad5-60d1-420e-9436-d19bf126bf16 -q 100 my_private_key.pem

EOF
}

# Default values
subjectRole="ocs-client"
storageCluster=""
storageQuotaInGiB=""

# Parse options
while getopts ":hr:c:q:" opt; do
    case ${opt} in
        h)
            usage
            exit 0
            ;;
        r)
            subjectRole=${OPTARG}
            ;;
        c)
            storageCluster=${OPTARG}
            ;;
    	q)
    		storageQuotaInGiB=${OPTARG}
    		;;
        \?)
            echo "Invalid option: -${OPTARG}" >&2
            usage
            exit 1
            ;;
        :)
            echo "Option -${OPTARG} requires an argument." >&2
            usage
            exit 1
            ;;
    esac
done
shift $((OPTIND -1))

# Check for private key file argument
if [ $# -lt 1 ]; then
    echo "Missing argument for key file!"
    usage
    exit 1
fi

KEY_FILE="${1}"

if [[ ! -f "${KEY_FILE}" ]]; then
    echo "Key file '${KEY_FILE}' not found!"
    usage
    exit 1
fi

# Generate a new UUID for the consumer ID
NEW_CONSUMER_ID="$(uuidgen || (tr -dc 'a-zA-Z0-9' < /dev/urandom | fold -w 36 | head -n 1) || echo "00000000-0000-0000-0000-000000000000")"

# Set expiration date to 2 days (172800 seconds) from now
EXPIRATION_DATE="$(( $(date +%s) + 172800 ))"

# Function to add variables to the JSON payload
add_var() {
  local key=$1
  local value=$2

  # Check if the value is non-empty
  if [[ -n "${value}" ]]; then
    if [[ -n "${JSON}" ]]; then
      JSON+=","
    fi

    # Keep expirationDate as a string, but store numbers as raw values
    if [[ "${key}" == "expirationDate" ]]; then
      JSON+="$(printf '"%s":"%s"' "${key}" "${value}")"
    elif [[ "${value}" =~ ^[0-9]+$ ]]; then
      JSON+="$(printf '"%s":%s' "${key}" "${value}")"
    else
      JSON+="$(printf '"%s":"%s"' "${key}" "${value}")"
    fi
  fi
}
JSON=""

# Create the JSON payload
add_var "id" "${NEW_CONSUMER_ID}"
add_var "expirationDate" "${EXPIRATION_DATE}"
add_var "subjectRole" "${subjectRole}"
add_var "storageCluster" "${storageCluster}"
add_var "storageQuotaInGiB" "${storageQuotaInGiB}"


PAYLOAD="$(echo -n "{${JSON}}" | base64 | tr -d "\n")"

SIG="$(echo -n "{${JSON}}"| openssl dgst -sha256 -sign "${KEY_FILE}" | base64 | tr -d "\n")"

cat <<< "${PAYLOAD}.${SIG}"
