#!/bin/bash

show_help()
{
  echo "Network split for cluster with 3 zones"
  echo "Usage: $(basename "${0}") [-d] <setup|teardown> <split-config>"
  echo
  echo "Argument split-config describes network split among 3 zones a, b and c"
  echo "eg. 'bc' means that connection between zones b and c is lost"
  echo "Examples of valid splits: bc, ab, ab-bc, ab-ac"
}

print_current_zone()
{
  for host_ip_addr in $(hostname -I); do
    for zone_name in ZONE_{A,B,C}; do
      for zone_host_ip_addr in ${!zone_name}; do
        if [[ "${zone_host_ip_addr}" = "${host_ip_addr}" ]]; then
          echo ${zone_name}
          exit
        fi
      done
    done
  done
}

if [[ $# = 0 ]]; then
  show_help
  exit
fi

# debug mode
if [[ $1 = "-d" ]]; then
  # this is done on purpose to print commands executed by this script instead
  # of executing them when debug mode is enabled
  # shellcheck disable=SC2209
  DEBUG_MODE=echo
  shift
else
  unset DEBUG_MODE
fi

# iptables mode (append rules or remove rules)
case $1 in
  help|-h)   show_help; exit;;
  setup)     OP="-A"; shift;;
  teardown)  OP="-D"; shift;;
  *)         show_help; exit 1
esac

# make sure expected zone env. variables are present, log their values
ERROR=0
for env_var in ZONE_A ZONE_B ZONE_C; do
  if [[ ! -v ${env_var} ]]; then
    echo "environment variable ${env_var} is not defined"
    ERROR=1
  else
    echo "$env_var=\"${!env_var}\""
  fi
done
if [[ $ERROR -eq 1 ]]; then
  exit 2
fi

# find out zone we are running in
current_zone=$(print_current_zone)

# report if we are actually running in one of the zones
if [[ -v ${current_zone} ]]; then
  echo "current zone: $current_zone"
else
  echo "current node doesn't belong to any zone, script can't continue"
  echo "output of 'hostname -I' command: $(hostname -I)"
  exit 3
fi

# load network split specification from command line
net_split_spec=${1//-/ }

# try to apply firewall rules for each network split specification
for i in ${net_split_spec}; do
  # make sure the split specification is upper case
  split=${i^^}
  # read the split configuration
  affected_zone=ZONE_${split:0:1}
  blocked_zone=ZONE_${split:1:1}
  if [[ ${OP} = "-A" ]]; then
    op_desc=blocked
  else
    op_desc="available again"
  fi
  # log and explain selected network split configuration
  echo "${i}: ${blocked_zone} will be ${op_desc} from ${affected_zone}"
  if [[ ${current_zone} = "${affected_zone}" ]]; then
    for node_addr in ${!blocked_zone}; do
      # block all packets from or to given node
      $DEBUG_MODE iptables ${OP} INPUT  -s "${node_addr}" -j DROP -v
      $DEBUG_MODE iptables ${OP} OUTPUT -d "${node_addr}" -j DROP -v
    done
  fi
done
