# IBM HCI Manual Node Power Operations

This guide explains how to perform node power operations on an IBM HCI
(Hardware Converged Infrastructure) cluster **manually** — without running
ocs-ci — using SSH, IPMI, and Redfish directly.

Use this when:
- ocs-ci is unavailable or you want to verify a node outside of a test run
- The cluster API is unreachable and you need to recover nodes
- You want to debug power management issues independently

---

## Architecture Overview

```
Test runner / you
      │
      │  SSH (rack_ssh_username / rack_ssh_password)
      ▼
  MGem / Rack host  (rackIP from rack-details JSON)
      │
      │  IPMI (Lenovo)  or  Redfish/curl (Dell)
      ▼
  Node BMC  (ipv4 / ipv6 from rack-details JSON)
```

All IPMI and Redfish commands are executed **on the MGem**, not from your
local machine.  The node BMC IPs are only routable from the MGem.

---

## Step 1 — Find Your Rack Details

The rack-details JSON is saved at:

```
<DATA_DIR>/rack_details/<cluster_name>.json
```

`DATA_DIR` defaults to `<ocs-ci-root>/data` but can be overridden with the
`OCSCI_DATA_DIR` environment variable.

Backups are also kept at:
- `<DATA_DIR>/rack_details/<cluster_name>_backup_<timestamp>.json`
- `~/<cluster_name>_rack_backup_<timestamp>.json`

> **If the file does not exist**, see
> [Step 1a — Generating the Rack Details File](#step-1a--generating-the-rack-details-file)
> before continuing.

---

## Step 1a — Generating the Rack Details File

The file is generated automatically when ocs-ci first initialises `IBMHCI`.
If it is missing entirely (first run, or the file was deleted), build it
manually using the steps below.

### Build manually (cluster API required)

Use this when the cluster is accessible.

**1. Collect BMC IPs from the kickstart ConfigMaps**

If the cluster is still partially accessible, extract the data directly:

```bash
# List all kickstart configmaps
oc get configmap -n ibm-spectrum-fusion-ns | grep kickstart-

# Dump one configmap (replace kickstart-<rack_serial> with your rack serial)
oc get configmap kickstart-<rack_serial> -n ibm-spectrum-fusion-ns -o jsonpath='{.data}' | python3 -m json.tool
```

The JSON value inside the configmap contains a
`computeNodeIntegratedManagementModules` array with each node's `OCPRole`,
`ipv4`, `ipv6`, `manufacturer`, and `secretName`.

**2. Fetch BMC credentials from node secrets**

```bash
# Get the secret for a node (secretName comes from the configmap above)
oc get secret <secretName> -n ibm-spectrum-fusion-ns \
  -o jsonpath='{.data.isfmgmtUserName}' | base64 -d && echo
oc get secret <secretName> -n ibm-spectrum-fusion-ns \
  -o jsonpath='{.data.isfmgmtUserPasswrd}' | base64 -d && echo
```

**3. Find the rackIP (MGem IP)**

The `rackIP` is not stored in the cluster — it lives in an external GitHub
rack-config file.  Ask your team/infrastructure owner for the MGem IP for
each rack serial.  It is the IP address you SSH into in Step 2.

Alternatively, check any existing backup file:

```bash
ls -lt ~/<cluster_name>_rack_backup_*.json 2>/dev/null | head -3
cat ~/<cluster_name>_rack_backup_<timestamp>.json | python3 -m json.tool \
  | grep -A2 rackIP
```

**4. Write the JSON file**

Create the directory and write the file manually:

```bash
mkdir -p <DATA_DIR>/rack_details

cat > <DATA_DIR>/rack_details/<cluster_name>.json <<'EOF'
{
  "<rack_serial>": {
    "nodes": {
      "<node_role>": {
        "ipv4": "<bmc_ipv4>",
        "ipv6": null,
        "manufacturer": "Lenovo",
        "role": "master",
        "username": "<bmc_username>",
        "password": "<bmc_password>"
      }
    },
    "rackInfo": {
      "ibmSerialNumber": "<rack_serial_upper>",
      "ibmMTM": "9155-R42",
      "rackGen": 2,
      "storageType": "fdf",
      "isfModel": "9155",
      "ipStack": "v4",
      "rackIP": "<mgem_ip>"
    }
  }
}
EOF

# Restrict permissions (file contains BMC passwords)
chmod 600 <DATA_DIR>/rack_details/<cluster_name>.json
```

**5. Verify the file is valid JSON**

```bash
python3 -m json.tool <DATA_DIR>/rack_details/<cluster_name>.json > /dev/null \
  && echo "JSON OK" || echo "JSON INVALID"
```

**6. Verify BMC reachability from the MGem**

SSH into the MGem and ping each BMC IP:

```bash
ssh <rack_ssh_username>@<rackIP>
ping -c 1 -W 5 <bmc_ipv4>
```

Once the file exists and the pings succeed, proceed from Step 2.

---

**Example config (multi-AZ cluster):**

```json
{
  "<rack_serial_1>": {
    "nodes": {
      "<node_role_1>": {
        "ipv4": "<bmc_ipv4_1>",
        "ipv6": null,
        "manufacturer": "Lenovo",
        "role": "master",
        "username": "<bmc_username>",
        "password": "<bmc_password>"
      },
      "<node_role_2>": {
        "ipv4": "<bmc_ipv4_2>",
        "ipv6": null,
        "manufacturer": "Lenovo",
        "role": "worker",
        "username": "<bmc_username>",
        "password": "<bmc_password>"
      }
    },
    "rackInfo": {
      "rackIP": "<mgem_ip_1>"
    }
  },
  "<rack_serial_2>": {
    "nodes": {
      "<node_role_1>": {
        "ipv4": "<bmc_ipv4_3>",
        "ipv6": null,
        "manufacturer": "Lenovo",
        "role": "master",
        "username": "<bmc_username>",
        "password": "<bmc_password>"
      }
    },
    "rackInfo": {
      "rackIP": "<mgem_ip_2>"
    }
  }
}
```

Key fields used in every operation:

| Field | Description |
|---|---|
| `rackIP` | IP of the MGem — the SSH hop host |
| `ipv4` / `ipv6` | BMC IP of the node (reachable only from MGem) |
| `username` / `password` | BMC credentials (IPMI or Redfish) |
| `manufacturer` | `Lenovo` → use IPMI; `Dell` → use Redfish |

---

## Step 2 — SSH into the MGem

```bash
ssh <rack_ssh_username>@<mgem_ip>
```

All commands in Steps 3–6 are run **inside this SSH session**.

---

## Step 3 — Verify BMC Reachability (Ping)

Before running any power command, confirm the node BMC is reachable from
the MGem.

**IPv4:**
```bash
ping -c 1 -W 5 <bmc_ipv4>
```

**IPv6:**
```bash
ping6 -c 1 -W 5 <bmc_ipv6>
```

A successful ping (`0% packet loss`) confirms the network path is healthy.
If ping fails, try another rack's MGem (see [Multi-AZ fallback](#multi-az-mgem-fallback)).

---

## Step 4 — Lenovo Nodes (IPMI)

All Lenovo operations use `ipmitool` via `lanplus`.

### Install ipmitool (if not present)

```bash
which ipmitool || yum install -y ipmitool || dnf install -y ipmitool
```

### Power status

```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power status
# Expected output: "Chassis Power is on"  or  "Chassis Power is off"
```

**IPv6:**
```bash
ipmitool -I lanplus -H "<bmc_ipv6>" -U <bmc_username> -P '<bmc_password>' power status
```
> Note: IPv6 addresses must be **quoted** for ipmitool.

### Power on

```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power on
```

### Power off (graceful)

```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power soft
```

### Power off (force)

```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power off
```

### Power cycle

```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power cycle
```

### Power reset

```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power reset
```

---

## Step 5 — Dell Nodes (Redfish)

All Dell operations use `curl` against the Redfish REST API.

> IPv6 addresses must be wrapped in `[brackets]` in URLs.

### Discover the Systems URI

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' \
  https://<bmc_ipv4>/redfish/v1/Systems
```

From the JSON response, note the `@odata.id` of the first member, e.g.
`/redfish/v1/Systems/System.Embedded.1`.  Use this as `<SYSTEM_URI>` below.

### Power status

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' \
  https://<bmc_ipv4><SYSTEM_URI> | grep -i powerstate
# Expected: "PowerState": "On"  or  "PowerState": "Off"
```

### Power on

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' -X POST \
  https://<bmc_ipv4><SYSTEM_URI>/Actions/ComputerSystem.Reset \
  -H 'Content-Type: application/json' \
  -d '{"ResetType": "On"}'
```

### Power off (graceful)

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' -X POST \
  https://<bmc_ipv4><SYSTEM_URI>/Actions/ComputerSystem.Reset \
  -H 'Content-Type: application/json' \
  -d '{"ResetType": "GracefulShutdown"}'
```

### Power off (force)

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' -X POST \
  https://<bmc_ipv4><SYSTEM_URI>/Actions/ComputerSystem.Reset \
  -H 'Content-Type: application/json' \
  -d '{"ResetType": "ForceOff"}'
```

### Power cycle

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' -X POST \
  https://<bmc_ipv4><SYSTEM_URI>/Actions/ComputerSystem.Reset \
  -H 'Content-Type: application/json' \
  -d '{"ResetType": "PowerCycle"}'
```

### Power reset (graceful)

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' -X POST \
  https://<bmc_ipv4><SYSTEM_URI>/Actions/ComputerSystem.Reset \
  -H 'Content-Type: application/json' \
  -d '{"ResetType": "GracefulRestart"}'
```

### Power reset (force)

```bash
curl -k -sS -f \
  -u <bmc_username>:'<bmc_password>' -X POST \
  https://<bmc_ipv4><SYSTEM_URI>/Actions/ComputerSystem.Reset \
  -H 'Content-Type: application/json' \
  -d '{"ResetType": "ForceRestart"}'
```

---

## Step 6 — Power on All Nodes (Recovery)

When the cluster API is unreachable and all nodes are off, power them all
on from the MGem without any ocs-ci involvement.

Repeat the following for each rack in your cluster, substituting the
rack's MGem IP and each node's BMC IP from the rack-details JSON:

```bash
ssh <rack_ssh_username>@<mgem_ip>

# Power on each node in this rack (one BMC IP per node)
for BMC_IP in <bmc_ipv4_1> <bmc_ipv4_2> <bmc_ipv4_3>; do
  echo "Powering on $BMC_IP"
  ipmitool -I lanplus -H $BMC_IP -U <bmc_username> -P '<bmc_password>' power on
done
```

Wait ~5 minutes then verify each node booted:

```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power status
```

---

## Multi-AZ MGem Fallback

In a multi-AZ cluster each rack has its own MGem (`rackIP`).  If a node's
BMC is not reachable from its own rack's MGem, try another rack's MGem.

Look up the `rackIP` values for all racks from the rack-details JSON, then:

```bash
# BMC not reachable from its own rack's MGem?
# SSH into another rack's MGem and try from there:
ssh <rack_ssh_username>@<other_mgem_ip>
ping -c 1 -W 5 <bmc_ipv4>
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' power status
```

---

## Quick Reference

| Operation | Lenovo (IPMI) | Dell (Redfish ResetType) |
|---|---|---|
| Status | `power status` | GET `<SYSTEM_URI>` → `PowerState` |
| Power on | `power on` | `On` |
| Power off graceful | `power soft` | `GracefulShutdown` |
| Power off force | `power off` | `ForceOff` |
| Power cycle | `power cycle` | `PowerCycle` |
| Reset graceful | `power reset` | `GracefulRestart` |
| Reset force | `power reset` (with force) | `ForceRestart` |

---

## Troubleshooting

**`ipmitool: command not found`**
```bash
yum install -y ipmitool   # RHEL/CentOS
dnf install -y ipmitool   # Fedora/newer RHEL
```

**`Error in open session response message : insufficient resources for session`**
Too many open IPMI sessions on the BMC.  Close them:
```bash
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' session info all
ipmitool -I lanplus -H <bmc_ipv4> -U <bmc_username> -P '<bmc_password>' session deactivate <session-id>
```

**`curl: (22) The requested URL returned error: 401`**
Wrong Redfish credentials.  Verify `username`/`password` from the rack-details JSON.

**Ping succeeds but IPMI/Redfish fails**
- Check credentials in the rack-details JSON (`username`, `password` fields).
- Verify ipmitool is installed on the MGem.
- For Dell nodes, ensure the Systems URI is correct by re-running the
  discovery curl command.

**Node BMC not pingable from own MGem**
- Try another rack's MGem (see [Multi-AZ MGem Fallback](#multi-az-mgem-fallback)).
- If no MGem can reach the BMC, there is a network-level issue that must
  be resolved before power operations are possible.
