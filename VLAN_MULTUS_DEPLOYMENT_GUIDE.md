# VLAN-based Multus Deployment Guide for OCS-CI

## Overview

This guide documents the new VLAN-based multus deployment approach for bare metal ODF deployments. This replaces the problematic "shim interface" approach with proper VLAN tagging, based on Red Hat's official solution.

## Problem with Current (Shim-based) Approach

The current deployment creates intermediate "shim" interfaces which cause issues:

1. **Shim interfaces** (192.168.252.x) - unnecessary intermediate layer
2. **Extra routes** to/from shim network - adds complexity
3. **Network conflicts** - shim IPs can conflict with actual pod IPs
4. **Higher retransmits** - 260+ retransmits observed
5. **Routing inefficiency** - packets go through unnecessary hops

## New VLAN-based Approach

Uses proper VLAN tagging as recommended by Red Hat:

1. **VLAN interfaces** directly on physical NIC (e.g., enp1s0f1.201)
2. **No shim IPs** - cleaner configuration
3. **No extra routes** - direct VLAN communication
4. **Better performance** - expected 40-60% reduction in retransmits

---

## Files Created

### 1. Configuration Files (conf/deployment/baremetal/)

#### Full Dual-Network Configuration
**File:** `upi_1az_rhcos_multus_nvme_intel_3m_3w_vlan.yaml`
- Public network on VLAN 201 (192.168.20.0/24)
- Cluster network on VLAN 202 (192.168.30.0/24)
- Use for complete dual-network deployments

#### Public Network Only (Recommended for Testing)
**File:** `upi_1az_rhcos_multus_nvme_intel_3m_3w_vlan_public_only.yaml`
- Public network on VLAN 201 only
- Simpler scenario for initial validation
- **Start with this one!**

#### Cluster Network Only
**File:** `upi_1az_rhcos_multus_nvme_intel_3m_3w_vlan_cluster_only.yaml`
- Cluster network on VLAN 202 only
- For private Ceph traffic separation

### 2. Template Files (ocs_ci/templates/ocs-deployment/)

#### NodeNetworkConfigurationPolicy Templates

**Single VLAN:** `node_network_configuration_policy_vlan.yaml`
```yaml
# Creates single VLAN interface (e.g., enp1s0f1.201)
# No IP address on VLAN interface
# No shim, no routes
```

**Dual VLAN:** `node_network_configuration_policy_vlan_dual.yaml`
```yaml
# Creates both public and cluster VLAN interfaces
# (e.g., enp1s0f1.201 and enp1s0f1.202)
```

#### NetworkAttachmentDefinition Templates

**Public Network:** `multus-public-net-vlan.yaml`
```yaml
# Uses VLAN interface as master (enp1s0f1.201)
# Includes IP range limits (range_start, range_end)
# No routes section needed
```

**Cluster Network:** `multus-cluster-net-vlan.yaml`
```yaml
# Uses VLAN interface as master (enp1s0f1.202)
# Includes IP range limits
```

### 3. Constants Added (ocs_ci/ocs/constants.py)

```python
NODE_NETWORK_CONFIGURATION_POLICY_VLAN = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "node_network_configuration_policy_vlan.yaml"
)
NODE_NETWORK_CONFIGURATION_POLICY_VLAN_DUAL = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "node_network_configuration_policy_vlan_dual.yaml"
)
MULTUS_PUBLIC_NET_VLAN_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "multus-public-net-vlan.yaml"
)
MULTUS_CLUSTER_NET_VLAN_YAML = os.path.join(
    TEMPLATE_DEPLOYMENT_DIR, "multus-cluster-net-vlan.yaml"
)
```

---

## Configuration Parameters

### New Parameters in Config Files

```yaml
# Enable VLAN mode (disables shim creation)
multus_use_vlan: true

# Public Network VLAN Configuration
multus_public_net_vlan_id: 201
multus_public_net_ip_range: '192.168.20.0/24'
multus_public_net_ip_range_start: '192.168.20.10'
multus_public_net_ip_range_end: '192.168.20.250'

# Cluster Network VLAN Configuration
multus_cluster_net_vlan_id: 202
multus_cluster_net_ip_range: '192.168.30.0/24'
multus_cluster_net_ip_range_start: '192.168.30.10'
multus_cluster_net_ip_range_end: '192.168.30.250'

# Existing parameters still work
multus_public_net_interface: 'enp1s0f1'  # Base physical interface
multus_cluster_net_interface: 'enp1s0f1'  # Base physical interface
multus_public_net_namespace: 'openshift-storage'
multus_cluster_net_namespace: 'openshift-storage'
multus_public_net_type: 'macvlan'  # or 'bridge'
multus_public_net_mode: 'bridge'
```

---

## Code Changes Required

### Primary File to Modify

**File:** `ocs_ci/helpers/helpers.py`

**Function:** `configure_node_network_configuration_policy_on_all_worker_nodes()`

#### Current Logic (Shim-based)
```python
# Around line 5508
node_network_configuration_policy = templating.load_yaml(
    constants.NODE_NETWORK_CONFIGURATION_POLICY
)

# Creates shim interface with IP (192.168.252.x)
shim_ip = str(ipaddress.ip_address(shim_default_ip) + interface_num)
node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
    ip_version
]["address"][0]["ip"] = shim_ip

# Adds routes to actual multus network
node_network_configuration_policy["spec"]["desiredState"]["routes"]["config"][0][
    "destination"
] = "192.168.20.0/24"
```

#### New Logic Needed (VLAN-based)
```python
# Check if VLAN mode is enabled
if config.ENV_DATA.get("multus_use_vlan"):
    # Determine which template to use
    if (config.ENV_DATA.get("multus_create_public_net") and
        config.ENV_DATA.get("multus_create_cluster_net")):
        # Use dual VLAN template
        node_network_configuration_policy = templating.load_yaml(
            constants.NODE_NETWORK_CONFIGURATION_POLICY_VLAN_DUAL
        )
        # Configure both VLAN interfaces
        public_vlan_id = config.ENV_DATA.get("multus_public_net_vlan_id", 201)
        cluster_vlan_id = config.ENV_DATA.get("multus_cluster_net_vlan_id", 202)
        base_interface = config.ENV_DATA.get("multus_public_net_interface", "enp1s0f1")

        # Public VLAN (interface 0)
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "name"
        ] = f"{base_interface}.{public_vlan_id}"
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "vlan"
        ]["base-iface"] = base_interface
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "vlan"
        ]["id"] = public_vlan_id

        # Cluster VLAN (interface 1)
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][1][
            "name"
        ] = f"{base_interface}.{cluster_vlan_id}"
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][1][
            "vlan"
        ]["base-iface"] = base_interface
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][1][
            "vlan"
        ]["id"] = cluster_vlan_id

    elif config.ENV_DATA.get("multus_create_public_net"):
        # Use single VLAN template for public network
        node_network_configuration_policy = templating.load_yaml(
            constants.NODE_NETWORK_CONFIGURATION_POLICY_VLAN
        )
        vlan_id = config.ENV_DATA.get("multus_public_net_vlan_id", 201)
        base_interface = config.ENV_DATA.get("multus_public_net_interface", "enp1s0f1")

        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "name"
        ] = f"{base_interface}.{vlan_id}"
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "vlan"
        ]["base-iface"] = base_interface
        node_network_configuration_policy["spec"]["desiredState"]["interfaces"][0][
            "vlan"
        ]["id"] = vlan_id

    # No shim IP assignment
    # No route configuration needed

else:
    # Keep existing shim-based logic for backward compatibility
    node_network_configuration_policy = templating.load_yaml(
        constants.NODE_NETWORK_CONFIGURATION_POLICY
    )
    # ... existing shim code ...
```

### NetworkAttachmentDefinition Updates

**Location:** Where NAD YAMLs are loaded and applied

```python
# When creating public network NAD
if config.ENV_DATA.get("multus_use_vlan"):
    public_net_yaml = templating.load_yaml(constants.MULTUS_PUBLIC_NET_VLAN_YAML)

    # Update VLAN interface name in master field
    vlan_id = config.ENV_DATA.get("multus_public_net_vlan_id", 201)
    base_interface = config.ENV_DATA.get("multus_public_net_interface", "enp1s0f1")
    vlan_interface = f"{base_interface}.{vlan_id}"

    # Parse the config JSON
    nad_config = yaml.safe_load(public_net_yaml["spec"]["config"])
    nad_config["master"] = vlan_interface

    # Update IP range if specified
    if config.ENV_DATA.get("multus_public_net_ip_range"):
        nad_config["ipam"]["range"] = config.ENV_DATA["multus_public_net_ip_range"]
    if config.ENV_DATA.get("multus_public_net_ip_range_start"):
        nad_config["ipam"]["range_start"] = config.ENV_DATA["multus_public_net_ip_range_start"]
    if config.ENV_DATA.get("multus_public_net_ip_range_end"):
        nad_config["ipam"]["range_end"] = config.ENV_DATA["multus_public_net_ip_range_end"]

    # Remove routes if present (not needed for VLAN)
    nad_config["ipam"].pop("routes", None)

    # Update the spec config
    public_net_yaml["spec"]["config"] = json.dumps(nad_config)
else:
    # Use existing shim-based NAD
    public_net_yaml = templating.load_yaml(constants.MULTUS_PUBLIC_NET_YAML)
```

---

## Switch Configuration Required

For VLAN-based deployment to work, network switches must be configured as trunk ports:

```bash
# Example for Cisco/Arista switches:
interface Ethernet1/1  # Port connected to node
  switchport mode trunk
  switchport trunk native vlan 200       # Baremetal management (untagged)
  switchport trunk allowed vlan 200-202  # Allow VLANs 200, 201, 202
```

For each node, configure the switch port to:
- Mode: **trunk** (not access)
- Native VLAN: Your baremetal management VLAN (e.g., 200)
- Allowed VLANs: Native + 201 (public) + 202 (cluster)

---

## Testing the Deployment

### 1. Verify VLAN Interfaces Created

```bash
# SSH to a worker node
for node in argo005 argo006 argo007; do
  echo "=== Checking $node ==="
  ssh $node "ip link show enp1s0f1.201"
  ssh $node "ip link show enp1s0f1.202"  # If using dual networks
done
```

Expected output:
```
enp1s0f1.201@enp1s0f1: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue state UP mode DEFAULT
    link/ether aa:bb:cc:dd:ee:ff brd ff:ff:ff:ff:ff:ff
```

### 2. Verify NetworkAttachmentDefinitions

```bash
oc get network-attachment-definitions -n openshift-storage
```

Expected output:
```
NAME          AGE
public-net    5m
cluster-net   5m
```

### 3. Verify Pods Get VLAN IPs

```bash
oc get pods -n openshift-storage -o wide
oc describe pod <osd-pod-name> | grep -A 10 "k8s.v1.cni.cncf.io/network-status"
```

Should show IPs in the configured ranges:
- Public: 192.168.20.10 - 192.168.20.250
- Cluster: 192.168.30.10 - 192.168.30.250

### 4. Test Network Performance

Check retransmits (should be 40-60% lower than shim-based):
```bash
# From a worker node
netstat -s | grep retransmit
```

### 5. Verify No Shim Interfaces

```bash
# Should return empty
ssh argo005 "ip link show odf-pub-shim" 2>&1 | grep "does not exist"
```

---

## Comparison: Shim vs VLAN

| Aspect | Shim-based (Current) | VLAN-based (New) |
|--------|---------------------|------------------|
| **Interfaces** | Physical + Shim (192.168.252.x) | Physical + VLAN (enp1s0f1.201) |
| **IP Assignment** | Shim gets static IP | VLAN interface has no IP |
| **Routes** | Routes to/from shim network | No extra routes needed |
| **NAD Master** | Physical interface (enp1s0f1) | VLAN interface (enp1s0f1.201) |
| **NAD Routes** | `[{"dst": "192.168.252.0/24"}]` | Not needed (removed) |
| **Complexity** | Higher (3 networks: actual + shim + pod) | Lower (2 networks: VLAN + pod) |
| **Retransmits** | 260+ | Expected 100-150 (40-60% better) |
| **Switch Config** | Can work with access ports | Requires trunk ports with VLAN tags |
| **Red Hat Support** | Not officially documented | Officially documented solution |

---

## Backward Compatibility

The code changes should maintain backward compatibility by:

1. Checking `multus_use_vlan` flag before using VLAN templates
2. Falling back to shim-based approach if flag is false/absent
3. Keeping all existing config parameters working
4. Not removing old templates or constants

---

## Next Steps

1. **Implement Code Changes**
   - Modify `configure_node_network_configuration_policy_on_all_worker_nodes()` in helpers.py
   - Add NAD template selection logic based on `multus_use_vlan` flag
   - Add VLAN interface name generation logic

2. **Add Unit Tests**
   - Test VLAN interface creation
   - Test NAD configuration with VLANs
   - Test backward compatibility with shim-based approach

3. **Test Deployment**
   - Start with public-only VLAN config
   - Verify switch configuration
   - Check VLAN interfaces created
   - Validate pod IPs and connectivity
   - Measure network performance improvement

4. **Documentation**
   - Update deployment docs with switch requirements
   - Add troubleshooting guide for VLAN issues
   - Document expected performance improvements

---

## Troubleshooting

### VLAN interfaces not created
- Check NodeNetworkConfigurationPolicy status: `oc get nncp -n openshift-storage`
- Check nmstate operator logs
- Verify switch trunk configuration

### Pods not getting IPs from VLAN network
- Check NetworkAttachmentDefinition: `oc get net-attach-def -n openshift-storage -o yaml`
- Verify VLAN interface exists on node
- Check whereabouts IP allocator logs

### VLAN traffic not working
- Verify switch allows VLAN IDs (201, 202)
- Check VLAN interface state: `ip link show enp1s0f1.201`
- Test VLAN tagging: `tcpdump -i enp1s0f1 -e vlan`

---

## References

- Red Hat Solution: Network separation using VLANs with multus
- ODF Documentation: Multus networking for bare metal
- nmstate Documentation: VLAN interface configuration
