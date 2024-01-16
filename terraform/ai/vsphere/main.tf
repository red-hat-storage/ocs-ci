// prepare some local variables
locals {
  control_planes = [for idx in range(var.control_plane_count) : "${var.cluster_id}-control-plane-${idx}"]
  compute_nodes  = [for idx in range(var.compute_count) : "${var.cluster_id}-compute-${idx}"]
  guest_id       = "rhel8_64Guest"
  dns_zone_id    = one(data.aws_route53_zone.dns_zone[*].zone_id)
}

// configure connection to vSphere
provider "vsphere" {
  user                 = var.vsphere_user
  password             = var.vsphere_password
  vsphere_server       = var.vsphere_server
  allow_unverified_ssl = true
}

// get vSphere DC
data "vsphere_datacenter" "dc" {
  name = var.vsphere_datacenter
}

// get vSphere Cluster
data "vsphere_compute_cluster" "compute_cluster" {
  name          = var.vsphere_cluster
  datacenter_id = data.vsphere_datacenter.dc.id
}

// get vSphere Data Store
data "vsphere_datastore" "datastore" {
  name          = var.vsphere_datastore
  datacenter_id = data.vsphere_datacenter.dc.id
}

// get vSphere storage policy
data "vsphere_storage_policy" "storage_policy" {
  name = var.vsphere_storage_policy
}

// get vSphere Network
data "vsphere_network" "network" {
  name                            = var.vm_network
  datacenter_id                   = data.vsphere_datacenter.dc.id
  distributed_virtual_switch_uuid = ""
}

// get DNS zone for creating API and Ingress A records
data "aws_route53_zone" "dns_zone" {
  count = var.base_domain != null ? 1 : 0
  name  = var.base_domain
}

// create DNS A record for API (only if api_ip is defined)
resource "aws_route53_record" "api_a_record" {
  count   = var.api_ip != null ? 1 : 0
  type    = "A"
  ttl     = "60"
  zone_id = local.dns_zone_id
  name    = "api.${var.cluster_id}.${var.base_domain}"
  records = [var.api_ip]
}

// create DNS A record for Ingress (only if ingress_ip is defined)
resource "aws_route53_record" "ingress_a_record" {
  count   = var.ingress_ip != null ? 1 : 0
  type    = "A"
  ttl     = "60"
  zone_id = local.dns_zone_id
  name    = "*.apps.${var.cluster_id}.${var.base_domain}"
  records = [var.ingress_ip]
}

// create Resource Pool for VMs
resource "vsphere_resource_pool" "resource_pool" {
  name                    = var.cluster_id
  parent_resource_pool_id = data.vsphere_compute_cluster.compute_cluster.resource_pool_id
}

// create Folder for VMs
resource "vsphere_folder" "folder" {
  path          = var.cluster_id
  type          = "vm"
  datacenter_id = data.vsphere_datacenter.dc.id
}

// upload discovery iso to vSphere data store to /discovery-iso directory
resource "vsphere_file" "discovery_iso" {
  datacenter         = var.vsphere_datacenter
  datastore          = var.vsphere_datastore
  source_file        = var.iso_image
  destination_file   = "/discovery-iso/${var.cluster_id}-discovery.iso"
  create_directories = true
}

// create Control Plane VMs
module "control_plane_vm" {
  count             = var.control_plane_count
  source            = "./vm"
  vmname            = local.control_planes[count.index]
  resource_pool_id  = vsphere_resource_pool.resource_pool.id
  datastore_id      = data.vsphere_datastore.datastore.id
  network_id        = data.vsphere_network.network.id
  folder_id         = vsphere_folder.folder.path
  guest_id          = local.guest_id
  num_cpus          = var.control_plane_num_cpus
  memory            = var.control_plane_memory
  system_disk_size  = var.system_disk_size
  data_disks_count  = var.control_plane_data_disks_count
  data_disks_size   = var.control_plane_data_disks_size
  storage_policy_id = data.vsphere_storage_policy.storage_policy.id
  iso_image         = vsphere_file.discovery_iso.destination_file
  nested_hv_enabled = true
}

// create Compute VMs
module "compute_vm" {
  count             = var.compute_count
  source            = "./vm"
  vmname            = local.compute_nodes[count.index]
  resource_pool_id  = vsphere_resource_pool.resource_pool.id
  datastore_id      = data.vsphere_datastore.datastore.id
  network_id        = data.vsphere_network.network.id
  folder_id         = vsphere_folder.folder.path
  guest_id          = local.guest_id
  num_cpus          = var.compute_num_cpus
  memory            = var.compute_memory
  system_disk_size  = var.system_disk_size
  data_disks_count  = var.compute_data_disks_count
  data_disks_size   = var.compute_data_disks_size
  storage_policy_id = data.vsphere_storage_policy.storage_policy.id
  iso_image         = vsphere_file.discovery_iso.destination_file
  nested_hv_enabled = true
}
