//////
// vSphere variables
//////

variable "vsphere_server" {
  type        = string
  description = "This is the vSphere server for the environment."
}

variable "vsphere_user" {
  type        = string
  description = "vSphere server user for the environment."
}

variable "vsphere_password" {
  type        = string
  description = "vSphere server password"
}

variable "vsphere_cluster" {
  type        = string
  default     = ""
  description = "This is the name of the vSphere cluster."
}

variable "vsphere_datacenter" {
  type        = string
  default     = ""
  description = "This is the name of the vSphere data center."
}

variable "vsphere_datastore" {
  type        = string
  default     = ""
  description = "This is the name of the vSphere data store."
}
variable "vm_network" {
  type        = string
  description = "This is the name of the publicly accessible network for cluster ingress and access."
  default     = "VM Network"
}

///////////
// cluster/all nodes related variables
///////////

variable "cluster_id" {
  type        = string
  description = "This cluster id must be of max length 27 and must have only alphanumeric or hyphen characters."
}

variable "iso_image" {
  type = string
}

variable "system_disk_size" {
  type    = string
  default = "120"
}

variable "base_domain" {
  type        = string
  default     = null
  description = "Base DNS domain, where should be the cluster records created"
}

variable "api_ip" {
  type        = string
  default     = null
  description = "API IP address, if not defined DNS record is not created"
}

variable "ingress_ip" {
  type        = string
  default     = null
  description = "Ingress IP address, if not defined DNS record is not created"
}

///////////
// control-plane machine variables
///////////

variable "control_plane_count" {
  type    = string
  default = "3"
}

variable "control_plane_memory" {
  type    = string
  default = "16384"
}

variable "control_plane_num_cpus" {
  type    = string
  default = "4"
}

//////////
// compute machine variables
//////////

variable "compute_count" {
  type    = string
  default = "3"
}

variable "compute_memory" {
  type    = string
  default = "65536"
}

variable "compute_num_cpus" {
  type    = string
  default = "16"
}

variable "compute_data_disks_count" {
  type    = string
  default = "2"
}

variable "compute_data_disks_size" {
  type    = string
  default = "256"
}
