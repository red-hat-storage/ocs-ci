variable "vmname" {
  type = string
}

variable "guest_id" {
  type = string
}

variable "resource_pool_id" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "datastore_id" {
  type = string
}

variable "network_id" {
  type = string
}

variable "memory" {
  type = string
}

variable "num_cpus" {
  type = string
}

variable "iso_image" {
  type = string
}

variable "system_disk_size" {
  type    = string
  default = "120"
}

variable "data_disks_count" {
  type    = string
  default = "0"
}

variable "data_disks_size" {
  type    = string
  default = "100"
}

variable "nested_hv_enabled" {
  type    = bool
  default = false
}
