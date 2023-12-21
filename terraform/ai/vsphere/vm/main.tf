resource "vsphere_virtual_machine" "vm" {
  name                        = var.vmname
  resource_pool_id            = var.resource_pool_id
  datastore_id                = var.datastore_id
  num_cpus                    = var.num_cpus
  memory                      = var.memory
  guest_id                    = var.guest_id
  folder                      = var.folder_id
  enable_disk_uuid            = "true"
  nested_hv_enabled           = var.nested_hv_enabled
  wait_for_guest_net_timeout  = "0"
  wait_for_guest_net_routable = "false"

  network_interface {
    network_id = var.network_id
  }

  disk {
    label            = "disk0"
    size             = 120
    thin_provisioned = true
  }

  # creates variable number of data disks for VM
  dynamic "disk" {
    for_each = [for idx in range(var.data_disks_count) : idx + 1]
    content {
      label            = "disk${disk.value}"
      unit_number      = disk.value
      size             = 256
      thin_provisioned = true
    }
  }

  cdrom {
    datastore_id = var.datastore_id
    path         = var.iso_image
  }

  extra_config = {
    "stealclock.enable" = "TRUE"
  }
}
