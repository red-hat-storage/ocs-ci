import logging

log = logging.getLogger(__name__)


# Directories Creation
def create_dir(node, args):
    node.exec_command(cmd='mkdir {}'.format(args))


# Directories deletion
def delete_dir(node, args):
    node.exec_command(cmd='rm -rf {}'.format(args))


# Create Pool
def create_pool(node, args):
    node.exec_command(cmd='sudo ceph osd pool create {} 128 128'.format(args))


# Delete Pool
def delete_pool(node, args):
    node.exec_command(cmd='sudo ceph osd pool delete {pool} {pool} --yes-i-really-really-mean-it'
                      .format(pool=args))


# Create Image
def create_image(node, *args):
    node.exec_command(cmd='sudo rbd create -s {} {}/{}'.format(args[0], args[1], args[2]))


# Resize Image
def resize_image(node, *args):
    node.exec_command(cmd='sudo rbd resize -s {} --allow-shrink {}/{}'.format(args[0], args[1], args[2]))


# Create Snap
def create_snap(node, *args):
    node.exec_command(cmd='sudo rbd snap create {}/{}@{}'.format(args[0], args[1], args[2]))


# Protect Snap
def protect_snap(node, *args):
    node.exec_command(cmd='sudo rbd snap protect {}/{}@{}'.format(args[0], args[1], args[2]))


# Create Clone
def create_clone(node, *args):
    node.exec_command(cmd='sudo rbd clone {pool}/{}@{} {pool}/{}'
                      .format(args[1], args[2], args[3], pool=args[0]))


# Export
def export_image(node, *args):
    node.exec_command(cmd='sudo rbd export {}/{} {}'.format(args[0], args[1], args[2]))


# Bench-write
def bench_write(node, *args):
    node.exec_command(cmd='sudo rbd bench-write {}/{}'.format(args[0], args[1]))


# Flatten
def flatten(node, *args):
    node.exec_command(cmd='sudo rbd flatten {}/{}'.format(args[0], args[1]))


# Lock Add
def lock(node, *args):
    node.exec_command(cmd='sudo rbd lock add {}/{} lok'.format(args[0], args[1]))
