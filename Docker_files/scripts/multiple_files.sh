#!/usr/bin/env sh

###############################################################################
#
# This script is executing the Linux-Kernel tgz file into path asked by the
# user via cli argument.
# This file should locate at /files as file.gz : /files/file.tgz
#
# After executing the file, it duplicate it as much time as asked by the
# user via cli argument + 1.
#
# At the end it report the total amount of data (df -h) and files which have
# in the target path.

# If the count argument is 0, the script only report the usage capacity and
# the number of files in the <target> directory (mount point)
#
# Args:
#   count (int): number of duplication
#   target (str): target path to copy files into
#
# Returns:
#   Total disk capacity used by the files
#   Total numbers of files
#
# Exit code :
#   0 - Completed successfully
#   1 - Not enough arguments
#   2 - The <count> argument is not a number
#   3 - The <target> path dows not exist
#
###############################################################################

function usage {
  echo ""
  echo "Usage : $0 <count> <target>"
  echo ""
  echo "  count  - The number of times to duplicate the files"
  echo "  target - The target path, where to copy the files"
  echo ""
  echo "Arguments order is mandatory !!!"
  echo ""
}

# Validating that all arguments was passed
if [[ $# -lt 2 ]] ; then
  echo ""
  echo "Error [1]: not all argument was defined !"
  usage
  exit 1
fi

# Validate the number of
COUNT=$1
[ -n "$COUNT" ] && [ "$COUNT" -eq "$COUNT" ] 2>/dev/null
if [ $? -ne 0 ]; then
   echo ""
   echo "Error [2]: count ($COUNT) is Not a number !!"
   usage
   exit 2
fi

# Validate that the target path exist
POD_PATH=$2
if [ ! -d $POD_PATH ] ; then
  echo ""
  echo "Error [3]: Target path ($POD_PATH) does not exist !!"
  usage
  exit 3
fi

KERNEL_FILES="/files/file.gz"
BASE_PATH="${POD_PATH}/folder0"

function get_results {
  # Calculating the usage capacity and number of files
  total_data=`df -h ${POD_PATH} | tail -n 1 | awk '{print $(NF-3)}'`
  num_of_files=`find ${POD_PATH} -type f | wc -l`

  # Report the results
  echo "Total Data is ${total_data}"
  echo "Number Of Files is ${num_of_files}"
}

if [[ $COUNT -eq 0 ]] ; then
  get_results
  exit 0
fi

mkdir -p ${BASE_PATH}
echo "Executing the linux kernel files"
tar xf ${KERNEL_FILES} -C ${BASE_PATH}

echo "Duplicate the kernel files $COUNT times..."
for x in $(seq $COUNT) ; do
  TARGET_PATH="${POD_PATH}/folder${x}"
  mkdir -p ${TARGET_PATH}
  cp -r ${BASE_PATH} ${TARGET_PATH}
  sync
done

get_results

exit 0
