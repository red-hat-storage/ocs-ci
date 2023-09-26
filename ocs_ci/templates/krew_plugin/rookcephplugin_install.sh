oc krew install rook-ceph
host=$(hostname)
if [[ "$host" == *"jagent"* ]]
then
  sudo cp ~/.krew/bin/kubectl-rook_ceph /usr/local/bin
fi
