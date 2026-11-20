oc krew install rook-ceph
host="$(hostname 2>/dev/null || cat /proc/sys/kernel/hostname 2>/dev/null || echo "${HOSTNAME:-}")"
case "$host" in
*jagent*)
  sudo cp ~/.krew/bin/kubectl-rook_ceph /usr/local/bin
  ;;
esac
