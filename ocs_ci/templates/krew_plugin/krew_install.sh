rm -rf -- ~/.krew
cd "$(mktemp -d)"
OS="$(uname | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m | sed -e 's/x86_64/amd64/' -e 's/\(arm\)\(64\)\?.*/\1\2/' -e 's/aarch64$/arm64/')"
KREW="krew-${OS}_${ARCH}" &&
curl -fsSLO "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz"
tar zxvf "${KREW}.tar.gz"
./"${KREW}" install krew

host="$(hostname 2>/dev/null || cat /proc/sys/kernel/hostname 2>/dev/null || echo "${HOSTNAME:-}")"
case "$host" in
*jagent*)
  sudo cp ~/.krew/bin/kubectl-krew /usr/local/bin
  ;;
esac

echo 'export PATH="${KREW_ROOT:-$HOME/.krew}/bin:$PATH"' >> ~/.bashrc
