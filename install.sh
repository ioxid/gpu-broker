#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
# Install gpu-broker system-wide. Run as root.
set -euo pipefail

[ "$(id -u)" = 0 ] || { echo "run as root: sudo $0"; exit 1; }
here=$(cd "$(dirname "$0")" && pwd)

install -d /usr/local/lib/gpu-broker /etc/gpu-broker \
           /var/lib/gpu-broker/jobs /var/log/gpu-broker /run/gpu-broker \
           /usr/local/share/doc/gpu-broker \
           /usr/local/share/fish/vendor_completions.d /etc/bash_completion.d
chmod 755 /var/lib/gpu-broker /var/lib/gpu-broker/jobs /run/gpu-broker

# code
install -m 0644 "$here/src/brokerd.py"  /usr/local/lib/gpu-broker/brokerd.py
install -m 0755 "$here/src/client.py"   /usr/local/lib/gpu-broker/client.py
install -m 0755 "$here/src/selftest.py" /usr/local/lib/gpu-broker/selftest.py

# CLIs (dispatch by basename)
for c in gpu-submit gpu-q gpu-log gpu-cancel gpu-lease; do
  ln -sf /usr/local/lib/gpu-broker/client.py /usr/local/bin/"$c"
done
ln -sf /usr/local/lib/gpu-broker/selftest.py /usr/local/bin/gpu-broker-test

# config (don't clobber an existing one)
[ -f /etc/gpu-broker/config.json ] || install -m 0644 "$here/config.example.json" /etc/gpu-broker/config.json
install -m 0644 "$here/supervisord.conf" /etc/gpu-broker/supervisord.conf
install -m 0644 "$here/README.md"        /usr/local/share/doc/gpu-broker/README.md

# shell completion (fish needs a per-command filename -> symlink each)
install -m 0644 "$here/completions/gpu-broker.fish" /usr/local/share/fish/vendor_completions.d/gpu-broker.fish
for c in gpu-submit gpu-q gpu-log gpu-cancel gpu-lease; do
  ln -sf gpu-broker.fish /usr/local/share/fish/vendor_completions.d/"$c".fish
done
install -m 0644 "$here/completions/gpu-broker.bash" /etc/bash_completion.d/gpu-broker

# group used by the uvm gate (harmless if it exists)
groupadd -f gpu

echo "gpu-broker installed."
echo "start the daemon:  supervisord -c /etc/gpu-broker/supervisord.conf"
echo "or run it directly: python3 /usr/local/lib/gpu-broker/brokerd.py"
echo "self-test:          gpu-broker-test"
