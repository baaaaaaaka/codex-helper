#!/bin/sh
set -eu

user="${SSH_TEST_USER:-ci}"
authorized_key="${SSH_AUTHORIZED_KEY:?SSH_AUTHORIZED_KEY is required}"

if ! id -u "$user" >/dev/null 2>&1; then
    useradd --create-home --shell /bin/sh "$user"
fi
# Debian marks accounts created by useradd as locked. OpenSSH rejects a
# locked account before it evaluates authorized_keys, even when password
# authentication is disabled. This test account has no password; it is only
# usable with the injected public key below.
passwd --delete "$user" >/dev/null

install -d -m 700 -o "$user" -g "$user" "/home/$user/.ssh"
printf '%s\n' "$authorized_key" > "/home/$user/.ssh/authorized_keys"
chown "$user:$user" "/home/$user/.ssh/authorized_keys"
chmod 600 "/home/$user/.ssh/authorized_keys"

mkdir -p /run/sshd
ssh-keygen -A

cat > /etc/ssh/sshd_config <<EOF
Port 2222
ListenAddress 0.0.0.0
HostKey /etc/ssh/ssh_host_ed25519_key
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
PubkeyAuthentication yes
AuthorizedKeysFile /home/$user/.ssh/authorized_keys
StrictModes no
AllowTcpForwarding yes
PermitOpen any
PidFile /run/sshd/proxy-recovery.pid
LogLevel ERROR
EOF

exec /usr/sbin/sshd -D -e -f /etc/ssh/sshd_config
