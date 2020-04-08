#!/bin/sh

# From https://docs.openshift.com/container-platform/3.9/creating_images/guidelines.html#use-uid
if ! whoami > /dev/null 2>&1; then
  if [ -w /etc/passwd ]; then
    echo "${USER_NAME:-default}:x:$(id -u):0:${USER_NAME:-default} user:${HOME}:/sbin/nologin" >> /etc/passwd
  fi
fi

if [ -d /docker-entrypoint.d/ ] && [ -n "$(ls -A /docker-entrypoint.d/)" ]; then
  for f in /docker-entrypoint.d/*; do
    # shellcheck source=/dev/null
    . "$f"
  done
fi

exec "$@"
