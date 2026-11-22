#!/usr/bin/env bash
# Keycloak in Docker for the opt-in Keycloak suite
# (crates/harness/tests/keycloak.rs; run it with DBT_STATE_KEYCLOAK=1).
#
#   scripts/keycloak.sh start   # (re)create the container, import the test realm, wait until ready
#   scripts/keycloak.sh stop    # remove the container
#
# Keycloak runs in dev mode (in-memory H2, plain HTTP) on 127.0.0.1 only,
# with crates/harness/keycloak/dbt-state-realm.json imported. Test-only:
# never point a deployment at it.
#
# DBT_STATE_KEYCLOAK_PORT (default 18080) is the published loopback port;
# set DBT_STATE_KEYCLOAK_URL=http://127.0.0.1:<port> for the suite when it
# is not the default. DBT_STATE_KEYCLOAK_CONTAINER names the container
# (default dbt-state-keycloak).
set -euo pipefail

image=quay.io/keycloak/keycloak:26.8.2
name=${DBT_STATE_KEYCLOAK_CONTAINER:-dbt-state-keycloak}
port=${DBT_STATE_KEYCLOAK_PORT:-18080}
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
realm_dir=$repo_dir/crates/harness/keycloak

case ${1:-} in
start)
  docker rm -f "$name" >/dev/null 2>&1 || true
  docker create --name "$name" -p "127.0.0.1:$port:8080" "$image" \
    start-dev --import-realm >/dev/null
  # Copy the realm in rather than bind-mount it: Keycloak runs as an
  # unprivileged user that may not be able to read a checkout made under a
  # restrictive umask. The archive names root as owner and makes the file
  # world-readable, which also suits a daemon that remaps user ids.
  python3 - "$realm_dir/dbt-state-realm.json" <<'PY' | docker cp - "$name:/opt/keycloak/data"
import io, sys, tarfile
realm = open(sys.argv[1], "rb").read()
with tarfile.open(fileobj=sys.stdout.buffer, mode="w|") as archive:
    folder = tarfile.TarInfo("import")
    folder.type, folder.mode = tarfile.DIRTYPE, 0o755
    archive.addfile(folder)
    file = tarfile.TarInfo("import/dbt-state-realm.json")
    file.size, file.mode = len(realm), 0o644
    archive.addfile(file, io.BytesIO(realm))
PY
  docker start "$name" >/dev/null
  discovery=http://127.0.0.1:$port/realms/dbt-state/.well-known/openid-configuration
  for _ in $(seq 1 90); do
    if curl -sf -o /dev/null "$discovery"; then
      echo "Keycloak is ready: http://127.0.0.1:$port (realm dbt-state)"
      exit 0
    fi
    sleep 2
  done
  docker logs --tail 50 "$name" >&2
  echo "Keycloak did not become ready at $discovery" >&2
  exit 1
  ;;
stop)
  docker rm -f "$name" >/dev/null
  ;;
*)
  echo "usage: $0 start|stop" >&2
  exit 2
  ;;
esac
