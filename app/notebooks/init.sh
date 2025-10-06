#!/usr/bin/env bash
# dbfs:/databricks/init/pgbench/install_pgbench.sh
# Installs PostgreSQL client + contrib (pgbench) at cluster startup

set -euo pipefail

LOG="/databricks/driver/pgbench_init.log"
exec > >(tee -a "${LOG}") 2>&1

echo "[pgbench-init] Starting at $(date)"

# Quick exit if already present
if command -v pgbench >/dev/null 2>&1; then
  echo "[pgbench-init] pgbench already installed: $(pgbench --version)"
  exit 0
fi

export DEBIAN_FRONTEND=noninteractive

retry() {
  # retry <retries> <sleep> <cmd...>
  local -r retries="$1"; shift
  local -r sleep_s="$1"; shift
  local n=0
  until "$@"; do
    n=$((n+1))
    if [[ $n -ge $retries ]]; then
      echo "[pgbench-init] Command failed after ${n} attempts: $*"
      return 1
    fi
    echo "[pgbench-init] Retry ${n}/${retries} after ${sleep_s}s: $*"
    sleep "${sleep_s}"
  done
}

echo "[pgbench-init] apt-get update"
retry 5 10 apt-get update -y

echo "[pgbench-init] Installing prerequisites"
retry 5 10 apt-get install -y --no-install-recommends wget ca-certificates gpg lsb-release

# Add PGDG repo if not present
PGDG_LIST="/etc/apt/sources.list.d/pgdg.list"
PG_KEYRING="/usr/share/keyrings/postgresql-archive-keyring.gpg"
if [[ ! -f "${PGDG_LIST}" ]]; then
  CODENAME="$(lsb_release -cs)"
  echo "[pgbench-init] Adding PGDG repo for ${CODENAME}"
  echo "deb [signed-by=${PG_KEYRING}] http://apt.postgresql.org/pub/repos/apt ${CODENAME}-pgdg main" \
    > "${PGDG_LIST}"
  echo "[pgbench-init] Fetching PG key"
  retry 5 10 bash -lc "wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o ${PG_KEYRING}"
fi

echo "[pgbench-init] apt-get update (PGDG)"
retry 5 10 apt-get update -y

echo "[pgbench-init] Installing postgresql-client-15 and postgresql-contrib-15"
retry 5 10 apt-get install -y --no-install-recommends postgresql-client-15 postgresql-contrib-15

if ! command -v pgbench >/dev/null 2>&1; then
  echo "[pgbench-init] ERROR: pgbench not found after install" >&2
  exit 2
fi

echo "[pgbench-init] Installed: $(pgbench --version)"
echo "[pgbench-init] Done at $(date)"
