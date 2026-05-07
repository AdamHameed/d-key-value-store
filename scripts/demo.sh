#!/usr/bin/env bash
set -euo pipefail

KVCTL="${KVCTL:-go run ./cmd/client --}"

say() {
  printf '\n==> %s\n' "$*"
}

wait_for_cluster() {
  say "waiting for Raft leader"
  for _ in $(seq 1 60); do
    if leader="$($KVCTL leader 2>/dev/null)" && [[ -n "$leader" ]]; then
      echo "leader=$leader"
      return 0
    fi
    sleep 1
  done
  echo "cluster did not elect a leader in time" >&2
  return 1
}

service_for_addr() {
  case "$1" in
    *5001) echo "node1" ;;
    *5002) echo "node2" ;;
    *5003) echo "node3" ;;
    *) echo "unknown" ;;
  esac
}

say "starting 3-node cluster"
docker compose up --build -d
wait_for_cluster

say "cluster status"
$KVCTL status

say "writing sample keys"
$KVCTL put language go
$KVCTL put consensus raft
$KVCTL put storage badger

say "reading sample keys"
printf 'language='
$KVCTL get language
printf 'consensus='
$KVCTL get consensus
printf 'storage='
$KVCTL get storage

leader="$($KVCTL leader)"
leader_service="$(service_for_addr "$leader")"
if [[ "$leader_service" == "unknown" ]]; then
  echo "could not map leader address $leader to compose service" >&2
  exit 1
fi

say "killing leader $leader_service ($leader)"
docker compose stop "$leader_service"
sleep 5
wait_for_cluster

say "status after failover"
$KVCTL status

say "writing after failover"
$KVCTL put failover survived
printf 'failover='
$KVCTL get failover

say "restarting killed node $leader_service"
docker compose start "$leader_service"
sleep 8

say "status after recovery"
$KVCTL status
printf 'recovered language='
$KVCTL get language
printf 'recovered failover='
$KVCTL get failover

say "demo complete"
