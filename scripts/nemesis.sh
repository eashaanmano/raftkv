#!/usr/bin/env bash
# Inject faults into the docker-compose cluster (real processes, real TCP).
#
#   scripts/nemesis.sh kill-leader        # SIGKILL the current leader
#   scripts/nemesis.sh isolate n3         # cut n3 off the network
#   scripts/nemesis.sh pause n2 5         # freeze n2 for 5s (GC-pause / VM-stall)
#   scripts/nemesis.sh heal               # reconnect + start everything
#   scripts/nemesis.sh chaos 120          # random faults for 120s
#
# Clock skew is only exercised by the in-process harness (go test ./jepsen):
# containers share the host kernel clock, so faking it here needs libfaketime.
set -euo pipefail
cd "$(dirname "$0")/.."
NODES=(n1 n2 n3 n4 n5)
NET="$(docker compose ps -q n1 | xargs docker inspect -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}')"

cid() { docker compose ps -q "$1"; }

leader() {
  docker compose run --rm -T client status 2>/dev/null |
    awk '/"state":"leader"/ {print $2; exit}'
}

case "${1:-}" in
  kill-leader)
    l="$(leader)"; [ -n "$l" ] || { echo "no leader found"; exit 1; }
    echo "$(date +%T.%3N) killing leader $l"
    docker kill -s KILL "$(cid "$l")" >/dev/null ;;
  isolate)
    echo "$(date +%T.%3N) isolating $2"
    docker network disconnect "$NET" "$(cid "$2")" ;;
  pause)
    echo "$(date +%T.%3N) pausing $2 for ${3:-5}s"
    docker pause "$(cid "$2")" >/dev/null; sleep "${3:-5}"; docker unpause "$(cid "$2")" >/dev/null ;;
  heal)
    for n in "${NODES[@]}"; do
      docker network connect "$NET" "$(cid "$n")" 2>/dev/null || true
      docker unpause "$(cid "$n")" 2>/dev/null || true
    done
    docker compose up -d "${NODES[@]}" >/dev/null
    echo "$(date +%T.%3N) healed" ;;
  chaos)
    end=$(( $(date +%s) + ${2:-60} ))
    while [ "$(date +%s)" -lt "$end" ]; do
      case $(( RANDOM % 3 )) in
        0) "$0" kill-leader || true ;;
        1) "$0" isolate "${NODES[$(( RANDOM % 5 ))]}" || true ;;
        2) "$0" pause "${NODES[$(( RANDOM % 5 ))]}" 3 || true ;;
      esac
      sleep $(( 2 + RANDOM % 4 )); "$0" heal; sleep 2
    done ;;
  *) sed -n '2,10p' "$0"; exit 2 ;;
esac
