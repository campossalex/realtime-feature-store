#!/usr/bin/env bash
# destroy.sh — tear down the betting_features_oss scenario
#
# Counterpart to start.sh. By default removes every container, volume and network
# this scenario created, so the next ./start.sh is a clean slate. The downloaded JARs
# (lib/) and pulled images are opt-in, because re-creating them means re-downloading
# ~300 MB of JARs and ~5 GB of images.
#
# This only touches Docker objects labelled with this Compose project. It does NOT
# touch the EC2 instance, the Terraform state under terraform/, other scenarios,
# or any image or volume outside this project.
#
# Usage:
#   cd scenarios/betting_features_oss
#   ./destroy.sh                 # containers + volumes + network
#   ./destroy.sh --all -y        # everything, no prompt
#   ./destroy.sh --help

set -euo pipefail
cd "$(dirname "$0")"

PROJECT="betting-features-oss"

WIPE_DATA=0
WIPE_JARS=0
WIPE_IMAGES=0
ASSUME_YES=0

usage() {
  cat <<'EOF'
destroy.sh — tear down the betting_features_oss scenario

Always removes:
  - all containers in the betting-features-oss Compose project (incl. orphans)
  - the named volumes: postgres-data, flink-checkpoints, fluss-remote-data,
    minio-data (the Iceberg lake), catalog-db-data (Lakekeeper's catalog)
  - the flink-net network
  - the locally built inference image

Optional:
  --data        also delete a fluss-remote-data/ directory left on the host by older
                versions of this repo (Fluss's remote data is now a volume, above)
  --jars        also delete lib/                (~300 MB; needs ./download-jars.sh again)
  --images      also remove the images this scenario pulled (~5 GB, shared with
                other OSS scenarios — they will re-pull them)
  --all         --data --jars --images
  -y, --yes     skip the confirmation prompt
  -h, --help    show this

Both --data and --jars target git-ignored generated directories; neither holds
anything you authored.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --data)   WIPE_DATA=1 ;;
    --jars)   WIPE_JARS=1 ;;
    --images) WIPE_IMAGES=1 ;;
    --all)    WIPE_DATA=1; WIPE_JARS=1; WIPE_IMAGES=1 ;;
    -y|--yes) ASSUME_YES=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "ERROR: unknown option '$1'"; echo; usage; exit 1 ;;
  esac
  shift
done

command -v docker &>/dev/null || { echo "ERROR: 'docker' not found"; exit 1; }

# ── Show what is actually there before removing anything ──────────────────────

echo "══════════════════════════════════════════════════════════════"
echo "  Destroy: $PROJECT"
echo "══════════════════════════════════════════════════════════════"
echo ""

containers=$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')
volumes=$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')

echo "  Containers to remove:  $containers"
if [ "$containers" != "0" ]; then
  docker ps -a --filter "label=com.docker.compose.project=$PROJECT" \
    --format '    {{.Names}}  ({{.State}})' | sort
fi

echo "  Volumes to remove:     $volumes"
if [ "$volumes" != "0" ]; then
  docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" \
    | sed 's/^/    /'
fi

[ "$WIPE_DATA" = "1" ] && \
  echo "  Host dir to delete:    fluss-remote-data/  ($(du -sh fluss-remote-data 2>/dev/null | cut -f1 || echo absent))"
[ "$WIPE_JARS" = "1" ] && \
  echo "  Host dir to delete:    lib/                ($(du -sh lib 2>/dev/null | cut -f1 || echo absent))"

if [ "$WIPE_IMAGES" = "1" ]; then
  echo "  Images to remove:"
  COMPOSE_PROFILES=lakehouse docker compose config --images 2>/dev/null | sort -u | sed 's/^/    /'
fi

echo ""
if [ "$containers" = "0" ] && [ "$volumes" = "0" ] && \
   [ "$WIPE_DATA" = "0" ] && [ "$WIPE_JARS" = "0" ] && [ "$WIPE_IMAGES" = "0" ]; then
  echo "  Nothing to do — already torn down."
  exit 0
fi

# ── Confirm ───────────────────────────────────────────────────────────────────

if [ "$ASSUME_YES" != "1" ]; then
  echo "  This is irreversible: the PostgreSQL database, the Fluss tables, the"
  echo "  Iceberg lake and all Flink checkpoints are deleted with the volumes."
  echo ""
  printf "  Type 'destroy' to continue: "
  read -r reply
  if [ "$reply" != "destroy" ]; then
    echo ""
    echo "  Aborted — nothing was removed."
    exit 1
  fi
  echo ""
fi

# ── Step 1: Compose teardown ──────────────────────────────────────────────────

echo "==> Removing containers, volumes and network..."
DOWN_ARGS=(--volumes --remove-orphans --rmi local)
[ "$WIPE_IMAGES" = "1" ] && DOWN_ARGS=(--volumes --remove-orphans --rmi all)
# The lakehouse profile too: otherwise `down` skips the lake containers when .env says
# simple mode (or is gone).
COMPOSE_PROFILES=lakehouse docker compose down "${DOWN_ARGS[@]}"

# `down` misses containers whose service was dropped from the compose file, and
# `--rmi local` leaves the built inference image if its service was already gone.
stragglers=$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")
if [ -n "$stragglers" ]; then
  echo "==> Removing $(echo "$stragglers" | wc -l | tr -d ' ') straggler container(s)..."
  # shellcheck disable=SC2086
  docker rm -f $stragglers >/dev/null
fi

leftover_vols=$(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT")
if [ -n "$leftover_vols" ]; then
  echo "==> Removing leftover volume(s)..."
  # shellcheck disable=SC2086
  docker volume rm $leftover_vols >/dev/null
fi

# ── Step 2: Host artifacts ────────────────────────────────────────────────────

if [ "$WIPE_DATA" = "1" ]; then
  echo "==> Deleting fluss-remote-data/ ..."
  # Fluss writes snapshots as root inside the container, so the host user may not
  # own them. Fall back to a throwaway container that does.
  rm -rf fluss-remote-data 2>/dev/null || \
    docker run --rm -v "$PWD:/w" -w /w alpine:3 rm -rf fluss-remote-data
  mkdir -p fluss-remote-data
fi

if [ "$WIPE_JARS" = "1" ]; then
  echo "==> Deleting lib/ ..."
  rm -rf lib
fi

# The mode start.sh recorded for Compose: the next ./start.sh writes it again.
rm -f .env

# ── Step 3: Report ────────────────────────────────────────────────────────────

echo ""
echo "══════════════════════════════════════════════════════════════"
echo "  Teardown complete"
echo "══════════════════════════════════════════════════════════════"
echo ""
echo "  Remaining containers: $(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')"
echo "  Remaining volumes:    $(docker volume ls -q --filter "label=com.docker.compose.project=$PROJECT" | wc -l | tr -d ' ')"
echo ""
echo "  Rebuild with:"
if [ "$WIPE_JARS" = "1" ]; then
  echo "    ./download-jars.sh && ./start.sh      # or ./start.sh --simple"
else
  echo "    ./start.sh             # complete"
  echo "    ./start.sh --simple    # simple: no lakehouse"
fi
echo "══════════════════════════════════════════════════════════════"
