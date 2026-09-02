#!/usr/bin/env bash
#
# setup.sh - install everything ./run.sh needs on a fresh Amazon Linux 2023 host.
#
#   sudo ./setup.sh         install Docker, the compose and buildx plugins, and git
#   ./setup.sh --check      report what is present and what is missing, install nothing
#
# Then log out and back in (for docker group membership) and run:
#
#   ./run.sh all
#
# Amazon Linux 2023 only. On macOS or Windows install Docker Desktop instead -- it ships the
# compose and buildx plugins already, and ./run.sh needs nothing else.
#
set -euo pipefail

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The compose plugin release asset is named with the raw `uname -m` value
# (docker-compose-linux-x86_64), while buildx names its assets with Go's arch spelling
# (buildx-v0.34.1.linux-amd64). Same machine, two different strings; getting them confused
# yields a 404 that curl happily writes to disk as a zero-byte "plugin".
readonly PLUGIN_DIR="/usr/local/lib/docker/cli-plugins"
readonly BUILDX_FALLBACK="v0.34.1"

# 'docker compose build' refuses to run against an older buildx, with exactly this message:
#   compose build requires buildx 0.17.0 or later
# A buildx that is merely present is therefore not good enough, and checking only that the
# plugin exists is how this gets missed -- an old buildx answers 'docker buildx version'
# perfectly happily and the build fails much later.
readonly MIN_BUILDX="0.17.0"

# --------------------------------------------------------------------------------------
# Output helpers (same vocabulary as run.sh)
# --------------------------------------------------------------------------------------
if [[ -t 1 && -z "${NO_COLOR:-}" ]]; then
    C_RESET=$'\033[0m'; C_BLUE=$'\033[34m'; C_GREEN=$'\033[32m'
    C_YELLOW=$'\033[33m'; C_RED=$'\033[31m'; C_BOLD=$'\033[1m'
else
    C_RESET=""; C_BLUE=""; C_GREEN=""; C_YELLOW=""; C_RED=""; C_BOLD=""
fi

step() { printf '\n%s==> %s%s\n' "${C_BLUE}${C_BOLD}" "$*" "${C_RESET}"; }
ok()   { printf '%s  ok%s %s\n' "${C_GREEN}" "${C_RESET}" "$*"; }
info() { printf '     %s\n' "$*"; }
warn() { printf '%s  warning%s %s\n' "${C_YELLOW}" "${C_RESET}" "$*" >&2; }
die()  { printf '\n%s  error%s %s\n' "${C_RED}${C_BOLD}" "${C_RESET}" "$*" >&2; exit 1; }

CHECK_ONLY=0
case "${1:-}" in
    --check) CHECK_ONLY=1 ;;
    "")      ;;
    *)       die "unknown argument '$1'. Usage: sudo ./setup.sh [--check]" ;;
esac

# --------------------------------------------------------------------------------------
# Host and privileges
# --------------------------------------------------------------------------------------
SUDO=""
if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
    elif [[ "$CHECK_ONLY" -eq 0 ]]; then
        die "run this as root or install sudo: 'sudo ./setup.sh'"
    fi
fi

PKG="dnf"
command -v dnf >/dev/null 2>&1 || PKG="yum"

check_host() {
    step "Checking the host"

    local id="" version_id="" pretty=""
    if [[ -r /etc/os-release ]]; then
        # shellcheck disable=SC1091
        . /etc/os-release
        id="${ID:-}"; version_id="${VERSION_ID:-}"; pretty="${PRETTY_NAME:-}"
    fi

    if [[ "$id" == "amzn" && "$version_id" == "2023" ]]; then
        ok "${pretty}"
    elif [[ "$id" == "amzn" && "$version_id" == "2" ]]; then
        # AL2 is the one place 'amazon-linux-extras install docker' is correct. On AL2023 that
        # command does not exist at all, and docker comes from the default repo instead.
        die "this is Amazon Linux 2, not 2023. Use 'sudo amazon-linux-extras install docker' there, or launch an AL2023 AMI."
    else
        warn "expected Amazon Linux 2023, found '${pretty:-unknown}'"
        info "continuing anyway; the ${PKG} package names may not match"
    fi

    case "$(uname -m)" in
        x86_64)  ARCH_GO="amd64"; ARCH_RAW="x86_64" ;;
        aarch64) ARCH_GO="arm64"; ARCH_RAW="aarch64" ;;
        *)       die "unsupported architecture $(uname -m); this stack needs x86_64 or aarch64" ;;
    esac
    ok "architecture $(uname -m)"

    # The stock AL2023 AMI ships an 8 GiB root volume. The images alone (Milvus, Flink, Kafka,
    # Grafana, Postgres, MinIO) plus the Maven cache come to roughly 12 GiB, so the default
    # fills up mid-build and surfaces as an opaque layer-write failure.
    local avail
    avail=$(df -Pk / | awk 'NR==2 {print int($4/1024/1024)}')
    if [[ "${avail:-0}" -lt 12 ]]; then
        warn "only ${avail} GiB free on /; the images and Maven cache want roughly 12 GiB"
        info "grow the EBS volume in the console, then: sudo growpart /dev/nvme0n1 1 && sudo xfs_growfs /"
    else
        ok "${avail} GiB free on /"
    fi
}

# --------------------------------------------------------------------------------------
# Packages
# --------------------------------------------------------------------------------------
install_packages() {
    step "Installing git and docker"

    # Deliberately not installing 'curl': AL2023 ships curl-minimal, and pulling in the full
    # curl package makes dnf resolve a conflict between the two. curl-minimal does everything
    # this script needs.
    local want=(git docker) missing=()
    local p
    for p in "${want[@]}"; do
        command -v "$p" >/dev/null 2>&1 || missing+=("$p")
    done

    if [[ ${#missing[@]} -eq 0 ]]; then
        ok "git and docker are already installed"
        return 0
    fi

    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "missing: ${missing[*]}"
        return 0
    fi

    info "installing: ${missing[*]}"
    $SUDO "$PKG" install -y "${missing[@]}" >/dev/null
    ok "installed ${missing[*]}"
}

start_docker() {
    step "Starting the Docker daemon"

    if $SUDO systemctl is-active --quiet docker 2>/dev/null; then
        ok "docker is running"
    elif [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "the docker daemon is not running"
        return 0
    else
        # --now starts it as well as enabling it, so the daemon survives the next reboot.
        # 'service docker start' works too but does not enable it, which is why a stopped
        # daemon after a reboot is such a common second-day surprise.
        $SUDO systemctl enable --now docker
        ok "docker started and enabled at boot"
    fi
}

# --------------------------------------------------------------------------------------
# CLI plugins
#
# AL2023's docker package ships the daemon and the CLI but neither the compose plugin nor
# buildx. run.sh needs both: every command goes through 'docker compose', and the connector
# image is built with buildx. Without them the failure reads as a missing subcommand rather
# than a missing package, which is not an obvious thing to go looking for.
# --------------------------------------------------------------------------------------
plugin_ok() {
    # A 404 saved by curl is a valid file and an executable one; only running it proves the
    # download worked. Both plugins answer a bare 'version' subcommand.
    docker "$1" version >/dev/null 2>&1
}

# The version docker actually resolves, which is not necessarily the one just installed:
# several directories are searched and the first hit wins. Reading it back from the CLI is
# the only honest check.
buildx_version() {
    docker buildx version 2>/dev/null | awk '{print $2}' | tr -d 'v'
}

# True when $1 >= $2, comparing dotted versions. sort -V does the work; equal versions sort
# first-listed, so the >= case falls out correctly.
version_ge() {
    [[ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -1)" == "$2" ]]
}

fetch_plugin() {
    local name="$1" url="$2" dest="${PLUGIN_DIR}/docker-$1"

    $SUDO mkdir -p "$PLUGIN_DIR"
    # Download to a temp path and move it into place only once it is verified, so an
    # interrupted or 404'd download never leaves a broken plugin that shadows a later good one.
    local tmp
    tmp=$($SUDO mktemp "${PLUGIN_DIR}/.docker-${name}.XXXXXX")
    if ! $SUDO curl -fsSL "$url" -o "$tmp"; then
        $SUDO rm -f "$tmp"
        die "could not download ${name} from ${url}"
    fi
    $SUDO chmod 0755 "$tmp"
    $SUDO mv -f "$tmp" "$dest"

    plugin_ok "$name" || die "${name} was installed to ${dest} but does not run. Delete it and re-run."
}

install_compose() {
    step "Installing the docker compose plugin"

    if plugin_ok compose; then
        ok "docker compose $(docker compose version --short 2>/dev/null || echo '?')"
        return 0
    fi
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "the docker compose plugin is missing"
        return 0
    fi

    # 'latest' rather than a pinned tag: compose's release asset name is stable, the project is
    # careful about compatibility, and pinning here means this script silently ages. Set
    # COMPOSE_VERSION=v2.29.7 to pin it for a reproducible build.
    local url
    if [[ -n "${COMPOSE_VERSION:-}" ]]; then
        url="https://github.com/docker/compose/releases/download/${COMPOSE_VERSION}/docker-compose-linux-${ARCH_RAW}"
    else
        url="https://github.com/docker/compose/releases/latest/download/docker-compose-linux-${ARCH_RAW}"
    fi

    fetch_plugin compose "$url"
    ok "docker compose $(docker compose version --short 2>/dev/null || echo 'installed')"
}

install_buildx() {
    step "Installing the docker buildx plugin"

    local current=""
    if plugin_ok buildx; then
        current=$(buildx_version)
    fi

    if [[ -n "$current" ]] && version_ge "$current" "$MIN_BUILDX"; then
        ok "docker buildx v${current}"
        return 0
    fi

    if [[ -n "$current" ]]; then
        warn "buildx v${current} is older than the v${MIN_BUILDX} 'docker compose build' needs"
        info "installing a current buildx into ${PLUGIN_DIR}, which docker searches first"
    fi
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        [[ -n "$current" ]] || warn "the docker buildx plugin is missing"
        return 0
    fi

    # buildx puts the version inside the asset name (buildx-v0.34.1.linux-amd64), so unlike
    # compose there is no /latest/download/ URL that can work. Resolve the tag from the
    # redirect that /releases/latest issues -- no API token, no jq.
    local version="${BUILDX_VERSION:-}"
    if [[ -z "$version" ]]; then
        version=$(curl -fsSLI -o /dev/null -w '%{url_effective}' \
                      https://github.com/docker/buildx/releases/latest 2>/dev/null \
                      | sed -n 's#.*/tag/##p') || true
    fi
    if [[ -z "$version" ]]; then
        warn "could not resolve the latest buildx release; falling back to ${BUILDX_FALLBACK}"
        version="$BUILDX_FALLBACK"
    fi

    fetch_plugin buildx \
        "https://github.com/docker/buildx/releases/download/${version}/buildx-${version}.linux-${ARCH_GO}"

    # Read the version back rather than trusting the tag we downloaded. If an older buildx sits
    # in a directory docker searches ahead of PLUGIN_DIR, it still wins and the build would fail
    # later with the same message that sent us here.
    local now
    now=$(buildx_version)
    if [[ -z "$now" ]] || ! version_ge "$now" "$MIN_BUILDX"; then
        warn "docker still resolves buildx v${now:-unknown} after installing ${version}"
        info "another copy is shadowing ${PLUGIN_DIR}/docker-buildx. Find it with:"
        info "  ls -l ~/.docker/cli-plugins/docker-buildx /usr/local/lib/docker/cli-plugins/docker-buildx /usr/lib/docker/cli-plugins/docker-buildx /usr/libexec/docker/cli-plugins/docker-buildx"
        die "buildx v${MIN_BUILDX} or later is required by 'docker compose build'"
    fi
    ok "docker buildx v${now}"
}

# --------------------------------------------------------------------------------------
# Let the invoking user talk to the daemon
# --------------------------------------------------------------------------------------
configure_docker_group() {
    step "Granting docker access"

    # Under sudo, $USER is root and SUDO_USER is the human. Adding root to the docker group is
    # pointless, so work out who actually needs it.
    local target="${SUDO_USER:-${USER:-$(id -un)}}"
    if [[ "$target" == "root" ]]; then
        ok "running as root; no group membership needed"
        info "if you normally use a non-root login, run: sudo usermod -aG docker <user>"
        return 0
    fi

    if id -nG "$target" 2>/dev/null | tr ' ' '\n' | grep -qx docker; then
        ok "${target} is already in the docker group"
        return 0
    fi
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        warn "${target} is not in the docker group"
        return 0
    fi

    $SUDO usermod -aG docker "$target"
    ok "added ${target} to the docker group"
    # Group membership is fixed at login, so the current shell still cannot reach the socket.
    warn "log out and back in before running ./run.sh, or the docker socket stays permission-denied"
    info "to test without logging out: newgrp docker"
}

# --------------------------------------------------------------------------------------
# Verify, the same way run.sh will
# --------------------------------------------------------------------------------------
verify() {
    step "Verifying"

    local failed=0
    if docker info >/dev/null 2>&1; then
        ok "docker $(docker version --format '{{.Server.Version}}' 2>/dev/null || echo '?') -- daemon reachable"
    else
        warn "the docker daemon is not reachable as $(id -un)"
        info "if setup just added you to the docker group, log out and back in"
        failed=1
    fi

    if plugin_ok compose; then
        ok "docker compose $(docker compose version --short 2>/dev/null || echo '?')"
    else
        warn "'docker compose' does not work"; failed=1
    fi

    local bx
    bx=$(buildx_version)
    if [[ -z "$bx" ]]; then
        warn "'docker buildx' does not work"; failed=1
    elif version_ge "$bx" "$MIN_BUILDX"; then
        ok "docker buildx v${bx}"
    else
        warn "docker buildx v${bx} is too old; 'docker compose build' needs v${MIN_BUILDX} or later"
        failed=1
    fi

    if command -v git >/dev/null 2>&1; then
        ok "git $(git --version | awk '{print $3}')"
    else
        warn "git is missing"; failed=1
    fi

    return "$failed"
}

# --------------------------------------------------------------------------------------
main() {
    if [[ "$CHECK_ONLY" -eq 1 ]]; then
        printf '%s  checking only; nothing will be installed%s\n' "${C_YELLOW}" "${C_RESET}"
    fi

    check_host
    install_packages
    start_docker
    install_compose
    install_buildx
    configure_docker_group

    if verify; then
        step "Ready"
        info "./run.sh all      build, start, seed and submit the pipeline"
        info "./run.sh help     every command"
        printf '\n'
    else
        step "Not ready yet"
        info "fix the warnings above, then re-run './setup.sh --check'"
        printf '\n'
        exit 1
    fi
}

main