#!/usr/bin/env bash
# The watch: this Plugin against what Yoke last published. The newest programs are installed from the module
# proxy, the Plugin is built and taken into try/ by them as anyone's would be, run under a real Core, and asked
# what ci/probe.sh asks. It leaves its results where the record writer reads them.
# Usage: watch.sh [results directory]
set -uo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
results="${1:-$root/.results}"
mkdir -p "$results"
date -u +%Y-%m-%dT%H:%M:%SZ > "$results/started"
# shellcheck source=/dev/null
source "$root/ci/probe.sh" # names PLUGIN, UNIT, CASE and defines probe

finish() { # <verdict line>
  echo "$1" | tee "$results/checks.txt"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$results/finished"
  [[ "$1" == pass* ]]
  exit $?
}

work="$(mktemp -d /tmp/w.XXXXXX)"
trap 'kill "${running:-0}" 2>/dev/null; rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/run" "$work/state" "$work/config/yoke"
export GOBIN="$work/bin" PATH="$work/bin:$PATH" XDG_RUNTIME_DIR="$work/run" XDG_STATE_HOME="$work/state" XDG_CONFIG_HOME="$work/config"
printf 'install_root: %s\n' "$work/apps" > "$work/config/yoke/manager.conf"

release="$(go list -m -f '{{.Version}}' github.com/yoke-project/yoke@latest)" || finish "FAIL  $CASE — the newest release of yoke cannot be read"
echo "watch: yoke $release"
echo "$release" > "$results/release"
for program in yoke yoke-compose yoke-core yoke-guardian yoke-auth; do
  go install "github.com/yoke-project/yoke/cmd/$program@$release" || finish "FAIL  $CASE — $program $release does not install"
done

cd "$root/try" || finish "FAIL  $CASE — no try/"
if ! yoke-compose plugins 2>/dev/null | grep -q "$PLUGIN"; then finish "FAIL  $CASE — try/ has not taken $PLUGIN"; fi
yoke-compose remove "units.$UNIT" > /dev/null || finish "FAIL  $CASE — $UNIT cannot be removed from try/"
rm -f "components/plugins/$UNIT" "manifests/$PLUGIN.yaml"
said="$(yoke-compose add plugin .. --yes 2>&1)" || finish "FAIL  $CASE — the Plugin is not taken under $release: $(tail -n 2 <<<"$said" | tr '\n' ' ')"

cd "$root" || exit 1
yoke app run --from try > "$work/run.log" 2>&1 &
running=$!
instance=""
for _ in $(seq 1 30); do
  instance="$(sed -n 's/ runs .*//p' "$work/run.log" | head -n 1)"
  [[ -n "$instance" ]] && grep -q 'is observable' "$work/run.log" && break
  sleep 1
done
grep -q 'is observable' "$work/run.log" || finish "FAIL  $CASE — the instance did not come up under $release: $(tail -n 3 "$work/run.log" | tr '\n' ' ')"
if why="$(probe "$instance")"; then verdict="pass  $CASE"; else verdict="FAIL  $CASE — under $release: $why"; fi
kill -INT "$running" 2>/dev/null
for _ in $(seq 1 30); do kill -0 "$running" 2>/dev/null || break; sleep 1; done
finish "$verdict"
