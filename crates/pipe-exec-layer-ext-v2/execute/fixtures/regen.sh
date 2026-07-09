#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: regen.sh [--check] [--contracts-dir PATH] [--work-dir PATH] [--print-metadata]

Regenerates ../gravity_hardfork.json from test_genesis.toml and
validator_genesis.json using the pinned gravity_chain_core_contracts ref.

Options:
  --check              Generate into a temp file and compare with the committed fixture.
  --contracts-dir PATH Use an existing contracts checkout as the git object source.
                       The script creates an isolated worktree and does not edit PATH.
  --work-dir PATH      Use PATH for temporary clones, worktrees, and generated files.
  --print-metadata     Print parsed TOML metadata and exit.
USAGE
}

MODE="write"
CONTRACTS_SOURCE_DIR=""
WORK_DIR=""
PRINT_METADATA=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)
      MODE="check"
      shift
      ;;
    --contracts-dir)
      CONTRACTS_SOURCE_DIR="$2"
      shift 2
      ;;
    --work-dir)
      WORK_DIR="$2"
      shift 2
      ;;
    --print-metadata)
      PRINT_METADATA=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

FIXTURE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXECUTE_DIR="$(cd "$FIXTURE_DIR/.." && pwd)"
TEST_GENESIS_TOML="$FIXTURE_DIR/test_genesis.toml"
VALIDATOR_GENESIS_JSON="$FIXTURE_DIR/validator_genesis.json"

if [[ -z "$WORK_DIR" ]]; then
  WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/gravity-genesis-fixture.XXXXXX")"
  CLEAN_WORK_DIR=1
else
  mkdir -p "$WORK_DIR"
  WORK_DIR="$(cd "$WORK_DIR" && pwd)"
  CLEAN_WORK_DIR=0
fi

METADATA_ENV="$WORK_DIR/metadata.env"
python3 - "$TEST_GENESIS_TOML" "$FIXTURE_DIR" > "$METADATA_ENV" <<'PY'
import json
import os
import shlex
import sys
import tomllib

toml_path, fixture_dir = sys.argv[1:3]
with open(toml_path, "rb") as f:
    data = tomllib.load(f)

deps = data["dependencies"]["genesis_contracts"]
genesis = data["genesis"]
output = data.get("output", {})
hardforks = genesis.get("hardforks", {})

output_path = output.get("path", "../gravity_hardfork.json")
if not os.path.isabs(output_path):
    output_path = os.path.normpath(os.path.join(fixture_dir, output_path))

values = {
    "CONTRACTS_REPO": deps["repo"],
    "CONTRACTS_REF": deps["ref"],
    "OUTPUT_PATH": output_path,
    "CHAIN_ID": str(genesis["chain_id"]),
    "GENESIS_TIMESTAMP": str(genesis["timestamp"]),
    "GRAVITY_MIN_BASE_FEE": "" if "gravity_min_base_fee" not in genesis else str(genesis["gravity_min_base_fee"]),
    "HARDFORKS_JSON": json.dumps(hardforks, separators=(",", ":")),
}

for key, value in values.items():
    print(f"{key}={shlex.quote(value)}")
PY

# shellcheck disable=SC1090
. "$METADATA_ENV"

if [[ "$PRINT_METADATA" == "1" ]]; then
  sed -n '1,120p' "$METADATA_ENV"
  exit 0
fi

ADDED_WORKTREE=0
CONTRACTS_DIR="$WORK_DIR/gravity_chain_core_contracts"
GENERATED_BASE="$WORK_DIR/genesis.base.json"
GENERATED_FINAL="$WORK_DIR/gravity_hardfork.generated.json"
TARGET_PATH="$OUTPUT_PATH"
if [[ "$MODE" == "check" ]]; then
  TARGET_WRITE_PATH="$GENERATED_FINAL"
else
  TARGET_WRITE_PATH="$TARGET_PATH"
fi

cleanup() {
  if [[ "$ADDED_WORKTREE" == "1" && -n "$CONTRACTS_SOURCE_DIR" ]]; then
    git -C "$CONTRACTS_SOURCE_DIR" worktree remove --force "$CONTRACTS_DIR" >/dev/null 2>&1 || true
  fi
  if [[ "$CLEAN_WORK_DIR" == "1" ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "required command not found: $1" >&2
    exit 1
  fi
}

require_cmd cargo
require_cmd forge
require_cmd git
require_cmd npm
require_cmd python3

prepare_contracts_checkout() {
  rm -rf "$CONTRACTS_DIR"

  if [[ -n "$CONTRACTS_SOURCE_DIR" ]]; then
    CONTRACTS_SOURCE_DIR="$(cd "$CONTRACTS_SOURCE_DIR" && pwd)"
    git -C "$CONTRACTS_SOURCE_DIR" rev-parse --git-dir >/dev/null
    git -C "$CONTRACTS_SOURCE_DIR" worktree add --detach "$CONTRACTS_DIR" "$CONTRACTS_REF"
    if [[ -d "$CONTRACTS_SOURCE_DIR/node_modules" && ! -e "$CONTRACTS_DIR/node_modules" ]]; then
      ln -s "$CONTRACTS_SOURCE_DIR/node_modules" "$CONTRACTS_DIR/node_modules"
    fi
    ADDED_WORKTREE=1
    return
  fi

  git init "$CONTRACTS_DIR"
  git -C "$CONTRACTS_DIR" remote add origin "$CONTRACTS_REPO"
  git -C "$CONTRACTS_DIR" fetch --depth 1 origin "$CONTRACTS_REF"
  git -C "$CONTRACTS_DIR" checkout --detach FETCH_HEAD
}

generate_base_genesis() {
  cd "$CONTRACTS_DIR"

  if [[ ! -d node_modules/forge-std || ! -d node_modules/@openzeppelin/contracts ]]; then
    if [[ -f package-lock.json ]]; then
      npm ci --ignore-scripts
    else
      npm install --ignore-scripts
    fi
  fi

  rm -rf out output account_alloc.json genesis.json

  forge build
  python3 scripts/helpers/extract_bytecode.py --out-dir out --output-dir out

  mkdir -p output
  cargo run --release --manifest-path genesis-tool/Cargo.toml -- \
    --log-file output/genesis_generation.log \
    generate \
    --byte-code-dir out \
    --config-file "$VALIDATOR_GENESIS_JSON" \
    --output output

  python3 scripts/helpers/combine_account_alloc.py output/genesis_contracts.json output/genesis_accounts.json
  python3 scripts/helpers/fix_hex_length.py account_alloc.json
  python3 scripts/helpers/genesis_generate.py \
    --template genesis-tool/config/genesis_template.json \
    --account-alloc account_alloc.json \
    --config-file "$VALIDATOR_GENESIS_JSON" \
    --output "$GENERATED_BASE"
}

apply_reth_overrides() {
  mkdir -p "$(dirname "$TARGET_WRITE_PATH")"
  python3 - \
    "$TEST_GENESIS_TOML" \
    "$GENERATED_BASE" \
    "$TARGET_WRITE_PATH" <<'PY'
import json
import sys
import tomllib

toml_path, input_path, output_path = sys.argv[1:4]
with open(toml_path, "rb") as f:
    test_genesis = tomllib.load(f)
with open(input_path, "r") as f:
    genesis = json.load(f)

config = genesis.setdefault("config", {})
settings = test_genesis["genesis"]

config["chainId"] = settings["chain_id"]
genesis["timestamp"] = hex(settings["timestamp"])

if "gravity_min_base_fee" in settings:
    config["gravityMinBaseFee"] = settings["gravity_min_base_fee"]
else:
    config.pop("gravityMinBaseFee", None)

for key, value in settings.get("hardforks", {}).items():
    config[key] = value

with open(output_path, "w") as f:
    json.dump(genesis, f, indent=2)
    f.write("\n")
PY
}

prepare_contracts_checkout
generate_base_genesis
apply_reth_overrides

if [[ "$MODE" == "check" ]]; then
  if cmp -s "$GENERATED_FINAL" "$TARGET_PATH"; then
    echo "gravity_hardfork.json matches regenerated fixture"
  else
    diff -u "$TARGET_PATH" "$GENERATED_FINAL" || true
    echo "gravity_hardfork.json is stale; run $0 to regenerate" >&2
    exit 1
  fi
else
  echo "wrote $TARGET_PATH"
fi
