#!/bin/sh
# PRE-EMPTIVE SETUP: idempotently creates the local development namespace.
set -eu

NAMESPACE="${DEFAULT_NAMESPACE:-default}"
ADDRESS="${TEMPORAL_ADDRESS:-temporal:7233}"

if temporal operator namespace describe -n "${NAMESPACE}" --address "${ADDRESS}" >/dev/null 2>&1; then
  exit 0
fi

temporal operator namespace create -n "${NAMESPACE}" --address "${ADDRESS}"
