#!/usr/bin/env bash
set -euo pipefail

ROLE="${ROLE:-nimbus}"

wait_port() {
	local host="$1" port="$2" label="${3:-$host:$port}"
	echo "[wait] Waiting for $label ..."
	for _ in $(seq 1 120); do
		if (echo >"/dev/tcp/$host/$port") >/dev/null 2>&1; then
			echo "[wait] $label is up."
			return 0
		fi
		sleep 1
	done
	echo "[wait] Timeout waiting for $label" >&2
	return 1
}

start_nimbus() {
	# Start Nimbus
	storm nimbus &
	echo "[storm] Nimbus starting..."

	# Wait for Nimbus Thrift (6627)
	wait_port 127.0.0.1 6627 "Nimbus (localhost:6627)"

	if [[ -n "${KAFKA_BROKERS:-}" ]]; then
		KAFKA_HOST="${KAFKA_BROKERS%%:*}"
		KAFKA_PORT="${KAFKA_BROKERS##*:}"
		wait_port "$KAFKA_HOST" "$KAFKA_PORT" "Kafka ($KAFKA_HOST:$KAFKA_PORT)"
	fi

	echo "[storm] Submitting topology..."
	storm jar /topology.jar com.example.TaxiTopology taxi-topo || true

	# Keep container alive with Nimbus
	wait
}

case "$ROLE" in
nimbus)
	start_nimbus
	;;
supervisor)
	exec storm supervisor
	;;
ui)
	exec storm ui
	;;
*)
	echo "Unknown ROLE=$ROLE" >&2
	exit 1
	;;
esac
