#!/bin/bash
set -e

ROLE="${ROLE:-nimbus}"

start_nimbus() {
	storm nimbus &
	echo "[storm] Nimbus starting..."
	for i in {1..60}; do
		nc -z 127.0.0.1 6627 && break || sleep 1
	done
	echo "[storm] Submitting topology..."
	storm jar /topology.jar com.example.TaxiTopology taxi-topo || true
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
