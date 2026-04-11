#!/usr/bin/env bash
# =============================================================================
# chaos.sh — Ripple chaos engineering toolkit
#
# Usage:
#   ./chaos.sh <command> [options]
#
# Commands:
#   register  <id> <url> [criticality]  Register a service with the probe engine
#   edge      <from> <to>               Add a dependency edge (from depends on to)
#   topology                            Dump the current topology as JSON
#   blast     <service-id>              Compute blast radius for a service
#   simulate  <service-id>              Hypothetical failure simulation
#   cascade   <service-id> [threshold]  Cascade failure simulation
#   kill      <service-id>              Deregister a service (simulates hard failure)
#   baseline  <hypothesis>              Capture steady-state baseline
#   evaluate  <hypothesis>              Evaluate steady-state hypothesis
#   events                              Stream live failure events (SSE)
#   help                                Show this help
# =============================================================================

set -euo pipefail

RIPPLE="${RIPPLE_URL:-http://localhost:8080}"

usage() {
  grep '^#' "$0" | sed 's/^# //' | sed 's/^#//'
  exit 0
}

require_jq() {
  if ! command -v jq &>/dev/null; then
    echo "⚠ jq not found — output will be raw JSON. Install jq for pretty printing."
    JQ="cat"
  else
    JQ="jq ."
  fi
}

register() {
  local id="${1:?service id required}"
  local url="${2:?endpoint URL required}"
  local criticality="${3:-5}"
  local interval="${4:-5000}"

  echo "→ Registering service [$id] at $url (criticality=$criticality)"
  curl -s -X POST "$RIPPLE/topology/services" \
    -H "Content-Type: application/json" \
    -d "{
      \"id\": \"$id\",
      \"name\": \"$id\",
      \"protocol\": \"HTTP\",
      \"endpoint\": \"$url\",
      \"criticality\": $criticality,
      \"probeIntervalMs\": $interval,
      \"latencyThresholdMs\": 500
    }" | $JQ
}

edge() {
  local from="${1:?'from' service id required}"
  local to="${2:?'to' service id required}"

  echo "→ Adding edge: $from depends on $to"
  curl -s -X POST "$RIPPLE/topology/edges" \
    -H "Content-Type: application/json" \
    -d "{\"from\": \"$from\", \"to\": \"$to\"}" | $JQ
}

topology() {
  echo "→ Current topology snapshot:"
  curl -s "$RIPPLE/topology" | $JQ
}

blast() {
  local id="${1:?service id required}"
  echo "→ Blast radius for [$id]:"
  curl -s -X POST "$RIPPLE/blast-radius/$id/simulate" | $JQ
}

cascade() {
  local id="${1:?service id required}"
  local threshold="${2:-0.5}"
  echo "→ Cascade simulation from [$id] (threshold=$threshold):"
  curl -s -X POST "$RIPPLE/blast-radius/$id/cascade?cascadeThreshold=$threshold" | $JQ
}

kill_service() {
  local id="${1:?service id required}"
  echo "→ Deregistering (killing) service [$id]"
  curl -s -X DELETE "$RIPPLE/topology/services/$id"
  echo "✓ Service [$id] removed from topology"
}

baseline() {
  local name="${1:?hypothesis name required}"
  echo "→ Capturing steady-state baseline for [$name]"
  curl -s -X POST "$RIPPLE/steady-state/$name/baseline" | $JQ
}

evaluate() {
  local name="${1:?hypothesis name required}"
  echo "→ Evaluating steady-state hypothesis [$name]"
  curl -s -X POST "$RIPPLE/steady-state/$name/evaluate" \
    -H "Content-Type: application/json" \
    -d '{"probeLatencyP99MaxMs":500,"circuitBreakersAllClosed":true,"subscriberLagMax":500}' | $JQ
}

stream_events() {
  echo "→ Streaming live failure events from $RIPPLE/events/stream"
  echo "  Press Ctrl-C to stop"
  echo ""
  curl -sN "$RIPPLE/events/stream"
}

health() {
  echo "→ System health:"
  curl -s "$RIPPLE/health" | $JQ
}

demo() {
  echo "=================================================="
  echo "  Ripple Demo: Linear Chain Cascade"
  echo "=================================================="
  echo ""
  require_jq

  echo "1. Registering services..."
  register "database"        "http://localhost:5432" 10 5000
  register "productcatalog"  "http://localhost:8081/health" 9 5000
  register "checkout"        "http://localhost:8082/health" 8 5000
  register "frontend"        "http://localhost:8083/health" 6 5000

  echo ""
  echo "2. Wiring dependencies..."
  edge "productcatalog" "database"
  edge "checkout" "productcatalog"
  edge "frontend" "checkout"

  echo ""
  echo "3. Capturing baseline..."
  baseline "linear-chain-demo"

  echo ""
  echo "4. Topology snapshot:"
  topology

  echo ""
  echo "5. Hypothetical failure: what if 'database' fails?"
  blast "database"

  echo ""
  echo "6. Cascade simulation (threshold=0.5):"
  cascade "database" 0.5

  echo ""
  echo "7. Evaluating steady-state (all services healthy):"
  evaluate "linear-chain-demo"

  echo ""
  echo "Demo complete. Start streaming events with:"
  echo "  ./chaos.sh events"
}

require_jq

case "${1:-help}" in
  register)  register  "${@:2}" ;;
  edge)      edge      "${@:2}" ;;
  topology)  topology           ;;
  blast)     blast     "${@:2}" ;;
  simulate)  blast     "${@:2}" ;;
  cascade)   cascade   "${@:2}" ;;
  kill)      kill_service "${@:2}" ;;
  baseline)  baseline  "${@:2}" ;;
  evaluate)  evaluate  "${@:2}" ;;
  events)    stream_events      ;;
  health)    health             ;;
  demo)      demo               ;;
  help|--help|-h) usage         ;;
  *)
    echo "Unknown command: $1"
    echo "Run './chaos.sh help' for usage"
    exit 1
    ;;
esac
