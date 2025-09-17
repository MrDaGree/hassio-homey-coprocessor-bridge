#!/bin/bash
set -euo pipefail

HOMEY_MODEL="$(node -e 'try{process.stdout.write(JSON.parse(require("fs").readFileSync("/data/options.json","utf8")).model||"")}catch(e){process.stdout.write("")}')"
export HOMEY_MODEL

export_pin() {
  local n="$1"
  [[ -d "/sys/class/gpio/gpio${n}" ]] || echo "$n" > /sys/class/gpio/export 2>/dev/null || true
  for i in {1..20}; do
    [[ -e "/sys/class/gpio/gpio${n}/value" ]] && break || sleep 0.05
  done
}

try() { "$@" 2>/dev/null || true; }

export_pin 6
try sh -c "echo in   > /sys/class/gpio/gpio6/direction"
if [[ -e /sys/class/gpio/gpio6/edge ]]; then
  try sh -c "echo both > /sys/class/gpio/gpio6/edge"
fi

export_pin 24
try sh -c "echo high > /sys/class/gpio/gpio24/direction" || {
  try sh -c "echo out > /sys/class/gpio/gpio24/direction"
  try sh -c "echo 1   > /sys/class/gpio/gpio24/value"
}

export_pin 25
try sh -c "echo high > /sys/class/gpio/gpio25/direction" || {
  try sh -c "echo out > /sys/class/gpio/gpio25/direction"
  try sh -c "echo 1   > /sys/class/gpio/gpio25/value"
}

for n in 24 25; do
  [[ -e /sys/class/gpio/gpio${n}/value ]] || echo "WARN: /sys/class/gpio/gpio${n}/value missing"
done

ln -sf /dev/ttyAMA2 /homey/uart_coprocessor_ctrl
ln -sf /dev/ttyAMA3 /homey/uart_coprocessor_prog

: > /data/esp32.log || true

exec node /app/mqtt_bridge.mjs