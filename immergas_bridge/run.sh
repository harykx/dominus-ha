#!/usr/bin/with-contenv bashio
set -euo pipefail

export IMMERGAS_HEATER_HOST="$(bashio::config 'heater_host')"
export IMMERGAS_HEATER_PORT="$(bashio::config 'heater_port')"
export IMMERGAS_HEATER_MAC="$(bashio::config 'heater_mac')"
export IMMERGAS_HEATER_PASSWORD="$(bashio::config 'heater_password')"
export IMMERGAS_MQTT_HOST="$(bashio::config 'mqtt_host')"
export IMMERGAS_MQTT_PORT="$(bashio::config 'mqtt_port')"
export IMMERGAS_MQTT_USERNAME="$(bashio::config 'mqtt_username')"
export IMMERGAS_MQTT_PASSWORD="$(bashio::config 'mqtt_password')"
export IMMERGAS_MQTT_CLIENT_ID="$(bashio::config 'mqtt_client_id')"
export IMMERGAS_TOPIC_ROOT="$(bashio::config 'topic_root')"
export IMMERGAS_FAST_POLL_SECONDS="$(bashio::config 'fast_poll_seconds')"
export IMMERGAS_SLOW_POLL_SECONDS="$(bashio::config 'slow_poll_seconds')"
export IMMERGAS_SOCKET_TIMEOUT_SECONDS="$(bashio::config 'socket_timeout_seconds')"
export IMMERGAS_RECONNECT_SECONDS="$(bashio::config 'reconnect_seconds')"
export IMMERGAS_LOG_LEVEL="$(bashio::config 'log_level')"
export IMMERGAS_DEBUG_FRAMES="$(bashio::config 'debug_frames')"

bashio::log.info "Starting Immergas Bridge"
exec /opt/venv/bin/python /app/immergas_bridge.py