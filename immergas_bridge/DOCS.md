# Immergas Bridge add-on

This add-on connects to the Immergas Domuninus module on port 2000, authenticates with the configured MAC address and password, drains the echoed auth string, polls key PDUs, and publishes entities to Home Assistant through MQTT discovery.

## Exposed entities

- Climate entity for zone 1
  - modes: `off`, `heat`
- DHW temperature sensor
- DHW setpoint number
- Boiler status sensors
- Zone count sensor

## Why only `off` and `heat`

This package intentionally does not expose cooling mode. It also does not expose `hot_water_only` as a selectable HVAC mode. If the device reports raw status `1`, the climate entity shows `off`, while the diagnostic status sensor still shows `hot_water_only`.

## Configuration

Set:
- `heater_host`
- `heater_mac`
- `heater_password`
- MQTT settings

If you use the Mosquitto broker add-on, `mqtt_host: core-mosquitto` is the normal choice.
