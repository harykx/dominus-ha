#!/usr/bin/with-contenv python3
from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
import threading
import time
from dataclasses import dataclass
from typing import Optional

import paho.mqtt.client as mqtt

LOG = logging.getLogger("immergas_bridge")

MAP1 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 "
MAP2 = "0SFGLcTdjaxZPyeK9QhptB5v7zJH3Mq1VibDfCAN6EgYRwXlo8Wk4mun2rOsUI="

PDU_BOILER_STATUS = 2000
PDU_ZONE1_TEMP = 2011
PDU_ZONE1_SET = 2015
PDU_DHW_SET = 2095
PDU_DHW_TEMP = 3016
PDU_FLAGS_2001 = 2001
PDU_FLAGS_2100 = 2100
PDU_FLAGS_2101 = 2101
PDU_NUM_ZONES = 4199


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


CONFIG = {
    "heater_host": env("IMMERGAS_HEATER_HOST", "192.168.1.50"),
    "heater_port": int(env("IMMERGAS_HEATER_PORT", "2000")),
    "heater_mac": env("IMMERGAS_HEATER_MAC", "00:11:22:33:44:55"),
    "heater_password": env("IMMERGAS_HEATER_PASSWORD", "YOUR_HEATER_PASSWORD"),
    "mqtt_host": env("IMMERGAS_MQTT_HOST", "core-mosquitto"),
    "mqtt_port": int(env("IMMERGAS_MQTT_PORT", "1883")),
    "mqtt_username": env("IMMERGAS_MQTT_USERNAME", ""),
    "mqtt_password": env("IMMERGAS_MQTT_PASSWORD", ""),
    "mqtt_client_id": env("IMMERGAS_MQTT_CLIENT_ID", "immergas-ha-bridge"),
    "topic_root": env("IMMERGAS_TOPIC_ROOT", "immergas"),
    "fast_poll_seconds": float(env("IMMERGAS_FAST_POLL_SECONDS", "15")),
    "slow_poll_seconds": float(env("IMMERGAS_SLOW_POLL_SECONDS", "60")),
    "socket_timeout_seconds": float(env("IMMERGAS_SOCKET_TIMEOUT_SECONDS", "2.0")),
    "reconnect_seconds": float(env("IMMERGAS_RECONNECT_SECONDS", "5")),
    "log_level": env("IMMERGAS_LOG_LEVEL", "INFO"),
    "debug_frames": env("IMMERGAS_DEBUG_FRAMES", "false").lower() in {"1", "true", "yes", "on"},
}


@dataclass
class BridgeState:
    heater_connected: bool = False
    boiler_status: Optional[int] = None
    zone1_current: Optional[float] = None
    zone1_target: Optional[float] = None
    dhw_current: Optional[float] = None
    dhw_target: Optional[float] = None
    zones: Optional[int] = None
    flags_2001: Optional[int] = None
    flags_2100: Optional[int] = None
    flags_2101: Optional[int] = None


class ImmergasClient:
    def __init__(self, host: str, port: int, mac: str, password: str, timeout: float, debug_frames: bool = False):
        self.host = host
        self.port = port
        self.mac = mac
        self.password = password
        self.timeout = timeout
        self.debug_frames = debug_frames
        self.sock: Optional[socket.socket] = None
        self.lock = threading.Lock()
        self.auth = self.make_auth(mac, password)

    @staticmethod
    def normalize_mac(mac: str) -> str:
        return mac.replace(":", "").replace("-", "").lower()

    @staticmethod
    def md5_hex_12(password: str) -> str:
        return hashlib.md5(password.encode("utf-8")).hexdigest()[:12]

    @classmethod
    def bp_encode(cls, text: str, move: int) -> str:
        out = []
        n = len(MAP1)
        for ch in text:
            idx = MAP1.find(ch)
            if idx == -1:
                out.append(ch)
            else:
                out.append(MAP2[(idx + move) % n])
        return "".join(out)

    @classmethod
    def make_auth(cls, mac: str, password: str) -> str:
        plain = f"{cls.normalize_mac(mac)} {cls.md5_hex_12(password)}"
        return "#D" + cls.bp_encode(plain, 2)

    @staticmethod
    def calc_crc(body: bytes) -> int:
        poly = 4129
        crc = 0xFFFF
        for c in body:
            for _ in range(8):
                if (crc & 1) == (c & 1):
                    crc >>= 1
                else:
                    crc = (crc >> 1) ^ poly
                c >>= 1
        return crc & 0xFFFF

    @classmethod
    def build_read_frame(cls, pdu: int, req_type: int = 0x00) -> bytes:
        body = bytes([req_type, (pdu >> 8) & 0xFF, pdu & 0xFF, 0x00, 0x00])
        crc = cls.calc_crc(body)
        return body + bytes([(crc >> 8) & 0xFF, crc & 0xFF])

    @classmethod
    def build_write_frame(cls, pdu: int, value: int, req_type: int = 0x90) -> bytes:
        body = bytes([req_type, (pdu >> 8) & 0xFF, pdu & 0xFF, (value >> 8) & 0xFF, value & 0xFF])
        crc = cls.calc_crc(body)
        return body + bytes([(crc >> 8) & 0xFF, crc & 0xFF])

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _recv_exact(self, length: int) -> bytes:
        assert self.sock is not None
        data = bytearray()
        while len(data) < length:
            chunk = self.sock.recv(length - len(data))
            if not chunk:
                raise ConnectionError("Heater socket closed")
            data.extend(chunk)
        return bytes(data)

    def _drain_post_auth(self) -> None:
        assert self.sock is not None
        time.sleep(0.2)
        original_timeout = self.sock.gettimeout()
        self.sock.settimeout(0.2)
        drained = bytearray()
        try:
            while True:
                chunk = self.sock.recv(256)
                if not chunk:
                    break
                drained.extend(chunk)
                if len(chunk) < 256:
                    break
        except socket.timeout:
            pass
        finally:
            self.sock.settimeout(original_timeout)
        if drained and self.debug_frames:
            LOG.info("RX post-auth: %s", drained.hex(" "))
            try:
                LOG.info("RX post-auth ascii: %s", drained.decode("ascii", errors="replace"))
            except Exception:
                pass

    def connect(self) -> None:
        self.close()
        sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        sock.settimeout(self.timeout)
        sock.sendall(self.auth.encode("ascii"))
        self.sock = sock
        self._drain_post_auth()
        LOG.info("Connected to heater at %s:%s", self.host, self.port)

    def exchange(self, frame: bytes) -> bytes:
        with self.lock:
            if self.sock is None:
                raise ConnectionError("Heater not connected")
            if self.debug_frames:
                LOG.info("TX frame: %s", frame.hex(" "))
            self.sock.sendall(frame)
            reply = self._recv_exact(7)
            if self.debug_frames:
                LOG.info("RX frame: %s", reply.hex(" "))
            crc_rx = (reply[5] << 8) | reply[6]
            crc_calc = self.calc_crc(reply[:5])
            if crc_rx != crc_calc:
                raise ValueError(f"CRC mismatch: rx=0x{crc_rx:04x}, calc=0x{crc_calc:04x}")
            return reply

    def read_pdu(self, pdu: int) -> tuple[int, int]:
        last_error: Optional[Exception] = None
        for req_type in (0x00, 0x80):
            try:
                reply = self.exchange(self.build_read_frame(pdu, req_type=req_type))
                rx_pdu = (reply[1] << 8) | reply[2]
                if rx_pdu != pdu:
                    raise ValueError(f"Unexpected reply PDU 0x{rx_pdu:04x}")
                value = (reply[3] << 8) | reply[4]
                return reply[0], value
            except Exception as exc:
                last_error = exc
        assert last_error is not None
        raise last_error

    def write_pdu(self, pdu: int, value: int) -> tuple[int, int]:
        reply = self.exchange(self.build_write_frame(pdu, value, req_type=0x90))
        rx_pdu = (reply[1] << 8) | reply[2]
        if rx_pdu != pdu:
            raise ValueError(f"Unexpected write reply PDU 0x{rx_pdu:04x}")
        echoed = (reply[3] << 8) | reply[4]
        return reply[0], echoed


class Bridge:
    def __init__(self, config: dict):
        self.config = config
        self.state = BridgeState()
        self.discovery_published = False
        self.stop_requested = False
        self.heater = ImmergasClient(
            host=config["heater_host"],
            port=config["heater_port"],
            mac=config["heater_mac"],
            password=config["heater_password"],
            timeout=config["socket_timeout_seconds"],
            debug_frames=config["debug_frames"],
        )
        self.mqtt = mqtt.Client(
            client_id=config["mqtt_client_id"],
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
        if config["mqtt_username"]:
            self.mqtt.username_pw_set(config["mqtt_username"], config["mqtt_password"])
        self.mqtt.will_set(self.topic("availability"), payload="offline", qos=1, retain=True)
        self.mqtt.on_connect = self.on_mqtt_connect
        self.mqtt.on_disconnect = self.on_mqtt_disconnect
        self.mqtt.on_message = self.on_mqtt_message

    def topic(self, suffix: str) -> str:
        return f"{self.config['topic_root']}/{suffix}"

    @staticmethod
    def boiler_status_to_hvac_mode(value: Optional[int]) -> str:
        return {0: "off", 1: "off", 2: "off", 3: "heat"}.get(value, "off")

    @staticmethod
    def boiler_status_to_label(value: Optional[int]) -> str:
        return {0: "off", 1: "hot_water_only", 2: "cooling", 3: "heating"}.get(value, "unknown")

    def mqtt_publish(self, topic: str, payload, retain: bool = True) -> None:
        self.mqtt.publish(topic, payload=str(payload), qos=1, retain=retain)

    def publish_availability(self) -> None:
        self.mqtt_publish(self.topic("availability"), "online" if self.state.heater_connected else "offline")

    def publish_discovery(self) -> None:
        device = {
            "identifiers": ["immergas_magis_combo_plus_v2"],
            "name": "Immergas Magis Combo Plus V2",
            "manufacturer": "Immergas",
            "model": "Magis Combo Plus V2",
        }
        climate_payload = {
            "name": "Immergas Zone 1",
            "unique_id": "immergas_zone1_climate",
            "device": device,
            "availability_topic": self.topic("availability"),
            "current_temperature_topic": self.topic("zone1/state/current_temperature"),
            "temperature_state_topic": self.topic("zone1/state/target_temperature"),
            "temperature_command_topic": self.topic("zone1/command/set_temperature"),
            "mode_state_topic": self.topic("zone1/state/hvac_mode"),
            "mode_command_topic": self.topic("zone1/command/set_hvac_mode"),
            "temp_step": 0.1,
            "precision": 0.1,
            "min_temp": 5.0,
            "max_temp": 35.0,
            "modes": ["off", "heat"],
        }
        self.mqtt.publish("homeassistant/climate/immergas_zone1/config", json.dumps(climate_payload), qos=1, retain=True)

        boiler_status_payload = {
            "name": "Immergas Boiler Status",
            "unique_id": "immergas_boiler_status_label",
            "device": device,
            "availability_topic": self.topic("availability"),
            "state_topic": self.topic("diagnostic/boiler_status_label"),
            "icon": "mdi:fire",
        }
        self.mqtt.publish("homeassistant/sensor/immergas_boiler_status/config", json.dumps(boiler_status_payload), qos=1, retain=True)

        boiler_raw_payload = {
            "name": "Immergas Boiler Status Raw",
            "unique_id": "immergas_boiler_status_raw",
            "device": device,
            "availability_topic": self.topic("availability"),
            "state_topic": self.topic("diagnostic/boiler_status_raw"),
            "icon": "mdi:numeric",
        }
        self.mqtt.publish("homeassistant/sensor/immergas_boiler_status_raw/config", json.dumps(boiler_raw_payload), qos=1, retain=True)

        dhw_temp_payload = {
            "name": "Immergas DHW Temperature",
            "unique_id": "immergas_dhw_temperature",
            "device": device,
            "availability_topic": self.topic("availability"),
            "state_topic": self.topic("dhw/state/current_temperature"),
            "unit_of_measurement": "°C",
            "device_class": "temperature",
            "state_class": "measurement",
        }
        self.mqtt.publish("homeassistant/sensor/immergas_dhw_temperature/config", json.dumps(dhw_temp_payload), qos=1, retain=True)

        dhw_set_payload = {
            "name": "Immergas DHW Setpoint",
            "unique_id": "immergas_dhw_setpoint",
            "device": device,
            "availability_topic": self.topic("availability"),
            "state_topic": self.topic("dhw/state/target_temperature"),
            "command_topic": self.topic("dhw/command/set_temperature"),
            "unit_of_measurement": "°C",
            "min": 30,
            "max": 65,
            "step": 0.1,
            "mode": "box",
            "icon": "mdi:thermometer-water",
        }
        self.mqtt.publish("homeassistant/number/immergas_dhw_setpoint/config", json.dumps(dhw_set_payload), qos=1, retain=True)

        zones_payload = {
            "name": "Immergas Zone Count",
            "unique_id": "immergas_zone_count",
            "device": device,
            "availability_topic": self.topic("availability"),
            "state_topic": self.topic("diagnostic/zones"),
            "icon": "mdi:counter",
        }
        self.mqtt.publish("homeassistant/sensor/immergas_zone_count/config", json.dumps(zones_payload), qos=1, retain=True)

        self.discovery_published = True
        LOG.info("Published Home Assistant discovery")

    def publish_state(self) -> None:
        if self.state.zone1_current is not None:
            self.mqtt_publish(self.topic("zone1/state/current_temperature"), f"{self.state.zone1_current:.1f}")
        if self.state.zone1_target is not None:
            self.mqtt_publish(self.topic("zone1/state/target_temperature"), f"{self.state.zone1_target:.1f}")
        if self.state.dhw_current is not None:
            self.mqtt_publish(self.topic("dhw/state/current_temperature"), f"{self.state.dhw_current:.1f}")
        if self.state.dhw_target is not None:
            self.mqtt_publish(self.topic("dhw/state/target_temperature"), f"{self.state.dhw_target:.1f}")
        if self.state.boiler_status is not None:
            self.mqtt_publish(self.topic("zone1/state/hvac_mode"), self.boiler_status_to_hvac_mode(self.state.boiler_status))
            self.mqtt_publish(self.topic("diagnostic/boiler_status_raw"), self.state.boiler_status)
            self.mqtt_publish(self.topic("diagnostic/boiler_status_label"), self.boiler_status_to_label(self.state.boiler_status))
        if self.state.zones is not None:
            self.mqtt_publish(self.topic("diagnostic/zones"), self.state.zones)
        self.publish_availability()

    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        LOG.info("Connected to MQTT: %s", reason_code)
        client.subscribe(self.topic("zone1/command/set_temperature"), qos=1)
        client.subscribe(self.topic("zone1/command/set_hvac_mode"), qos=1)
        client.subscribe(self.topic("dhw/command/set_temperature"), qos=1)
        self.publish_availability()
        if not self.discovery_published:
            self.publish_discovery()
            self.publish_state()

    def on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties):
        LOG.warning("Disconnected from MQTT: %s", reason_code)

    def on_mqtt_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8").strip()
        LOG.info("MQTT command %s = %s", topic, payload)
        try:
            if topic == self.topic("zone1/command/set_temperature"):
                raw = int(round(float(payload) * 10.0))
                _, echoed = self.heater.write_pdu(PDU_ZONE1_SET, raw)
                self.state.zone1_target = echoed / 10.0
                self.mqtt_publish(self.topic("zone1/state/target_temperature"), f"{self.state.zone1_target:.1f}")
                return
            if topic == self.topic("dhw/command/set_temperature"):
                raw = int(round(float(payload) * 10.0))
                _, echoed = self.heater.write_pdu(PDU_DHW_SET, raw)
                self.state.dhw_target = echoed / 10.0
                self.mqtt_publish(self.topic("dhw/state/target_temperature"), f"{self.state.dhw_target:.1f}")
                return
            if topic == self.topic("zone1/command/set_hvac_mode"):
                mapping = {"off": 0, "heat": 3}
                if payload not in mapping:
                    LOG.warning("Ignoring unsupported hvac mode: %s", payload)
                    return
                _, echoed = self.heater.write_pdu(PDU_BOILER_STATUS, mapping[payload])
                self.state.boiler_status = echoed
                self.mqtt_publish(self.topic("zone1/state/hvac_mode"), self.boiler_status_to_hvac_mode(echoed))
                self.mqtt_publish(self.topic("diagnostic/boiler_status_raw"), echoed)
                self.mqtt_publish(self.topic("diagnostic/boiler_status_label"), self.boiler_status_to_label(echoed))
                return
        except Exception:
            LOG.exception("Failed to process MQTT command")
            self.state.heater_connected = False
            self.publish_availability()
            self.heater.close()

    def connect_mqtt(self) -> None:
        LOG.info("Connecting to MQTT at %s:%s", self.config["mqtt_host"], self.config["mqtt_port"])
        self.mqtt.connect(self.config["mqtt_host"], self.config["mqtt_port"], keepalive=30)
        self.mqtt.loop_start()

    def ensure_heater(self) -> None:
        if self.state.heater_connected:
            return
        self.heater.connect()
        self.state.heater_connected = True
        self.publish_availability()

    def poll_fast(self) -> None:
        _, value = self.heater.read_pdu(PDU_BOILER_STATUS)
        self.state.boiler_status = value
        _, value = self.heater.read_pdu(PDU_ZONE1_TEMP)
        self.state.zone1_current = value / 10.0
        _, value = self.heater.read_pdu(PDU_ZONE1_SET)
        self.state.zone1_target = value / 10.0
        _, value = self.heater.read_pdu(PDU_DHW_TEMP)
        self.state.dhw_current = value / 10.0
        _, value = self.heater.read_pdu(PDU_DHW_SET)
        self.state.dhw_target = value / 10.0
        self.publish_state()

    def poll_slow(self) -> None:
        _, value = self.heater.read_pdu(PDU_NUM_ZONES)
        self.state.zones = value
        _, value = self.heater.read_pdu(PDU_FLAGS_2001)
        self.state.flags_2001 = value
        _, value = self.heater.read_pdu(PDU_FLAGS_2100)
        self.state.flags_2100 = value
        _, value = self.heater.read_pdu(PDU_FLAGS_2101)
        self.state.flags_2101 = value
        self.publish_state()

    def run(self) -> None:
        self.connect_mqtt()
        next_fast = 0.0
        next_slow = 0.0
        while not self.stop_requested:
            try:
                self.ensure_heater()
                now = time.monotonic()
                if now >= next_fast:
                    self.poll_fast()
                    next_fast = now + self.config["fast_poll_seconds"]
                if now >= next_slow:
                    self.poll_slow()
                    next_slow = now + self.config["slow_poll_seconds"]
                time.sleep(0.1)
            except KeyboardInterrupt:
                raise
            except Exception:
                LOG.exception("Bridge loop error")
                self.state.heater_connected = False
                self.publish_availability()
                self.heater.close()
                time.sleep(self.config["reconnect_seconds"])

    def stop(self) -> None:
        self.stop_requested = True
        try:
            self.mqtt.loop_stop()
        except Exception:
            pass
        self.heater.close()


def main() -> int:
    logging.basicConfig(
        level=getattr(logging, CONFIG["log_level"].upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    bridge = Bridge(CONFIG)
    try:
        bridge.run()
    except KeyboardInterrupt:
        LOG.info("Stopping bridge")
    finally:
        bridge.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
