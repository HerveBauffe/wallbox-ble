#!/usr/bin/env python3
import asyncio
import json
import logging
import random
import sys
from bleak import BleakClient
from gmqtt import Client as MQTTClient
from gmqtt.mqtt.constants import MQTTv311

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOGGER = logging.getLogger("wallbox_ble_mqtt")

# ==================== CONFIGURATION ====================
WALLBOX_MAC = "xx:xx:xx:xx:xx:xx"

MQTT_BROKER = "192.168.1.xxx"       # IP of your MQTT Broker
MQTT_PORT = 1883
MQTT_USER = "MQTT-USERNAME"              # Leave empty "" if unused
MQTT_PASSWORD = "MQTT-PASSWORD"               # Leave empty "" if unused

DEVICE_ID = "wallbox_ble"          
DEVICE_NAME = "Wallbox BLE"
# =======================================================

class WallboxBLEApiConst:
    UART_SERVICE_UUID = "331a36f5-2459-45ea-9d95-6142f0c4b307"
    UART_RX_CHAR_UUID = "a9da6040-0823-4995-94ec-9ce41ca28833"
    UART_TX_CHAR_UUID = "a73e9a10-628f-4494-a099-12efaf72258f"

    GET_STATUS = "r_dat"
    GET_MAX_AVAILABLE_CURRENT = "r_fsI"
    LOCK = "w_lck"
    SET_MAX_CHARGING_CURRENT = "w_mxI"
    START_STOP_CHARGING = "w_cha"

    STATUS_CODES = [
        "READY", "CHARGING", "CONNECTED_WAITING_CAR", "CONNECTED_WAITING_SCHEDULE",
        "PAUSED", "SCHEDULE_END", "LOCKED", "ERROR", "CONNECTED_WAITING_CURRENT_ASSIGNATION",
        "UNCONFIGURED_POWER_SHARING", "QUEUE_BY_POWER_BOOST", "DISCHARGING",
        "CONNECTED_WAITING_ADMIN_AUTH_FOR_MID", "CONNECTED_MID_SAFETY_MARGIN_EXCEEDED",
        "OCPP_UNAVAILABLE", "OCPP_CHARGE_FINISHING", "OCPP_RESERVED", "UPDATING",
        "QUEUE_BY_ECO_SMART"
    ]

class WallboxBLEApiClient:
    def __init__(self, address: str):
        self.address = address
        self.client = None
        self._rx_buffer = bytearray()
        self._rx_event = asyncio.Event()

    async def notification_handler(self, sender, data):
        self._rx_buffer.extend(data)
        self._rx_event.set()

    def clear_rx_queue(self):
        self._rx_buffer.clear()
        self._rx_event.clear()

    async def connect(self):
        LOGGER.info(f"Connexion Bluetooth à la Wallbox ({self.address})...")
        self.client = BleakClient(self.address)
        await self.client.connect()
        
        try:
            await self.client.pair()
        except Exception:
            pass

        await self.client.start_notify(WallboxBLEApiConst.UART_TX_CHAR_UUID, self.notification_handler)
        LOGGER.info("Bluetooth connecté et prêt !")

    async def get_parsed_response(self, request_id):
        while True:
            await self._rx_event.wait()
            self._rx_event.clear()
            try:
                first_open = self._rx_buffer.find(b'{')
                last_close = self._rx_buffer.rfind(b'}')
                if first_open != -1 and last_close != -1 and last_close > first_open:
                    json_bytes = self._rx_buffer[first_open:last_close+1]
                    parsed_data = json.loads(json_bytes.decode('utf-8'))
                    if parsed_data.get("id") == request_id:
                        return parsed_data.get("r")
            except Exception:
                pass
            await asyncio.sleep(0.05)

    async def request(self, method, parameter=None):
        if not self.client or not self.client.is_connected:
            LOGGER.warning("Commande avortée : Bluetooth déconnecté.")
            return False, None

        request_id = random.randint(1, 999)
        
        if parameter is not None:
            payload = f'{{"met":"{method}","par":{parameter},"id":{request_id}}}'
        else:
            payload = f'{{"met":"{method}","id":{request_id}}}'
            
        data = payload.encode("utf8")
        packet = b"EaE" + bytes([len(data)]) + data
        packet = packet + bytes([sum(c for c in packet) % 256])

        self.clear_rx_queue()

        try:
            await asyncio.wait_for(
                self.client.write_gatt_char(WallboxBLEApiConst.UART_RX_CHAR_UUID, packet, True), 
                timeout=2.0
            )
            response = await asyncio.wait_for(self.get_parsed_response(request_id), timeout=3.0)
            return True, response
        except asyncio.TimeoutError:
            LOGGER.warning(f"Timeout sur la requête BLE {method}")
            return False, None
        except Exception as e:
            LOGGER.error(f"Erreur d'exécution BLE {method} : {e}")
            return False, None


# BLE Client global instance
ble_client_global = None

def publish_discovery(mqtt_client):
    dev_info = {"identifiers": [DEVICE_ID], "name": DEVICE_NAME, "manufacturer": "Wallbox"}
    state_topic = f"homeassistant/sensor/{DEVICE_ID}/state"
    
    # Status (Main text sensor)
    mqtt_client.publish(f"homeassistant/sensor/{DEVICE_ID}_status/config", json.dumps({
        "name": "Status", "state_topic": state_topic,
        "value_template": "{{ value_json.status }}", "unique_id": f"{DEVICE_ID}_status", "device": dev_info
    }), qos=1, retain=True)

    # Wallbox Ready Binary Sensor (st == 0)
    mqtt_client.publish(f"homeassistant/binary_sensor/{DEVICE_ID}_ready/config", json.dumps({
        "name": "Ready", "state_topic": state_topic,
        "value_template": "{{ 'ON' if value_json.status_code == 0 else 'OFF' }}",
        "device_class": "connectivity", "unique_id": f"{DEVICE_ID}_ready", "device": dev_info
    }), qos=1, retain=True)

    # Wallbox Charging Binary Sensor (st == 1)
    mqtt_client.publish(f"homeassistant/binary_sensor/{DEVICE_ID}_charging/config", json.dumps({
        "name": "Charging", "state_topic": state_topic,
        "value_template": "{{ 'ON' if value_json.status_code == 1 else 'OFF' }}",
        "device_class": "battery_charging", "unique_id": f"{DEVICE_ID}_charging", "device": dev_info
    }), qos=1, retain=True)

    # Lock
    mqtt_client.publish(f"homeassistant/lock/{DEVICE_ID}/config", json.dumps({
        "name": "Lock", "state_topic": state_topic,
        "value_template": "{{ 'LOCKED' if value_json.status_code == 6 else 'UNLOCKED' }}",
        "command_topic": f"homeassistant/lock/{DEVICE_ID}/set", "unique_id": f"{DEVICE_ID}_lock", "device": dev_info
    }), qos=1, retain=True)

    # Current
    mqtt_client.publish(f"homeassistant/number/{DEVICE_ID}_current/config", json.dumps({
        "name": "Charge Current", "state_topic": state_topic,
        "value_template": "{{ value_json.charge_current }}", "command_topic": f"homeassistant/number/{DEVICE_ID}/set",
        "min": 6, "max": 32, "step": 1, "unit_of_measurement": "A", "device_class": "current",
        "unique_id": f"{DEVICE_ID}_current", "device": dev_info
    }), qos=1, retain=True)

    # Charge Switch
    mqtt_client.publish(f"homeassistant/switch/{DEVICE_ID}_charge/config", json.dumps({
        "name": "Charge Switch", "state_topic": state_topic,
        "value_template": "{{ 'ON' if value_json.status_code == 1 else 'OFF' }}",
        "command_topic": f"homeassistant/switch/{DEVICE_ID}/set", "unique_id": f"{DEVICE_ID}_switch", "device": dev_info
    }), qos=1, retain=True)

    # Power Sensor
    mqtt_client.publish(f"homeassistant/sensor/{DEVICE_ID}_power/config", json.dumps({
        "name": "Charging Power", "state_topic": state_topic,
        "value_template": "{{ value_json.charging_power }}", "unit_of_measurement": "kW",
        "device_class": "power", "state_class": "measurement", "unique_id": f"{DEVICE_ID}_power", "device": dev_info
    }), qos=1, retain=True)

    # Energy Meter
    mqtt_client.publish(f"homeassistant/sensor/{DEVICE_ID}_energy/config", json.dumps({
        "name": "Session Energy", "state_topic": state_topic,
        "value_template": "{{ value_json.session_energy }}", "unit_of_measurement": "kWh",
        "device_class": "energy", "state_class": "total_increasing", "unique_id": f"{DEVICE_ID}_energy", "device": dev_info
    }), qos=1, retain=True)

async def handle_mqtt_command(topic, msg_payload):
    """Background BLE Call from synchronous MQTT."""
    global ble_client_global
    try:
        if f"lock/{DEVICE_ID}/set" in topic:
            state = 1 if msg_payload == "LOCK" else 0
            await ble_client_global.request(WallboxBLEApiConst.LOCK, state)
        elif f"number/{DEVICE_ID}/set" in topic:
            amps = int(float(msg_payload))
            await ble_client_global.request(WallboxBLEApiConst.SET_MAX_CHARGING_CURRENT, amps)
        elif f"switch/{DEVICE_ID}/set" in topic:
            state = 1 if msg_payload == "ON" else 0
            await ble_client_global.request(WallboxBLEApiConst.START_STOP_CHARGING, state)
    except Exception as e:
        LOGGER.error(f"Error while sending command to charger : {e}")

def on_message(client, topic, payload, qos, properties):
    """Synchronous Callback required by gmqtt."""
    try:
        msg_payload = payload.decode()
        LOGGER.info(f"MQTT command received : {topic} -> {msg_payload}")
        
        if ble_client_global is not None and ble_client_global.client and ble_client_global.client.is_connected:
            # Pushing asynchronous execution in main event loop
            asyncio.create_task(handle_mqtt_command(topic, msg_payload))
        else:
            LOGGER.warning("MQTT Command ignored : the charger is not ready over Bluetooth.")
    except Exception as e:
        LOGGER.error(f"Error processing MQTT command : {e}")

def on_connect(client, flags, rc, properties):
    """Synchronous Callback required by gmqtt."""
    LOGGER.info("✅ Succesfully connected to MQTT Broker.")
    client.subscribe(f"homeassistant/lock/{DEVICE_ID}/set")
    client.subscribe(f"homeassistant/number/{DEVICE_ID}/set")
    client.subscribe(f"homeassistant/switch/{DEVICE_ID}/set")
    publish_discovery(client)

def on_disconnect(client, packet, exc=None):
    """Synchronous Callback required by gmqtt."""
    LOGGER.warning(f"❌ Disconnected from MQTT Broker. Motive : {exc}")


async def main():
    global ble_client_global
    
    ble_client_global = WallboxBLEApiClient(WALLBOX_MAC)
    
    mqtt_client = MQTTClient(DEVICE_ID)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.on_disconnect = on_disconnect
    
    if MQTT_USER and MQTT_PASSWORD:
        mqtt_client.set_auth_credentials(MQTT_USER, MQTT_PASSWORD)
        
    await mqtt_client.connect(MQTT_BROKER, MQTT_PORT, version=MQTTv311)

    max_charge_current = 32

    while True:
        try:
            await ble_client_global.connect()
            
            # 1. Max current state
            ok, max_curr = await ble_client_global.request(WallboxBLEApiConst.GET_MAX_AVAILABLE_CURRENT)
            if ok and max_curr:
                max_charge_current = max_curr

            while ble_client_global.client.is_connected:
                # 2. Status query loop
                ok, data = await ble_client_global.request(WallboxBLEApiConst.GET_STATUS)
                if ok and data:
                    LOGGER.info(f"Raw r_dat received: {data}")
                    
                    status_code = data.get("st", 0)
                    status_name = WallboxBLEApiConst.STATUS_CODES[status_code] if status_code < len(WallboxBLEApiConst.STATUS_CODES) else "UNKNOWN"
                    charge_current = data.get("cur", 6)
                    
                    # Charging power extraction ('cp' key is already in kW)
                    charging_power = round(data.get("cp", 0.0), 2)
                    
                    # Retrieval of session energy ('en' key is in daWh, divide by 100.0 for kWh)
                    session_energy = round(data.get("en", 0.0) / 100.0, 2)
                    
                    payload = {
                        "status": status_name,
                        "status_code": status_code,
                        "charge_current": charge_current,
                        "max_charge_current": max_charge_current,
                        "charging_power": charging_power,
                        "session_energy": session_energy
                    }
                    LOGGER.info(f"Update sent to MQTT : {payload}")
                    mqtt_client.publish(f"homeassistant/sensor/{DEVICE_ID}/state", json.dumps(payload), retain=True)
                
                await asyncio.sleep(10)
                
        except Exception as e:
            LOGGER.error(f"Error communicating : {e}. New attempt in 10 seconds...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("Script stopped, exiting gracefully.")