#!/usr/bin/env python3
import asyncio
import logging
import sys
from bleak import BleakClient, BleakScanner
from bleak.exc import BleakError

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
LOGGER = logging.getLogger("wallbox_discovery")

# Pulsar Plus BLE Mac Adress
WALLBOX_MAC = "xx:xx:xx:xx:xx:xx"

async def main():
    LOGGER.info("🔍 Step 1 : Checking if we see the charger")
    device = await BleakScanner.find_device_by_address(WALLBOX_MAC, timeout=10.0)
    
    if not device:
        LOGGER.error(f"❌ Unable to find charger with adress {WALLBOX_MAC}.")
        LOGGER.info("👉 Advice : Get closer to the charger, make sure your phone app is completely closed and there are no other connections")
        return

    LOGGER.info(f"✅ Charger found !")
    LOGGER.info(f"⏳ Step 2 : Attempting to connect to {WALLBOX_MAC}...")

    try:
        async with BleakClient(device) as client:
            if client.is_connected:
                LOGGER.info("🎉 Successfully connected to the Wallbox !")
                LOGGER.info("------------------------------------------------------------")
                LOGGER.info("🗺️ Exploration of the services (Objects) :")
                LOGGER.info("------------------------------------------------------------")
                
                # Go over every GATT services of the charger
                for service in client.services:
                    print(f"\n[SERVICE] UUID: {service.uuid} ({service.description})")
                    
                    # Go over all the characteristics of this service
                    for char in service.characteristics:
                        # Extract properties (Read, Write, Notify, etc.)
                        props = ", ".join(char.properties)
                        print(f"  ├── [CHARACTERISTIC] UUID: {char.uuid}")
                        print(f"  │   ├── Description : {char.description}")
                        print(f"  │   └── Properties  : [{props}]")
                        
                        # If the characteristic has descriptors
                        for descriptor in char.descriptors:
                            print(f"  │       └── [Descriptor] UUID: {descriptor.uuid}")
                
                LOGGER.info("------------------------------------------------------------")
                LOGGER.info("✅ End of exploration.")
            else:
                LOGGER.error("❌ Failed to connect (The client refused the connection).")
                
    except BleakError as e:
        LOGGER.error(f"💥 Bleak error during communication : {e}")
    except Exception as e:
        LOGGER.error(f"💥 An unexpected error occurred : {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOGGER.info("User stopped the scan. Exiting gracefully.")