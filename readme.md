This project was made for my Wallbox Pulsar Plus EV Charger

It has been inspired by https://github.com/botts7/esp32-wallbox and https://github.com/jagheterfredrik/wallbox-ble

# Instructions
## Setting up Python
Create a venv
```
python3 -m .venv venv
```
Activate it
```
source .venv/bin/activate
```
Install dependencies
```
pip3 install bleak gmqtt
```
## Configuration
Go in wallbox.py and edit the CONFIGURATION section,
You should put your charger's MAC, and MQTT broker identifiers.

## SystemD service
Copy the wallbox-gateway.service in /etc/systemd/system/
Then execute 
```
sudo systemctl daemon-reload
sudo systemctl start wallbox-gateway.service
```