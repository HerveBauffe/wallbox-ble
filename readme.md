This project was made for my Wallbox Pulsar Plus EV Charger

It has been inspired by https://github.com/botts7/esp32-wallbox and https://github.com/jagheterfredrik/wallbox-ble

# Instructions
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

Copy the wallbox-gateway.service in /etc/systemd/system/
Then execute 
```
sudo systemctl daemon-reload
sudo systemctl start wallbox-gateway.service
```