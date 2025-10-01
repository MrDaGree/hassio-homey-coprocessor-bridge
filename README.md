<p align="center">
  <img src="homey_copro/icon.png" width="200"/>
</p>

# Homey Coprocessor Bridge (MQTT)

This Home Assistant add-on exposes the **Homey Coprocessor** to Home Assistant over **MQTT**, allowing you to publish coprocessor events/state and receive commands through MQTT topics. It’s intended to help integrate Z-Wave/other coprocessor functionality from Homey into a Home Assistant environment.

---

## Features

- Publishes coprocessor **status**, **events**, and **metrics** to MQTT
- Listens for **command** topics (e.g., node actions) and forwards them to the coprocessor
- Optional Home Assistant **MQTT Discovery** for auto-creating entities
- Robust connection handling to both the MQTT broker and the serial/IPC underlying the coprocessor
- Lightweight Node (ESM) implementation (`mqtt_bridge.mjs`)

---

## Installation

1. **Add the repository**  
   In Home Assistant, go to **Settings → Add-ons → Add-on Store → ⋮ → Repositories**, add your GitHub repository URL, then **Refresh**. The add-on will appear in the store list. :contentReference[oaicite:1]{index=1}

2. **Install the add-on**  
   Open **Homey Coprocessor Bridge**, click **Install**, then enable:
   - Start on boot
   - Watchdog
   - Show in sidebar (optional)

3. **Configure**

4. **Start** the add-on and monitor **Logs** for `MQTT connected` and `Coprocessor connected`.

