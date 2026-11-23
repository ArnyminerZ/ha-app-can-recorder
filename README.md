# Ducato CAN Recorder

A Home Assistant OS app for recording passive CAN observations received from an ESPHome CAN proxy over MQTT. Start and stop captures from the Home Assistant sidebar and download completed CSV files.

> **Status:** Initial research build. The MQTT-to-CSV pipeline has been tested using a simulated frame; live vehicle CAN recording has not yet been validated. Use only with a CAN interface configured in **LISTENONLY** mode.

## Install as a Home Assistant app repository

1. In **Home Assistant → Settings → Apps**, open the app store and its **⋮ → Repositories** menu.
2. Add this repository URL: `https://github.com/ArnyminerZ/ha-app-can-recorder`.
3. Refresh the app store; install **Ducato CAN Recorder**.
4. Configure the existing Mosquitto broker (usually `core-mosquitto:1883`) and MQTT username/password in the app's configuration.
5. Start the app and enable **Show in sidebar**.

If you already installed a local copy at `/local_apps/ducato_can_recorder`, **stop and uninstall the local app before installing this repository-backed copy** to avoid duplicate recorders and MQTT client IDs. Recordings reside in `/share/ducato-can-recordings/`; back up any files you want to retain before uninstalling.

## MQTT protocol

The ESPHome firmware must implement the corresponding capture producer. Publishing to `furgo/can/control`:

- `START:<session-id>` — begin capture.
- `STOP:<session-id>` — stop capture and flush frames.

ESPHome publishes:

- `furgo/can/device_status`: `STARTED:<session-id>` and `STOPPED:<session-id>:<dropped>`
- `furgo/can/device_stats`: `<session-id>,<last-sequence>,<dropped>`
- `furgo/can/frames`: newline-separated rows `<session-id>,<seq>,<esp-ms>,<CAN-ID-hex>,<extended-0-or-1>,<dlc>,<payload-hex>`

The recorder saves `received_utc,session,seq,esp_ms,can_id_hex,extended,dlc,data_hex`. It excludes known identifying standard IDs `0x3E0` and `0x082` and diagnostic-style extended IDs `0x18DAxxxx`. Do not assume other IDs are free of personal information.

## Notes

- No commands are sent to the **vehicle** CAN bus. This app only sends MQTT commands to the ESPHome device, which must remain in CAN `LISTENONLY`.
- MQTT/Wi-Fi can drop frames. Check missing sequence numbers and ESP drop counts after every session.
- Recordings are written incrementally to the HAOS shared folder. Per-file limit defaults to 250 MB; old recordings are **not automatically deleted**.
- Completed CSV downloads are served through Home Assistant ingress, not a publicly exposed host port.
- The diagnostic test endpoint remains available for troubleshooting, but the test button is hidden in the regular UI.
