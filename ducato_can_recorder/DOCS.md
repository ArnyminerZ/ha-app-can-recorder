# Ducato CAN Recorder

Use the sidebar interface to start and stop a capture and download completed CSV files.

The existing Mosquitto broker is required, together with an ESPHome CAN listener using the `furgo/can/*` MQTT protocol documented in the repository README.

## Configuration

- `mqtt_host`: Mosquitto hostname (usually `core-mosquitto`).
- `mqtt_port`: Default 1883.
- `mqtt_username` / `mqtt_password`: Credentials for your existing MQTT account.
- `max_recording_mb`: Maximum size of a single recording (default 250 MB).

CSV recordings persist under `/share/ducato-can-recordings/` and must be removed manually when no longer needed. Recordings may reveal vehicle activity; don't publish them indiscriminately.

## Diagnostics

A correctly connected ESPHome node responds to a start request with `STARTED:<session-id>`. On stop, it responds with `STOPPED:<session-id>:<dropped>`. The recorder finalizes after a seven-second timeout if there is no stop acknowledgment and marks the file potentially incomplete.

The standalone diagnostic test HTTP route is deliberately not shown as a regular UI button. The MQTT-to-CSV pathway was verified using a simulated frame; real CAN traffic and long-session throughput still require testing.

The CAN device must remain configured as **LISTENONLY**; this app does not transmit on CAN.
