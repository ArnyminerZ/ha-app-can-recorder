# AGENTS.md — Ducato CAN Recorder

Instructions for human maintainers and coding agents working in this repository. Read this document before modifying the app or its release workflow.

## Purpose and system boundaries

This is a **Home Assistant OS app**, not ESPHome firmware. It records passive vehicle CAN observations forwarded over Wi-Fi/MQTT by a separate Waveshare ESP32-S3-RS485-CAN device installed in a Fiat Ducato-based camper van. Its user-facing interface runs through **Home Assistant ingress**, and the Raspberry Pi running HAOS stores CSVs persistently.

```text
Fiat Ducato 125-kbps CAN bus
  | hardware CAN receiver — strictly LISTENONLY, no vehicle CAN TX
  v
ESPHome on Waveshare ESP32-S3-RS485-CAN
  | (a) Existing decoders -> RS485 Modbus RTU slave 2 -> alarm / HA
  | (b) Optional captured frames -> van Wi-Fi / MQTT
  v
Mosquitto broker in Home Assistant OS (usually core-mosquitto:1883)
  |
  v
This app: paho-mqtt + Flask server (port 8099, HAOS ingress)
  |                         ^
  |                         | Start/Stop/Download/Delete from HA sidebar
  v                         |
/share/ducato-can-recordings/ — CSV files
```

**Critical safety invariant: do not transmit onto vehicle CAN.** The ESPHome CAN interface is at **125 kbps**, TX GPIO15, RX GPIO16, and **`mode: LISTENONLY`**. The ESPHome firmware also serves alarm-critical data via **RS485 Modbus slave 2, map version 2** (UART TX17, RX18, direction GPIO21, 9600 8N1). This repository does not control that Modbus service; do not change its layout, CAN mode, or established decoders while modifying this app. The MQTT `START`/`STOP` commands only control *research logging on ESPHome*, never vehicle CAN.

The ESPHome firmware is managed separately in Home Assistant and is **not included in this repository**. Maintain wire compatibility with the deployed firmware; firmware/recorder protocol changes must be coordinated and backward-compatible where possible. The companion research documentation has been tracked separately in the private `ArnyminerZ/furgo` repository. Never include the user's vehicle identifiers, MQTT credentials, API encryption secrets, actual vehicle logs, or VIN in public commits.

## Repository layout

| Path | Purpose |
| --- | --- |
| `repository.yaml` | HAOS app repository metadata |
| `ducato_can_recorder/config.yaml` | App manifest, **canonical app version**, supported HAOS architectures, GHCR image, MQTT configuration, ingress, writable `share` mapping |
| `ducato_can_recorder/Dockerfile` | Python Alpine image, Flask and paho-mqtt |
| `ducato_can_recorder/recorder.py` | Flask UI and REST endpoints, MQTT callbacks, session state, CSV writes and file deletion |
| `ducato_can_recorder/DOCS.md` | In-app documentation |
| `.github/workflows/build.yaml` | Builds `amd64`/`aarch64` images and publishes multi-architecture image, then creates versioned GitHub Releases |
| `README.md` | Public installation guide and My Home Assistant install badge |

The app uses `/data/options.json` for Supervisor-provided config. It binds Flask to port **8099**, exposed via **ingress**, not a public host port. MQTT connects to the existing Mosquitto app, generally `core-mosquitto:1883`. The client ID currently is `haos-ducato-can-recorder`: do not run two instances with the same ID (such as both local and repository-installed apps). Preserve the app `slug` if possible, as changing it can break updates and storage associations.

## MQTT protocol — must stay in sync with ESPHome

MQTT topic names are hard-coded today; there is no configurable prefix.

| Topic | Direction | Payload |
| --- | --- | --- |
| `furgo/can/control` | App -> ESPHome | `START:<session>` or `STOP:<session>` |
| `furgo/can/device_status` | ESPHome -> app | `STARTED:<session>` or `STOPPED:<session>:<dropped_count>` |
| `furgo/can/device_stats` | ESPHome -> app | `<session>,<sequence_counter>,<dropped_count>` |
| `furgo/can/frames` | ESPHome -> app | One or more newline-separated, **seven-field** rows `session,seq,esp_ms,can_id_hex,extended,dlc,data_hex` |
| `furgo/can/recorder/state` | App -> HA/MQTT | `ON` or `OFF` |
| `furgo/can/recorder/status` | App -> HA/MQTT | JSON object returned by `public_status()` |
| `furgo/can/recorder/set` | HA/MQTT -> app | `START` or `STOP` (the optional MQTT-discovered HA switch) |

Each recording generates a **new UUID session**. The app accepts frames and status messages **only if their session matches**; arbitrary test payloads with placeholder sessions are ignored. The ESP32 increments its sequence for captured frames (including frames that cannot be enqueued), buffers them in RAM (current firmware queue maximum **256** entries) and sends batches up to **10** lines every **25 ms** via MQTT while connected. The firmware drops when the queue is full and reports a dropped count; the app tracks observable sequence gaps as a separate indication of loss.

The ESPHome-side setting `record_extended: "false"` defaults extended-frame recording to **off**, because diagnostic traffic may include identifiers. Standard CAN IDs `0x3E0` and `0x082` are excluded from the research stream, and the app additionally excludes 29-bit IDs matching the `0x18DAxxxx` diagnostic range. This is **not a comprehensive privacy scrub**; treat CSV recordings as sensitive vehicle telemetry and do not commit samples collected from the van.

Test-only protocol: the hidden `POST /api/test-frame` endpoint publishes one synthetic message with CAN ID `7FE`, sequence `0`, and data `1122334455667788` for the active session. It is sent to **MQTT**, never CAN. The UI intentionally does **not** show this button, and the app counts synthetic test frames separately.

## Recording lifecycle and files

Session states: **idle -> starting -> recording -> stopping -> idle**.

1. Start only while idle and MQTT connected: create CSV, write header, generate session, send `START:<session>`.
2. Confirm `STARTED` from ESPHome; if no acknowledgment within **10 seconds**, finalize as incomplete.
3. Accept session-matching frames, validate payload size/hex/DLC, write rows incrementally. Flask and MQTT callbacks share a reentrant `LOCK`.
4. Stop: send `STOP:<session>`, continue draining, then finalize on `STOPPED`. Fallback after **7 seconds** of waiting; mark potentially incomplete.
5. Completed files can be downloaded/deleted from the ingress UI. The current open file is **not deletable or downloadable**. Deletion uses `DELETE /api/files/<name>`, validates filename, rejects symlinks/non-files, and confirms in the browser before deleting. Keep the server-side active-file guard: frontend checks alone are never sufficient.

CSV columns in order: `received_utc,session,seq,esp_ms,can_id_hex,extended,dlc,data_hex`. Timestamps are when the **Raspberry Pi receives** frames (UTC); `esp_ms` is the ESP32's monotonic uptime, **not a UTC event timestamp**. Do not claim per-frame wall-clock timestamps are recorded at CAN receipt. File names currently look like `ducato_can_YYYY-MM-DD_HH-MM-SS_<session-prefix>.csv`.

Files are in `/share/ducato-can-recordings/`, which is mounted writable by HAOS. The `max_recording_mb` option (default **250**) caps a single recording; it does **not** cap aggregate folder size or automatically prune old recordings. Deletion is permanent. Be careful with buffering/file-size checks, sudden restarts, and limited Pi storage. Maintain incremental writes, explicit flush/finalize, safe filename handling, and protection against symlink traversal.

The frontend is embedded in the `HTML` string in `recorder.py`, polled at roughly **2-second** intervals. Only show **Start** in `idle`, only show **Stop** in `recording`, and disable/withhold controls during transitions. Keep HAOS ingress-relative paths for fetch, downloads, and API calls. Do not expose unprotected direct HTTP ports or move recordings to `/config/www`.

## Diagnostics, known limits, tests

The **MQTT -> recorder -> CSV -> downloaded file** path has been tested with one synthetic frame. The real van CAN data pipeline and prolonged/high-throughput recording are **not yet formally verified**. `device_stats` showing `session,0,0` means MQTT control works but no frames have been captured; it is plausible while the CAN bus sleeps.

Do not infer perfect capture from `dropped=0` alone: there can be CAN-controller RX overruns, Wi-Fi/MQTT loss, process restarts, and buffering failures. Test with the vehicle stationary first and check frame counters, missing sequences, dropped counters, and downloaded CSV before a driving experiment.

At minimum for code changes:
- Run `python -m py_compile ducato_can_recorder/recorder.py`.
- Verify the YAML files parse, app manifest version and image name are consistent, and the workflow is valid.
- Exercise start/stop, asynchronous acknowledgment/timeout, MQTT frame batches, session mismatch, malformed payload, duplicate sequence, CSV header, download, deletion of completed CSV, blocked deletion of open CSV, and filename/path traversal.
- Confirm a hidden synthetic frame still works when explicitly invoked, and that no vehicle CAN transmission code has been introduced.
- Check HAOS ingress behavior and a real-device recording before claiming full end-to-end production validation.

### Existing implementation risks to review before further expansion

- The app's size-limit check observes the on-disk file size; Python buffering may delay visibility. Consider explicit accounted byte counts.
- The first sequence check does not fully characterize loss before the first observed frame. Document it honestly.
- A `STARTED`/control ACK, successful MQTT publish or zero reported drops does not prove actual CAN reception.
- `Flask` runs via its built-in server; evaluate production serving/auth and load before supporting public multi-user deployment.
- MQTT-discovered switch state may require correct availability/status reporting and retention semantics; don't assume every disconnect is handled perfectly.

## Versioning and publishing

Use **SemVer-like app versions**, e.g. `0.1.3`, from `ducato_can_recorder/config.yaml`. For each shipped app change:
1. Change code/docs as necessary; update `README.md` and `DOCS.md` if behavior changes.
2. **Bump the manifest version** to a new, unused version, e.g. `0.1.4`. Do not republish an existing tag after modifying app code.
3. Push to the default branch `master`. `.github/workflows/build.yaml` builds `amd64` and `aarch64` architecture images and publishes the multi-arch manifest under **`ghcr.io/arnyminerz/ha-app-can-recorder:<version>`**.
4. **Only after a successful image build** create GitHub Release `v<version>` and its matching git tag, with generated release notes. The workflow automates this when a release does not yet exist. Never publish a release if the image build failed.
5. Make sure the GHCR package is **public** and the image is pullable by HAOS without credentials. The source repository being public does not automatically make the package public.
6. Refresh the Home Assistant app store and verify a real HAOS update. Preserve `config.yaml` version, image tag, GitHub tag and release alignment.

Important: build workflow uses the pinned Home Assistant composable builder actions. **Always keep the GHCR registry prefix lowercase** (`ghcr.io/arnyminerz`) in both prepare and publish steps; GitHub repo owner capitalizes `ArnyminerZ` but Docker image names must be lowercase. GitHub Actions can rebuild on workflow edits with the existing version; do not overwrite already shipped version tags or GitHub releases. Documentation-only changes need no app version bump and should not trigger a release.

Do not add secrets, actual VIN-bearing CAN captures, MQTT passwords, API keys, or other private telemetry to workflow logs, code, issues, or releases.
