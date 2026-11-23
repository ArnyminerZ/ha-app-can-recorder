# Ducato CAN Recorder

[![Add Ducato CAN Recorder to Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2FArnyminerZ%2Fha-app-can-recorder)

A Home Assistant OS app for recording passive CAN observations received from an ESPHome CAN proxy over MQTT. Start and stop captures from the Home Assistant sidebar, download completed CSV files, and delete recordings you no longer need.

> **Status:** Initial research build. The MQTT-to-CSV pipeline has been tested using a simulated frame; live vehicle CAN recording has not yet been validated. Use only with a CAN interface configured in **LISTENONLY** mode.

## Install as a Home Assistant app repository

Click **Add to Home Assistant** above to prefill the repository URL, then select **Ducato CAN Recorder** in the app store and install it. Alternatively, follow the manual steps below.

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
- Recordings are written incrementally to the HAOS shared folder. Per-file limit defaults to 250 MB; use **Delete** next to a completed CSV to remove it, after confirmation. Old recordings are **not automatically deleted**; deletion is permanent. Open recordings cannot be deleted.
- Completed CSV downloads are served through Home Assistant ingress, not a publicly exposed host port.
- The diagnostic test endpoint remains available for troubleshooting, but the test button is hidden in the regular UI.

## Prebuilt images (GHCR)

A GitHub Actions workflow builds and publishes multi-architecture images for `amd64` and `aarch64` from changes to `ducato_can_recorder/` or the build workflow. The app manifest points to `ghcr.io/arnyminerz/ha-app-can-recorder`, with the app `version` used as the image tag. The repository uses Home Assistant's composable image builder actions and also supports manual workflow runs.

**Maintainer setup:** Once the workflow has successfully created the package, visit your GitHub profile → **Packages** → `ha-app-can-recorder` → **Package settings** → **Change visibility** → **Public**. GitHub Container Registry packages can default to private even when the source repository is public. Confirm the versioned multi-architecture image exists before attempting a fresh install. For subsequent releases, bump `ducato_can_recorder/config.yaml` version before pushing changed app code; version tags are treated as immutable.

Track builds at https://github.com/ArnyminerZ/ha-app-can-recorder/actions/workflows/build.yaml.

## Versions and GitHub Releases

Every new app version should have a matching **GitHub Release** and immutable tag (`v0.1.3`, `v0.1.4`, etc.). The manifest at `ducato_can_recorder/config.yaml` is the source of truth for the version. On a push that triggers the build workflow, GitHub Actions builds/publishes the GHCR image `ghcr.io/arnyminerz/ha-app-can-recorder:<version>` and, **only after the image build succeeds**, creates the corresponding GitHub Release with automatically generated release notes. Existing images and releases are not overwritten. If you only edit documentation, no new app version is needed.

Future maintainers and coding agents: see [AGENTS.md](AGENTS.md) for the MQTT protocol, ESPHome relationship, safety constraints, diagnostics, and release checklist.
