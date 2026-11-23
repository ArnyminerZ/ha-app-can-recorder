#!/usr/bin/env python3
"""Home Assistant OS ingress app for passive MQTT CAN recordings."""
import csv
import json
import logging
import os
import pathlib
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, abort, jsonify, render_template_string, send_file
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ducato-can")
OPTIONS = json.loads(pathlib.Path("/data/options.json").read_text())
DIR = pathlib.Path("/share/ducato-can-recordings")
DIR.mkdir(parents=True, exist_ok=True)
LIMIT_BYTES = int(OPTIONS.get("max_recording_mb", 250)) * 1024 * 1024
LOCK = threading.RLock()
app = Flask(__name__)
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="haos-ducato-can-recorder", clean_session=True)
if OPTIONS.get("mqtt_username"):
    client.username_pw_set(OPTIONS["mqtt_username"], OPTIONS.get("mqtt_password", ""))

session = None
fp = None
writer = None
file_path = None
started = 0.0
frames = 0
last_seq = 0
test_frames = 0
lost = 0
dropped_esp = 0
is_connected = False
state = "idle"
reason = ""
ack_deadline = 0.0


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def publish(topic, payload):
    if is_connected:
        client.publish(topic, payload, qos=1, retain=False)


def public_status():
    return dict(
        state=state, session=session, file=file_path.name if file_path else None,
        frames=frames, test_frames=test_frames, lost_sequences=lost,
        dropped_on_esp=dropped_esp,
        duration_s=round(time.monotonic() - started, 1) if session else 0,
        bytes=file_path.stat().st_size if file_path and file_path.exists() else 0,
        mqtt_connected=is_connected, reason=reason,
    )


def publish_state():
    publish("furgo/can/recorder/state", "ON" if state in ("starting", "recording", "stopping") else "OFF")
    publish("furgo/can/recorder/status", json.dumps(public_status()))


def finalize(note=""):
    global session, fp, writer, state, reason, ack_deadline
    if fp:
        fp.flush()
        os.fsync(fp.fileno())
        fp.close()
    fp = None
    writer = None
    session = None
    ack_deadline = 0.0
    state = "idle"
    reason = note
    publish_state()


def begin():
    global session, fp, writer, file_path, started, frames
    global last_seq, test_frames, lost, dropped_esp, state, reason
    with LOCK:
        if state != "idle":
            return False, "A recording is already active"
        if not is_connected:
            return False, "MQTT is disconnected"
        session = uuid.uuid4().hex
        file_path = DIR / (
            "ducato_can_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            + "_" + session[:8] + ".csv"
        )
        fp = file_path.open("w", encoding="utf-8", newline="", buffering=65536)
        writer = csv.writer(fp)
        writer.writerow(["received_utc", "session", "seq", "esp_ms",
                         "can_id_hex", "extended", "dlc", "data_hex"])
        started, frames, last_seq, test_frames, lost, dropped_esp = (
            time.monotonic(), 0, 0, 0, 0, 0
        )
        state = "starting"
        reason = "Waiting for ESP32 acknowledgment"
        publish("furgo/can/control", "START:" + session)
        publish_state()
        return True, session


def stop():
    global state, reason, ack_deadline
    with LOCK:
        if state not in ("recording", "starting"):
            return False, "No active recording"
        state = "stopping"
        reason = "Draining remaining frames; awaiting ESP32 acknowledgment"
        ack_deadline = time.monotonic() + 7
        publish("furgo/can/control", "STOP:" + session)
        publish_state()
        return True, "Stopping"


def on_connect(cl, userdata, flags, reason_code, properties):
    global is_connected
    with LOCK:
        is_connected = not reason_code.is_failure
        if is_connected:
            for topic in (
                "furgo/can/frames", "furgo/can/device_status",
                "furgo/can/device_stats", "furgo/can/recorder/set",
            ):
                cl.subscribe(topic, qos=1)
            publish("homeassistant/switch/ducato_can_recorder/config", json.dumps({
                "name": "Ducato CAN Recording",
                "unique_id": "furgo_ducato_can_recording",
                "state_topic": "furgo/can/recorder/state",
                "command_topic": "furgo/can/recorder/set",
                "payload_on": "START", "payload_off": "STOP",
                "state_on": "ON", "state_off": "OFF",
                "device": {
                    "identifiers": ["furgo_can_recorder"],
                    "name": "Ducato CAN Recorder",
                },
            }))
            publish_state()
            log.info("Connected to MQTT broker")
        else:
            log.warning("MQTT connection refused: %s", reason_code)


def on_disconnect(cl, userdata, flags, reason_code, properties):
    global is_connected
    with LOCK:
        is_connected = False
        log.warning("MQTT disconnected: %s", reason_code)


def on_message(cl, userdata, message):
    global frames, last_seq, test_frames, lost, dropped_esp, state, reason
    data = message.payload.decode("utf-8", errors="replace")
    with LOCK:
        if message.topic == "furgo/can/recorder/set":
            if data == "START":
                begin()
            elif data == "STOP":
                stop()
            return
        if message.topic == "furgo/can/device_status":
            if session and data == "STARTED:" + session and state == "starting":
                state, reason = "recording", "ESP32 confirmed recording"
                publish_state()
            elif session and data.startswith("STOPPED:" + session + ":") and state == "stopping":
                try:
                    dropped_esp = max(dropped_esp, int(data.rsplit(":", 1)[1]))
                except ValueError:
                    pass
                finalize("Completed; ESP32 reports %d dropped frames" % dropped_esp)
            return
        if message.topic == "furgo/can/device_stats" and session:
            parts = data.split(",")
            if len(parts) == 3 and parts[0] == session:
                try:
                    dropped_esp = max(dropped_esp, int(parts[2]))
                except ValueError:
                    pass
            return
        if message.topic != "furgo/can/frames" or not session or state not in ("starting", "recording", "stopping"):
            return
        for row in data.splitlines():
            parts = row.split(",")
            if len(parts) != 7 or parts[0] != session:
                continue
            try:
                seq, uptime = int(parts[1]), int(parts[2])
                identifier, ext, dlc, payload = parts[3], int(parts[4]), int(parts[5]), parts[6]
                can_id = int(identifier, 16)
                if ext not in (0, 1) or not 0 <= dlc <= 8 or len(payload) != 2 * dlc:
                    continue
                if not all(c in "0123456789ABCDEFabcdef" for c in payload):
                    continue
                if (not ext and can_id in (0x3E0, 0x082)) or (
                    ext and (can_id & 0x1FFF0000) == 0x18DA0000
                ):
                    continue
                # Diagnostic self-test frame retained for troubleshooting but hidden from UI.
                simulation = (
                    seq == 0 and can_id == 0x7FE and ext == 0 and dlc == 8
                    and payload.upper() == "1122334455667788"
                )
                if simulation:
                    if test_frames >= 1:
                        continue
                    test_frames += 1
                else:
                    if seq <= last_seq:
                        continue
                    if last_seq and seq > last_seq + 1:
                        lost += seq - last_seq - 1
                    last_seq = seq
                writer.writerow([now(), parts[0], seq, uptime, identifier, ext, dlc, payload])
                frames += 1
            except (TypeError, ValueError, OSError):
                log.exception("Discarded malformed CAN record")
        if file_path and file_path.stat().st_size >= LIMIT_BYTES and state != "stopping":
            stop()
            reason = "Maximum recording size reached; stopping"


client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.connect_async(
    OPTIONS.get("mqtt_host", "core-mosquitto"),
    int(OPTIONS.get("mqtt_port", 1883)), 60,
)
client.loop_start()


def watchdog():
    while True:
        time.sleep(1)
        with LOCK:
            if fp is not None:
                fp.flush()
            if state == "stopping" and ack_deadline and time.monotonic() >= ack_deadline:
                finalize("Stopped without ESP32 acknowledgment; recording may be incomplete")
            elif state == "starting" and time.monotonic() - started > 10:
                finalize("No ESP32 start acknowledgment; check MQTT/firmware (incomplete)")


threading.Thread(target=watchdog, daemon=True).start()

HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#f8fafc">
<title>Ducato CAN Recorder</title>
<style>
:root {
  color-scheme: light dark;
  --rec-bg: var(--primary-background-color,#f6f8fb);
  --rec-card: var(--card-background-color,#ffffff);
  --rec-text: var(--primary-text-color,#1f2937);
  --rec-muted: var(--secondary-text-color,#687385);
  --rec-border: var(--divider-color,#e1e6ed);
  --rec-accent: var(--primary-color,#1976d2);
  --rec-hover: var(--secondary-background-color,#eff3f7);
  --rec-danger: #b3261e;
  --rec-danger-bg: #fff1f0;
  --rec-success: #236c44;
  --rec-success-bg: #e9f7ee;
  --rec-radius: 14px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --rec-bg: var(--primary-background-color,#111820);
    --rec-card: var(--card-background-color,#202933);
    --rec-text: var(--primary-text-color,#edf1f5);
    --rec-muted: var(--secondary-text-color,#aab5c2);
    --rec-border: var(--divider-color,#39434f);
    --rec-hover: var(--secondary-background-color,#293540);
    --rec-danger: #ff7770;
    --rec-danger-bg: #432627;
    --rec-success: #78d9a0;
    --rec-success-bg: #1b3b2b;
  }
}
*,*::before,*::after{box-sizing:border-box}
[hidden]{display:none!important}
body{margin:0;padding:28px 18px 48px;font:400 14px/1.5 Roboto,"Segoe UI",system-ui,-apple-system,sans-serif;background:var(--rec-bg);color:var(--rec-text)}
.shell{width:min(100%,960px);margin:0 auto}
.page-header{display:flex;align-items:center;gap:14px;margin:0 0 24px}
.app-icon{width:50px;height:50px;flex:none;border-radius:15px;display:grid;place-items:center;background:var(--rec-accent);color:#fff}
.app-icon .icon{width:27px;height:27px}
h1{font-size:23px;font-weight:650;letter-spacing:-.5px;line-height:1.18;margin:0}
.subheading{margin:5px 0 0;color:var(--rec-muted);font-size:13px}
.panel{background:var(--rec-card);border:1px solid var(--rec-border);border-radius:var(--rec-radius);overflow:hidden;box-shadow:0 2px 8px rgba(12,27,45,.04)}
.panel-pad{padding:22px}
.panel+.panel{margin-top:18px}
.header-row{display:flex;align-items:flex-start;gap:12px;justify-content:space-between;flex-wrap:wrap}
.section-title{font-size:16px;font-weight:650;margin:0}
.section-hint{margin:4px 0 0;color:var(--rec-muted);font-size:13px}
.icon{height:20px;width:20px;display:inline-block;flex:none;fill:currentColor;vertical-align:middle}
.icon-sm{height:17px;width:17px}
.badges{display:flex;flex-wrap:wrap;gap:8px}
.badge{display:inline-flex;align-items:center;gap:6px;min-height:28px;padding:4px 10px;border-radius:999px;background:var(--rec-hover);font-size:12px;font-weight:600;color:var(--rec-muted);white-space:nowrap}
.badge .icon{height:15px;width:15px}
.badge.online{color:var(--rec-success);background:var(--rec-success-bg)}
.badge.offline{color:var(--rec-danger);background:var(--rec-danger-bg)}
.badge.recording{color:var(--rec-danger);background:var(--rec-danger-bg)}
.badge.transition{color:var(--rec-accent);background:var(--rec-hover)}
.stats{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-top:22px}
.metric{min-width:0;background:var(--rec-bg);border:1px solid var(--rec-border);border-radius:11px;padding:13px 14px}
.metric-label{display:flex;align-items:center;gap:7px;color:var(--rec-muted);font-size:12px}
.metric-label .icon{height:17px;width:17px}
.metric-value{margin-top:8px;font-size:24px;line-height:1.1;font-weight:650;font-variant-numeric:tabular-nums;letter-spacing:-.5px}
.metric-unit{font-size:12px;font-weight:400;color:var(--rec-muted)}
.session{font-size:12px;color:var(--rec-muted);overflow-wrap:anywhere;margin:16px 0 0}
.session code{font-family:ui-monospace,Consolas,monospace;color:var(--rec-text)}
.controls{display:flex;align-items:center;flex-wrap:wrap;gap:12px;margin-top:20px;padding-top:18px;border-top:1px solid var(--rec-border)}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;min-height:42px;padding:10px 17px;border-radius:10px;border:1px solid var(--rec-border);background:var(--rec-card);color:var(--rec-text);font:inherit;font-weight:600;cursor:pointer;transition:background .15s ease,filter .15s ease}
.btn:hover:not(:disabled){filter:brightness(.96)}
.btn:focus-visible{outline:3px solid var(--rec-accent);outline-offset:2px}
.btn:disabled{opacity:.45;cursor:not-allowed}
.btn.primary{background:var(--rec-accent);color:#fff;border-color:transparent}
.btn.stop{background:var(--rec-danger);color:#fff;border-color:transparent}
.btn.ghost{min-height:36px;padding:7px 11px;background:transparent}
.btn.ghost.danger{color:var(--rec-danger)}
.btn.ghost.danger:hover:not(:disabled){background:var(--rec-danger-bg);filter:none}
.message{display:flex;align-items:center;gap:8px;min-height:22px;font-size:13px;color:var(--rec-muted);margin:12px 0 0}
.message:empty{display:none}
.message.error{color:var(--rec-danger)}
.files-header{display:flex;align-items:center;gap:8px;margin-bottom:16px}
.files-header .icon{color:var(--rec-muted)}
.file-list{list-style:none;margin:0;padding:0}
.file-row{display:flex;align-items:center;gap:12px;min-width:0;padding:14px 20px;border-top:1px solid var(--rec-border)}
.file-row:first-child{border-top:none}
.file-icon{color:var(--rec-accent);display:grid;place-items:center;width:36px;height:36px;background:var(--rec-hover);border-radius:9px;flex:none}
.file-main{flex:1;min-width:0}
.file-name{font-size:13px;font-weight:600;overflow-wrap:anywhere}
.file-meta{font-size:12px;color:var(--rec-muted);margin-top:3px}
.file-actions{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}
.empty{padding:42px 20px;text-align:center;color:var(--rec-muted)}
.empty .icon{width:35px;height:35px;opacity:.5;margin-bottom:12px}
.empty p{margin:5px 0}
.empty strong{color:var(--rec-text);font-weight:600}
@media(max-width:650px){
 body{padding:20px 12px 36px}
 .panel-pad{padding:18px}
 .stats{grid-template-columns:repeat(2,minmax(0,1fr))}
 .file-row{flex-wrap:wrap;padding:14px}
 .file-main{min-width:calc(100% - 54px)}
 .file-actions{width:100%;justify-content:flex-start;padding-left:48px}
 .file-actions .btn{flex:1}
}
@media(max-width:340px){.stats{grid-template-columns:1fr}.page-header{gap:10px}h1{font-size:19px}}
@media(prefers-reduced-motion:reduce){.btn{transition:none}}
</style>
</head>
<body>
<svg aria-hidden="true" width="0" height="0" style="position:absolute;overflow:hidden"><defs>
<symbol id="mdi-car-connected" viewBox="0 0 24 24"><path d="M5,14H19L17.5,9.5H6.5L5,14M17.5,19A1.5,1.5 0 0,0 19,17.5A1.5,1.5 0 0,0 17.5,16A1.5,1.5 0 0,0 16,17.5A1.5,1.5 0 0,0 17.5,19M6.5,19A1.5,1.5 0 0,0 8,17.5A1.5,1.5 0 0,0 6.5,16A1.5,1.5 0 0,0 5,17.5A1.5,1.5 0 0,0 6.5,19M18.92,9L21,15V23A1,1 0 0,1 20,24H19A1,1 0 0,1 18,23V22H6V23A1,1 0 0,1 5,24H4A1,1 0 0,1 3,23V15L5.08,9C5.28,8.42 5.85,8 6.5,8H17.5C18.15,8 18.72,8.42 18.92,9M12,0C14.12,0 16.15,0.86 17.65,2.35L16.23,3.77C15.11,2.65 13.58,2 12,2C10.42,2 8.89,2.65 7.77,3.77L6.36,2.35C7.85,0.86 9.88,0 12,0M12,4C13.06,4 14.07,4.44 14.82,5.18L13.4,6.6C13.03,6.23 12.53,6 12,6C11.5,6 10.97,6.23 10.6,6.6L9.18,5.18C9.93,4.44 10.94,4 12,4Z" /></symbol>
<symbol id="mdi-record-circle-outline" viewBox="0 0 24 24"><path d="M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2M12,4A8,8 0 0,1 20,12A8,8 0 0,1 12,20A8,8 0 0,1 4,12A8,8 0 0,1 12,4M12,9A3,3 0 0,0 9,12A3,3 0 0,0 12,15A3,3 0 0,0 15,12A3,3 0 0,0 12,9Z" /></symbol>
<symbol id="mdi-stop-circle-outline" viewBox="0 0 24 24"><path d="M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2M12,4C16.41,4 20,7.59 20,12C20,16.41 16.41,20 12,20C7.59,20 4,16.41 4,12C4,7.59 7.59,4 12,4M9,9V15H15V9" /></symbol>
<symbol id="mdi-download-outline" viewBox="0 0 24 24"><path d="M13,5V11H14.17L12,13.17L9.83,11H11V5H13M15,3H9V9H5L12,16L19,9H15V3M19,18H5V20H19V18Z" /></symbol>
<symbol id="mdi-delete-outline" viewBox="0 0 24 24"><path d="M6,19A2,2 0 0,0 8,21H16A2,2 0 0,0 18,19V7H6V19M8,9H16V19H8V9M15.5,4L14.5,3H9.5L8.5,4H5V6H19V4H15.5Z" /></symbol>
<symbol id="mdi-file-delimited-outline" viewBox="0 0 24 24"><path d="M14 2H6C4.9 2 4 2.9 4 4V20C4 21.1 4.9 22 6 22H18C19.1 22 20 21.1 20 20V8L14 2M18 20H6V4H13V9H18V20M10 19L12 15H9V10H15V15L13 19H10" /></symbol>
<symbol id="mdi-wifi" viewBox="0 0 24 24"><path d="M12,21L15.6,16.2C14.6,15.45 13.35,15 12,15C10.65,15 9.4,15.45 8.4,16.2L12,21M12,3C7.95,3 4.21,4.34 1.2,6.6L3,9C5.5,7.12 8.62,6 12,6C15.38,6 18.5,7.12 21,9L22.8,6.6C19.79,4.34 16.05,3 12,3M12,9C9.3,9 6.81,9.89 4.8,11.4L6.6,13.8C8.1,12.67 9.97,12 12,12C14.03,12 15.9,12.67 17.4,13.8L19.2,11.4C17.19,9.89 14.7,9 12,9Z" /></symbol>
<symbol id="mdi-wifi-off" viewBox="0 0 24 24"><path d="M2.28,3L1,4.27L2.47,5.74C2.04,6 1.61,6.29 1.2,6.6L3,9C3.53,8.6 4.08,8.25 4.66,7.93L6.89,10.16C6.15,10.5 5.44,10.91 4.8,11.4L6.6,13.8C7.38,13.22 8.26,12.77 9.2,12.47L11.75,15C10.5,15.07 9.34,15.5 8.4,16.2L12,21L14.46,17.73L17.74,21L19,19.72M12,3C9.85,3 7.8,3.38 5.9,4.07L8.29,6.47C9.5,6.16 10.72,6 12,6C15.38,6 18.5,7.11 21,9L22.8,6.6C19.79,4.34 16.06,3 12,3M12,9C11.62,9 11.25,9 10.88,9.05L14.07,12.25C15.29,12.53 16.43,13.07 17.4,13.8L19.2,11.4C17.2,9.89 14.7,9 12,9Z" /></symbol>
<symbol id="mdi-clock-outline" viewBox="0 0 24 24"><path d="M12,20A8,8 0 0,0 20,12A8,8 0 0,0 12,4A8,8 0 0,0 4,12A8,8 0 0,0 12,20M12,2A10,10 0 0,1 22,12A10,10 0 0,1 12,22C6.47,22 2,17.5 2,12A10,10 0 0,1 12,2M12.5,7V12.25L17,14.92L16.25,16.15L11,13V7H12.5Z" /></symbol>
<symbol id="mdi-database-outline" viewBox="0 0 24 24"><path d="M12 3C7.58 3 4 4.79 4 7V17C4 19.21 7.59 21 12 21S20 19.21 20 17V7C20 4.79 16.42 3 12 3M18 17C18 17.5 15.87 19 12 19S6 17.5 6 17V14.77C7.61 15.55 9.72 16 12 16S16.39 15.55 18 14.77V17M18 12.45C16.7 13.4 14.42 14 12 14C9.58 14 7.3 13.4 6 12.45V9.64C7.47 10.47 9.61 11 12 11C14.39 11 16.53 10.47 18 9.64V12.45M12 9C8.13 9 6 7.5 6 7S8.13 5 12 5C15.87 5 18 6.5 18 7S15.87 9 12 9Z" /></symbol>
<symbol id="mdi-alert-circle-outline" viewBox="0 0 24 24"><path d="M11,15H13V17H11V15M11,7H13V13H11V7M12,2C6.47,2 2,6.5 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2M12,20A8,8 0 0,1 4,12A8,8 0 0,1 12,4A8,8 0 0,1 20,12A8,8 0 0,1 12,20Z" /></symbol>
<symbol id="mdi-check-circle-outline" viewBox="0 0 24 24"><path d="M12 2C6.5 2 2 6.5 2 12S6.5 22 12 22 22 17.5 22 12 17.5 2 12 2M12 20C7.59 20 4 16.41 4 12S7.59 4 12 4 20 7.59 20 12 16.41 20 12 20M16.59 7.58L10 14.17L7.41 11.59L6 13L10 17L18 9L16.59 7.58Z" /></symbol>
<symbol id="mdi-information-outline" viewBox="0 0 24 24"><path d="M11,9H13V7H11M12,20C7.59,20 4,16.41 4,12C4,7.59 7.59,4 12,4C16.41,4 20,7.59 20,12C20,16.41 16.41,20 12,20M12,2A10,10 0 0,0 2,12A10,10 0 0,0 12,22A10,10 0 0,0 22,12A10,10 0 0,0 12,2M11,17H13V11H11V17Z" /></symbol>
</defs></svg>
<main class="shell">
<header class="page-header">
  <div class="app-icon"><svg class="icon"><use href="#mdi-car-connected"></use></svg></div>
  <div><h1>CAN Recorder</h1><p class="subheading">Passive vehicle CAN capture · ESPHome / MQTT</p></div>
</header>
<section class="panel" aria-labelledby="capture-title"><div class="panel-pad">
  <div class="header-row">
    <div><h2 id="capture-title" class="section-title">Recording</h2><p id="reason" class="section-hint">Connecting to recorder…</p></div>
    <div class="badges">
      <span id="recordBadge" class="badge">Loading</span>
      <span id="mqttBadge" class="badge">MQTT</span>
    </div>
  </div>
  <div class="stats" aria-label="Current or most recent recording statistics">
    <div class="metric"><div class="metric-label"><svg class="icon"><use href="#mdi-database-outline"></use></svg> Frames</div><div class="metric-value" id="frames">—</div></div>
    <div class="metric"><div class="metric-label"><svg class="icon"><use href="#mdi-alert-circle-outline"></use></svg> Missing frames</div><div class="metric-value" id="missing">—</div></div>
    <div class="metric"><div class="metric-label"><svg class="icon"><use href="#mdi-clock-outline"></use></svg> Duration</div><div class="metric-value" id="duration">—</div></div>
    <div class="metric"><div class="metric-label"><svg class="icon"><use href="#mdi-database-outline"></use></svg> File size</div><div class="metric-value" id="bytes">—</div></div>
  </div>
  <p id="session" class="session" hidden>Session: <code id="sessionValue"></code></p>
  <div class="controls">
    <button class="btn primary" id="start" type="button" hidden disabled><svg class="icon"><use href="#mdi-record-circle-outline"></use></svg> Start recording</button>
    <button class="btn stop" id="stop" type="button" hidden disabled><svg class="icon"><use href="#mdi-stop-circle-outline"></use></svg> Stop recording</button>
    <span id="transitionHint" class="section-hint" hidden>Finishing recording…</span>
  </div>
  <p id="message" class="message" role="status" aria-live="polite"></p>
</div></section>
<section class="panel" aria-labelledby="saved-title">
  <div class="panel-pad files-header">
    <svg class="icon"><use href="#mdi-file-delimited-outline"></use></svg>
    <div><h2 id="saved-title" class="section-title">Saved recordings</h2><p class="section-hint">Download or manage completed CSV files</p></div>
    <span class="badge" id="fileCount" style="margin-left:auto">0</span>
  </div>
  <div id="files"><div class="empty"><p>Loading recordings…</p></div></div>
</section>
</main>
<script>
const el=id=>document.getElementById(id);
const startBtn=el('start'),stopBtn=el('stop'),messageEl=el('message');
let pending=false, lastStatus=null;
const icons={
 download:'<svg class="icon icon-sm" aria-hidden="true"><use href="#mdi-download-outline"></use></svg>',
 delete:'<svg class="icon icon-sm" aria-hidden="true"><use href="#mdi-delete-outline"></use></svg>',
 empty:'<svg class="icon" aria-hidden="true"><use href="#mdi-file-delimited-outline"></use></svg>'
};
const fmt=n=>new Intl.NumberFormat().format(n||0);
const size=n=>n>=1048576?(n/1048576).toFixed(1)+' MB':n>=1024?(n/1024).toFixed(1)+' KB':fmt(n)+' B';
const duration=n=>{const seconds=Math.floor(n||0),m=Math.floor(seconds/60),h=Math.floor(m/60);return h?h+'h '+(m%60)+'m':m?m+'m '+seconds%60+'s':seconds+'s';};
function showMessage(text,isError=false){messageEl.textContent=text||'';messageEl.classList.toggle('error',isError);}
function renderStatus(s){
 lastStatus=s;
 const names={idle:'Idle',starting:'Starting',recording:'Recording',stopping:'Stopping'};
 const badge=el('recordBadge');
 badge.textContent=names[s.state]||s.state;
 badge.className='badge'+(s.state==='recording'?' recording':s.state==='starting'||s.state==='stopping'?' transition':'');
 const mqtt=el('mqttBadge');
 mqtt.textContent=s.mqtt_connected?'MQTT connected':'MQTT disconnected';
 mqtt.className='badge '+(s.mqtt_connected?'online':'offline');
 el('reason').textContent=s.reason|| (s.state==='idle'?'Ready for a new recording':'Waiting for ESPHome…');
 el('frames').textContent=fmt(s.frames);
 el('missing').textContent=fmt(s.lost_sequences);
 el('duration').textContent=duration(s.duration_s);
 el('bytes').textContent=size(s.bytes);
 el('session').hidden=!s.session;
 el('sessionValue').textContent=s.session||'';
 startBtn.hidden=s.state!=='idle';
 stopBtn.hidden=s.state!=='recording';
 startBtn.disabled=pending||!s.mqtt_connected||s.state!=='idle';
 stopBtn.disabled=pending||s.state!=='recording';
 el('transitionHint').hidden=!['starting','stopping'].includes(s.state);
 el('transitionHint').textContent=s.state==='starting'?'Waiting for ESP32 to start…':'Finishing recording…';
}
function fileRow(f){
 const row=document.createElement('li');row.className='file-row';
 const fileIcon=document.createElement('span');fileIcon.className='file-icon';
 fileIcon.innerHTML=icons.empty;
 const details=document.createElement('div');details.className='file-main';
 const name=document.createElement('div');name.className='file-name';name.textContent=f.name;
 const meta=document.createElement('div');meta.className='file-meta';meta.textContent=size(f.size)+' · CSV recording';
 details.append(name,meta);
 const actions=document.createElement('div');actions.className='file-actions';
 const download=document.createElement('a');download.className='btn ghost';
 download.href='download/'+encodeURIComponent(f.name);
 download.download=f.name;
 download.innerHTML=icons.download+' Download';
 download.setAttribute('aria-label','Download '+f.name);
 const del=document.createElement('button');del.className='btn ghost danger';del.type='button';
 del.innerHTML=icons.delete+' Delete';
 del.disabled=pending;
 del.setAttribute('aria-label','Delete '+f.name);
 del.addEventListener('click',()=>removeFile(f.name));
 actions.append(download,del);
 row.append(fileIcon,details,actions);
 return row;
}
function renderFiles(files,status){
 const completed=files.filter(f=>f.name!==status.file||status.state==='idle');
 el('fileCount').textContent=fmt(completed.length);
 const list=document.createElement('ul');list.className='file-list';
 for(const f of completed)list.append(fileRow(f));
 if(completed.length){el('files').replaceChildren(list);return;}
 const empty=document.createElement('div');empty.className='empty';
 empty.innerHTML=icons.empty+'<p><strong>No saved recordings</strong></p><p>Completed recordings will appear here.</p>';
 el('files').replaceChildren(empty);
}
async function refresh(){
 try{
  const statusResponse=await fetch('api/status',{cache:'no-store'});
  if(!statusResponse.ok)throw Error('Could not load recorder status');
  const status=await statusResponse.json();
  renderStatus(status);
  const filesResponse=await fetch('api/files',{cache:'no-store'});
  if(!filesResponse.ok)throw Error('Could not load saved recordings');
  renderFiles(await filesResponse.json(),status);
 }catch(e){showMessage(String(e),true);}
}
async function act(action){
 if(pending)return;
 pending=true;startBtn.disabled=true;stopBtn.disabled=true;
 try{
  const response=await fetch('api/'+action,{method:'POST'});
  const result=await response.json();
  if(!response.ok||!result.ok)throw Error(result.message||'Request failed');
  showMessage(action==='start'?'Recording requested': 'Stop requested');
 }catch(e){showMessage(String(e),true);}
 finally{pending=false;await refresh();}
}
async function removeFile(name){
 if(pending)return;
 if(!confirm('Permanently delete "'+name+'"? This cannot be undone.'))return;
 pending=true;
 try{
  const response=await fetch('api/files/'+encodeURIComponent(name),{method:'DELETE'});
  const result=await response.json();
  if(!response.ok||!result.ok)throw Error(result.message||'Could not delete recording');
  showMessage('Recording deleted');
 }catch(e){showMessage(String(e),true);}
 finally{pending=false;await refresh();}
}
startBtn.addEventListener('click',()=>act('start'));
stopBtn.addEventListener('click',()=>act('stop'));
setInterval(refresh,2000);
refresh();
</script>
</body></html>"""


@app.get("/")
def page():
    return render_template_string(HTML)


@app.get("/api/status")
def status_api():
    with LOCK:
        return jsonify(public_status())


@app.get("/api/files")
def files_api():
    return jsonify([
        {"name": x.name, "size": x.stat().st_size}
        for x in sorted(DIR.glob("ducato_can_*.csv"), reverse=True)[:200]
    ])


@app.delete("/api/files/<name>")
def delete_file(name):
    """Delete a completed CSV, never the currently open recording."""
    if (name != pathlib.Path(name).name or
            not name.endswith(".csv") or not name.startswith("ducato_can_")):
        return jsonify(ok=False, message="Invalid recording filename"), 400
    with LOCK:
        target = DIR / name
        if target.is_symlink() or not target.is_file():
            return jsonify(ok=False, message="Recording not found"), 404
        if target == file_path and fp is not None:
            return jsonify(ok=False, message="Stop recording before deleting its file"), 409
        try:
            target.unlink()
        except OSError:
            log.exception("Failed to delete recording: %s", name)
            return jsonify(ok=False, message="Could not delete recording"), 500
        log.info("Deleted completed CAN recording: %s", name)
        return jsonify(ok=True, message="Deleted " + name)


@app.post("/api/start")
def start_api():
    ok, result = begin()
    return jsonify(message=result, ok=ok), 200 if ok else 409


@app.post("/api/stop")
def stop_api():
    ok, result = stop()
    return jsonify(message=result, ok=ok), 200 if ok else 409


@app.post("/api/test-frame")
def test_frame():
    """Hidden diagnostic: inject synthetic MQTT row, never transmitting on CAN."""
    with LOCK:
        if state not in ("starting", "recording") or not session:
            return jsonify(message="Start a recording first", ok=False), 409
        if not is_connected:
            return jsonify(message="MQTT disconnected", ok=False), 503
        if test_frames:
            return jsonify(message="Test frame already received in this session", ok=False), 409
        payload = f"{session},0,0,7FE,0,8,1122334455667788"
        info = client.publish("furgo/can/frames", payload, qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            return jsonify(message="MQTT publish failed", ok=False), 503
        return jsonify(message="Simulated frame published over MQTT", ok=True)


@app.get("/download/<name>")
def download(name):
    if (name != pathlib.Path(name).name or
            not name.endswith(".csv") or not name.startswith("ducato_can_")):
        abort(404)
    target = DIR / name
    if not target.is_file():
        abort(404)
    with LOCK:
        if target == file_path and fp is not None:
            abort(409, "Stop recording before downloading")
    return send_file(target, as_attachment=True, download_name=name, mimetype="text/csv")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8099, threaded=True)
