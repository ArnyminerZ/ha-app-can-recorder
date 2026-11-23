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

HTML = """<!doctype html><html><head><meta name='viewport' content='width=device-width,initial-scale=1'>
<style>body{font-family:system-ui;margin:24px;max-width:800px;background:#111;color:#eee}
button{padding:12px 16px;margin:5px;font-size:17px}a{color:#7bd}li{margin:12px 0}
.box{border:1px solid #666;border-radius:12px;padding:18px}</style>
</head><body><h2>🚐 Ducato CAN Recorder</h2><div class='box'>
<p><b>Status:</b> <span id='state'>Loading</span></p>
<p id='stats'></p><p id='session'></p>
<button onclick='act("start")'>▶ Start recording</button>
<button onclick='act("stop")'>⏹ Stop recording</button>
<p id='message'></p></div><h3>Completed recordings</h3><ul id='files'></ul>
<script>
async function refresh(){try{
 let s=await(await fetch('api/status',{cache:'no-store'})).json();
 document.getElementById('state').textContent=s.state+(s.mqtt_connected?' · MQTT online':' · MQTT OFFLINE');
 document.getElementById('stats').textContent='Frames: '+s.frames+' · Missing seq: '+s.lost_sequences+' · ESP dropped: '+s.dropped_on_esp+' · Duration: '+s.duration_s+'s · Bytes on disk: '+s.bytes+' · '+s.reason;
 document.getElementById('session').textContent=s.session?'Current session: '+s.session:'';
 if(s.state==='idle') document.getElementById('message').textContent='';
 let a=await(await fetch('api/files',{cache:'no-store'})).json();
 let completed=a.filter(f=>f.name!==s.file || s.state==='idle');
 document.getElementById('files').innerHTML=completed.length?completed.map(f=>'<li><a href="download/'+encodeURIComponent(f.name)+'">'+f.name+'</a> ('+(f.size/1024/1024).toFixed(2)+' MB)</li>').join(''):'<li>No completed recordings yet</li>';
 }catch(e){document.getElementById('message').textContent=e}}
async function act(action){try{let r=await fetch('api/'+action,{method:'POST'});let d=await r.json();document.getElementById('message').textContent=d.message || JSON.stringify(d);await refresh()}catch(e){document.getElementById('message').textContent=e}}
setInterval(refresh,2000);refresh();</script></body></html>"""


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
