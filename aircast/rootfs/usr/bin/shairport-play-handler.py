#!/usr/bin/env python3
"""
Shairport play/stop hooks.

Starts a reconnectable HTTP MP3 stream from the Shairport pipe and calls
media_player.play_media on the configured Home Assistant entity.

Timing (important for XiaoAI):
  before_play_begins -> start HTTP/ffmpeg (no play_media yet)
  after_play_begins  -> wait until audio bytes flow, then play_media
  after_play_ends    -> soft-stop speaker, restore configured volume, tear down stream
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Tuple

import requests

OPTIONS_FILE = "/data/options.json"
ENV_FILE = "/tmp/xiaoair-env.json"
PLAY_LOG = "/tmp/xiaoair-play.log"
STREAM_PORT_BASE = 7000
AUDIO_READY_BYTES = 8192


def log(msg: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {msg}"
    print(line, file=sys.stderr, flush=True)
    try:
        with open(PLAY_LOG, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass


def load_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def load_options() -> Dict[str, Any]:
    return load_json(OPTIONS_FILE)


def supervisor_token() -> Optional[str]:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if token:
        return token
    return load_json(ENV_FILE).get("SUPERVISOR_TOKEN")


def get_local_ip() -> str:
    env = load_json(ENV_FILE)
    if env.get("local_ip"):
        return str(env["local_ip"])
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        local_ip = sock.getsockname()[0]
        sock.close()
        return local_ip
    except Exception:  # noqa: BLE001
        return "127.0.0.1"


def state_path(entity_id: str) -> str:
    return f"/tmp/shairport_state_{entity_id.replace('.', '_')}.json"


def pid_path(entity_id: str) -> str:
    return f"/tmp/shairport_serve_{entity_id.replace('.', '_')}.pid"


def audio_ready_path(entity_id: str) -> str:
    return f"/tmp/shairport_audio_ready_{entity_id.replace('.', '_')}"


def restore_volume_enabled() -> bool:
    value = load_options().get("restore_volume_on_disconnect", True)
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off"}
    return bool(value)


def configured_restore_volume() -> float:
    """Preset volume (0..1) from addon option restore_volume_percent (default 20)."""
    raw = load_options().get("restore_volume_percent", 20)
    try:
        percent = float(raw)
    except (TypeError, ValueError):
        percent = 20.0
    if percent < 0.0:
        percent = 0.0
    if percent > 100.0:
        percent = 100.0
    return percent / 100.0


def ha_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {supervisor_token()}",
        "Content-Type": "application/json",
    }


def wait_for_port(port: int, timeout: float = 8.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.3):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def wait_port_free(port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", port))
            sock.close()
            return
        except OSError:
            sock.close()
            time.sleep(0.1)


def kill_pid(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGTERM)
    except Exception:  # noqa: BLE001
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:  # noqa: BLE001
            pass
    time.sleep(0.2)
    try:
        os.killpg(pid, signal.SIGKILL)
    except Exception:  # noqa: BLE001
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:  # noqa: BLE001
            pass


def stop_serve(entity_id: str) -> None:
    path = pid_path(entity_id)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                pid = int(handle.read().strip())
            kill_pid(pid)
        except Exception as err:  # noqa: BLE001
            log(f"stop_serve kill failed: {err}")
        try:
            os.remove(path)
        except OSError:
            pass
    try:
        os.remove(audio_ready_path(entity_id))
    except OSError:
        pass


class Fanout:
    """Broadcast ffmpeg chunks; keep short preroll for late subscribers."""

    def __init__(self, entity_id: str) -> None:
        self._lock = threading.Lock()
        self._clients: List[Queue] = []
        self._preroll = bytearray()
        self._preroll_max = 65536
        self._bytes = 0
        self._entity_id = entity_id
        self.audio_ready = threading.Event()

    def subscribe(self) -> Queue:
        queue: Queue = Queue(maxsize=64)
        with self._lock:
            if self._preroll:
                try:
                    queue.put_nowait(bytes(self._preroll))
                except Exception:  # noqa: BLE001
                    pass
            self._clients.append(queue)
        return queue

    def unsubscribe(self, queue: Queue) -> None:
        with self._lock:
            if queue in self._clients:
                self._clients.remove(queue)

    def publish(self, data: bytes) -> None:
        with self._lock:
            self._preroll.extend(data)
            if len(self._preroll) > self._preroll_max:
                del self._preroll[: len(self._preroll) - self._preroll_max]
            self._bytes += len(data)
            ready_now = self._bytes >= AUDIO_READY_BYTES and not self.audio_ready.is_set()
            clients = list(self._clients)
        if ready_now:
            self.audio_ready.set()
            try:
                with open(audio_ready_path(self._entity_id), "w", encoding="utf-8") as handle:
                    handle.write(str(self._bytes))
            except OSError:
                pass
            # after_play_begins is unreliable on some Shairport builds — trigger here.
            threading.Thread(
                target=auto_play_media,
                args=(self._entity_id,),
                daemon=True,
            ).start()
        for queue in clients:
            try:
                queue.put_nowait(data)
            except Exception:  # noqa: BLE001
                try:
                    _ = queue.get_nowait()
                    queue.put_nowait(data)
                except Exception:  # noqa: BLE001
                    pass


class ReusableHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def run_serve(entity_id: str, pipe_path: str, port_offset: int) -> None:
    options = load_options()
    stream_format = str(options.get("stream_format") or "mp3").lower()
    port = STREAM_PORT_BASE + port_offset
    stream_path = "/live.mp3" if stream_format == "mp3" else "/live.wav"
    content_type = "audio/mpeg" if stream_format == "mp3" else "audio/wav"
    fanout = Fanout(entity_id)

    if stream_format == "wav":
        ffmpeg_cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            "-f", "s16le", "-ar", "44100", "-ac", "2", "-i", pipe_path,
            "-f", "wav", "pipe:1",
        ]
    else:
        ffmpeg_cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "warning",
            "-fflags", "nobuffer", "-flags", "low_delay",
            "-f", "s16le", "-ar", "44100", "-ac", "2", "-i", pipe_path,
            "-f", "mp3", "-b:a", "192k", "pipe:1",
        ]

    log(f"serve start entity={entity_id} port={port} pipe={pipe_path}")
    log(f"ffmpeg: {' '.join(ffmpeg_cmd)}")

    ffmpeg = subprocess.Popen(
        ffmpeg_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )

    def pump_stderr() -> None:
        assert ffmpeg.stderr is not None
        for raw in iter(ffmpeg.stderr.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip()
            if line:
                log(f"ffmpeg: {line}")

    def pump_stdout() -> None:
        assert ffmpeg.stdout is not None
        while True:
            chunk = ffmpeg.stdout.read(2048)
            if not chunk:
                break
            fanout.publish(chunk)
        log("ffmpeg stdout ended")

    threading.Thread(target=pump_stderr, daemon=True).start()
    threading.Thread(target=pump_stdout, daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, fmt: str, *args: Any) -> None:
            log(f"http: {self.address_string()} {fmt % args}")

        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?", 1)[0] not in (stream_path, "/"):
                self.send_error(404)
                return
            queue = fanout.subscribe()
            try:
                # Wait for real audio before headers — XiaoAI drops empty streams.
                try:
                    first = queue.get(timeout=20)
                except Empty:
                    log("http: no audio yet — closing client without headers")
                    return
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Pragma", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(first)
                self.wfile.flush()
                while True:
                    try:
                        data = queue.get(timeout=60)
                    except Empty:
                        log("http client idle timeout")
                        break
                    self.wfile.write(data)
                    self.wfile.flush()
            except Exception as err:  # noqa: BLE001
                log(f"http client gone: {err}")
            finally:
                fanout.unsubscribe(queue)

        def do_HEAD(self) -> None:  # noqa: N802
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()

    wait_port_free(port)
    try:
        server = ReusableHTTPServer(("0.0.0.0", port), Handler)
    except OSError as err:
        log(f"✗ bind :{port} failed: {err}")
        try:
            os.killpg(ffmpeg.pid, signal.SIGTERM)
        except Exception:  # noqa: BLE001
            pass
        return

    log(f"HTTP listening on 0.0.0.0:{port}{stream_path}")

    def shutdown(*_args: Any) -> None:
        log("serve shutting down")
        try:
            server.shutdown()
        except Exception:  # noqa: BLE001
            pass
        try:
            os.killpg(ffmpeg.pid, signal.SIGTERM)
        except Exception:  # noqa: BLE001
            try:
                ffmpeg.terminate()
            except Exception:  # noqa: BLE001
                pass

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    with open(pid_path(entity_id), "w", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))

    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        shutdown()
        try:
            os.remove(pid_path(entity_id))
        except OSError:
            pass
        try:
            os.remove(audio_ready_path(entity_id))
        except OSError:
            pass


def set_ha_volume(entity_id: str, level: float) -> Tuple[bool, str]:
    """media_player.volume_set only — never touch mute."""
    ok, detail = call_service(
        "media_player/volume_set",
        {"entity_id": entity_id, "volume_level": level},
    )
    if ok:
        return True, f"volume_set {level:.3f} ({detail})"
    return False, detail


def restore_pre_volume(entity_id: str) -> None:
    """One volume_set after pause. Do not change mute."""
    if not restore_volume_enabled():
        return
    level = configured_restore_volume()
    percent = round(level * 100.0)
    log(f"restoring configured volume={level:.3f} ({percent}%)")
    ok, detail = set_ha_volume(entity_id, level)
    if ok:
        log(f"✓ {detail}")
    else:
        log(f"✗ restore {detail}")


def call_service(service: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    token = supervisor_token()
    if not token:
        return False, "SUPERVISOR_TOKEN missing"
    try:
        response = requests.post(
            f"http://supervisor/core/api/services/{service}",
            headers=ha_headers(),
            json=payload,
            timeout=10,
        )
        if response.status_code >= 400:
            return False, f"{response.status_code} {response.text[:200]}"
        return True, str(response.status_code)
    except Exception as err:  # noqa: BLE001
        return False, str(err)


def play_media(entity_id: str, stream_url: str, media_content_type: str) -> bool:
    log(f"play_media {entity_id} <- {stream_url}")
    ok, detail = call_service(
        "media_player/play_media",
        {
            "entity_id": entity_id,
            "media_content_id": stream_url,
            "media_content_type": media_content_type,
        },
    )
    if ok:
        log(f"✓ play_media accepted ({detail})")
    else:
        log(f"✗ play_media failed: {detail}")
    return ok


_auto_play_lock = threading.Lock()


def auto_play_media(entity_id: str) -> None:
    """Send play_media once audio is encoded (idempotent)."""
    with _auto_play_lock:
        path = state_path(entity_id)
        state = load_json(path)
        if not state:
            log(f"auto_play: no state yet for {entity_id}")
            return
        if state.get("play_media_sent"):
            return
        # Refresh URL with current best LAN IP (state may have been written early).
        port = STREAM_PORT_BASE + int(state.get("port", 0))
        stream_format = str(load_options().get("stream_format") or "mp3").lower()
        stream_name = "live.mp3" if stream_format == "mp3" else "live.wav"
        stream_url = f"http://{get_local_ip()}:{port}/{stream_name}"
        media_content_type = str(state.get("media_content_type") or "music")
        time.sleep(0.15)
        if play_media(entity_id, stream_url, media_content_type):
            state["play_media_sent"] = True
            state["stream_url"] = stream_url
            try:
                with open(path, "w", encoding="utf-8") as handle:
                    json.dump(state, handle)
            except OSError:
                pass


def stop_media(entity_id: str) -> bool:
    """Xiaomi MIOT often rejects media_stop — try several services."""
    payload = {"entity_id": entity_id}
    attempts = (
        "media_player/media_stop",
        "media_player/media_pause",
        "media_player/turn_off",
    )
    for service in attempts:
        ok, detail = call_service(service, payload)
        if ok:
            log(f"✓ stop via {service} ({detail})")
            return True
        log(f"stop via {service} failed: {detail}")
    log("⚠ could not stop media_player (ignored)")
    return False


def handle_start(entity_id: str, pipe_path: str, port_offset: str) -> None:
    """before_play_begins: bring up HTTP/ffmpeg only."""
    offset = int(port_offset)
    port = STREAM_PORT_BASE + offset
    options = load_options()
    stream_format = str(options.get("stream_format") or "mp3").lower()
    stream_name = "live.mp3" if stream_format == "mp3" else "live.wav"
    stream_url = f"http://{get_local_ip()}:{port}/{stream_name}"

    log(f"Playback starting for {entity_id}")
    log(f"pipe={pipe_path} url={stream_url} token={'yes' if supervisor_token() else 'no'}")

    stop_serve(entity_id)
    wait_port_free(port)

    proc = subprocess.Popen(
        [
            sys.executable,
            "/usr/bin/shairport-play-handler.py",
            "serve",
            entity_id,
            pipe_path,
            str(offset),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=os.environ.copy(),
    )
    log(f"serve pid={proc.pid}")

    if not wait_for_port(port, timeout=8.0):
        log(f"✗ HTTP port {port} did not open in time")
        return

    with open(state_path(entity_id), "w", encoding="utf-8") as handle:
        json.dump(
            {
                "entity_id": entity_id,
                "pipe_path": pipe_path,
                "port": offset,
                "stream_url": stream_url,
                "serve_pid": proc.pid,
                "media_content_type": str(
                    options.get("media_content_type") or "music"
                ),
            },
            handle,
        )
    log("✓ stream server ready (waiting for AirPlay audio before play_media)")


def handle_play(entity_id: str) -> None:
    """Optional after_play_begins hook — usually already handled by auto_play_media."""
    log(f"after_play_begins hook for {entity_id}")
    ready = audio_ready_path(entity_id)
    deadline = time.time() + 12.0
    while time.time() < deadline:
        if os.path.exists(ready):
            break
        time.sleep(0.1)
    auto_play_media(entity_id)


def handle_stop(entity_id: str) -> None:
    log(f"Playback stopping for {entity_id}")
    path = state_path(entity_id)
    try:
        stop_media(entity_id)
        restore_pre_volume(entity_id)
        stop_serve(entity_id)
        if os.path.exists(path):
            os.remove(path)
    except Exception as err:  # noqa: BLE001
        log(f"✗ Error in stop handler: {err}")
        try:
            restore_pre_volume(entity_id)
        except Exception:  # noqa: BLE001
            pass


def airplay_volume_to_ha(airplay_db: float) -> float:
    """Map AirPlay dB (0..-30, -144 as 0) -> volume 0..1. Mute is not used."""
    if airplay_db <= -100:
        return 0.0
    level = (airplay_db + 30.0) / 30.0
    if level < 0.0:
        level = 0.0
    if level > 1.0:
        level = 1.0
    return level


def handle_volume(entity_id: str, airplay_db_raw: str) -> None:
    """Forward iPhone volume keys via volume_set only (PCM stays full-scale)."""
    try:
        airplay_db = float(airplay_db_raw)
    except ValueError:
        log(f"✗ volume: bad value {airplay_db_raw!r}")
        return

    level = airplay_volume_to_ha(airplay_db)
    log(f"volume airplay={airplay_db} dB -> ha={level:.3f}")

    ok, detail = set_ha_volume(entity_id, level)
    if ok:
        log(f"✓ {detail}")
    else:
        log(f"✗ volume_set failed: {detail}")


def main() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: shairport-play-handler.py <start|play|stop|serve|volume> "
            "<entity_id> [pipe_path|airplay_db] [port]",
            file=sys.stderr,
        )
        sys.exit(1)

    command = sys.argv[1]
    entity_id = sys.argv[2]

    if command == "start":
        if len(sys.argv) < 5:
            log("start command requires pipe_path and port")
            sys.exit(1)
        handle_start(entity_id, sys.argv[3], sys.argv[4])
    elif command == "play":
        handle_play(entity_id)
    elif command == "stop":
        handle_stop(entity_id)
    elif command == "volume":
        # Shairport appends: ... volume <entity> >>log 2>&1 <db>
        # After shell redirect parsing, argv is: volume entity <db>
        if len(sys.argv) < 4:
            log("volume command requires airplay dB argument")
            sys.exit(1)
        handle_volume(entity_id, sys.argv[3])
    elif command == "serve":
        if len(sys.argv) < 5:
            log("serve command requires pipe_path and port")
            sys.exit(1)
        run_serve(entity_id, sys.argv[3], int(sys.argv[4]))
    else:
        log(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
