#!/usr/bin/env python3

# ==============================================================================
# SAHA LIVE - Regional Ingestor v3.3
# ==============================================================================
# Stateless field worker for the Saha orchestrator model.
#
# Each hour the worker asks the backend for instructions, records HLS with FFmpeg,
# then uploads + registers the PREVIOUS hour in a background thread while
# starting the next recording (no gap between hours).
#
# USAGE:
#   python3 saha-regional-ingestor.py [COMMAND]
#
# COMMANDS:
#   start            Run the ingestor loop (foreground)
#   start --daemon   Run as a background daemon
#   stop             Stop a running daemon
#   status           Show whether the daemon is running
#   test-task        Fetch the next task from the backend (smoke test)
#   test-upload      Fetch a task and upload a connectivity test file to R2
#
# REQUIRED .env:
#   BACKEND_URL=https://api.innovatex.dev
#   WORKER_API_KEY=<per-venue key from admin registration>
# ==============================================================================

from __future__ import annotations

import argparse
import copy
import datetime
import json
import logging
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    import boto3
    from botocore.client import Config
    from dotenv import load_dotenv
    import requests
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install boto3 python-dotenv requests")
    sys.exit(1)

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_FILE = SCRIPT_DIR / ".env"

if ENV_FILE.exists():
    load_dotenv(dotenv_path=ENV_FILE)
else:
    load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "https://api.innovatex.dev").rstrip("/")
WORKER_API_KEY = os.getenv("WORKER_API_KEY", os.getenv("X_WORKER_KEY", ""))
WORK_DIR = os.getenv("WORK_DIR", "/tmp/saha-ingestor")
PID_FILE = os.getenv("PID_FILE", "/tmp/saha-ingestor.pid")
LOG_FILE = os.getenv("LOG_FILE", "/tmp/saha-ingestor.log")
TASK_POLL_INTERVAL = int(os.getenv("TASK_POLL_INTERVAL", "30"))
# Optional: force recording length in seconds (e.g. 600 for a 10-minute test)
RECORDING_DURATION_SECONDS = os.getenv("RECORDING_DURATION_SECONDS", "").strip()

_stop_event = threading.Event()
log = logging.getLogger("saha-ingestor")


@dataclass
class UploadJob:
    """A finished recording waiting for R2 upload + API registration."""

    venue_slug: str
    match_time: str
    storage: dict[str, Any]
    out_dir: Path


_upload_queue: queue.Queue[UploadJob | None] | None = None
_upload_threads: list[threading.Thread] = []
_pending_match_times: set[str] = set()
_match_lock = threading.Lock()


@dataclass
class IngestTask:
    status: str
    venue_slug: str
    match_time: str | None = None
    stream_url: str | None = None
    storage: dict[str, Any] | None = None
    settings: dict[str, Any] | None = None
    message: str | None = None
    wait_until: str | None = None
    sleep_seconds: int | None = None
    recording_window: dict[str, str] | None = None

    @property
    def is_ready(self) -> bool:
        return self.status == "ready"

    @property
    def should_record(self) -> bool:
        """ready = top of hour; in_progress = resume after outage/reboot mid-match."""
        return self.status in ("ready", "in_progress")

    @property
    def is_recovery(self) -> bool:
        return self.status == "in_progress"

    @classmethod
    def from_response(cls, data: dict[str, Any]) -> IngestTask:
        return cls(
            status=data["status"],
            venue_slug=data["venueSlug"],
            match_time=data.get("matchTime"),
            stream_url=data.get("streamUrl", "").strip() or None,
            storage=data.get("storage"),
            settings=data.get("settings"),
            message=data.get("message"),
            wait_until=data.get("waitUntil"),
            sleep_seconds=data.get("sleepSeconds"),
            recording_window=data.get("recordingWindow"),
        )


def recording_duration_secs(task: IngestTask) -> int:
    if RECORDING_DURATION_SECONDS:
        return int(RECORDING_DURATION_SECONDS)
    if task.settings:
        return int(task.settings.get("duration", 3600))
    return 3600


def recording_start_number(task: IngestTask, out_dir: Path) -> int:
    """HLS segment index for FFmpeg (-hls_start_number). Uses API hint and local files."""
    api_start = 0
    if task.settings:
        api_start = int(task.settings.get("startNumber", 0))
    local_max = max_segment_index(out_dir)
    if local_max >= 0:
        # Keep segments already on disk after a power loss; continue after the last file.
        return max(api_start, local_max + 1)
    return api_start


def max_segment_index(out_dir: Path) -> int:
    highest = -1
    for path in out_dir.glob("output*.ts"):
        suffix = path.stem.replace("output", "")
        if suffix.isdigit():
            highest = max(highest, int(suffix))
    return highest


def has_recorded_segments(out_dir: Path) -> bool:
    return max_segment_index(out_dir) >= 0


def rebuild_hls_manifest(out_dir: Path, segment_time: int = 10) -> None:
    """Rewrite index.m3u8 from all outputN.ts on disk (merges pre-outage + recovery segments)."""
    segments = sorted(
        [p for p in out_dir.glob("output*.ts") if p.stem.replace("output", "").isdigit()],
        key=lambda p: int(p.stem.replace("output", "")),
    )
    if not segments:
        return
    target = segment_time + 1
    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{target}",
        "#EXT-X-MEDIA-SEQUENCE:0",
    ]
    for seg in segments:
        lines.append(f"#EXTINF:{segment_time}.0,")
        lines.append(seg.name)
    lines.append("#EXT-X-ENDLIST")
    (out_dir / "index.m3u8").write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("Rebuilt playlist with %d segment(s): %s", len(segments), out_dir / "index.m3u8")


def setup_logging(daemon_mode: bool = False) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if daemon_mode:
        handlers = [logging.FileHandler(LOG_FILE, mode="a")]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  [%(levelname)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
        force=True,
    )


def worker_headers() -> dict[str, str]:
    return {"X-WORKER-KEY": WORKER_API_KEY}


def validate_config(require_ffmpeg: bool = True) -> None:
    missing = []
    if not WORKER_API_KEY:
        missing.append("WORKER_API_KEY")
    if not BACKEND_URL:
        missing.append("BACKEND_URL")
    if missing:
        log.error("Missing required env vars: %s", ", ".join(missing))
        log.error("Copy scripts/saha-edge.env.example to scripts/.env and fill in the values.")
        sys.exit(1)

    if require_ffmpeg:
        for cmd in ("ffmpeg", "ffprobe"):
            if subprocess.run(["which", cmd], capture_output=True).returncode != 0:
                log.error("'%s' not found. Install it: sudo apt install ffmpeg", cmd)
                sys.exit(1)


def fetch_task() -> IngestTask:
    url = f"{BACKEND_URL}/api/ingestion/task"
    try:
        resp = requests.get(url, headers=worker_headers(), timeout=30)
    except requests.RequestException as e:
        raise RuntimeError(f"Could not reach backend at {url}: {e}") from e

    if resp.status_code == 401:
        raise RuntimeError("Invalid WORKER_API_KEY — check the key assigned to this venue.")
    if not resp.ok:
        raise RuntimeError(f"Task request failed ({resp.status_code}): {resp.text}")

    return IngestTask.from_response(resp.json())


def storage_endpoint_url(storage: dict[str, Any]) -> str:
    endpoint = storage["endpoint"].strip()
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint.rstrip("/")

    use_ssl = storage.get("useSsl", True)
    scheme = "https" if use_ssl else "http"
    port = str(storage.get("port", "443" if use_ssl else "80"))

    if (use_ssl and port == "443") or (not use_ssl and port == "80"):
        return f"{scheme}://{endpoint}"

    return f"{scheme}://{endpoint}:{port}"


def get_r2_client(storage: dict[str, Any]):
    return boto3.client(
        "s3",
        endpoint_url=storage_endpoint_url(storage),
        aws_access_key_id=storage["accessKey"],
        aws_secret_access_key=storage["secretKey"],
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
        region_name="auto",
    )


def normalize_prefix(prefix: str) -> str:
    return prefix if prefix.endswith("/") else f"{prefix}/"


def upload_folder_to_r2(local_dir: Path, storage: dict[str, Any]) -> None:
    client = get_r2_client(storage)
    bucket = storage["bucket"]
    prefix = normalize_prefix(storage["prefix"])

    files = [f for f in local_dir.iterdir() if f.is_file()]
    log.info("Uploading %d files to s3://%s/%s", len(files), bucket, prefix)

    for file_path in files:
        key = f"{prefix}{file_path.name}"
        log.info("  ↑ %s → s3://%s/%s", file_path.name, bucket, key)
        client.upload_file(str(file_path), bucket, key)

    log.info("Upload complete.")


def register_match_time(match_time: str, storage: dict[str, Any], venue_slug: str) -> None:
    url = f"{BACKEND_URL}/api/ingestion/register"
    payload = {
        "matchTime": match_time,
        "minioPath": normalize_prefix(storage["prefix"]),
    }

    try:
        resp = requests.post(url, json=payload, headers=worker_headers(), timeout=30)
    except requests.RequestException as e:
        raise RuntimeError(f"Could not reach backend for registration: {e}") from e

    if resp.status_code in (200, 201):
        log.info("Backend registered match: %s @ %s", venue_slug, match_time)
    elif resp.status_code == 409:
        log.warning("Match already registered (409): %s", resp.text)
    else:
        raise RuntimeError(f"Registration failed ({resp.status_code}): {resp.text}")


def process_upload_job(job: UploadJob) -> None:
    log.info(
        "[upload] Starting background upload for %s @ %s",
        job.venue_slug,
        job.match_time,
    )
    try:
        upload_folder_to_r2(job.out_dir, job.storage)
        register_match_time(job.match_time, job.storage, job.venue_slug)
        cleanup_local(job.out_dir)
        log.info("[upload] Finished %s @ %s", job.venue_slug, job.match_time)
    except Exception as e:
        log.error(
            "[upload] Failed for %s @ %s: %s (files kept at %s)",
            job.venue_slug,
            job.match_time,
            e,
            job.out_dir,
        )
    finally:
        with _match_lock:
            _pending_match_times.discard(job.match_time)


def upload_worker_loop() -> None:
    assert _upload_queue is not None
    while True:
        job = _upload_queue.get()
        try:
            if job is None:
                break
            process_upload_job(job)
        finally:
            _upload_queue.task_done()


def start_upload_workers() -> None:
    global _upload_queue, _upload_threads
    _upload_queue = queue.Queue()
    thread = threading.Thread(target=upload_worker_loop, name="saha-upload", daemon=True)
    thread.start()
    _upload_threads = [thread]
    log.info("Background upload worker started (record next hour while uploading).")


def enqueue_upload(task: IngestTask, out_dir: Path) -> None:
    if _upload_queue is None or not task.storage or not task.match_time:
        raise RuntimeError("Upload queue not initialized")
    job = UploadJob(
        venue_slug=task.venue_slug,
        match_time=task.match_time,
        storage=copy.deepcopy(task.storage),
        out_dir=out_dir,
    )
    with _match_lock:
        _pending_match_times.add(task.match_time)
    _upload_queue.put(job)
    log.info(
        "Queued upload for %s @ %s (%d file(s)) — recording loop continues",
        job.venue_slug,
        job.match_time,
        len([f for f in out_dir.iterdir() if f.is_file()]),
    )


def drain_upload_queue(timeout: float | None = None) -> None:
    if _upload_queue is None:
        return
    log.info("Waiting for background upload(s) to finish...")
    if timeout is not None:
        done = threading.Event()

        def _waiter() -> None:
            _upload_queue.join()
            done.set()

        threading.Thread(target=_waiter, daemon=True).start()
        if not done.wait(timeout):
            log.warning("Upload drain timed out after %ds", timeout)
            return
    else:
        _upload_queue.join()
    log.info("All background uploads finished.")


def parse_match_time(match_time: str) -> datetime.datetime:
    # Backend formats match time in server local time (field timezone).
    return datetime.datetime.strptime(match_time, "%Y-%m-%d_%H-%M")


def wait_until_match_start(match_time: str, *, skip_wait: bool = False) -> bool:
    if skip_wait:
        return not _stop_event.is_set()

    target = parse_match_time(match_time)

    while not _stop_event.is_set():
        now = datetime.datetime.now()
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            return True

        sleep_for = min(remaining, 30)
        log.info(
            "Waiting %.0fs until match start %s (venue time slot)...",
            sleep_for,
            match_time,
        )
        _stop_event.wait(sleep_for)

    return False


def record_match(task: IngestTask) -> Path | None:
    if not task.stream_url or not task.match_time or not task.settings:
        log.error("Incomplete task payload for recording")
        return None

    out_dir = Path(WORK_DIR) / task.venue_slug / task.match_time
    out_dir.mkdir(parents=True, exist_ok=True)

    duration_secs = recording_duration_secs(task)
    if duration_secs <= 0:
        log.error("Recording duration is 0 — nothing to record")
        return out_dir if has_recorded_segments(out_dir) else None

    segment_time = int(task.settings.get("segmentTime", 10))
    start_number = recording_start_number(task, out_dir)
    segment_file = str(out_dir / "output%d.ts")
    manifest_file = str(out_dir / "index.m3u8")

    existing = max_segment_index(out_dir) + 1 if has_recorded_segments(out_dir) else 0

    # Detect if input is HLS (not RTSP) - RTSP options don't apply to HLS inputs
    is_hls_input = task.stream_url.lower().endswith('.m3u8') or '/openlive/' in task.stream_url.lower()
    
    cmd = ["ffmpeg", "-loglevel", "warning"]
    
    # Input options (must come before -i)
    if not is_hls_input:
        # RTSP-specific options
        cmd.extend([
            "-rtsp_transport", "tcp",
        ])
    
    # Generic reconnection options (work for both RTSP and HLS)
    cmd.extend([
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "30",
        "-i", task.stream_url,
        # Output options (must come after -i)
        "-t", str(duration_secs),
        "-c:v", "copy",
        "-c:a", "aac",
        "-f", "hls",
        "-hls_time", str(segment_time),
        "-hls_playlist_type", "vod",
        "-start_number", str(start_number),
        "-hls_segment_filename", segment_file,
        "-y",
        manifest_file,
    ])

    host = urlparse(task.stream_url).netloc or task.stream_url[:60]
    if task.is_recovery or existing > 0:
        log.info(
            "Recording resume | venue=%s | match=%s | %ds remaining | "
            "segments on disk=%d | hls_start_number=%d",
            task.venue_slug,
            task.match_time,
            duration_secs,
            existing,
            start_number,
        )
    else:
        log.info(
            "Recording started | venue=%s | match=%s | duration=%ds",
            task.venue_slug,
            task.match_time,
            duration_secs,
        )
    log.info("  Stream host: %s", host)
    log.info("  Output: %s", out_dir)

    log.info("FFmpeg cmd: %s", " ".join(cmd))

    ffmpeg_ok = False
    try:
        proc = subprocess.run(cmd, timeout=duration_secs + 180)
        ffmpeg_ok = proc.returncode == 0
        if not ffmpeg_ok:
            log.error("FFmpeg exited with code %d", proc.returncode)
    except subprocess.TimeoutExpired:
        log.error("FFmpeg recording timed out.")

    if has_recorded_segments(out_dir):
        rebuild_hls_manifest(out_dir, segment_time)
        if ffmpeg_ok:
            log.info("Recording complete: %s", out_dir)
        else:
            log.warning(
                "Recording interrupted — uploading %d segment(s) saved on disk",
                max_segment_index(out_dir) + 1,
            )
        return out_dir

    return None


def cleanup_local(folder: Path) -> None:
    import shutil

    try:
        shutil.rmtree(folder)
        log.info("Cleaned up local temp: %s", folder)
    except Exception as e:
        log.warning("Could not clean up %s: %s", folder, e)


def wait_until_idle_over(task: IngestTask) -> bool:
    remaining = task.sleep_seconds if task.sleep_seconds is not None else TASK_POLL_INTERVAL
    window = task.recording_window
    window_label = f"{window['startTime']}–{window['endTime']}" if window else "scheduled hours"

    while not _stop_event.is_set() and remaining > 0:
        sleep_for = min(remaining, 60)
        log.info(
            "Outside recording window (%s). Sleeping %ds (%s remaining) ...",
            window_label,
            sleep_for,
            task.message or "",
        )
        _stop_event.wait(sleep_for)
        remaining -= sleep_for

    return not _stop_event.is_set()


def process_one_match(task: IngestTask) -> bool:
    if not task.match_time or not wait_until_match_start(
        task.match_time,
        skip_wait=task.is_recovery,
    ):
        return False

    out_dir = record_match(task)
    if not out_dir or not out_dir.exists():
        return False

    try:
        enqueue_upload(task, out_dir)
        return True
    except Exception as e:
        log.error("Could not queue upload: %s", e)
        return False


def handle_signal(sig, _frame) -> None:
    log.info("Signal %d received. Finishing current work then stopping...", sig)
    _stop_event.set()


def run_ingestor() -> None:
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    validate_config()
    log.info("==============================================")
    log.info("SAHA INGESTOR v3.3 STARTING (parallel upload + mid-match recovery)")
    log.info("  Backend: %s", BACKEND_URL)
    log.info("  Worker:  %s...", WORKER_API_KEY[:12] + "..." if len(WORKER_API_KEY) > 12 else WORKER_API_KEY)
    log.info("  Work dir: %s", WORK_DIR)
    if RECORDING_DURATION_SECONDS:
        log.info("  Recording override: %ss", RECORDING_DURATION_SECONDS)
    log.info("==============================================")

    start_upload_workers()

    while not _stop_event.is_set():
        try:
            task = fetch_task()
        except RuntimeError as e:
            log.error("Failed to fetch task: %s", e)
            _stop_event.wait(TASK_POLL_INTERVAL)
            continue

        if not task.should_record:
            window = task.recording_window
            window_hint = (
                f"{window.get('startTime')}–{window.get('endTime')}"
                if window
                else "no daily window (24/7) or not returned by API"
            )
            log.info(
                "Idle | venue=%s | %s | window=%s | waitUntil=%s | sleep=%ss",
                task.venue_slug,
                task.message or "Outside recording hours",
                window_hint,
                task.wait_until or "—",
                task.sleep_seconds,
            )
            if not wait_until_idle_over(task):
                break
            continue

        log.info(
            "Task received | venue=%s | match=%s | status=%s",
            task.venue_slug,
            task.match_time,
            task.status,
        )

        while task.match_time and task.match_time in _pending_match_times:
            if _stop_event.is_set():
                break
            log.info(
                "Upload still running for %s — waiting before next record...",
                task.match_time,
            )
            _stop_event.wait(5)
            try:
                task = fetch_task()
            except RuntimeError:
                break
            if not task.should_record:
                break

        success = process_one_match(task)
        if not success and not _stop_event.is_set():
            log.error(
                "Match cycle failed for %s. Retrying task fetch in %ds...",
                task.match_time,
                TASK_POLL_INTERVAL,
            )
            _stop_event.wait(TASK_POLL_INTERVAL)
            continue

        if _stop_event.is_set():
            break

        # Short pause, then fetch the next hour (upload runs in background).
        _stop_event.wait(TASK_POLL_INTERVAL)

    log.info("Stopping — waiting for pending uploads...")
    if _upload_queue is not None:
        _upload_queue.put(None)
    drain_upload_queue()
    log.info("Ingestor stopped cleanly.")


def start_daemon() -> None:
    pid = os.fork()
    if pid > 0:
        with open(PID_FILE, "w", encoding="utf-8") as f:
            f.write(str(pid))
        print(f"Ingestor daemon started (PID: {pid})")
        print(f"Logs: {LOG_FILE}")
        print(f"Stop: python3 {Path(__file__).name} stop")
        sys.exit(0)

    os.setsid()
    setup_logging(daemon_mode=True)
    run_ingestor()


def stop_daemon() -> None:
    if not Path(PID_FILE).exists():
        print("No PID file found. Is the daemon running?")
        return

    with open(PID_FILE, encoding="utf-8") as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, signal.SIGTERM)
        os.remove(PID_FILE)
        print(f"Sent SIGTERM to PID {pid}. Daemon will stop after the current match finishes.")
    except ProcessLookupError:
        print(f"Process {pid} not found. Removing stale PID file.")
        os.remove(PID_FILE)


def status_daemon() -> None:
    if not Path(PID_FILE).exists():
        print("Ingestor: NOT RUNNING")
        return

    with open(PID_FILE, encoding="utf-8") as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, 0)
        print(f"Ingestor: RUNNING (PID: {pid})")
        print(f"Logs:    {LOG_FILE}")
        print(f"Backend: {BACKEND_URL}")
    except ProcessLookupError:
        print(f"Ingestor: NOT RUNNING (stale PID file for {pid})")


def mask_task_for_display(task: IngestTask) -> dict[str, Any]:
    data: dict[str, Any] = {
        "status": task.status,
        "venueSlug": task.venue_slug,
        "message": task.message,
        "waitUntil": task.wait_until,
        "recordingWindow": task.recording_window,
    }
    if task.match_time:
        data["matchTime"] = task.match_time
    if task.stream_url:
        url = task.stream_url
        data["streamUrl"] = url[:80] + "..." if len(url) > 80 else url
    if task.storage:
        data["storage"] = {
            **task.storage,
            "accessKey": task.storage.get("accessKey", "")[:6] + "****",
            "secretKey": "****",
        }
    if task.settings:
        data["settings"] = task.settings
    return data


def test_task() -> None:
    validate_config(require_ffmpeg=False)
    log.info("Fetching next task from %s ...", BACKEND_URL)
    task = fetch_task()
    print(json.dumps(mask_task_for_display(task), indent=2))
    log.info("Task fetch successful for venue '%s'.", task.venue_slug)


def test_upload() -> None:
    validate_config(require_ffmpeg=False)
    log.info("Fetching task credentials and testing R2 upload...")
    task = fetch_task()
    if not task.should_record:
        log.error("Task is idle (outside recording window). Set venue hours or try during the window.")
        sys.exit(1)

    client = get_r2_client(task.storage)
    bucket = task.storage["bucket"]
    prefix = normalize_prefix(task.storage["prefix"])
    test_key = f"{prefix}_saha_connectivity_test.txt"
    body = (
        f"Saha Live R2 test — {datetime.datetime.now(datetime.timezone.utc).isoformat()}"
    ).encode()

    try:
        client.put_object(Bucket=bucket, Key=test_key, Body=body)
        log.info("R2 upload successful: s3://%s/%s", bucket, test_key)
        client.delete_object(Bucket=bucket, Key=test_key)
        log.info("Test object deleted.")
    except Exception as e:
        log.error("R2 test FAILED: %s", e)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Saha Live Regional Ingestor — orchestrated field worker (v3.3)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_start = subparsers.add_parser("start", help="Start the ingestor loop")
    p_start.add_argument("--daemon", action="store_true", help="Run as a background daemon")

    subparsers.add_parser("stop", help="Stop the running daemon")
    subparsers.add_parser("status", help="Show daemon status")
    subparsers.add_parser("test-task", help="Fetch the next ingestion task from the backend")
    subparsers.add_parser("test-upload", help="Fetch a task and test R2 upload connectivity")

    args = parser.parse_args()

    if args.command == "start":
        if args.daemon:
            setup_logging(daemon_mode=True)
            start_daemon()
        else:
            setup_logging(daemon_mode=False)
            run_ingestor()
    elif args.command == "stop":
        stop_daemon()
    elif args.command == "status":
        status_daemon()
    elif args.command == "test-task":
        setup_logging()
        test_task()
    elif args.command == "test-upload":
        setup_logging()
        test_upload()


if __name__ == "__main__":
    main()
