#!/usr/bin/env python3
import argparse
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, render_template, request

import update_dne

LOG_PATH = Path(os.getenv("DNE_LOG_PATH", "logs/dne_sync.log"))

app = Flask(__name__)

status = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "last_error": None,
}
status_lock = threading.Lock()


def build_logger() -> logging.Logger:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("dne_web")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def run_update(args: argparse.Namespace) -> None:
    logger = build_logger()
    with status_lock:
        status["running"] = True
        status["last_run"] = datetime.utcnow().isoformat()
        status["last_result"] = None
        status["last_error"] = None

    try:
        logger.info("Iniciando sincronização via interface web.")
        update_dne.run_sync(args, logger)
        with status_lock:
            status["last_result"] = "success"
    except Exception as exc:  # noqa: BLE001 - registro de erro é necessário aqui
        logger.exception("Erro durante sincronização: %s", exc)
        with status_lock:
            status["last_result"] = "failed"
            status["last_error"] = str(exc)
    finally:
        with status_lock:
            status["running"] = False


def parse_args_from_env() -> argparse.Namespace:
    parser = update_dne.build_parser()
    return parser.parse_args([])


def tail_log(lines: int = 200) -> str:
    if not LOG_PATH.exists():
        return ""
    content = LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()
    return "\n".join(content[-lines:])


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/run")
def run():
    with status_lock:
        if status["running"]:
            return jsonify({"status": "running"}), 409

    args = parse_args_from_env()
    thread = threading.Thread(target=run_update, args=(args,), daemon=True)
    thread.start()
    return jsonify({"status": "started"})


@app.get("/status")
def get_status():
    with status_lock:
        return jsonify(status)


@app.get("/logs")
def get_logs():
    lines = request.args.get("lines", default=200, type=int)
    return jsonify({"logs": tail_log(lines)})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
