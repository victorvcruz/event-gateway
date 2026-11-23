#!/usr/bin/env python3
"""One-shot deterministic crash controller for the local chaos stack."""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse


class Store:
    def __init__(self):
        self.lock = threading.Lock()
        self.rules = {}
        self.hits = {}

    def put_rule(self, rule_id, rule):
        required = {"process", "point"}
        if not required.issubset(rule) or rule.get("action", "kill") != "kill":
            raise ValueError("process, point and action=kill are required")
        normalized = {
            "id": rule_id,
            "process": str(rule["process"]),
            "point": str(rule["point"]),
            "match": dict(rule.get("match", {})),
            "action": "kill",
            "remaining": int(rule.get("remaining", 1)),
        }
        if normalized["remaining"] < 1:
            raise ValueError("remaining must be positive")
        with self.lock:
            self.rules[rule_id] = normalized
            self.hits.pop(rule_id, None)
        return normalized

    def delete_rule(self, rule_id):
        with self.lock:
            return self.rules.pop(rule_id, None) is not None

    def record_hit(self, payload):
        with self.lock:
            for rule_id, rule in self.rules.items():
                if rule["remaining"] < 1:
                    continue
                if payload.get("process") != rule["process"] or payload.get("point") != rule["point"]:
                    continue
                if any(payload.get(key) != value for key, value in rule["match"].items()):
                    continue
                rule["remaining"] -= 1
                hit = dict(payload)
                hit["rule_id"] = rule_id
                hit["recorded_at"] = time.time()
                self.hits[rule_id] = hit
                return "kill", rule_id
        return "continue", None

    def hit(self, rule_id):
        with self.lock:
            return self.hits.get(rule_id)

    def snapshot(self):
        with self.lock:
            return {"rules": list(self.rules.values()), "hits": list(self.hits.values())}

    def reset(self):
        with self.lock:
            self.rules.clear()
            self.hits.clear()


STORE = Store()


class Handler(BaseHTTPRequestHandler):
    server_version = "chaos-controller/1"

    def log_message(self, fmt, *args):
        print("[chaos-controller] " + fmt % args, flush=True)

    def read_json(self):
        size = int(self.headers.get("Content-Length", "0"))
        if size < 1 or size > 65536:
            raise ValueError("invalid content length")
        return json.loads(self.rfile.read(size))

    def write_json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/healthz":
            self.write_json(200, {"status": "ok"})
            return
        if path == "/state":
            self.write_json(200, STORE.snapshot())
            return
        if path.startswith("/hits/"):
            rule_id = unquote(path[len("/hits/"):])
            hit = STORE.hit(rule_id)
            self.write_json(200 if hit else 404, hit or {"error": "not found"})
            return
        self.write_json(404, {"error": "not found"})

    def do_PUT(self):
        path = urlparse(self.path).path
        if not path.startswith("/rules/"):
            self.write_json(404, {"error": "not found"})
            return
        try:
            rule = STORE.put_rule(unquote(path[len("/rules/"):]), self.read_json())
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.write_json(400, {"error": str(exc)})
            return
        self.write_json(200, rule)

    def do_DELETE(self):
        path = urlparse(self.path).path
        if not path.startswith("/rules/"):
            self.write_json(404, {"error": "not found"})
            return
        deleted = STORE.delete_rule(unquote(path[len("/rules/"):]))
        self.write_json(200 if deleted else 404, {"deleted": deleted})

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/reset":
            STORE.reset()
            self.write_json(200, {"status": "reset"})
            return
        if path != "/hit":
            self.write_json(404, {"error": "not found"})
            return
        try:
            payload = self.read_json()
            if not payload.get("process") or not payload.get("point"):
                raise ValueError("process and point are required")
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.write_json(400, {"error": str(exc)})
            return
        action, rule_id = STORE.record_hit(payload)
        self.write_json(200, {"action": action, "rule_id": rule_id})


if __name__ == "__main__":
    port = int(os.environ.get("CHAOS_CONTROLLER_PORT", "8475"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"[chaos-controller] listening on {port}", flush=True)
    server.serve_forever()
