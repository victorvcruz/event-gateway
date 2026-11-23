#!/usr/bin/env python3
"""Run one external command with a process-group timeout."""

from __future__ import annotations

import os
import signal
import subprocess
import sys


def main() -> int:
    if len(sys.argv) < 4 or sys.argv[2] != "--":
        print(f"usage: {sys.argv[0]} SECONDS -- COMMAND [ARG ...]", file=sys.stderr)
        return 2

    try:
        timeout = float(sys.argv[1])
    except ValueError:
        print("timeout must be a number", file=sys.stderr)
        return 2
    if timeout <= 0:
        print("timeout must be greater than zero", file=sys.stderr)
        return 2

    process = subprocess.Popen(sys.argv[3:], start_new_session=True)
    try:
        return process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        print(
            f"command timed out after {timeout:g}s: {sys.argv[3]}",
            file=sys.stderr,
        )
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            return process.wait()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
        return 124


if __name__ == "__main__":
    raise SystemExit(main())
