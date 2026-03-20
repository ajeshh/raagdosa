#!/usr/bin/env python3
"""
RaagDosa UI — shared CLI output primitives.

Colors, progress bars, formatting helpers, and interactive prompts.
Used by raagdosa.py, raagdosa_scanner.py, and other modules.
"""
from __future__ import annotations

import datetime as dt
import platform
import sys
from pathlib import Path
from threading import Lock
from typing import Optional

# ─────────────────────────────────────────────
# Terminal detection
# ─────────────────────────────────────────────
_IS_TTY = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

try:
    import readchar as _readchar
    _HAS_READCHAR = True
except Exception:
    _readchar = None
    _HAS_READCHAR = False


# ─────────────────────────────────────────────
# ANSI colors
# ─────────────────────────────────────────────
class C:
    if _IS_TTY:
        RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
        GREEN = "\033[32m"; YELLOW = "\033[33m"; RED = "\033[31m"
        CYAN = "\033[36m"; BLUE = "\033[34m"; MAGENTA = "\033[35m"
    else:
        RESET = BOLD = DIM = GREEN = YELLOW = RED = CYAN = BLUE = MAGENTA = ""


# ─────────────────────────────────────────────
# Verbosity
# ─────────────────────────────────────────────
QUIET = 0; NORMAL = 1; VERBOSE = 2
_verbosity = NORMAL


def set_verbosity(v: int) -> None:
    global _verbosity
    _verbosity = v


def out(msg: str = "", level: int = NORMAL, file=None) -> None:
    if _verbosity >= level:
        print(msg, file=file or sys.stdout)


def err(msg: str) -> None:
    print(f"{C.RED}{msg}{C.RESET}", file=sys.stderr)


def warn(msg: str) -> None:
    if _verbosity >= QUIET:
        print(f"{C.YELLOW}⚠  {msg}{C.RESET}")


def ok_msg(msg: str) -> None:
    out(f"{C.GREEN}✓  {msg}{C.RESET}")


# ─────────────────────────────────────────────
# Status / confidence formatting
# ─────────────────────────────────────────────
def status_tag(dest: str) -> str:
    m = {
        "clean": f"{C.GREEN}[CLEAN ]{C.RESET}",
        "review": f"{C.YELLOW}[REVIEW]{C.RESET}",
        "duplicate": f"{C.RED}[DUPE  ]{C.RESET}",
    }
    return m.get(dest, f"[{dest.upper()[:6]:6}]")


def conf_color(c: float) -> str:
    if c >= 0.90:
        return f"{C.GREEN}{c:.2f}{C.RESET}"
    if c >= 0.75:
        return f"{C.YELLOW}{c:.2f}{C.RESET}"
    return f"{C.RED}{c:.2f}{C.RESET}"


def conf_bar(c: float, width: int = 20) -> str:
    """Render a confidence bar: ████████░░░░ 0.75"""
    filled = int(c * width)
    empty = width - filled
    if c >= 0.80:
        color = C.GREEN
    elif c >= 0.50:
        color = C.YELLOW
    else:
        color = C.RED
    return f"{color}{'█' * filled}{'░' * empty}{C.RESET}  {c:.2f}"


def risk_color(risk: str) -> str:
    """Color a risk tier label."""
    if risk == "safe":
        return f"{C.GREEN}{risk}{C.RESET}"
    if risk == "moderate":
        return f"{C.YELLOW}{risk}{C.RESET}"
    return f"{C.RED}{risk}{C.RESET}"


def human_size(nbytes: int) -> str:
    """Format bytes into human-readable size."""
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1024:
            return f"{nbytes:.1f}{unit}" if unit != "B" else f"{nbytes}{unit}"
        nbytes /= 1024
    return f"{nbytes:.1f}PB"


# ─────────────────────────────────────────────
# Progress bars
# ─────────────────────────────────────────────
class Progress:
    """Count-based progress bar for folder/file scanning."""

    def __init__(self, total: int, label: str = "Scanning"):
        self.total = total
        self.current = 0
        self.label = label
        self._active = _IS_TTY and _verbosity >= NORMAL
        self._lock = Lock()
        self._start = dt.datetime.now()

    def tick(self, msg: str = "") -> None:
        with self._lock:
            self.current += 1
            if not self._active:
                return
            pct = int(self.current / max(self.total, 1) * 100)
            bw = 22
            filled = int(bw * self.current / max(self.total, 1))
            bar = "█" * filled + "░" * (bw - filled)
            elapsed = (dt.datetime.now() - self._start).total_seconds()
            rate = self.current / elapsed if elapsed > 0 else 0
            remaining = self.total - self.current
            eta_s = remaining / rate if rate > 0 else 0
            if eta_s > 3600:
                eta = f"~{eta_s / 3600:.0f}h"
            elif eta_s > 60:
                eta = f"~{eta_s / 60:.0f}m"
            elif eta_s > 0:
                eta = f"~{eta_s:.0f}s"
            else:
                eta = ""
            rate_s = f"{rate:.0f}/s" if rate >= 1 else f"{rate * 60:.0f}/min"
            line = (f"\r{C.CYAN}{self.label}{C.RESET} [{bar}] "
                    f"{self.current}/{self.total} {pct}%"
                    f"  {C.DIM}{rate_s}  {eta}  {msg[:28]:<28}{C.RESET}")
            print(line, end="", flush=True)

    def done(self) -> None:
        if self._active:
            elapsed = (dt.datetime.now() - self._start).total_seconds()
            rate = self.current / elapsed if elapsed > 0 else 0
            print(f"  {C.DIM}({elapsed:.1f}s, {rate:.0f} folders/s){C.RESET}")


class SizeProgress:
    """Progress bar driven by bytes transferred, not count."""

    def __init__(self, total_bytes: int, total_count: int, label: str = "Moving"):
        self.total_bytes = max(total_bytes, 1)
        self.total_count = total_count
        self.bytes_done = 0
        self.count_done = 0
        self.label = label
        self._active = _IS_TTY and _verbosity >= NORMAL
        self._lock = Lock()
        self._start = dt.datetime.now()

    def tick(self, nbytes: int, msg: str = "") -> None:
        with self._lock:
            self.bytes_done += nbytes
            self.count_done += 1
            if not self._active:
                return
            pct = int(self.bytes_done / self.total_bytes * 100)
            bw = 22
            filled = int(bw * self.bytes_done / self.total_bytes)
            bar = "█" * filled + "░" * (bw - filled)
            elapsed = (dt.datetime.now() - self._start).total_seconds()
            rate = self.bytes_done / elapsed if elapsed > 0 else 0
            remaining = self.total_bytes - self.bytes_done
            eta_s = remaining / rate if rate > 0 else 0
            if eta_s > 3600:
                eta = f"~{eta_s / 3600:.0f}h"
            elif eta_s > 60:
                eta = f"~{eta_s / 60:.0f}m"
            elif eta_s > 0:
                eta = f"~{eta_s:.0f}s"
            else:
                eta = ""
            rate_s = f"{human_size(int(rate))}/s" if rate >= 1024 else ""
            line = (f"\r{C.CYAN}{self.label}{C.RESET} [{bar}] "
                    f"{self.count_done}/{self.total_count} {pct}%"
                    f"  {C.DIM}{rate_s}  {eta}  {msg[:28]:<28}{C.RESET}")
            print(line, end="", flush=True)

    def done(self) -> None:
        if self._active:
            elapsed = (dt.datetime.now() - self._start).total_seconds()
            print(f"  {C.DIM}({elapsed:.1f}s, {human_size(self.bytes_done)}){C.RESET}")


# ─────────────────────────────────────────────
# Interactive input
# ─────────────────────────────────────────────
def read_key(prompt: str = "  > ") -> str:
    """Read a single keypress if readchar is available, otherwise fall back to input()."""
    if _HAS_READCHAR and _IS_TTY:
        print(prompt, end="", flush=True)
        try:
            ch = _readchar.readkey()
        except (EOFError, KeyboardInterrupt):
            print()
            return "q"
        if ch in ("\r", "\n"):
            print()
            return ""
        if ch == " ":
            print("tracks")
            return "b"
        print(ch)
        if ch in ("R",):
            return ch
        return ch.lower()
    try:
        raw = input(prompt).strip()
        if raw in ("R",):
            return raw
        return raw.lower()
    except (EOFError, KeyboardInterrupt):
        return "q"


def open_in_finder(path: Path) -> None:
    """Open a folder in the system file manager."""
    import subprocess as _sp
    if not path.exists():
        warn(f"Path does not exist: {path}")
        return
    system = platform.system()
    try:
        if system == "Darwin":
            _sp.Popen(["open", str(path)])
        elif system == "Linux":
            _sp.Popen(["xdg-open", str(path)])
        elif system == "Windows":
            _sp.Popen(["explorer", str(path)])
        else:
            warn(f"Unsupported platform for open: {system}")
            return
        out(f"  {C.CYAN}Opened:{C.RESET} {path.name}")
    except Exception as e:
        err(f"  Could not open folder: {e}")


# ─────────────────────────────────────────────
# Tag proposal display helpers
# ─────────────────────────────────────────────
def format_tag_proposal(field: str, old_val: Optional[str], new_val: str,
                        confidence: float, risk: str) -> str:
    """Format a single tag proposal for CLI display."""
    old_display = old_val if old_val else f"{C.DIM}(empty){C.RESET}"
    arrow = f" {C.CYAN}→{C.RESET} "
    risk_display = risk_color(risk)
    conf_display = conf_color(confidence)
    return (f"    {field:16s} {old_display}{arrow}{C.BOLD}{new_val}{C.RESET}"
            f"  [{risk_display}] {conf_display}")


def format_tag_proposal_compact(field: str, old_val: Optional[str],
                                new_val: str) -> str:
    """Compact single-line tag change display."""
    old_display = old_val if old_val else "(empty)"
    return f"{field}: {old_display} → {new_val}"
