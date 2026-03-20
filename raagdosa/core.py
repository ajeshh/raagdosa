"""
RaagDosa core — dataclasses, shared utilities, globals, stop handling.

LEAF module: may import from raagdosa.ui but nothing else in the package.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import re
import signal
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

from raagdosa.ui import C


# ─────────────────────────────────────────────
# Graceful stop (SIGINT)
# ─────────────────────────────────────────────
_stop_after_current = False
_force_stop = False
_sigint_count = 0


def _sigint_handler(sig, frame) -> None:
    global _stop_after_current, _force_stop, _sigint_count
    _sigint_count += 1
    if _sigint_count == 1:
        _stop_after_current = True
        print(f"\n{C.YELLOW}⚡ Ctrl+C — finishing current folder then stopping. "
              f"Press again to force quit.{C.RESET}")
    else:
        _force_stop = True
        print(f"\n{C.RED}⚡ Force stop.{C.RESET}")
        sys.exit(130)


def register_stop_handler() -> None:
    signal.signal(signal.SIGINT, _sigint_handler)


def should_stop() -> bool:
    return _stop_after_current or _force_stop


# ─────────────────────────────────────────────
# Core utilities
# ─────────────────────────────────────────────
def now_iso() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def slugify(s: str, max_len: int = 24) -> str:
    """Convert a string to a lowercase, hyphen-separated filesystem-safe slug."""
    s = unicodedata.normalize("NFC", s.strip().lower())
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s[:max_len] if len(s) > max_len else s


def make_session_id(profile: str = "", source_folder: str = "",
                    session_name: str = "") -> str:
    """
    Human-readable session ID: YYYY-MM-DD_HH-MM_<name-or-profile>_<source-folder-slug>
    With --session-name: 2026-03-08_14-30_bandcamp-friday
    Without:             2026-03-08_14-30_incoming_slsk-complete-march
    """
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M")
    parts = [ts]
    if session_name:
        parts.append(slugify(session_name, 40))
    else:
        if profile:
            parts.append(slugify(profile, 20))
        if source_folder:
            parts.append(slugify(Path(source_folder).name, 32))
    return "_".join(p for p in parts if p)


# ─────────────────────────────────────────────
# Dataclasses — shared across modules
# ─────────────────────────────────────────────
@dataclasses.dataclass
class FolderStats:
    tracks_total: int
    tracks_tagged: int
    tracks_unreadable: int
    extensions: Dict[str, int]
    format_duplicates: List[str] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class FolderProposal:
    folder_path: str
    folder_name: str
    proposed_folder_name: str
    target_path: str
    destination: str
    confidence: float
    decision: Dict[str, Any]
    stats: FolderStats


def fp_from_dict(d: Dict[str, Any]) -> FolderProposal:
    sd = d.get("stats", {})
    stats = (FolderStats(
        tracks_total=sd.get("tracks_total", 0),
        tracks_tagged=sd.get("tracks_tagged", 0),
        tracks_unreadable=sd.get("tracks_unreadable", 0),
        extensions=sd.get("extensions", {}),
        format_duplicates=sd.get("format_duplicates", []),
    ) if isinstance(sd, dict) else sd)
    return FolderProposal(
        folder_path=d["folder_path"],
        folder_name=d["folder_name"],
        proposed_folder_name=d["proposed_folder_name"],
        target_path=d["target_path"],
        destination=d["destination"],
        confidence=d["confidence"],
        decision=d["decision"],
        stats=stats,
    )


@dataclasses.dataclass
class CrateTrackPlan:
    """Per-track routing plan within a crate explosion."""
    source_path: str
    filename: str
    artist: Optional[str]
    title: Optional[str]
    target_path: str
    destination: str
    confidence: float
    reason: str
    embedded_release: Optional[str] = None
    track_number: Optional[int] = None


@dataclasses.dataclass
class EmbeddedRelease:
    """A coherent album/EP found embedded within a DJ crate."""
    artist: str
    artist_display: str
    album: str
    album_display: str
    tracks: List[Path]
    track_numbers: List[int]
    total_in_album: Optional[int]
    is_partial: bool
