"""
RaagDosa pipeline — pure-logic pipeline infrastructure.

Layer 6: imports from core (L0), files (L1), session (L3).
No terminal output (UI-agnostic per architecture doc).

The orchestration functions (_run_core, _run_triage, cmd_go,
_interactive_streaming) remain in raagdosa_main.py because they
are deeply coupled to terminal I/O (Progress bars, input prompts,
color-coded output).
"""
from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from raagdosa.core import FolderProposal


# ─────────────────────────────────────────────
# Performance tiers
# ─────────────────────────────────────────────
_PERF_TIERS: Dict[str, Dict[str, int]] = {
    "slow":   {"workers": 1, "lookahead": 1,  "sleep_copy_ms": 50},
    "medium": {"workers": 2, "lookahead": 4,  "sleep_copy_ms": 10},
    "fast":   {"workers": 4, "lookahead": 8,  "sleep_copy_ms": 0},
    "ultra":  {"workers": 8, "lookahead": 16, "sleep_copy_ms": 0},
}


def resolve_perf_settings(cfg: Dict[str, Any], cli_tier: Optional[str] = None) -> Dict[str, Any]:
    """Return resolved performance settings, merging tier defaults with per-key overrides."""
    pc = cfg.get("performance", {})
    tier_name = (cli_tier or pc.get("tier", "medium")).lower()
    if tier_name not in _PERF_TIERS:
        tier_name = "medium"
    base = dict(_PERF_TIERS[tier_name])
    # Per-key overrides win
    if "workers"               in pc: base["workers"]        = int(pc["workers"])
    if "streaming_lookahead"   in pc: base["lookahead"]      = int(pc["streaming_lookahead"])
    if "sleep_between_moves_ms" in pc: base["sleep_copy_ms"] = int(pc["sleep_between_moves_ms"])
    # Legacy: scan.workers still honoured if no performance section
    if "workers" not in (cfg.get("performance") or {}) and "workers" in cfg.get("scan", {}):
        base["workers"] = int(cfg["scan"]["workers"])
    return base


def detect_recommended_tier() -> str:
    """Lightweight hardware heuristic — no psutil required."""
    cores = os.cpu_count() or 1
    ram_gb = 0
    try:
        import platform
        if platform.system() == "Darwin":
            import subprocess as _sp
            r = _sp.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, timeout=2)
            ram_gb = int(r.stdout.strip()) // 1024 // 1024 // 1024
        elif platform.system() == "Linux":
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal"):
                        ram_gb = int(line.split()[1]) // 1024 // 1024; break
    except Exception:
        pass
    if cores <= 2 or (0 < ram_gb <= 8):  return "slow"
    if cores <= 4 or (0 < ram_gb <= 16): return "medium"
    if cores <= 8 or (0 < ram_gb <= 32): return "fast"
    return "ultra"


# ─────────────────────────────────────────────
# Triage — proposal tier splitting
# ─────────────────────────────────────────────
FORCE_HOLD_REASONS: Set[str] = {
    "duplicate_in_run", "already_in_clean", "heuristic_fallback", "unreadable_ratio_high",
}


def triage_proposals(
    proposals: List[FolderProposal],
    auto_threshold: float,
) -> Dict[str, List[FolderProposal]]:
    """
    Split proposals into HIGH / MID / PROB tiers.

    HIGH:  conf >= auto_threshold AND destination == 'clean' AND no review-forcing flags
    PROB:  destination == 'review' OR has FORCE_HOLD flags
    MID:   everything else (dest=clean, conf < auto_threshold, no force-hold)

    Keys 'auto' and 'hold' are kept as aliases for backward compat.
    """
    high: List[FolderProposal] = []
    mid: List[FolderProposal] = []
    prob: List[FolderProposal] = []
    for p in proposals:
        reasons = set(p.decision.get("route_reasons", []))
        is_force_hold = bool(reasons & FORCE_HOLD_REASONS)
        if p.confidence >= auto_threshold and p.destination == "clean" and not is_force_hold:
            high.append(p)
        elif p.destination == "review" or is_force_hold:
            prob.append(p)
        else:
            mid.append(p)
    high.sort(key=lambda p: p.confidence, reverse=True)
    mid.sort(key=lambda p: p.confidence, reverse=True)
    prob.sort(key=lambda p: p.confidence)  # worst first
    return {"high": high, "mid": mid, "prob": prob, "auto": high, "hold": mid + prob}


# ─────────────────────────────────────────────
# Duplicate comparison (pure logic)
# ─────────────────────────────────────────────
_AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".aiff", ".wav", ".ogg", ".opus"}


def _norm_title(s: str) -> str:
    """Normalise a title for fuzzy comparison: lowercase, strip numbers/punctuation."""
    s = (s or "").lower().strip()
    s = re.sub(r'^\d+[.\-_\s]+', '', s)   # strip leading track number
    s = re.sub(r'[^\w\s]', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def _title_similarity(a: str, b: str) -> float:
    """Simple Jaccard word-set similarity for title matching."""
    wa = set(_norm_title(a).split())
    wb = set(_norm_title(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / len(wa | wb)


def compare_with_existing(
    incoming_path: Path,
    existing_path: Path,
    incoming_tags: List[Dict],
    existing_tags: List[Dict],
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compare incoming folder against existing Clean folder.

    Returns a result dict:
        outcome:  exact_duplicate | missing_tracks | format_upgrade |
                  lower_quality_mp3 | partial_overlap | unknown
        missing_in_existing:  List[Path]  (files in incoming not in existing)
        matched:  int
        unmatched_existing: int
        incoming_formats: set
        existing_formats: set
    """
    dc = cfg.get("duplicates", {})
    title_thresh = float(dc.get("title_match_threshold", 0.90))
    size_tol = float(dc.get("size_match_tolerance", 0.01))

    inc_audio = [p for p in incoming_path.iterdir()
                 if p.is_file() and p.suffix.lower() in _AUDIO_EXTS]
    ex_audio = [p for p in existing_path.iterdir()
                if p.is_file() and p.suffix.lower() in _AUDIO_EXTS]

    inc_formats = {p.suffix.lower() for p in inc_audio}
    ex_formats = {p.suffix.lower() for p in ex_audio}

    # Build title→path maps from tags
    def tag_map(files, tags_list):
        m = {}
        for f, t in zip(files, tags_list or [{}] * len(files)):
            title = (t or {}).get("title") or _norm_title(f.stem)
            m[_norm_title(title)] = f
        return m

    inc_map = tag_map(inc_audio, incoming_tags)
    ex_map = tag_map(ex_audio, existing_tags)

    # Find tracks in incoming not matched in existing
    missing_in_existing: List[Path] = []
    for inc_title, inc_file in inc_map.items():
        best = max(((_title_similarity(inc_title, ex_t), ex_t) for ex_t in ex_map), default=(0, ""))
        if best[0] < title_thresh:
            missing_in_existing.append(inc_file)

    matched = len(inc_map) - len(missing_in_existing)

    # Determine outcome
    # Format upgrade: incoming is FLAC, existing is MP3 only
    if ".flac" in inc_formats and ".flac" not in ex_formats and matched >= max(1, len(inc_map) - 1):
        outcome = "format_upgrade"
    # Lower quality: incoming is MP3, existing has FLAC
    elif ".flac" in ex_formats and ".flac" not in inc_formats and matched >= max(1, len(inc_map) - 1):
        outcome = "lower_quality_mp3"
    # Exact duplicate: all tracks match, similar sizes
    elif not missing_in_existing and inc_formats == ex_formats:
        inc_total = sum(f.stat().st_size for f in inc_audio)
        ex_total = sum(f.stat().st_size for f in ex_audio)
        size_ok = abs(inc_total - ex_total) / max(ex_total, 1) <= size_tol if ex_total else True
        outcome = "exact_duplicate" if size_ok else "partial_overlap"
    elif missing_in_existing and matched > 0:
        outcome = "missing_tracks"
    else:
        outcome = "partial_overlap"

    return {
        "outcome": outcome,
        "missing_in_existing": missing_in_existing,
        "matched": matched,
        "unmatched_existing": len(ex_map) - matched,
        "incoming_formats": inc_formats,
        "existing_formats": ex_formats,
        "incoming_count": len(inc_audio),
        "existing_count": len(ex_audio),
    }


# ─────────────────────────────────────────────
# Genre roots resolution
# ─────────────────────────────────────────────
def resolve_genre_roots(cfg: Dict[str, Any], cli_roots: Optional[List[str]] = None) -> Set[str]:
    """Return effective set of genre root folder names (CLI override + config persistent list)."""
    roots: Set[str] = set()
    # Config persistent list
    for item in cfg.get("genre_roots", []) or []:
        if isinstance(item, str):
            roots.add(item)
        elif isinstance(item, dict) and item.get("name"):
            roots.add(item["name"])
    # CLI flag overrides (session-only, not written to config)
    if cli_roots:
        for r in cli_roots:
            roots.add(r.strip())
    return roots


# ─────────────────────────────────────────────
# Date parsing
# ─────────────────────────────────────────────
def parse_since(val: Optional[str], cfg: Dict[str, Any]) -> Optional[dt.datetime]:
    """Parse --since value into a datetime. Raises ValueError on bad input."""
    if not val:
        return None
    if val == "last_run":
        from raagdosa.session import manifest_get_last_run
        return manifest_get_last_run(cfg)
    try:
        return dt.datetime.fromisoformat(val)
    except Exception:
        raise ValueError(f"Cannot parse --since '{val}'. Use ISO date or 'last_run'.")


# ─────────────────────────────────────────────
# Filesystem utilities
# ─────────────────────────────────────────────
def folder_size(path: Path) -> int:
    """Total bytes of all files under *path*."""
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file():
                total += f.stat().st_size
    except (OSError, PermissionError):
        pass
    return total


def folder_mtime(path: Path) -> float:
    """Most recent modification time of a folder (0.0 on error)."""
    try:
        return path.stat().st_mtime
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
# Skip-set initialization
# ─────────────────────────────────────────────
_SKIP_AUDIO_EXTENSIONS_DEFAULT: Set[str] = {".sfk", ".asd", ".reapeaks", ".pkf", ".db", ".lrc"}
_SKIP_FOLDER_NAMES_DEFAULT: Set[str] = {"__MACOSX", "__macosx"}


def build_skip_sets(cfg: Dict[str, Any]) -> Tuple[Set[str], Set[str]]:
    """
    Build skip sets from config, merged with hardcoded defaults.

    Returns (skip_extensions, skip_folder_names).
    Unlike the monolith's _init_skip_sets which mutates globals, this returns
    the computed sets for the caller to use.
    """
    sc = cfg.get("scan", {})
    skip_exts = set(_SKIP_AUDIO_EXTENSIONS_DEFAULT) | {
        e.lower() for e in (sc.get("skip_sidecar_extensions") or [])
    }
    skip_folders = set(_SKIP_FOLDER_NAMES_DEFAULT) | set(sc.get("skip_system_folders") or [])
    return skip_exts, skip_folders
