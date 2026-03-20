"""
RaagDosa session — manifest, logging paths, root derivation, DJ database detection.

Layer 3: imports from core (L0), files (L1), tags (L1). No terminal output.
"""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from raagdosa.core import now_iso, slugify
from raagdosa.files import ensure_dir, iter_jsonl


# ─────────────────────────────────────────────
# Config validation
# ─────────────────────────────────────────────
def validate_config(cfg: Dict[str, Any], app_version: str) -> List[str]:
    """Return a list of config warnings (empty = OK)."""
    warns: List[str] = []
    cv = str(cfg.get("app", {}).get("version", ""))
    if cv and cv != app_version:
        warns.append(
            f"Config version '{cv}' (script is v{app_version}). "
            "Run 'raagdosa init --update' to review new options.")
    for sec in ["scan", "decision", "review_rules", "year", "track_rename", "logging"]:
        if sec not in cfg:
            warns.append(f"Missing section '{sec}' — defaults used.")
    ap = cfg.get("active_profile")
    if not ap:
        warns.append("active_profile not set.")
    elif ap not in cfg.get("profiles", {}):
        warns.append(f"active_profile '{ap}' not found.")
    conf = cfg.get("review_rules", {}).get("min_confidence_for_clean", 0.85)
    if not (0.0 < float(conf) <= 1.0):
        warns.append(f"min_confidence_for_clean={conf} out of range (0,1].")
    return warns


# ─────────────────────────────────────────────
# Manifest — persistent Clean index
# ─────────────────────────────────────────────
def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")


def _mfpath(cfg: Dict[str, Any]) -> Path:
    return Path(cfg.get("logging", {}).get("root_dir", "logs")) / "clean_manifest.json"


def read_manifest(cfg: Dict[str, Any], app_version: str = "") -> Dict[str, Any]:
    p = _mfpath(cfg)
    if not p.exists():
        return {"version": app_version, "last_run": None, "entries": {}}
    try:
        return _read_json(p)
    except Exception:
        return {"version": app_version, "last_run": None, "entries": {}}


def write_manifest(cfg: Dict[str, Any], m: Dict[str, Any]) -> None:
    ensure_dir(_mfpath(cfg).parent)
    _write_json(_mfpath(cfg), m)


def manifest_add(cfg: Dict[str, Any], name: str, entry: Dict[str, Any],
                 app_version: str = "") -> None:
    from raagdosa.tags import normalize_unicode
    m = read_manifest(cfg, app_version)
    m["entries"][normalize_unicode(name)] = {**entry, "committed_at": now_iso()}
    write_manifest(cfg, m)


def manifest_has(cfg: Dict[str, Any], name: str, app_version: str = "") -> bool:
    from raagdosa.tags import normalize_unicode
    return normalize_unicode(name) in read_manifest(cfg, app_version).get("entries", {})


def manifest_set_last_run(cfg: Dict[str, Any], app_version: str = "") -> None:
    m = read_manifest(cfg, app_version)
    m["last_run"] = now_iso()
    write_manifest(cfg, m)


def manifest_get_last_run(cfg: Dict[str, Any], app_version: str = "") -> Optional[dt.datetime]:
    ts = read_manifest(cfg, app_version).get("last_run")
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(ts)
    except Exception:
        return None


# ─────────────────────────────────────────────
# Proposal path validation (anti-traversal)
# ─────────────────────────────────────────────
def validate_proposal_paths(raw_props: List[Dict], allowed_roots: List[Path]) -> List[str]:
    viols: List[str] = []
    resolved = [r.resolve() for r in allowed_roots]
    for p in raw_props:
        t = Path(p.get("target_path", ""))
        try:
            rt = t.resolve()
            if not any(str(rt).startswith(str(r)) for r in resolved):
                viols.append(f"Target escapes allowed roots: {t}")
        except Exception as e:
            viols.append(f"Cannot resolve {t}: {e}")
    return viols


# ─────────────────────────────────────────────
# DJ database detection
# ─────────────────────────────────────────────
_DJ_PATTERNS_DEFAULT = [
    "export.pdb", "database2", "rekordbox.xml",
    "_Serato_", "Serato Scratch", "Serato DJ", "PIONEER",
]


def find_dj_databases(source_root: Path, cfg: Dict[str, Any] = None) -> List[str]:
    patterns = list(
        (cfg or {}).get("dj_safety", {}).get("database_patterns") or _DJ_PATTERNS_DEFAULT)
    found: List[str] = []
    for pat in patterns:
        try:
            matches = list(source_root.rglob(f"*{pat}*"))
        except Exception:
            matches = []
        if matches:
            found.append(f"'{pat}' ({len(matches)})")
    return found


# ─────────────────────────────────────────────
# Log rotation
# ─────────────────────────────────────────────
def rotate_log_if_needed(log_path: Path, max_mb: float = 10.0) -> None:
    if not log_path.exists():
        return
    if log_path.stat().st_size / (1024 * 1024) > max_mb:
        archive = log_path.with_suffix(
            f".{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.archive.jsonl")
        log_path.rename(archive)


# ─────────────────────────────────────────────
# Root derivation
# ─────────────────────────────────────────────
def _rname(profile: Dict, key: str, default: str) -> str:
    return profile.get(key, default)


def derive_wrapper_root(profile: Dict, source_root: Path) -> Path:
    """
    The raagdosa wrapper folder — parent of Clean/, Review/, logs/.
    Config key: profiles.<name>.wrapper_folder_name  (default: 'raagdosa')
    """
    base = source_root.parent if profile.get("clean_mode") == "inside_parent" else source_root
    return base / _rname(profile, "wrapper_folder_name", "raagdosa")


def derive_clean_root(profile: Dict, source_root: Path) -> Path:
    return derive_wrapper_root(profile, source_root) / _rname(profile, "clean_folder_name", "Clean")


def derive_review_root(profile: Dict, source_root: Path) -> Path:
    return derive_wrapper_root(profile, source_root) / _rname(profile, "review_folder_name", "Review")


def derive_clean_albums_root(profile: Dict, source_root: Path) -> Path:
    return derive_clean_root(profile, source_root) / _rname(profile, "clean_albums_folder_name", "Albums")


def derive_review_albums_root(profile: Dict, source_root: Path) -> Path:
    return derive_review_root(profile, source_root) / _rname(profile, "review_albums_folder_name", "Albums")


def derive_duplicates_root(profile: Dict, source_root: Path) -> Path:
    return derive_review_root(profile, source_root) / _rname(profile, "duplicates_folder_name", "Duplicates")


def ensure_roots(profile: Dict, source_root: Path, create: bool = True) -> Dict[str, Path]:
    roots = {
        "clean_root":     derive_clean_root(profile, source_root),
        "review_root":    derive_review_root(profile, source_root),
        "clean_albums":   derive_clean_albums_root(profile, source_root),
        "clean_tracks":   derive_clean_root(profile, source_root) / _rname(profile, "clean_tracks_folder_name", "Tracks"),
        "review_albums":  derive_review_albums_root(profile, source_root),
        "duplicates":     derive_duplicates_root(profile, source_root),
        "review_orphans": derive_review_root(profile, source_root) / _rname(profile, "orphans_folder_name", "Orphans"),
    }
    if create:
        for p in roots.values():
            ensure_dir(p)
    return roots


def setup_logging_paths(cfg: Dict[str, Any], profile: Dict[str, Any],
                        source_root: Path, profile_name: str = "") -> None:
    """
    Resolve all logging paths and mutate cfg["logging"] in-place with absolute paths.
    Call once per command after profile and source_root are known.
    """
    wrapper = derive_wrapper_root(profile, source_root)
    lcfg = cfg.setdefault("logging", {})
    log_base = wrapper / lcfg.get("root_dir", "logs")
    log_root = log_base / slugify(profile_name, 40) if profile_name else log_base
    ensure_dir(log_root)
    lcfg["root_dir"]          = str(log_root)
    lcfg["session_dir"]       = str(log_root / "sessions")
    lcfg["history_log"]       = str(log_root / "history.jsonl")
    lcfg["skipped_log"]       = str(log_root / "skipped.jsonl")
    lcfg["track_history_log"] = str(log_root / "track-history.jsonl")
    lcfg["track_skipped_log"] = str(log_root / "track-skipped.jsonl")


def resolve_log_paths_from_active_profile(cfg: Dict[str, Any]) -> None:
    """
    Resolve logging paths for commands that don't take an explicit profile arg
    (history, undo, sessions).  Uses active_profile + source_root to find the
    wrapper folder.  Safe no-op if profile/source_root are not available.
    """
    prof_name = cfg.get("active_profile", "")
    prof = cfg.get("profiles", {}).get(prof_name, {})
    if not prof:
        return
    try:
        source_root = Path(prof.get("source_root", "")).expanduser()
        if source_root.exists():
            setup_logging_paths(cfg, prof, source_root)
    except Exception:
        pass


def resolve_last_session(cfg: Dict[str, Any]) -> Optional[str]:
    """Return the most recent session_id from the history log."""
    resolve_log_paths_from_active_profile(cfg)
    hist_path = Path(cfg.get("logging", {}).get("history_log", ""))
    if not hist_path.name:
        return None
    hist = iter_jsonl(hist_path)
    for h in reversed(hist):
        sid = h.get("session_id", "")
        if sid:
            return sid
    return None


def load_last_session(cfg: Dict[str, Any]) -> Optional[Path]:
    """Return path to the most recent session's proposals.json, or None."""
    resolve_log_paths_from_active_profile(cfg)
    sdir = Path(cfg.get("logging", {}).get("session_dir", ""))
    if not sdir.exists():
        return None
    sessions = sorted([d for d in sdir.iterdir() if d.is_dir()], reverse=True)
    for s in sessions:
        pp = s / "proposals.json"
        if pp.exists():
            return pp
    return None
