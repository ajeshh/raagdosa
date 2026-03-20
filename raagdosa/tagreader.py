"""
RaagDosa TagReader — config-aware tag reading with caching.

Layer 2: imports from tags (L1), files (L1), core (L0), ui (L0).

Provides:
  TagCache        — persistent tag cache keyed by file path + mtime
  read_audio_tags — config-driven tag extraction via mutagen
  _get_tag_cache  — lazy singleton accessor
  reset_tag_cache — clear the singleton (for tests / reload)
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from raagdosa import APP_VERSION
from raagdosa.core import now_iso
from raagdosa.files import ensure_dir, file_mtime, read_json
from raagdosa.tags import MutagenFile, mutagen_first
from raagdosa.ui import C, VERBOSE, out


# ─────────────────────────────────────────────────────────────────
# Tag cache — persisted between runs
# ─────────────────────────────────────────────────────────────────

class TagCache:
    """
    Persistent tag cache. Keyed by absolute path → {mtime, tags}.
    Thread-safe for concurrent reads/writes during parallel scan.
    """
    def __init__(self, cache_path: Path):
        self._path = cache_path
        self._lock = Lock()
        self._dirty = False
        self._data: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = read_json(self._path)
            # Support versioned format
            self._data = raw.get("entries", raw) if isinstance(raw, dict) else {}
        except Exception:
            self._data = {}

    def get(self, path: Path) -> Optional[Dict[str, Optional[str]]]:
        """Return cached tags if file mtime matches, else None."""
        key = str(path.resolve())
        mtime = file_mtime(path)
        with self._lock:
            entry = self._data.get(key)
        if entry and abs(entry.get("mtime", 0) - mtime) < 0.01:
            return entry["tags"]
        return None

    def set(self, path: Path, tags: Dict[str, Optional[str]]) -> None:
        """Store tags for path with current mtime."""
        key = str(path.resolve())
        mtime = file_mtime(path)
        with self._lock:
            self._data[key] = {"mtime": mtime, "tags": tags}
            self._dirty = True

    def save(self) -> None:
        """Flush cache to disk if dirty. Atomic write via temp file."""
        with self._lock:
            if not self._dirty:
                return
            tmp = self._path.with_suffix(".tmp")
            try:
                ensure_dir(self._path.parent)
                payload = {"version": APP_VERSION, "saved": now_iso(), "entries": self._data}
                tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(',', ':')), encoding="utf-8")
                tmp.replace(self._path)
                self._dirty = False
            except Exception:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass

    def evict_missing(self) -> int:
        """Remove entries for files that no longer exist. Returns count removed."""
        removed = 0
        with self._lock:
            keys = list(self._data.keys())
        for k in keys:
            if not Path(k).exists():
                with self._lock:
                    self._data.pop(k, None)
                    removed += 1
                self._dirty = True
        return removed

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._data)


# Module-level cache singleton — initialised in scan_folders
_tag_cache: Optional[TagCache] = None


def _get_tag_cache(cfg: Dict[str, Any]) -> Optional[TagCache]:
    global _tag_cache
    if _tag_cache is not None:
        return _tag_cache
    if not cfg.get("scan", {}).get("tag_cache_enabled", True):
        return None
    cache_path = Path(cfg.get("logging", {}).get("root_dir", "logs")) / "tag_cache.json"
    _tag_cache = TagCache(cache_path)
    return _tag_cache


def reset_tag_cache() -> None:
    global _tag_cache
    _tag_cache = None


# ─────────────────────────────────────────────────────────────────
# Config-aware tag reading
# ─────────────────────────────────────────────────────────────────

def read_audio_tags(path: Path, cfg: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Read audio tags from a file, using cache when available."""
    # Cache check — skip mutagen entirely for unchanged files
    cache = _tag_cache
    if cache is not None:
        cached = cache.get(path)
        if cached is not None:
            return cached

    keys = cfg.get("tags", {})
    result: Dict[str, Optional[str]] = {
        k: None for k in [
            "album", "albumartist", "artist", "title", "tracknumber",
            "discnumber", "year", "bpm", "key", "genre", "label", "compilation",
        ]
    }
    if MutagenFile is None:
        return result
    try:
        mf = MutagenFile(str(path), easy=True)
        if not mf or not getattr(mf, "tags", None):
            if cache is not None:
                cache.set(path, result)
            return result
        t = mf.tags
        result["album"]       = mutagen_first(t, keys.get("album_keys", ["album"]))
        result["albumartist"] = mutagen_first(t, keys.get("albumartist_keys", ["albumartist"]))
        result["artist"]      = mutagen_first(t, keys.get("artist_keys", ["artist"]))
        result["title"]       = mutagen_first(t, keys.get("title_keys", ["title"]))
        result["tracknumber"] = mutagen_first(t, keys.get("tracknumber_keys", ["tracknumber"]))
        result["discnumber"]  = mutagen_first(t, keys.get("discnumber_keys", ["discnumber"]))
        result["bpm"]         = mutagen_first(t, keys.get("bpm_keys", ["bpm", "tbpm"]))
        result["key"]         = mutagen_first(t, keys.get("key_keys", ["initialkey", "key"]))
        result["genre"]       = mutagen_first(t, keys.get("genre_keys", ["genre"]))
        result["label"]       = mutagen_first(t, keys.get("label_keys", ["organization", "label", "publisher"]))
        # v9.0: compilation flag for DJ crate detection
        _comp_val = mutagen_first(t, ["compilation", "TCMP", "cpil"])
        if _comp_val and str(_comp_val).strip() in ("1", "true", "True", "yes", "Yes"):
            result["compilation"] = "1"
        for yk in keys.get("year_keys_prefer", ["date", "year"]):
            if yk in t:
                yv = t.get(yk)
                yv = yv[0] if isinstance(yv, list) else yv
                if yv:
                    m = re.search(r"(\d{4})", str(yv))
                    if m:
                        result["year"] = m.group(1)
                        break
    except Exception as e:
        out(f"  {C.DIM}Tag read failed: {e}{C.RESET}", level=VERBOSE)
    if cache is not None:
        cache.set(path, result)
    return result
