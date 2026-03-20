"""
RaagDosa library — library path resolution, templates, genre/BPM/key/label normalization.

Layer 3: imports from files (L1) for sanitize_name. No side effects, no terminal output.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from raagdosa.files import sanitize_name


# ─────────────────────────────────────────────
# Built-in library templates
# ─────────────────────────────────────────────
BUILTIN_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "standard":      {"name": "Standard Archive",   "template": "{artist}/{album}",
                      "description": "Artist → Album. Safe default for any collection.",
                      "requires": []},
    "dated":         {"name": "Dated Archive",       "template": "{artist}/{year} - {album}",
                      "description": "Artist → Year - Album. Chronological discography view.",
                      "requires": ["year"]},
    "flat":          {"name": "Flat",                "template": "{artist} - {album}",
                      "description": "Artist - Album in one folder. Minimal depth, fast browsing.",
                      "requires": []},
    "bpm":           {"name": "DJ — BPM Zones",      "template": "{bpm_range}/{artist} - {album}",
                      "description": "BPM range → Artist - Album. Tempo-first for single-genre DJs.",
                      "requires": ["bpm"], "note": "Best for track-level use. Album BPM uses average."},
    "genre-bpm":     {"name": "DJ — Genre + BPM",    "template": "{genre}/{bpm_range}/{artist} - {album}",
                      "description": "Genre → BPM → Artist - Album. Open-format DJ structure.",
                      "requires": ["genre", "bpm"], "note": "Best for track-level use."},
    "genre-bpm-key": {"name": "DJ — Harmonic",       "template": "{genre}/{bpm_range}/{camelot_key}/{artist} - {album}",
                      "description": "Genre → BPM → Key → Artist. Harmonic mixing structure.",
                      "requires": ["genre", "bpm", "key"], "note": "Track-level only. Albums span multiple keys."},
    "genre":         {"name": "Genre Curator",       "template": "{genre}/{artist}/{album}",
                      "description": "Genre → Artist → Album. Best for large multi-genre collections.",
                      "requires": ["genre"]},
    "label":         {"name": "Label Archive",       "template": "{label}/{artist} - {album}",
                      "description": "Label → Artist - Album. For label-focused collectors.",
                      "requires": ["label"], "note": "Label tag is often unpopulated."},
    "decade":        {"name": "Era / Decade",        "template": "{decade}/{genre}/{artist} - {album}",
                      "description": "Decade → Genre → Artist - Album. Era-first browsing.",
                      "requires": ["year", "genre"]},
}


# ─────────────────────────────────────────────
# Library config resolution
# ─────────────────────────────────────────────
def _resolve_lib_cfg(profile: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Merge library config: profile-level overrides global, key by key."""
    global_lib = cfg.get("library", {})
    profile_lib = profile.get("library", {})
    if not profile_lib:
        return dict(global_lib)
    merged = dict(global_lib)
    merged.update(profile_lib)
    return merged


# ─────────────────────────────────────────────
# Token helpers
# ─────────────────────────────────────────────
def _derive_decade(year: Optional[int]) -> str:
    if not year:
        return ""
    return f"{(year // 10) * 10}s"


def normalize_genre(genre: Optional[str], cfg: Dict[str, Any]) -> str:
    """Normalize a raw genre tag using the genre_map in config, if present."""
    if not genre:
        return ""
    g = genre.strip()
    genre_map = cfg.get("genre_map", {})
    for raw, canonical in genre_map.items():
        if g.lower() == raw.lower():
            return canonical
    return g


# ── Camelot wheel mapping ─────────────────────────────────────────────────
_CAMELOT_MAP: Dict[str, str] = {
    # Minor keys → A column
    "Abm": "1A", "G#m": "1A",
    "Ebm": "2A", "D#m": "2A",
    "Bbm": "3A", "A#m": "3A",
    "Fm": "4A",
    "Cm": "5A",
    "Gm": "6A",
    "Dm": "7A",
    "Am": "8A",
    "Em": "9A",
    "Bm": "10A",
    "F#m": "11A", "Gbm": "11A",
    "C#m": "12A", "Dbm": "12A",
    # Major keys → B column
    "B": "1B", "Cb": "1B",
    "F#": "2B", "Gb": "2B",
    "C#": "3B", "Db": "3B",
    "Ab": "4B", "G#": "4B",
    "Eb": "5B", "D#": "5B",
    "Bb": "6B", "A#": "6B",
    "F": "7B",
    "C": "8B",
    "G": "9B",
    "D": "10B",
    "A": "11B",
    "E": "12B",
}


def raw_key_to_camelot(key: Optional[str]) -> str:
    """Convert a raw musical key tag to Camelot notation. Returns '' if unmapped."""
    if not key:
        return ""
    k = key.strip()
    if k in _CAMELOT_MAP:
        return _CAMELOT_MAP[k]
    # Normalize: "A minor" → "Am", "C major" → "C"
    k2 = re.sub(r"\s*(minor|min)\s*$", "m", k, flags=re.IGNORECASE)
    k2 = re.sub(r"\s*(major|maj)\s*$", "", k2, flags=re.IGNORECASE)
    k2 = k2.strip()
    if k2 in _CAMELOT_MAP:
        return _CAMELOT_MAP[k2]
    # Try capitalizing first letter ("am" → "Am", "c" → "C")
    k3 = k2[0].upper() + k2[1:] if len(k2) > 1 else k2.upper()
    if k3 in _CAMELOT_MAP:
        return _CAMELOT_MAP[k3]
    return ""


def compute_bpm_range(bpm: Optional[float], cfg: Dict[str, Any]) -> str:
    """Bucket a BPM value into a range string using config-defined buckets."""
    if not bpm or bpm <= 0:
        return ""
    bpm_cfg = cfg.get("bpm_buckets", {})
    # Check named zones first (order matters)
    named = bpm_cfg.get("named_zones", {})
    for zone_name, bounds in named.items():
        if isinstance(bounds, (list, tuple)) and len(bounds) == 2:
            if bounds[0] <= bpm <= bounds[1]:
                return zone_name
    # Fall back to numeric bucket
    width = int(bpm_cfg.get("width", 10))
    lo = int(bpm // width) * width
    hi = lo + width - 1
    return f"{lo}-{hi}"


def normalize_label(label: Optional[str], cfg: Dict[str, Any]) -> str:
    """Normalize a raw label/publisher tag. Strips common suffixes."""
    if not label:
        return ""
    l = label.strip()
    for _ in range(3):
        l2 = re.sub(
            r"\s*(Records?|Recordings?|Music|Entertainment|Ltd\.?|Inc\.?|LLC)\s*$",
            "", l, flags=re.IGNORECASE).strip()
        if l2 == l:
            break
        l = l2
    return l


# ─────────────────────────────────────────────
# Library path resolution
# ─────────────────────────────────────────────
def resolve_library_path(
    base: Path, artist: str, album: str, year: Optional[int],
    is_flac_only: bool, is_va: bool, is_single: bool,
    is_mix: bool, cfg: Dict[str, Any],
    profile: Optional[Dict[str, Any]] = None,
    genre: Optional[str] = None,
    bpm: Optional[float] = None,
    key: Optional[str] = None,
    label: Optional[str] = None,
) -> Path:
    lib = _resolve_lib_cfg(profile or {}, cfg)
    template = lib.get("template", "{artist}/{album}")
    va_folder = lib.get("va_folder", "_Various Artists")
    singles_folder = lib.get("singles_folder", "Singles")
    mixes_folder = lib.get("mixes_folder", "_Mixes")
    unknown = lib.get("unknown_artist_label", "_Unknown")
    flac_seg = bool(lib.get("flac_segregation", False))

    artist_c = sanitize_name(artist or unknown)
    album_c = sanitize_name(album or "_Untitled")
    album_y = f"{album_c} ({year})" if year else album_c

    if is_mix:
        return base / mixes_folder / album_y
    if is_va:
        return base / va_folder / album_y
    if is_single:
        return base / artist_c / singles_folder
    if flac_seg and is_flac_only:
        return base / artist_c / "FLAC" / album_c

    # Resolve all token values for template substitution
    genre_val = normalize_genre(genre, cfg)
    decade_val = _derive_decade(year)
    bpm_range_val = compute_bpm_range(bpm, cfg)
    camelot_val = raw_key_to_camelot(key)
    label_val = normalize_label(label, cfg)

    # Fallback labels for missing token data
    genre_fallback = lib.get("genre_fallback", "_Unsorted")
    decade_fallback = lib.get("decade_fallback", "_Unknown Era")
    bpm_fallback = lib.get("bpm_fallback", "_Unknown BPM")
    key_fallback = lib.get("key_fallback", "_Unknown Key")
    label_fallback = lib.get("label_fallback", "_Unknown Label")

    tokens = {
        "artist": artist_c, "album": album_c,
        "year": year or "", "album_year": album_y,
        "genre": genre_val or genre_fallback,
        "decade": decade_val or decade_fallback,
        "bpm_range": bpm_range_val or bpm_fallback,
        "camelot_key": camelot_val or key_fallback,
        "label": label_val or label_fallback,
    }
    try:
        sub = template.format(**tokens)
    except KeyError:
        sub = f"{artist_c}/{album_c}"
    return base / sub
