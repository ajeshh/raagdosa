"""
RaagDosa scanning — folder content classification, VA detection, EP detection.

Layer 4: imports from tags (L1), scoring (L2), naming (L2). No terminal output.
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any, Counter as CounterT, Dict, List, Optional, Set, Tuple, Union

from raagdosa.tags import normalize_unicode
from raagdosa.scoring import compute_dominant


# ─────────────────────────────────────────────
# EP detection
# ─────────────────────────────────────────────
_EP_NAME_RE = re.compile(r'(?:^|[\s\[\(])E\.?P\.?(?:$|[\s\]\)])', re.I)


def detect_ep(audio_files: List[Path], cfg: Dict[str, Any],
              folder_name: str = "") -> bool:
    """True if track count is in the EP range OR folder/album name contains an EP keyword."""
    ep = cfg.get("ep_detection", {})
    if not ep.get("enabled", True):
        return False
    mn = int(ep.get("min_tracks", 2))
    mx = int(ep.get("max_tracks", 6))
    if mn <= len(audio_files) <= mx:
        return True
    if folder_name and _EP_NAME_RE.search(folder_name):
        return True
    return False


# ─────────────────────────────────────────────
# Mix / chart folder classifier
# ─────────────────────────────────────────────
_MIX_FOLDER_KW = re.compile(
    r'\b(mix(tape)?|presents|sessions?|podcast|promo\s+set|live\s+at|'
    r'compiled\s+by|mixed\s+by|chart|top\s*\d+|best\s+of|'
    r'greatest\s+hits|collection|discography|anthology)\b', re.I)


def classify_folder_content(
    audio_files: List[Path],
    folder_name: str,
    all_tags: List[Dict[str, Optional[str]]],
    cfg: Dict[str, Any],
) -> str:
    """
    Returns one of: 'album' | 'va' | 'mix' | 'ep'
    """
    mc = cfg.get("mix_detection", {})
    enabled = mc.get("enabled", True)

    if detect_ep(audio_files, cfg, folder_name):
        return "ep"

    if not enabled:
        return "album"

    extra_kw = [k.lower() for k in (mc.get("folder_name_keywords", []) or [])]
    if _MIX_FOLDER_KW.search(folder_name) or any(k in folder_name.lower() for k in extra_kw):
        return "mix"

    tagged = [t for t in all_tags if any(v for v in t.values())]
    if not tagged:
        return "album"

    artists = [t.get("artist", "").strip().lower() for t in tagged if t.get("artist")]
    albumartists = [t.get("albumartist", "").strip().lower() for t in tagged
                    if t.get("albumartist")]
    _va_kw_set = {
        "various artists", "various", "va", "v/a", "v.a.", "v.a", "vvaa",
        "varios artistas", "varios", "artistes variés", "aa.vv.",
        "разные исполнители", "различные исполнители", "сборник",
        "verschiedene interpreten", "diverse interpreten",
        "artistas varios", "vários artistas", "vários intérpretes",
    }

    if albumartists:
        _aa_dom = Counter(albumartists).most_common(1)[0][0] if albumartists else ""
        if _aa_dom and _aa_dom not in _va_kw_set:
            return "album"

    if len(artists) >= 3:
        unique_ratio = len(set(artists)) / len(artists)
        if unique_ratio >= float(mc.get("unique_artist_ratio_mix", 0.80)):
            if albumartists:
                aa_set = set(albumartists)
                if any(x in aa_set for x in _va_kw_set):
                    return "va"
            return "mix"

    return "album"


# ─────────────────────────────────────────────
# VA detection
# ─────────────────────────────────────────────
def detect_va(aa_norm: str, track_artists: Union[Counter, List[str]],
              cfg: Dict[str, Any]) -> bool:
    """
    Return True if this folder looks like a Various Artists release.

    v6.1 PHILOSOPHY: Default to album, not VA. Only flag VA when:
      (a) albumartist tag explicitly says VA, OR
      (b) heuristic ratio is HIGH (>=0.75) AND there's no dominant artist.
    """
    vc = cfg.get("various_artists", {})
    matches = {m.lower() for m in vc.get("albumartist_matches", [])}

    if aa_norm and aa_norm in matches:
        return True

    if not vc.get("enable_heuristics", True):
        return False

    if aa_norm and aa_norm not in matches:
        return False

    if isinstance(track_artists, Counter):
        non_empty = {k: v for k, v in track_artists.items() if k}
        if not non_empty:
            return False
        total = sum(non_empty.values())
        unique = len(non_empty)
    else:
        non_empty_list = [a for a in track_artists if a]
        if not non_empty_list:
            return False
        total = len(non_empty_list)
        unique = len(set(non_empty_list))

    threshold = float(vc.get("unique_artist_ratio_above", 0.75))
    if (unique / total) < threshold:
        return False

    if isinstance(track_artists, Counter) and non_empty:
        top_count = max(non_empty.values())
        if top_count / total >= 0.40:
            return False

    return True


# ─────────────────────────────────────────────
# Year picking
# ─────────────────────────────────────────────
def pick_year(year_counts: Counter, tracks_with_year: int, total: int,
              cfg: Dict[str, Any]) -> Tuple[Optional[int], Dict[str, Any]]:
    yc = cfg.get("year", {})
    if not yc.get("enabled", True) or total == 0:
        return None, {"included": False}
    pres = tracks_with_year / total
    if pres < float(yc.get("require_presence_ratio", 0.50)):
        return None, {"included": False, "reason": "presence_ratio_low"}
    yv, ys, _ = compute_dominant(year_counts)
    if yv is None:
        return None, {"included": False, "reason": "no_year_votes"}
    if ys < float(yc.get("agreement_threshold", 0.70)):
        return None, {"included": False, "reason": "agreement_low"}
    try:
        y = int(yv)
    except Exception:
        return None, {"included": False, "reason": "year_not_int"}
    amin = int(yc.get("allowed_range", {}).get("min", 1900))
    amax = int(yc.get("allowed_range", {}).get("max", 2100))
    if not (amin <= y <= amax):
        return None, {"included": False, "reason": "year_out_of_range"}
    return y, {"included": True, "presence_ratio": pres, "agreement": ys}


# ─────────────────────────────────────────────
# Format duplicate detection
# ─────────────────────────────────────────────
def detect_format_dupes(files: List[Path]) -> List[str]:
    by_stem: Dict[str, List[str]] = {}
    for f in files:
        by_stem.setdefault(normalize_unicode(f.stem.lower()), []).append(f.suffix.lower())
    return [f"{s}: {', '.join(sorted(set(e)))}" for s, e in by_stem.items()
            if len(set(e)) > 1]
