"""
RaagDosa tracks — track filename building, title cleanup, artist/title parsing.

Layer 5 (CLI-only per architecture — but these specific functions are pure logic).
Imports from naming (L2), scoring (L2), files (L1), tags (L1).
No terminal output.
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from raagdosa.tags import normalize_unicode
from raagdosa.files import sanitize_name
from raagdosa.naming import (
    detect_garbage_name, detect_mojibake, strip_trailing_domains,
    _smart_title_case_v43,
)
from raagdosa.scoring import parse_vinyl_track, parse_int_prefix


# ─────────────────────────────────────────────
# Filename regex constants
# ─────────────────────────────────────────────
_TRACK_DISC_COMPOUND = re.compile(r"^(\d{1})-(\d{2})\s+")
_TRACK_VINYL_STEM = re.compile(r"^([A-Da-d]\d{1,2})\s*[-–—]?\s+")
_BEATPORT_MULTI_ARTIST = re.compile(
    r"^(.+?)\s+-\s+(.+?,\s*.+?)(?:\s+\(([^)]+)\))?\s*$")


# ─────────────────────────────────────────────
# BPM/DJ encoding detection
# ─────────────────────────────────────────────
def detect_bpm_dj_encoding(stem: str) -> bool:
    return bool(re.match(
        r"^\d{2,3}\s*bpm|^\d{2,3}\s*[-–]\s*[A-Ga-g][#b]?\s*(m|min|maj)?\s*[-–]",
        stem.strip(), re.IGNORECASE))


# ─────────────────────────────────────────────
# Beatport filename handling
# ─────────────────────────────────────────────
def is_beatport_format(stem: str, folder_name: str = "") -> bool:
    """Detect Beatport-exported filenames."""
    if stem != stem.lstrip():
        return True
    fn_lower = folder_name.lower()
    if fn_lower.startswith(("beatport", "traxsource")):
        return True
    m = _BEATPORT_MULTI_ARTIST.match(stem)
    if m:
        title_part, artists_part = m.group(1), m.group(2)
        if "," in artists_part and "," not in title_part:
            if not re.match(r"^\d{1,3}[\s\-]", title_part):
                return True
    return False


def invert_beatport_filename(stem: str,
                             keep_all_artists: bool = False,
                             ) -> Tuple[str, str, str]:
    """Invert Beatport "Title - Artist1, Artist2 (Mix)" → (artist, title, mix_suffix)."""
    stem = stem.strip()
    mix_suffix = ""
    m_mix = re.search(
        r"\s+(\([^)]+(?:mix|remix|edit|version|dub|instrumental|reprise|rework)[^)]*\))\s*$",
        stem, re.I)
    if m_mix:
        mix_suffix = " " + m_mix.group(1)
        stem = stem[:m_mix.start()].strip()
    parts = re.split(r"\s+-\s+", stem, maxsplit=1)
    if len(parts) == 2:
        raw_title, raw_artists = parts[0].strip(), parts[1].strip()
    else:
        return stem, "", mix_suffix
    artist_list = [a.strip() for a in raw_artists.split(",")]
    if keep_all_artists:
        primary = ", ".join(artist_list)
    else:
        primary = artist_list[0]
    return primary, raw_title, mix_suffix


# ─────────────────────────────────────────────
# Title cleanup
# ─────────────────────────────────────────────
def cleanup_title(title: str, cfg: Dict[str, Any]) -> str:
    tc = cfg.get("title_cleanup", {})
    if not tc.get("enabled", True):
        return title.strip()
    o = title.strip().lstrip("\ufeff\u200b\u200c\u200d\u00a0")
    n = tc.get("normalize", {})
    if n.get("replace_underscores", True):
        o = o.replace("_", " ")
    if tc.get("strip_trailing_domains", True):
        o = strip_trailing_domains(o)
    if tc.get("strip_trailing_handles", True):
        o = re.sub(r"(\s*[-–—]\s*)?@[\w\.\-]+\s*$", "", o.strip())
    o = re.sub(r'[\s_]+\d{9,}(?:\s*[-–—]\s*\S[^-–—]*)?$', '', o).strip()
    phrases = [ph.lower() for ph in (tc.get("strip_trailing_phrases", []) or [])]
    keep = [k.lower() for k in (tc.get("keep_parenthetical_if_contains", []) or [])]

    def prot(seg: str) -> bool:
        return any(k in seg.lower() for k in keep)

    changed = True
    while changed:
        changed = False
        m = re.search(r"(\s*[\(\[]([^)\]]+)[\)\]]\s*)$", o)
        if m and not prot(m.group(2)) and any(ph in m.group(2).lower() for ph in phrases):
            o = o[:m.start()].strip()
            changed = True
            continue
        m2 = re.search(r"(\s*[-–—]\s*([^-–—]+)\s*)$", o)
        if m2 and any(ph in m2.group(2).lower() for ph in phrases) and not prot(m2.group(2)):
            o = o[:m2.start()].strip()
            changed = True
            continue
    if n.get("collapse_whitespace", True):
        o = re.sub(r"\s+", " ", o).strip()
    if n.get("trim_dots_spaces", True):
        o = o.rstrip(". ").strip()
    o = re.sub(r"\s*[-–—]+\s*$", "", o).strip()
    _alpha = [w for w in o.split() if any(c.isalpha() for c in w)]
    if _alpha and all(w.islower() for w in _alpha):
        o = _smart_title_case_v43(o, cfg)
    return o


# ─────────────────────────────────────────────
# Artist/title parsing from filename
# ─────────────────────────────────────────────
def parse_artist_title_from_fn(stem: str, folder_name: str = "",
                               cfg: Optional[Dict[str, Any]] = None,
                               ) -> Tuple[Optional[str], Optional[str]]:
    """Parse artist and title from a filename stem."""
    if cfg is None:
        cfg = {}
    beatport_aware = cfg.get("beatport_aware", True)
    keep_all = cfg.get("beatport_keep_all_artists", False)

    raw_stem = stem
    stem = stem.lstrip()

    m_disc = _TRACK_DISC_COMPOUND.match(stem)
    if m_disc:
        stem = stem[m_disc.end():]

    m_vinyl = _TRACK_VINYL_STEM.match(stem)
    if m_vinyl:
        stem = stem[m_vinyl.end():]

    if beatport_aware and is_beatport_format(raw_stem, folder_name):
        artist, title, _ = invert_beatport_filename(raw_stem, keep_all_artists=keep_all)
        if artist and title:
            return artist.strip(), title.strip()

    s = normalize_unicode(re.sub(r"\s+", " ", stem.strip()))
    if detect_bpm_dj_encoding(s):
        return None, None
    s = re.sub(r"_-_", " - ", s).replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r'[\s_]+\d{9,}(?:\s*[-–—]\s*\S[^-–—]*)?$', '', s).strip()
    s = re.sub(r"^[\[\(][^\]\)]+[\]\)]\s*", "", s)
    s2 = re.sub(r"^\(?\d{1,3}\)?\s*[-–—\.]\s*", "", s)
    s2 = re.sub(r"^\d{1,3}\s+", "", s2)
    parts = [p.strip() for p in re.split(r"\s+[-–—]\s+", s2) if p.strip()]
    if len(parts) < 2:
        parts = [p.strip() for p in re.split(r"\s*[-–—]\s*", s2) if p.strip()]
    _MUSIC_TERMS = {"mix", "rmx", "vip", "dub", "edit", "live", "demo", "ost", "dj", "va", "lp"}
    if len(parts) >= 3:
        _last = parts[-1]
        if re.match(r'^[a-z]{2,5}$', _last) and _last not in _MUSIC_TERMS:
            parts = parts[:-1]
    if len(parts) >= 2:
        return parts[0], " - ".join(parts[1:])
    if parts:
        return None, parts[0]
    return None, None


# ─────────────────────────────────────────────
# Mix suffix extraction
# ─────────────────────────────────────────────
def extract_mix_suffix(title: str, cfg: Dict[str, Any]) -> Tuple[str, str]:
    mc = cfg.get("mix_info", {})
    kw = [k.lower() for k in (mc.get("detect_keywords", []) or [])]
    if not mc.get("enabled", True) or not kw:
        return title, ""
    m = re.search(r"\s*(\(([^)]+)\))\s*$", title)
    if m and any(k in m.group(2).lower() for k in kw):
        return title[:m.start()].strip(), f" {m.group(1)}"
    m2 = re.search(r"\s*[-–—]\s*([^-–—]+)\s*$", title)
    if m2 and any(k in m2.group(1).lower() for k in kw):
        candidate = m2.group(1).strip()
        if "," not in candidate and len(candidate) <= 60:
            clean = title[:m2.start()].strip()
            sty = mc.get("style", "parenthetical")
            return (clean, f" - {candidate}") if sty == "dash" else (clean, f" ({candidate})")
    return title, ""


# ─────────────────────────────────────────────
# Folder classification for track renaming
# ─────────────────────────────────────────────
def classify_folder_for_tracks(decision: Dict[str, Any],
                               cfg: Dict[str, Any]) -> str:
    if bool(decision.get("is_va", False)):
        return "various"
    if float(decision.get("dominant_album_share", 0.0)) < float(
            cfg.get("decision", {}).get("album_dominance_threshold", 0.75)):
        return "mixed"
    dom_art_share = float(decision.get("dominant_artist_share", 1.0))
    if dom_art_share < 0.50:
        dom_aa = (decision.get("dominant_albumartist") or "").strip().lower()
        dom_art = (decision.get("dominant_artist") or "").strip().lower()
        if dom_aa and dom_art and dom_art.startswith(dom_aa):
            return "album"
        return "various"
    return "album"


# ─────────────────────────────────────────────
# Track filename builder (pure logic)
# ─────────────────────────────────────────────
def build_track_filename(
    classification: str,
    tags: Dict[str, Optional[str]],
    src: Path,
    cfg: Dict[str, Any],
    decision: Dict[str, Any],
    disc_multi: bool,
    total_tracks: int = 0,
) -> Tuple[Optional[str], float, str, Dict[str, Any]]:
    """Build a clean filename for a track. Returns (name, confidence, reason, meta)."""
    trc = cfg.get("track_rename", {})
    pat = trc.get("patterns", {})
    ext = src.suffix.lower()
    tag_title = (tags.get("title") or "").strip()
    tag_artist = (tags.get("artist") or "").strip()
    track_raw = (tags.get("tracknumber") or "").strip()
    disc_raw = (tags.get("discnumber") or "").strip()
    folder_name = src.parent.name
    meta: Dict[str, Any] = {}

    _tag_title_clean = (tag_title and len(tag_title) >= 2
                        and not detect_garbage_name(tag_title)
                        and not detect_mojibake(tag_title))
    _tag_artist_clean = (tag_artist and len(tag_artist) >= 2
                         and not detect_garbage_name(tag_artist)
                         and not detect_mojibake(tag_artist))

    fn_artist, fn_title = parse_artist_title_from_fn(
        src.stem, folder_name=folder_name, cfg=cfg)

    # Title: prefer clean tag, fall back to filename
    if _tag_title_clean:
        title = tag_title
        meta["title_src"] = "tag"
        if not re.search(r'(?<![a-zA-Z])(?:feat\.?|ft\.?|featuring)(?![a-zA-Z])',
                         title, re.I):
            _feat_search_strs = ([fn_title, fn_artist] if fn_title
                                 else ([fn_artist] if fn_artist else []))
            _feat_pat_re = re.compile(
                r'(?:^|[,\s\(\[])(?:feat\.?|ft\.?|featuring)(?![a-zA-Z])\s+'
                r'([^\)\],\-]+)', re.I)
            _collab = None
            for _fs in _feat_search_strs:
                if not _fs:
                    continue
                _feat_m = _feat_pat_re.search(_fs)
                if _feat_m:
                    _collab = _feat_m.group(1).strip().rstrip(')], ').strip()
                    if _collab:
                        break
            if _collab:
                title = f"{title} (feat. {_collab})"
                meta["title_src"] = "tag+feat_from_fn"
    elif fn_title:
        title = fn_title
        meta["title_src"] = "filename"
    else:
        title = tag_title
        meta["title_src"] = "tag_fallback" if title else "none"

    if title:
        title = cleanup_title(title, cfg)
    if not title:
        return None, 0.0, "missing_title", {}
    title_c, mix_suf = extract_mix_suffix(title, cfg)
    title_c = cleanup_title(title_c, cfg)
    if not title_c:
        return None, 0.0, "title_cleaned_empty", {}

    # Artist: prefer clean tag, fall back to filename
    if _tag_artist_clean:
        artist = tag_artist
        meta["artist_src"] = "tag"
    elif fn_artist:
        artist = fn_artist
        meta["artist_src"] = "filename"
    else:
        artist = tag_artist
        meta["artist_src"] = "tag_fallback" if artist else "none"
    if artist and cfg.get("artists", {}).get("feature_handling", {}).get(
            "normalize_tokens", True):
        artist = re.sub(r"\b(featuring|feat\.?|ft\.?)(?=\s|,|$)", "feat.",
                        artist, flags=re.IGNORECASE)
        artist = re.sub(r"\s+", " ", artist).strip()

    if classification == "various" and fn_artist and fn_title and _tag_artist_clean:
        _ta_n = normalize_unicode(tag_artist.lower().strip())
        _fn_t_n = normalize_unicode(fn_title.lower().strip())
        _fn_t_first = _fn_t_n.split(" - ")[0].strip()
        if (_fn_t_first == _ta_n
                and normalize_unicode(fn_artist.lower().strip()) != _ta_n):
            artist = fn_artist
            meta["artist_src"] = "filename_confusion_override"

    vinyl = parse_vinyl_track(track_raw.split("/")[0].strip()) if track_raw else None
    if vinyl:
        track_n = vinyl[2]
        meta["vinyl_side"] = vinyl[0]
        meta["track_src"] = "vinyl_notation"
    else:
        track_n = parse_int_prefix(track_raw) if track_raw else None

    if track_n is None:
        om = re.match(r"^(\d{1,3})", src.stem.strip())
        if om:
            raw_num = int(om.group(1))
            if 100 <= raw_num <= 999 and raw_num % 100 >= 1:
                track_n = raw_num % 100
                meta["track_src"] = "filename_disc_compound"
                meta["fn_disc_n"] = raw_num // 100
            else:
                track_n = raw_num
                meta["track_src"] = "filename_order"
    else:
        if (total_tracks > 0 and track_n > total_tracks
                and meta.get("track_src") not in (
                    "filename_order", "filename_disc_compound", "vinyl_notation")):
            om = re.match(r"^(\d{1,3})", src.stem.strip())
            if om:
                raw_num = int(om.group(1))
                fn_n2 = raw_num % 100 if 100 <= raw_num <= 999 else raw_num
                if 1 <= fn_n2 <= total_tracks:
                    if 100 <= raw_num <= 999 and raw_num % 100 >= 1:
                        track_n = fn_n2
                        meta["fn_disc_n"] = raw_num // 100
                        meta["track_src"] = "filename_disc_compound"
                    else:
                        track_n = fn_n2
                        meta["track_src"] = "filename_order_sanity"

    if track_n is None:
        if (classification == "album"
                and trc.get("track_numbers", {}).get("required_for_album", True)):
            return None, 0.0, "missing_track_number", {}
        elif classification == "various":
            if not artist:
                return None, 0.0, "missing_artist", meta
            tmpl = pat.get("mixed", "{artist} - {title}{mix_suffix}{ext}")
            return (sanitize_name(tmpl.format(
                artist=artist, title=title_c, mix_suffix=mix_suf,
                ext=ext, disc_prefix="", track=0)),
                0.85, "ok_no_tracknum", meta)

    # Guard: strip leading "NN - " from title when matching filename digits
    if title_c:
        _stem_m = re.match(r"^(\d{1,3})", src.stem.strip())
        if _stem_m:
            _fn_num = int(_stem_m.group(1))
            _fn_num2 = _fn_num % 100 if 100 <= _fn_num <= 999 else _fn_num
            _num_m = re.match(r'^(\d{1,3})\s*[-–—]\s*(.+)$', title_c)
            if _num_m and int(_num_m.group(1)) == _fn_num2:
                title_c = _num_m.group(2).strip() or title_c

    # Guard: strip leading "ArtistName - " from title if it matches albumartist/artist
    _fold = lambda s: unicodedata.normalize(
        'NFKD', normalize_unicode(s)).encode("ascii", "ignore").decode("ascii").lower()
    if classification == "album" and decision and title_c:
        _aa = (decision.get("albumartist_display") or "").strip()
        if _aa:
            _aa_f = _fold(_aa)
            if len(_aa_f.split()) == len(_aa.split()) and _aa_f:
                _sep_m = re.match(re.escape(_aa_f) + r'\s*[-–—]\s*', _fold(title_c))
                if _sep_m and _sep_m.end() < len(_fold(title_c)):
                    title_c = title_c[_sep_m.end():].strip() or title_c
    if classification == "various" and artist and title_c:
        _art_f = _fold(artist)
        if len(_art_f.split()) == len(artist.split()) and _art_f:
            _sep_m = re.match(re.escape(_art_f) + r'\s*[-–—]\s*', _fold(title_c))
            if _sep_m and _sep_m.end() < len(_fold(title_c)):
                title_c = title_c[_sep_m.end():].strip() or title_c

    # Strip trailing [LabelName] bracket from track titles
    if title_c:
        _tc_bracket_m = re.search(r'\s*\[([^\]]+)\]\s*$', title_c)
        if _tc_bracket_m:
            _bracket_content = _tc_bracket_m.group(1).strip()
            _keep_terms = set(cfg.get("title_cleanup", {}).get(
                "keep_parenthetical_if_contains", []))
            _is_music = (any(k in _bracket_content.lower() for k in _keep_terms)
                         if _keep_terms else False)
            if not _is_music and _bracket_content:
                title_c = title_c[:_tc_bracket_m.start()].strip() or title_c

    disc_prefix = ""
    if trc.get("disc", {}).get("enabled", True):
        disc_n = parse_int_prefix(disc_raw) if disc_raw else None
        if disc_n is None:
            disc_n = meta.get("fn_disc_n")
        use_disc = disc_multi or bool(meta.get("fn_disc_n"))
        if use_disc and disc_n:
            disc_prefix = trc.get("disc", {}).get("format", "{disc}-").format(disc=disc_n)

    if classification == "album":
        if track_n is None:
            return None, 0.0, "missing_track_number", {}
        tmpl = pat.get("album", "{disc_prefix}{track:02d} - {title}{mix_suffix}{ext}")
        return (sanitize_name(tmpl.format(
            disc_prefix=disc_prefix, track=int(track_n),
            title=title_c, mix_suffix=mix_suf, ext=ext)),
            0.95, "ok", meta)
    if classification == "various":
        if not artist:
            return None, 0.0, "missing_artist", meta
        if track_n is None:
            return None, 0.0, "missing_track_number", meta
        tmpl = pat.get("various",
                       "{disc_prefix}{track:02d} - {artist} - {title}{mix_suffix}{ext}")
        fname = tmpl.format(
            disc_prefix=disc_prefix, track=int(track_n),
            artist=artist, title=title_c, mix_suffix=mix_suf, ext=ext)
        return sanitize_name(fname), 0.92, "ok", meta
    if not artist:
        return None, 0.0, "missing_artist", meta
    if track_n is not None:
        tmpl = pat.get("various",
                       "{disc_prefix}{track:02d} - {artist} - {title}{mix_suffix}{ext}")
        fname = tmpl.format(
            disc_prefix=disc_prefix, track=int(track_n),
            artist=artist, title=title_c, mix_suffix=mix_suf, ext=ext)
        return sanitize_name(fname), 0.90, "ok", meta
    tmpl = pat.get("mixed", "{artist} - {title}{mix_suffix}{ext}")
    return (sanitize_name(tmpl.format(
        artist=artist, title=title_c, mix_suffix=mix_suf,
        ext=ext, disc_prefix="", track=0)),
        0.90, "ok", meta)
