#!/usr/bin/env python3
"""
RaagDosa Tags — shared tag reading, normalization, detection, and scoring.

Used by both the main raagdosa.py CLI and the raagdosa_scanner.py training tool.
This module has NO config file dependency and NO database dependency.
"""
from __future__ import annotations

import dataclasses
import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from mutagen import File as MutagenFile
except Exception:
    MutagenFile = None


# ─────────────────────────────────────────────────────────────────
# Constants — Tag key mapping
# ─────────────────────────────────────────────────────────────────

# Default tag key priority lists (mutagen easy-tag keys)
TAG_KEY_MAP = {
    "title":        ["title", "TIT2"],
    "artist":       ["artist", "TPE1"],
    "album_artist": ["albumartist", "album_artist", "album artist", "TPE2"],
    "album":        ["album", "TALB"],
    "year":         ["originaldate", "original_date", "date", "year", "TDRC", "TYER"],
    "track_number": ["tracknumber", "track", "TRCK"],
    "disc_number":  ["discnumber", "disc", "TPOS"],
    "genre":        ["genre", "TCON"],
    "bpm":          ["bpm", "tbpm", "TBPM"],
    "key":          ["initialkey", "key", "tkey", "TKEY"],
    "label":        ["organization", "label", "publisher", "TPUB"],
    "comment":      ["comment", "COMM", "COMM::eng"],
    "isrc":         ["isrc", "TSRC"],
    "encoder":      ["encodedby", "encoder", "TENC", "TSSE"],
    "compilation":  ["compilation", "TCMP"],
    "grouping":     ["grouping", "TIT1", "contentgroup"],
    "remixer":      ["remixer", "TPE4"],
    "composer":     ["composer", "TCOM"],
    "catalog_number": ["catalognumber", "TXXX:CATALOGNUMBER"],
}

AUDIO_EXTENSIONS = frozenset({
    ".mp3", ".flac", ".m4a", ".aac", ".ogg", ".opus",
    ".wav", ".aiff", ".aif", ".wma", ".alac",
})


# ─────────────────────────────────────────────────────────────────
# Noise detection patterns
# ─────────────────────────────────────────────────────────────────

NOISE_PATTERNS: Dict[str, List[Tuple[str, str, float]]] = {
    "title": [
        (r'\s*\[Official\s*(?:Audio|Video|Music\s*Video)\]', "youtube", 0.99),
        (r'\s*\((?:Official\s*)?(?:Audio|Video|Lyric\s*Video)\)', "youtube", 0.99),
        (r'\s*\[(?:FREE?\s*(?:DL|DOWNLOAD|D/L))\]', "promo", 0.98),
        (r'\s*\((?:FREE?\s*(?:DL|DOWNLOAD|D/L))\)', "promo", 0.98),
        (r'\s*[-–]\s*(?:FREE?\s*(?:DL|DOWNLOAD))', "promo", 0.97),
        (r'\s*(?:OUT NOW|BUY (?:NOW|LINK))', "promo", 0.97),
        (r'\s*\[(?:FLAC|MP3|WAV|AIFF|320(?:kbps)?|HQ|HD)\]', "format", 0.98),
        (r'\s*\((?:FLAC|MP3|WAV|AIFF|320(?:kbps)?|HQ|HD)\)', "format", 0.98),
        (r'\s*\[(?:hd|hq|4k|1080p|720p)\]', "quality", 0.97),
        (r'\s*\((?:lyrics?|lyric\s+video)\)', "youtube", 0.97),
        (r'\s*\[(?:lyrics?|lyric\s+video)\]', "youtube", 0.97),
        (r'\s*-\s*lyrics?\s*$', "youtube", 0.95),
    ],
    "artist": [
        (r'\s*\d{2,3}\s*[Bb][Pp][Mm]', "bpm_in_artist", 0.95),
        (r'\s*\b\d{1,2}[AaBb]\b', "key_in_artist", 0.85),
        (r'\s*\[(?:FLAC|MP3|320)\]', "format_in_artist", 0.98),
    ],
    "comment": [
        (r'(?i)(?:bpm\s*supreme|dj\s*city|zip\s*dj|djcity|club\s*killers)', "pool_tag", 0.95),
        (r'(?i)(?:digital\s*dj\s*pool|late\s*night\s*record\s*pool)', "pool_tag", 0.95),
        (r'(?i)for\s+promotional\s+use\s+only', "promo", 0.98),
        (r'(?i)not\s+for\s+(?:re)?sale', "promo", 0.97),
        (r'(?i)purchased?\s+(?:at|from|on)\s+', "purchase", 0.90),
        (r'(?i)downloaded?\s+(?:from|via)\s+', "purchase", 0.90),
        (r'https?://\S+', "url", 0.85),
        (r'www\.\S+', "url", 0.85),
        (r'[\w.+-]+@[\w-]+\.[\w.-]+', "email", 0.95),
        (r'(?i)(?:©|\(c\))\s*\d{4}', "copyright", 0.80),
        (r'(?i)(?:encoded|ripped|converted)\s+(?:by|with|using)\s+', "encoder", 0.90),
        (r'(?i)LAME\s*[\d.]+', "encoder", 0.95),
    ],
    "album": [
        (r'\s*\[(?:FLAC|MP3|WAV|320)\]', "format_in_album", 0.98),
        (r'\s*\((?:FLAC|MP3|WAV|320)\)', "format_in_album", 0.98),
    ],
}


# ─────────────────────────────────────────────────────────────────
# Key / BPM extraction patterns
# ─────────────────────────────────────────────────────────────────

BPM_PATTERNS = [
    (r'(\d{2,3})\s*[Bb][Pp][Mm]', 0.95),
    (r'[Bb][Pp][Mm]\s*[:=]?\s*(\d{2,3})', 0.92),
    (r'\b(\d{2,3})\s*bpm\b', 0.90),
]

KEY_PATTERNS = [
    (r'\b(\d{1,2}[AaBb])\b', "camelot", 0.95),
    (r'(?<!\w)[Kk]ey\s*[:=]?\s*([A-G][#b]?m?)(?!\w)', "musical_explicit", 0.90),
    (r'\b([A-G][#b]?\s*(?:min(?:or)?|maj(?:or)?))\b', "musical_full", 0.88),
    (r'(?<![A-Za-z])([A-G][#b]?m)(?![A-Za-z])', "musical_short", 0.60),
]

KEY_PREFIX_PATTERN = re.compile(r'^(\d{1,2}[AaBb])\s*[-–—]\s*(.+)')


# ─────────────────────────────────────────────────────────────────
# Artist / title patterns
# ─────────────────────────────────────────────────────────────────

FEAT_PATTERN = re.compile(
    r'\s*[\(\[]?\s*(?:feat\.?|ft\.?|featuring|with)\s+.*$',
    re.IGNORECASE)

VS_PATTERN = re.compile(
    r'\s+(?:vs\.?|versus|x)\s+',
    re.IGNORECASE)

ORIGINAL_MIX_PATTERN = re.compile(
    r'\s*[-–—]\s*(?:Original(?:\s+Mix)?)\s*$'
    r'|\s*\(Original(?:\s+Mix)?\)\s*$',
    re.IGNORECASE)

FILENAME_NOISE_PATTERNS = [
    (re.compile(r'(?:www\.[\w.-]+\.\w{2,4})'), "url_in_filename", 0.95),
    (re.compile(r'(?:https?://\S+)'), "url_in_filename", 0.95),
    (re.compile(r'youtube_[A-Za-z0-9_-]{11}_audio'), "youtube_download", 0.98),
    (re.compile(r'\[_?YouConvert\.net_?\]'), "converter_tag", 0.98),
    (re.compile(r'\[_?(?:yt1s|savefrom|y2mate|mp3juices).*?\]', re.I), "converter_tag", 0.97),
]

SINGLES_KEYWORDS = re.compile(
    r'(?i)^(?:singles?|loose|tracks|songs|dubplates?|cuts)$')


# ─────────────────────────────────────────────────────────────────
# Mojibake detection
# ─────────────────────────────────────────────────────────────────

MOJIBAKE_MAP = {
    'Ã¶': 'ö', 'Ã¤': 'ä', 'Ã¼': 'ü', 'Ã©': 'é',
    'Ã\xad': 'í', 'Ã±': 'ñ', 'Ã³': 'ó', 'Ã¡': 'á',
    'Ã ': 'à', 'Ã¢': 'â', 'Ã§': 'ç', 'Ã¨': 'è',
    'Ã®': 'î', 'Ã´': 'ô', 'Ã¹': 'ù', 'Ã»': 'û',
    '\u00e2\u0080\u0099': '\u2019', '\u00e2\u0080\u009c': '\u201c',
    '\u00e2\u0080\u009d': '\u201d',
    '\u00e2\u0080\u0094': '—', '\u00e2\u0080\u0093': '–',
    '\u00e2\u0080\u00a2': '•',
}


# ─────────────────────────────────────────────────────────────────
# Risk tiers
# ─────────────────────────────────────────────────────────────────

RISK_TIERS = {
    "noise_removal":        "safe",
    "whitespace":           "safe",
    "comment_cleanup":      "safe",
    "original_mix_strip":   "safe",
    "feat_normalize":       "safe",
    "filename_noise_flag":  "safe",
    "artist_mismatch_flag": "safe",
    "fill_album_artist":    "moderate",
    "bpm_extraction":       "moderate",
    "bpm_cleanup":          "moderate",
    "key_extraction":       "moderate",
    "key_prefix_strip":     "moderate",
    "genre_normalize":      "moderate",
    "fill_from_folder":     "moderate",
    "artist_normalize":     "destructive",
    "encoding_repair":      "destructive",
    "id3_upgrade":          "destructive",
}

RISK_THRESHOLDS = {
    "safe":        0.85,
    "moderate":    0.92,
    "destructive": 0.98,
}


# ─────────────────────────────────────────────────────────────────
# Genre / organizational word detection
# ─────────────────────────────────────────────────────────────────

_GENRE_AND_ORG_WORDS = {
    "electronic", "electronica", "house", "techno", "trance", "ambient",
    "bass", "dubstep", "drum and bass", "drum & bass", "dnb", "d&b",
    "breaks", "breakbeat", "hip-hop", "hip hop", "rap", "jazz", "soul",
    "funk", "rock", "indie", "pop", "classical", "reggae", "dub",
    "world", "latin", "afrobeat", "folk", "metal", "punk", "country",
    "r&b", "rnb", "blues", "gospel", "ska", "dancehall", "garage",
    "jungle", "hardcore", "gabber", "psytrance", "goa", "downtempo",
    "chillout", "lounge", "trip-hop", "trip hop", "glitch", "idm",
    "experimental", "noise", "industrial", "synthwave", "vaporwave",
    "lo-fi", "lofi", "soundtrack", "score", "opera",
    "deep", "liquid", "dark", "minimal", "acid", "progressive",
    "melodic", "organic", "soulful", "funky", "nu", "hard", "soft",
    "singles", "loose", "tracks", "songs", "misc", "various", "va",
    "compilation", "compilations", "mixes", "mix", "remixes", "edits",
    "bootlegs", "mashups", "promos", "promo", "complete", "discography",
    "collection", "best of", "greatest hits",
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
    "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
    "unsorted", "to sort", "downloads", "incoming", "new", "old",
    "archive", "backup", "temp", "dump", "inbox", "todo",
}

_MUSIC_ACRONYMS = {"DJ", "MC", "VA", "XRS", "MF", "DMX", "BPM", "RZA", "GZA",
                   "ODB", "RJD2", "MPC", "KRS", "DMC", "FX", "AK", "JB"}


# ─────────────────────────────────────────────────────────────────
# Folder intelligence constants
# ─────────────────────────────────────────────────────────────────

STRUCTURE_TYPES = {
    "artist_album":   "Artist/Album — structured, high trust",
    "compilation":    "VA compilation — multiple artists, one album",
    "catchall":       "Unsorted catchall — flat dump, low trust",
    "singles":        "Loose singles — mixed artists, no album coherence",
    "discography":    "Artist discography container",
    "dj_crate":       "DJ crate/set prep folder",
    "unknown":        "Insufficient data to classify",
}

CATCHALL_KEYWORDS = re.compile(
    r'(?i)\b(?:unsorted|to\s*sort|downloads?|incoming|misc|dump|random|temp|'
    r'new\s*music|promos?|edits|tools|untitled|unfiled|inbox|to\s*(?:tag|fix|review))\b')

CRATE_KEYWORDS = re.compile(
    r'(?i)\b(?:set\s*prep|gig|live|festival|club|opening|closing|warm\s*up|'
    r'peak\s*hour|chill|deep|bangers|classics|favourites?|favorites?|rotation)\b')

FOLDER_PARSE_PATTERNS = [
    (re.compile(r'^(.+?)\s*[-–—]\s*(.+?)\s*[\(\[](\d{4})[\)\]]$'),
     ("artist", "album", "year")),
    (re.compile(r'^(.+?)\s*[-–—]\s*(.+?)$'),
     ("artist", "album")),
    (re.compile(r'^(.+?)\s*[\(\[](\d{4})[\)\]]$'),
     ("album", "year")),
]


# ─────────────────────────────────────────────────────────────────
# Tag reading
# ─────────────────────────────────────────────────────────────────

def normalize_unicode(s: str) -> str:
    """Normalize unicode to NFC form."""
    return unicodedata.normalize("NFC", s or "")


def mutagen_first(tag_obj: Any, keys: List[str]) -> Optional[str]:
    """Try keys in order on a mutagen tag object, return first match as string."""
    if not tag_obj:
        return None
    for k in keys:
        if k in tag_obj:
            v = tag_obj.get(k)
            if isinstance(v, list):
                v = v[0] if v else None
            if v is not None:
                return str(v)
    return None


def read_tags(file_path: Path,
              tag_key_map: Optional[Dict[str, List[str]]] = None
              ) -> Dict[str, Optional[str]]:
    """Read all tag fields from an audio file using mutagen.

    Args:
        file_path: Path to audio file
        tag_key_map: Optional override for tag key mapping. Uses TAG_KEY_MAP if None.

    Returns:
        Dict of field_name → value (only non-empty fields included)
    """
    if MutagenFile is None:
        return {}
    try:
        mf = MutagenFile(str(file_path), easy=True)
    except Exception:
        return {}
    if mf is None or mf.tags is None:
        return {}

    key_map = tag_key_map or TAG_KEY_MAP
    result = {}
    for field_name, keys in key_map.items():
        val = mutagen_first(mf.tags, keys)
        if val is not None:
            val = val.strip()
            if val:
                result[field_name] = val

    return result


def write_tag(file_path: Path, field: str, value: str,
              tag_key_map: Optional[Dict[str, List[str]]] = None) -> bool:
    """Write a single tag field to an audio file.

    Returns True on success, False on failure.
    """
    if MutagenFile is None:
        return False
    try:
        mf = MutagenFile(str(file_path), easy=True)
        if mf is None:
            return False
        if mf.tags is None:
            mf.add_tags()
        key_map = tag_key_map or TAG_KEY_MAP
        keys = key_map.get(field, [field])
        # Write to the first key
        mf.tags[keys[0]] = [value]
        mf.save()
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────
# Detection functions
# ─────────────────────────────────────────────────────────────────

def detect_noise(field_name: str, value: str) -> List[Tuple[str, str, str, float]]:
    """Detect noise patterns in a tag value.
    Returns list of (category, pattern, matched_text, confidence)."""
    findings = []
    patterns = NOISE_PATTERNS.get(field_name, [])
    for pat_str, category, conf in patterns:
        m = re.search(pat_str, value, re.I)
        if m:
            findings.append((category, pat_str, m.group(0).strip(), conf))
    return findings


def clean_noise(field_name: str, value: str) -> str:
    """Remove detected noise patterns from a tag value. Returns cleaned string."""
    cleaned = value
    for category, pat_str, matched, conf in detect_noise(field_name, value):
        cleaned = cleaned.replace(matched, "").strip()
    cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
    return cleaned if cleaned != value else value


def detect_mojibake(value: str) -> Optional[Tuple[str, float]]:
    """Detect and attempt to repair mojibake.
    Returns (repaired_value, confidence) or None."""
    repaired = value
    found = False
    for bad, good in MOJIBAKE_MAP.items():
        if bad in repaired:
            repaired = repaired.replace(bad, good)
            found = True
    if found:
        return (repaired, 0.92)
    try:
        test = value.encode("latin-1").decode("utf-8")
        if test != value and not any(c in test for c in ['\ufffd', '\x00']):
            return (test, 0.85)
    except (UnicodeDecodeError, UnicodeEncodeError):
        pass
    return None


def extract_bpm_from_value(value: str) -> Optional[Tuple[int, float, str]]:
    """Extract BPM from a non-BPM field. Returns (bpm, confidence, matched_text)."""
    for pat_str, conf in BPM_PATTERNS:
        m = re.search(pat_str, value, re.I)
        if m:
            bpm = int(m.group(1))
            if 50 <= bpm <= 220:
                return (bpm, conf, m.group(0))
    return None


def extract_key_from_value(value: str) -> Optional[Tuple[str, str, float, str]]:
    """Extract musical key from a non-key field.
    Returns (key_value, notation_type, confidence, matched_text)."""
    for pat_str, notation, conf in KEY_PATTERNS:
        m = re.search(pat_str, value)
        if m:
            key_val = m.group(1).strip()
            if notation == "camelot":
                num = int(re.match(r'(\d+)', key_val).group(1))
                if not (1 <= num <= 12):
                    continue
            return (key_val, notation, conf, m.group(0))
    return None


def detect_key_prefix_in_artist(artist: str) -> Optional[Tuple[str, str]]:
    """Detect if artist tag is a Camelot key prefix like '10B - Artist Name'.
    Returns (key, real_artist) or None."""
    m = KEY_PREFIX_PATTERN.match(artist.strip())
    if m:
        key_part = m.group(1).upper()
        rest = m.group(2).strip()
        num = int(re.match(r'(\d+)', key_part).group(1))
        letter = key_part[-1].upper()
        if 1 <= num <= 12 and letter in ('A', 'B') and rest:
            return (key_part, rest)
    return None


def strip_feat_from_artist(artist: str) -> str:
    """Strip featuring suffix from artist to get primary artist for album_artist."""
    return FEAT_PATTERN.sub('', artist).strip()


def detect_filename_noise(filename: str) -> List[Tuple[str, str, float]]:
    """Detect noise patterns embedded in filenames.
    Returns list of (category, matched_text, confidence)."""
    stem = Path(filename).stem
    results = []
    for pattern, category, conf in FILENAME_NOISE_PATTERNS:
        m = pattern.search(stem)
        if m:
            results.append((category, m.group(0), conf))
    return results


# ─────────────────────────────────────────────────────────────────
# Artist normalization
# ─────────────────────────────────────────────────────────────────

def normalize_artist(name: str) -> str:
    """Normalize artist name for comparison (lowercase, connectors unified)."""
    s = unicodedata.normalize("NFC", name)
    s = s.lower().strip()
    s = re.sub(r'\s*[&+×]\s*|\s+(?:and|vs\.?|versus|feat\.?|featuring|ft\.?)\s+',
               ' & ', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s)
    return s


def score_artist_spelling(name: str) -> float:
    """Score how 'correct' an artist spelling looks. Higher = better.

    Evaluates casing quality, connector style, and formatting.
    Used to pick the best variant when multiple spellings exist.
    """
    score = 0.0
    words = name.split()
    if not words:
        return 0.0
    if name == name.lower():
        score -= 3.0
    if name == name.upper() and len(words) > 1:
        score -= 2.0
    title_words = 0
    for w in words:
        upper_w = w.upper()
        if upper_w in _MUSIC_ACRONYMS:
            if w == upper_w:
                score += 1.5
            elif w[0].isupper() and not w[1:].isupper():
                score -= 0.5
        elif w[0].isupper():
            title_words += 1
        elif w.lower() in ("of", "the", "in", "on", "at", "to", "for", "and", "vs"):
            score += 0.1
    if words and title_words / len(words) >= 0.5:
        score += 1.0
    if " & " in name:
        score += 0.5
    if re.search(r'\band\b', name, re.I):
        score -= 0.3
    if " And " in name:
        score -= 0.5
    return score


def compute_tag_completeness(tags: Dict[str, Optional[str]]) -> float:
    """Score 0.0-1.0 based on how many important fields are present."""
    weights = {
        "artist": 0.20, "title": 0.20, "album": 0.15,
        "album_artist": 0.10, "year": 0.10, "genre": 0.08,
        "bpm": 0.07, "key": 0.05, "label": 0.03, "isrc": 0.02,
    }
    score = 0.0
    for field, weight in weights.items():
        if tags.get(field):
            score += weight
    return round(score, 3)


# ─────────────────────────────────────────────────────────────────
# Genre / org word detection
# ─────────────────────────────────────────────────────────────────

def looks_like_genre_or_org(name: str) -> bool:
    """Check if a folder-parsed 'artist' is actually a genre or organizational word."""
    normalized = name.strip().lower()
    if normalized in _GENRE_AND_ORG_WORDS:
        return True
    words = re.split(r'[\s&/+,]+', normalized)
    if all(w in _GENRE_AND_ORG_WORDS or len(w) <= 1 for w in words if w):
        return True
    return False


# ─────────────────────────────────────────────────────────────────
# Folder intelligence
# ─────────────────────────────────────────────────────────────────

@dataclasses.dataclass
class FolderContext:
    """Result of folder-level analysis. Passed to per-file proposal generation."""
    folder_path: str
    folder_name: str
    structure_type: str
    structure_confidence: float
    context_score: float
    file_count: int
    parsed_artist: Optional[str] = None
    parsed_album: Optional[str] = None
    parsed_year: Optional[int] = None
    dominant_artist: Optional[str] = None
    dominant_artist_share: float = 0.0
    dominant_album: Optional[str] = None
    dominant_album_share: float = 0.0
    artist_agreement: float = 0.0
    album_agreement: float = 0.0
    inferred_artist: Optional[str] = None
    inferred_album: Optional[str] = None
    inferred_year: Optional[int] = None
    is_va: bool = False
    is_catchall: bool = False
    is_crate: bool = False
    has_track_numbers: bool = False
    track_numbers_sequential: bool = False
    tag_completeness_avg: float = 0.0
    tag_completeness_min: float = 0.0
    evidence: List[str] = dataclasses.field(default_factory=list)


def analyze_folder_context(folder_path: str,
                           file_tags: List[Dict[str, Optional[str]]]) -> FolderContext:
    """Analyze a folder's files to determine structure type and context score.

    This runs BEFORE per-file proposal generation and feeds context into it.
    Returns a FolderContext that influences proposal confidence.
    """
    folder_name = Path(folder_path).name
    ctx = FolderContext(
        folder_path=folder_path,
        folder_name=folder_name,
        structure_type="unknown",
        structure_confidence=0.0,
        context_score=0.5,
        file_count=len(file_tags),
    )

    if not file_tags:
        return ctx

    evidence = []

    # ── 1. Parse folder name
    for pattern, fields in FOLDER_PARSE_PATTERNS:
        m = pattern.match(folder_name)
        if m:
            groups = m.groups()
            for i, field in enumerate(fields):
                val = groups[i].strip()
                if field == "artist":
                    ctx.parsed_artist = val
                elif field == "album":
                    ctx.parsed_album = val
                elif field == "year":
                    try:
                        y = int(val)
                        if 1900 <= y <= 2100:
                            ctx.parsed_year = y
                    except ValueError:
                        pass
            break

    if ctx.parsed_artist:
        evidence.append(f"folder name parsed: artist='{ctx.parsed_artist}'")
    if ctx.parsed_album:
        evidence.append(f"folder name parsed: album='{ctx.parsed_album}'")

    # ── 2. Cross-file tag voting
    artists: Counter = Counter()
    albums: Counter = Counter()
    years: Counter = Counter()
    completeness_scores = []
    track_nums = []

    for tags in file_tags:
        if tags.get("artist"):
            artists[normalize_artist(tags["artist"])] += 1
        if tags.get("album"):
            albums[tags["album"].lower().strip()] += 1
        if tags.get("year"):
            ym = re.search(r'\d{4}', tags["year"])
            if ym:
                years[int(ym.group())] += 1
        tn = tags.get("track_number")
        if tn:
            m = re.match(r'(\d+)', tn)
            if m:
                track_nums.append(int(m.group(1)))
        completeness_scores.append(compute_tag_completeness(tags))

    n = len(file_tags)

    if artists:
        top_artist, top_count = artists.most_common(1)[0]
        ctx.dominant_artist = top_artist
        ctx.dominant_artist_share = top_count / n
        ctx.artist_agreement = top_count / n
    if albums:
        top_album, top_count = albums.most_common(1)[0]
        ctx.dominant_album = top_album
        ctx.dominant_album_share = top_count / n
        ctx.album_agreement = top_count / n
    if years:
        top_year, _ = years.most_common(1)[0]
        ctx.inferred_year = top_year

    ctx.tag_completeness_avg = sum(completeness_scores) / n if completeness_scores else 0
    ctx.tag_completeness_min = min(completeness_scores) if completeness_scores else 0

    # Track number analysis
    if track_nums:
        ctx.has_track_numbers = True
        sorted_nums = sorted(track_nums)
        expected = list(range(sorted_nums[0], sorted_nums[0] + len(sorted_nums)))
        ctx.track_numbers_sequential = (sorted_nums == expected)
        if not ctx.track_numbers_sequential:
            evidence.append(f"track numbers not sequential: {sorted_nums}")

    # ── 3. VA / catchall / crate detection
    if len(artists) > 1 and ctx.dominant_artist_share < 0.60:
        ctx.is_va = True
        evidence.append(f"VA detected: {len(artists)} artists, "
                        f"dominant share {ctx.dominant_artist_share:.0%}")

    if CATCHALL_KEYWORDS.search(folder_name):
        ctx.is_catchall = True
        evidence.append(f"catchall keyword in folder name: '{folder_name}'")
    elif (ctx.tag_completeness_avg < 0.20 and len(artists) > 3
          and ctx.dominant_artist_share < 0.40 and ctx.dominant_album_share < 0.30):
        ctx.is_catchall = True
        evidence.append("catchall inferred: low completeness + no dominant artist/album")

    if CRATE_KEYWORDS.search(folder_name):
        ctx.is_crate = True
        evidence.append(f"crate keyword in folder name: '{folder_name}'")

    # ── 4. Structure classification
    if ctx.is_catchall:
        ctx.structure_type = "catchall"
        ctx.structure_confidence = 0.85
    elif ctx.is_crate:
        ctx.structure_type = "dj_crate"
        ctx.structure_confidence = 0.80
    elif (ctx.parsed_artist and ctx.dominant_artist
          and normalize_artist(ctx.parsed_artist) == ctx.dominant_artist
          and ctx.artist_agreement >= 0.70):
        ctx.structure_type = "artist_album"
        ctx.structure_confidence = 0.90
        evidence.append(f"folder artist '{ctx.parsed_artist}' matches tag vote "
                        f"({ctx.artist_agreement:.0%} agreement)")
    elif (ctx.parsed_artist and ctx.dominant_artist
          and normalize_artist(ctx.parsed_artist) == ctx.dominant_artist
          and ctx.artist_agreement >= 0.50):
        ctx.structure_type = "artist_album"
        ctx.structure_confidence = 0.70
        evidence.append(f"folder artist '{ctx.parsed_artist}' matches tag vote "
                        f"({ctx.artist_agreement:.0%} agreement, relaxed threshold)")
    elif ctx.parsed_artist and ctx.dominant_artist_share >= 0.70:
        ctx.structure_type = "artist_album"
        ctx.structure_confidence = 0.75
        evidence.append(f"strong artist agreement ({ctx.dominant_artist_share:.0%}) "
                        f"without folder name confirmation")
    elif (ctx.parsed_artist and ctx.parsed_album
          and len(ctx.parsed_artist) > 2
          and not looks_like_genre_or_org(ctx.parsed_artist)
          and not looks_like_genre_or_org(ctx.parsed_album)
          and len(artists) > 0):
        ctx.structure_type = "artist_album"
        ctx.structure_confidence = 0.55
        evidence.append(f"folder name parses as artist/album: "
                        f"'{ctx.parsed_artist}' - '{ctx.parsed_album}' "
                        f"(no tag match, {len(artists)} distinct artists in tags)")
    elif ctx.is_va and ctx.album_agreement >= 0.60:
        ctx.structure_type = "compilation"
        ctx.structure_confidence = 0.80
        evidence.append(f"VA with album agreement {ctx.album_agreement:.0%}")
    elif SINGLES_KEYWORDS.match(folder_name):
        ctx.structure_type = "singles"
        ctx.structure_confidence = 0.85
        evidence.append(f"folder name '{folder_name}' is a known singles keyword")
    elif len(artists) > 3 and ctx.dominant_album_share < 0.40:
        ctx.structure_type = "singles"
        ctx.structure_confidence = 0.70
        evidence.append("many artists, no dominant album → singles collection")
    else:
        ctx.structure_type = "unknown"
        ctx.structure_confidence = 0.40

    # ── 5. Infer best artist/album from all signals
    if ctx.dominant_artist_share >= 0.70 and ctx.dominant_artist:
        ctx.inferred_artist = ctx.dominant_artist
        evidence.append(f"inferred artist from tag vote: '{ctx.dominant_artist}' "
                        f"({ctx.dominant_artist_share:.0%})")
    elif ctx.parsed_artist:
        ctx.inferred_artist = ctx.parsed_artist
        evidence.append(f"inferred artist from folder name: '{ctx.parsed_artist}'")

    if ctx.dominant_album_share >= 0.60 and ctx.dominant_album:
        ctx.inferred_album = ctx.dominant_album
    elif ctx.parsed_album:
        ctx.inferred_album = ctx.parsed_album

    # ── 6. Context score (folder trustworthiness)
    score = 0.5  # neutral baseline

    structure_bonus = {
        "artist_album": 0.30, "compilation": 0.20, "dj_crate": 0.10,
        "singles": 0.0, "catchall": -0.20, "discography": 0.15,
        "unknown": -0.10,
    }
    score += structure_bonus.get(ctx.structure_type, 0) * ctx.structure_confidence
    score += ctx.tag_completeness_avg * 0.15

    if ctx.artist_agreement >= 0.80:
        score += 0.10
    elif ctx.artist_agreement >= 0.50:
        score += 0.05

    if ctx.has_track_numbers and ctx.track_numbers_sequential:
        score += 0.05
    elif ctx.has_track_numbers and not ctx.track_numbers_sequential:
        score -= 0.05

    ctx.context_score = round(max(0.1, min(1.0, score)), 3)
    ctx.evidence = evidence

    return ctx


def apply_folder_context(proposals: List[Dict[str, Any]],
                         ctx: FolderContext,
                         tags: Dict[str, Optional[str]],
                         filename: str) -> List[Dict[str, Any]]:
    """Enhance per-file proposals with folder context.

    Adjusts confidence based on folder context score and adds folder-level proposals.
    """
    enhanced = []

    for prop in proposals:
        risk = RISK_TIERS.get(prop["fix_type"], "moderate")
        if risk == "safe":
            context_weight = 0.15
        elif risk == "moderate":
            context_weight = 0.35
        else:
            context_weight = 0.50

        original_conf = prop["confidence"]
        adjusted_conf = (original_conf * (1 - context_weight)
                         + original_conf * ctx.context_score * context_weight)
        prop["confidence"] = round(adjusted_conf, 4)

        if ctx.context_score < 0.4:
            prop["reason"] += f" [low-trust folder: {ctx.structure_type}]"
        elif ctx.context_score > 0.7:
            prop["reason"] += f" [high-trust folder: {ctx.structure_type}]"

        enhanced.append(prop)

    # ── Folder-derived proposals ──────────────────────────────

    # Fill missing artist from folder context
    if (not tags.get("artist") and ctx.inferred_artist
            and len(ctx.inferred_artist.strip()) > 2
            and not looks_like_genre_or_org(ctx.inferred_artist)):
        if ctx.structure_type == "artist_album" and ctx.artist_agreement >= 0.80:
            conf = max(0.45, 0.85 * ctx.context_score)
            reason = (f"artist empty; inferred '{ctx.inferred_artist}' from folder "
                      f"({ctx.structure_type}, {ctx.artist_agreement:.0%} sibling agreement)")
        elif ctx.parsed_artist:
            conf = max(0.35, 0.60 * ctx.context_score)
            reason = (f"artist empty; inferred '{ctx.inferred_artist}' from folder name parse "
                      f"(no tag confirmation)")
        else:
            conf = max(0.25, 0.40 * ctx.context_score)
            reason = f"artist empty; weak inference '{ctx.inferred_artist}' from folder context"

        enhanced.append({
            "field_name": "artist", "old_value": None,
            "new_value": ctx.inferred_artist, "fix_type": "fill_from_folder",
            "confidence": round(conf, 4), "reason": reason,
        })

    # Fill missing album from folder context
    if not tags.get("album") and ctx.inferred_album:
        if ctx.album_agreement >= 0.60:
            conf = max(0.40, 0.75 * ctx.context_score)
            reason = (f"album empty; inferred '{ctx.inferred_album}' from folder "
                      f"({ctx.album_agreement:.0%} sibling agreement)")
        elif ctx.parsed_album:
            conf = max(0.35, 0.55 * ctx.context_score)
            reason = f"album empty; inferred '{ctx.inferred_album}' from folder name"
        else:
            conf = max(0.25, 0.35 * ctx.context_score)
            reason = f"album empty; weak inference from folder context"

        enhanced.append({
            "field_name": "album", "old_value": None,
            "new_value": ctx.inferred_album, "fix_type": "fill_from_folder",
            "confidence": round(conf, 4), "reason": reason,
        })

    # Fill missing year from folder context
    if not tags.get("year") and ctx.inferred_year:
        conf = max(0.35, 0.65 * ctx.context_score)
        enhanced.append({
            "field_name": "year", "old_value": None,
            "new_value": str(ctx.inferred_year), "fix_type": "fill_from_folder",
            "confidence": round(conf, 4),
            "reason": f"year empty; inferred {ctx.inferred_year} from folder context",
        })

    # Flag: file artist disagrees with folder dominant artist
    file_artist = tags.get("artist")
    if (file_artist and ctx.inferred_artist
            and ctx.structure_type == "artist_album"
            and normalize_artist(file_artist) != normalize_artist(ctx.inferred_artist)
            and ctx.artist_agreement >= 0.80):
        enhanced.append({
            "field_name": "artist",
            "old_value": file_artist,
            "new_value": file_artist,
            "fix_type": "artist_mismatch_flag",
            "confidence": 0.0,
            "reason": (f"artist '{file_artist}' differs from folder dominant "
                       f"'{ctx.inferred_artist}' ({ctx.artist_agreement:.0%})"),
        })

    return enhanced
