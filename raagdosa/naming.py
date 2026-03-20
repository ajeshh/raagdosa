"""
RaagDosa naming — folder name pre-processor, title case, bracket/noise stripping,
garbage tag detection, normalization for voting, folder name heuristic parsing.

Layer 2: imports from tags (L1). No side effects, no terminal output.
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from raagdosa.tags import normalize_unicode


# ─────────────────────────────────────────────
# Cyrillic → Latin lookalike normalisation
# ─────────────────────────────────────────────
_CYRILLIC_MAP: Dict[str, str] = {
    "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "І": "I",
    "К": "K", "М": "M", "О": "O", "Р": "P", "Т": "T", "Х": "X",
    "а": "a", "е": "e", "і": "i", "о": "o", "р": "p", "с": "c",
    "х": "x", "у": "y",
}

_COUNTRY_CODES: Set[str] = {
    "AU", "UK", "US", "CA", "DE", "FR", "JP", "NL", "SE", "NO", "DK", "FI",
    "IT", "ES", "PT", "PL", "RU", "BR", "MX", "NZ", "ZA", "BE", "CH", "AT", "IE",
}

_ITUNES_GENRE_BUCKETS: Set[str] = {
    "Alternative", "Blues", "Children's Music", "Classical", "Comedy", "Country",
    "Dance", "Electronic", "Folk", "Hip-Hop/Rap", "Holiday", "Indie Pop",
    "Jazz", "Latin", "New Age", "Opera", "Pop", "R&B/Soul", "Reggae", "Religious",
    "Rock", "Singer/Songwriter", "Soundtracks", "Spoken Word", "Vocal", "World",
    "Ambient", "Bass", "Breaks", "Deep House", "Disco", "Drum & Bass", "Dub",
    "Dubstep", "Electro", "Funk", "Garage", "Grime", "Hard Techno", "House",
    "Industrial", "Jungle", "Minimal Techno", "Progressive House", "Psychedelic",
    "Reggaeton", "Rave", "Soul", "Techno", "Trance", "Trip Hop", "UK Garage",
    "Afrobeats", "Afro House", "Melodic House & Techno", "Organic Electronic",
    "Organic House", "Downtempo", "Glitch Hop", "IDM", "Lo-fi", "Experimental",
    "Noise", "Post-Rock", "Shoegaze", "Dream Pop", "Art Rock", "Avant-garde",
    "Contemporary Classical", "Electroacoustic", "Sound Art", "New Wave",
    "Punk", "Hardcore", "Metal", "Grunge", "Emo", "Post-Punk", "Gothic Rock",
    "Psychedelic Rock", "Stoner Rock", "Doom Metal", "Death Metal", "Black Metal",
    "Rap", "Trap", "Cloud Rap", "Drill", "UK Drill", "Afrorap",
    "Dancehall", "Dub Reggae", "Rocksteady", "Ska", "Cumbia", "Merengue", "Salsa",
    "Bossa Nova", "Samba", "Forro", "Baile Funk", "Afropop", "Highlife", "Afrojuju",
    "Amapiano", "Gqom",
}


# ─────────────────────────────────────────────
# Windows reserved filename sanitisation
# ─────────────────────────────────────────────
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
    "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
}


def sanitize_windows_reserved(name: str) -> str:
    """Append underscore if name (sans extension) is a Windows reserved word."""
    stem = Path(name).stem.upper()
    if stem in _WINDOWS_RESERVED:
        return name + "_"
    return name


def sanitize_name(name: str, repl: str = " - ", trim: bool = True) -> str:
    name = re.sub(r"[\/\\]", " ", name)
    name = re.sub(r'[\:\*\?\"\<\>\|]', repl, name)
    name = re.sub(r"\s+", " ", name).strip()
    return name.rstrip(". ").strip() if trim else name


def normalise_extension(path: Path) -> Optional[Path]:
    """Return new Path with lowercase extension if it needs renaming, else None."""
    if path.suffix and path.suffix != path.suffix.lower():
        return path.with_suffix(path.suffix.lower())
    return None


# ─────────────────────────────────────────────
# Bracket content classifier
# ─────────────────────────────────────────────
_BRACKET_YEAR = re.compile(r'^(19|20)\d{2}$')
_BRACKET_FORMAT = re.compile(
    r'\b(mp3|flac|aac|ogg|wav|aiff|320|256|192|128|lossless|hi.?res|24.?bit|'
    r'vinyl|vinyl.?rip|web|cd|dvd)\b', re.I)
_BRACKET_EDITION = re.compile(
    r'\b(deluxe|expanded|anniversary|remaster(ed)?|special|bonus|collector|'
    r'limited|explicit|censored|edition|version|complete|extended|import|original)\b', re.I)
_BRACKET_REMIX = re.compile(
    r'\b(remix|edit|mix|rework|bootleg|mashup|vip|flip|dub|club|radio|'
    r'instrumental|acapella|version)\b', re.I)
_BRACKET_PROMO = re.compile(
    r'www\.|\.com|\.net|\.org|free\s+download|promo\s+only|for\s+promo|'
    r'not\s+for\s+sale|leaked|rip|ripped|uploaded|download', re.I)
_BRACKET_NOISE = re.compile(
    r'^\s*(hd|hq|\d+k|\d+\s*hz|\d+\s*kbps|stereo|mono|clean|dirty)\s*$', re.I)


def classify_bracket(text: str) -> str:
    """Classify bracket contents: year|format|edition|remix|promo|noise|unknown."""
    t = text.strip()
    if _BRACKET_YEAR.match(t):
        return "year"
    if _BRACKET_PROMO.search(t):
        return "promo"
    if _BRACKET_FORMAT.search(t):
        return "format"
    if _BRACKET_EDITION.search(t):
        return "edition"
    if _BRACKET_REMIX.search(t):
        return "remix"
    if _BRACKET_NOISE.match(t):
        return "noise"
    return "unknown"


# ─────────────────────────────────────────────
# Mojibake detection
# ─────────────────────────────────────────────
_MOJIBAKE_CHARS = set('ÃÂ€„†‡ˆ‰Šš›œžŸ¡¢£¤¥¦§¨©ª«¬\xad®¯°±²³´µ¶·¸¹º»¼½¾¿')
_MOJIBAKE_SEQ = re.compile(r'Ã[\x80-\xff]|Â[\x80-\xff]', re.S)


def detect_mojibake(s: str) -> bool:
    """True if the string likely contains double-encoded or misencoded Unicode."""
    if not s:
        return False
    if _MOJIBAKE_SEQ.search(s):
        return True
    return sum(1 for c in s if c in _MOJIBAKE_CHARS) >= 3


# ─────────────────────────────────────────────
# Garbage naming detection
# ─────────────────────────────────────────────
_PROMO_WATERMARK = re.compile(
    r'www\.|\.com\b|\.net\b|\.org\b|free\s+download|promo\s+only|not\s+for\s+sale|'
    r'leaked|visit\s+us|check\s+out|follow\s+us|\bvk\.com\b|\bsoundcloud\.com\b', re.I)


def detect_garbage_name(name: str) -> List[str]:
    """Return list of garbage reasons (empty = clean). Does not modify the name."""
    reasons: List[str] = []
    groups = re.findall(r'[\[\(][^\[\]\(\)]*[\]\)]', name)
    if len(groups) >= 5:
        reasons.append("token_flood")
    if _PROMO_WATERMARK.search(name):
        reasons.append("promo_watermark")
    if detect_mojibake(name):
        reasons.append("mojibake")
    words = [w for w in name.split() if w.isalpha()]
    if len(words) >= 4 and all(w.isupper() for w in words) and len(name) > 60:
        reasons.append("all_caps_long")
    return reasons


def strip_bracket_stack(name: str, max_passes: int = 8) -> str:
    """Repeatedly strip trailing bracket groups classified as noise/promo."""
    result = name.strip()
    for _ in range(max_passes):
        m = re.search(r'\s*[\[\(]([^\[\]\(\)]*)[\]\)]\s*$', result)
        if not m:
            break
        cls = classify_bracket(m.group(1))
        if cls in ("noise", "promo", "format"):
            result = result[:m.start()].strip()
        else:
            break
    return result


# ─────────────────────────────────────────────
# Display name noise stripping
# ─────────────────────────────────────────────
_DISPLAY_NOISE_PATTERNS = [
    re.compile(r'\s*\[official\s+(?:audio|video|music\s+video)\]\s*$', re.I),
    re.compile(r'\s*\(official\s+(?:audio|video|music\s+video)\)\s*$', re.I),
    re.compile(r'\s*-\s*official\s+(?:audio|video|music\s+video)\s*$', re.I),
    re.compile(r'\s*\[(?:hd|hq|4k|1080p|720p)\]\s*$', re.I),
    re.compile(r'\s*\((?:lyrics?|lyric\s+video)\)\s*$', re.I),
    re.compile(r'\s*\[(?:lyrics?|lyric\s+video)\]\s*$', re.I),
    re.compile(r'\s*-\s*lyrics?\s*$', re.I),
]


def strip_display_noise(name: str) -> str:
    """Strip common display/upload noise suffixes from album/track names."""
    result = name
    for pat in _DISPLAY_NOISE_PATTERNS:
        result = pat.sub('', result).strip()
    return result


# ─────────────────────────────────────────────
# Disc indicator stripping from album names
# ─────────────────────────────────────────────
_DISC_INDICATOR = re.compile(r'\s*[-–—:,]\s*(?:disc|cd|disk)\s*\d+\s*$', re.I)
_DISC_INDICATOR2 = re.compile(r'\s*\((?:disc|cd|disk)\s*\d+\)\s*$', re.I)
_DISC_INDICATOR3 = re.compile(r'\s*\[(?:disc|cd|disk)\s*\d+\]\s*$', re.I)


def strip_disc_indicator(album_name: str) -> str:
    """Remove trailing disc/cd indicators for unified matching. Volume/Vol preserved."""
    s = album_name
    for pat in (_DISC_INDICATOR, _DISC_INDICATOR2, _DISC_INDICATOR3):
        s = pat.sub('', s).strip()
    return s


# ─────────────────────────────────────────────
# Title case
# ─────────────────────────────────────────────
def apply_title_case(s: str, cfg: Dict[str, Any]) -> str:
    """
    Apply intelligent title-casing with configurable exceptions.
    Handles: ALL CAPS, all lowercase, Every Word Capitalised (over-casing).
    Respects title_case.never_cap and title_case.always_cap lists from config.
    """
    if not s:
        return s
    tc = cfg.get("title_case", {})
    never: Set[str] = {w.lower() for w in (tc.get("never_cap", []) or [])}
    always: Set[str] = {w.lower() for w in (tc.get("always_cap", []) or [])}

    _default_small = {
        "a", "an", "the", "and", "but", "or", "for", "nor",
        "on", "at", "to", "by", "in", "of", "vs", "via", "feat", "feat.",
    }
    never = never | _default_small

    words = s.split()
    if not words:
        return s

    alpha_words = [w for w in words if any(c.isalpha() for c in w)]
    if not alpha_words:
        return s
    is_all_caps = all(w.isupper() for w in alpha_words if len(w) > 1)
    is_all_lower = all(w.islower() for w in alpha_words)

    if not (is_all_caps or is_all_lower):
        return s

    result = []
    for i, word in enumerate(words):
        core = re.sub(r'^[^\w]+|[^\w]+$', '', word)
        core_lower = core.lower()
        if core_lower in always:
            result.append(
                word.upper() if len(core) <= 3
                else word[0:word.index(core)] + core.upper() + word[word.index(core) + len(core):]
            )
        elif i == 0 or core_lower not in never:
            if core:
                idx = word.index(core)
                result.append(
                    word[:idx] + core[0].upper() + core[1:].lower() + word[idx + len(core):]
                )
            else:
                result.append(word)
        else:
            result.append(word.lower())
    return " ".join(result)


def smart_title_case(s: str, cfg: Optional[Dict[str, Any]] = None) -> str:
    """
    Intelligent title case — handles both ALL CAPS and all-lowercase inputs.
    Delegates to _smart_title_case_v43 for lowercase; handles ALL CAPS inline.
    """
    if not s:
        return s
    words = s.split()
    alpha_words = [w for w in words if any(c.isalpha() for c in w)]
    if not alpha_words:
        return s
    if all(w.isupper() for w in alpha_words if len(w) > 1):
        small = {"a", "an", "the", "and", "but", "or", "for", "nor",
                 "on", "at", "to", "by", "in", "of", "vs"}
        return " ".join(
            w.capitalize() if i == 0 or w.lower() not in small else w.lower()
            for i, w in enumerate(words)
        )
    if all(w.islower() for w in alpha_words):
        return _smart_title_case_v43(s, cfg)
    return s


def _smart_title_case_v43(s: str, cfg: Optional[Dict[str, Any]] = None) -> str:
    """v4.3 Smart Title Case — fires only on all-lowercase input."""
    if not s:
        return s
    alpha_words = [w for w in s.split() if any(c.isalpha() for c in w)]
    if not alpha_words or not all(w.islower() for w in alpha_words):
        return s

    tc = (cfg or {}).get("title_case", {})
    if not tc.get("auto_titlecase_lowercase_folders", True):
        return s

    _SMALL = {
        "a", "an", "the", "and", "but", "or", "for", "nor", "of", "in", "on", "at",
        "to", "by", "up", "as", "vs", "via", "feat", "feat.", "ft.", "b/w", "vs.",
    }
    never = _SMALL | {w.lower() for w in (tc.get("never_cap", []) or [])}
    _ALWAYS_UPPER = {"dj", "mc", "ep", "lp", "va", "uk", "us", "la", "nyc", "ny",
                     "ii", "iii", "iv", "vi"}
    always = _ALWAYS_UPPER | {w.lower() for w in (tc.get("always_cap", []) or [])}

    words = s.split()
    result = []
    for i, word in enumerate(words):
        m = re.match(r'^([^\w]*)(.+?)([^\w]*)$', word)
        if not m:
            result.append(word)
            continue
        lead, core, trail = m.group(1), m.group(2), m.group(3)
        core_lower = core.lower()
        is_first = (i == 0)
        is_last = (i == len(words) - 1)
        if core_lower in always:
            result.append(lead + core.upper() + trail)
        elif is_first or is_last or core_lower not in never:
            result.append(lead + core[0].upper() + core[1:] + trail)
        else:
            result.append(lead + core_lower + trail)
    return " ".join(result)


# ─────────────────────────────────────────────
# Label-as-albumartist detection
# ─────────────────────────────────────────────
def detect_label_as_albumartist(albumartist: str) -> bool:
    """True if the albumartist tag looks like a record label, not an artist name."""
    if not albumartist:
        return False
    return bool(re.search(
        r'\b(Records?|Discos?|Recordings?|Label|Music\s+Group|Inc\.?|Ltd\.?|Disques?)\b',
        albumartist, re.IGNORECASE))


# ─────────────────────────────────────────────
# v4.3 folder-name pre-processor regexes
# ─────────────────────────────────────────────
_SCENE_RELEASE_SUFFIX = re.compile(
    r'[\s_-]+(WEB|FLAC|MP3|CD|VINYL|DIGITAL|WEB-FLAC|WEB-MP3)'
    r'[-_]+(\d{4})[-_]+[A-Z0-9]{2,12}\s*$', re.IGNORECASE)

_FORMAT_BRACKET_TRAIL = re.compile(
    r'\s*\[\s*(FLAC|MP3|320|256|192|128|VBR|CBR|WEB|16Bit[^\]]*|24Bit[^\]]*'
    r'|lossless|hi.?res|vinyl.?rip)\s*\]\s*$', re.IGNORECASE)
_FORMAT_PAREN_TRAIL = re.compile(
    r'\s*\(\s*(FLAC|MP3|320|256|192|128|VBR|CBR|\d{2,3}\s*[Kk]bps)\s*\)\s*$',
    re.IGNORECASE)

_CATALOG_TAIL = re.compile(
    r'\s*[\(\[\{]\s*[A-Z]{1,6}\d{2,6}[A-Z]?\s*[\)\]\}]\s*$')

_CURLY_NOISE = re.compile(r'\s*\{[^}]{1,40}\}\s*')

_DUPLICATE_YEAR = re.compile(
    r'^((19|20)\d{2})(.*?)\(\s*\2\d{2}\s*\)\s*$')

_LABEL_4DASH = re.compile(
    r'^(?P<label>.+?)\s+-\s+(?P<year>(?:19|20)\d{2})\s+-\s+(?P<artist>.+?)\s+-\s+(?P<album>.+)$')

_DOUBLE_DASH = re.compile(r'--+')

_MID_PAREN_YEAR = re.compile(r'\s+-\s+\(((19|20)\d{2})\)\s+')

_MID_BRACKET_YEAR = re.compile(
    r'^(?P<pre>.+?)\s{1,2}\[(?P<year>(19|20)\d{2})\]\s*(?:[-\u2013]\s*)?(?P<post>.+)$')

_HASH_CHECKSUM_TAIL = re.compile(r'[-_][0-9a-f]{7,12}$', re.IGNORECASE)

_CD_BITRATE_SLUG = re.compile(
    r'\s+(?:cd|cdl?)\s+\d{2,3}(?:\s*kbps?)?\s+[a-z0-9]{2,8}\s*$', re.IGNORECASE)

_LABEL_BRACKET_TRAIL = re.compile(
    r'\s*\[(?:warp|ninja|brainfeeder|hyperdub|kranky|ghostly|erased\s*tapes|'
    r'zencd|strike|sol\s*selectas|!k7|anticon|mush|def\s*jux|big\s*dada)'
    r'[^\]]{0,30}\]\s*', re.IGNORECASE)

_TILDE_SEP = re.compile(r'\s*~\s*')

_TYPE_BRACKET_TRAIL = re.compile(
    r'\s*\[\s*(anthology|collection|box\s*set|compilation|bootleg|unreleased)\s*\]\s*$',
    re.IGNORECASE)


# ─────────────────────────────────────────────
# Pre-processor helper functions
# ─────────────────────────────────────────────
def _normalise_cyrillic_lookalikes(s: str) -> str:
    """Replace Cyrillic characters that look identical to Latin ones."""
    return "".join(_CYRILLIC_MAP.get(c, c) for c in s)


def _strip_catalog_prefix(name: str) -> str:
    """Strip catalog-ID-style prefix like 'ANJDEE786D Artist - Album'."""
    m = re.match(r"^([A-Z]{2,8}[0-9]{2,8}[A-Z0-9]*)\s+(.+)$", name)
    if m:
        code = m.group(1)
        if code in _COUNTRY_CODES:
            return name
        return m.group(2).strip()
    return name


def _strip_leading_bracket_catalog(name: str) -> str:
    """Strip a leading [CATALOG-CODE] that looks like a label code."""
    m = re.match(r"^\[([^\]]+)\]\s*(.*)$", name)
    if not m:
        return name
    code, rest = m.group(1).strip(), m.group(2).strip()
    if not rest:
        return name
    has_digits = bool(re.search(r"\d", code))
    is_word_only = bool(re.match(r"^[A-Za-z\s]+$", code)) and len(code.split()) <= 2
    if has_digits and not is_word_only:
        return rest
    if re.search(r"[A-Za-z]+\d+", code):
        return rest
    return name


def _strip_bang_delimiters(name: str) -> str:
    """Strip !!! … !!! from start and end, preserving internal !"""
    name = re.sub(r"^!{2,}\s*", "", name)
    name = re.sub(r"\s*!{2,}$", "", name)
    return name.strip()


def _strip_self_released(name: str) -> str:
    return re.sub(
        r"\s*\(self[- ]?released(?:\s+cd)?\)\s*", " ",
        name, flags=re.IGNORECASE,
    ).strip()


def _strip_mashup_keyword(name: str) -> Tuple[str, bool]:
    if re.search(r'\bmashup\s+album\b', name, re.IGNORECASE):
        cleaned = re.sub(r'\bmashup\s+album\b', '', name, flags=re.IGNORECASE).strip()
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        return cleaned, True
    return name, False


def _strip_beatport_compilation_noise(name: str) -> str:
    """Clean Beatport/Traxsource compilation folder names."""
    n = name
    if re.search(r"^Beatport\.[A-Za-z]", n):
        n = n.replace(".", " ")
    n = re.sub(r"^Best\s+New\s+(?:Hype\s+)?", "", n, flags=re.IGNORECASE)
    n = _strip_beatport_trailing_date(n)
    n = re.sub(r"  +", " ", n).strip()
    n = n.strip(". _").strip()
    return n


def _is_discography_folder(name: str) -> bool:
    return bool(re.search(
        r'\b(discography|complete\s+collection|complete\s+works|all\s+albums)\b',
        name, re.IGNORECASE))


def _strip_bitrate_noise(name: str) -> str:
    return re.sub(
        r'\s*[\(\[]\s*(?:\d+\s*kbps|mp3|flac|320|256|192|128|lossless)\s*[\)\]]\s*',
        ' ', name, flags=re.IGNORECASE,
    ).strip()


def _strip_beatport_trailing_date(name: str) -> str:
    _MONTHS = ["january", "february", "march", "april", "may", "june",
               "july", "august", "september", "october", "november", "december"]
    m = re.search(r"_\s+(" + "|".join(_MONTHS) + r")\s+(\d{4})\s*$", name, re.IGNORECASE)
    if m:
        return name[:m.start()].strip() + f" ({m.group(1).capitalize()} {m.group(2)})"
    m2 = re.search(r"_\s+(\d{4})[.\-](\d{2})\s*$", name)
    if m2:
        yr, mo = int(m2.group(1)), int(m2.group(2))
        if 1 <= mo <= 12:
            mname = ["January", "February", "March", "April", "May", "June",
                     "July", "August", "September", "October", "November", "December"][mo - 1]
            return name[:m2.start()].strip() + f" ({mname} {yr})"
    return name


def _strip_scene_release_suffix(name: str) -> Tuple[str, bool]:
    """Strip scene release group suffix. Returns (cleaned, was_stripped)."""
    if '_' in name and ' ' not in name:
        expanded = name.replace('_', ' ')
        m = _SCENE_RELEASE_SUFFIX.search(expanded)
        if m:
            return expanded[:m.start()].strip().rstrip('- '), True
    m = _SCENE_RELEASE_SUFFIX.search(name)
    if m:
        return name[:m.start()].strip().rstrip('-_ '), True
    return name, False


def _normalise_double_dash_slug(name: str) -> str:
    """Convert Artist--Album_Name → Artist - Album Name."""
    if '--' not in name:
        return name
    name = _DOUBLE_DASH.sub(' - ', name)
    name = name.replace('_', ' ')
    return re.sub(r'\s+', ' ', name).strip()


def _extract_mid_paren_year(name: str) -> Tuple[str, Optional[str]]:
    m = _MID_PAREN_YEAR.search(name)
    if m:
        year = m.group(1)
        cleaned = name[:m.start()] + ' - ' + name[m.end():]
        return re.sub(r'\s+', ' ', cleaned).strip(), year
    return name, None


def _extract_mid_bracket_year(name: str) -> Tuple[str, Optional[str]]:
    m = _MID_BRACKET_YEAR.match(name)
    if m:
        pre = m.group('pre').strip()
        year = m.group('year')
        post = m.group('post').strip()
        return f"{pre} - {post}", year
    return name, None


# ─────────────────────────────────────────────
# Main pre-processor
# ─────────────────────────────────────────────
def apply_folder_pre_processor(name: str, cfg: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    v4.3 folder name pre-processor (28-step pipeline).
    Returns (cleaned_name, metadata_dict) where metadata may contain:
      year, folder_type, extracted_label, noise_stripped flags.
    """
    meta: Dict[str, Any] = {}
    nc = cfg.get("title_cleanup", {})

    # ── v4.1 pipeline ──────────────────────────────────────────
    name = _normalise_cyrillic_lookalikes(name)
    name = re.sub(r"  +", " ", name).strip()

    if nc.get("url_decode", True):
        import urllib.parse
        name = urllib.parse.unquote(name)

    if name.startswith("._"):
        name = name[2:]

    if nc.get("strip_bang_delimiters", True):
        name = _strip_bang_delimiters(name)

    name = _strip_catalog_prefix(name)
    name = _strip_leading_bracket_catalog(name)
    name = _strip_self_released(name)

    name, is_mashup = _strip_mashup_keyword(name)
    if is_mashup:
        meta["folder_type"] = "mashup"

    name = _strip_beatport_compilation_noise(name)

    if _is_discography_folder(name):
        meta["folder_type"] = "discography"
        name = _strip_bitrate_noise(name)

    m = re.match(r"^(.+?)\s*-\s*((?:19|20)\d{2})\.\s*(.+)$", name)
    if m:
        meta["year"] = m.group(2)
        name = f"{m.group(1).strip()} - {m.group(3).strip()}"

    name = re.sub(r"\s+", " ", name).strip()

    # ── v4.3 pipeline ──────────────────────────────────────────
    name, was_scene = _strip_scene_release_suffix(name)
    if was_scene:
        meta["noise_scene_stripped"] = True

    if '--' in name:
        name = _normalise_double_dash_slug(name)

    name = _TILDE_SEP.sub(' - ', name)
    name = _CURLY_NOISE.sub(' ', name).strip()
    name = _LABEL_BRACKET_TRAIL.sub('', name).strip()
    name = _TYPE_BRACKET_TRAIL.sub('', name).strip()

    for _ in range(4):
        prev = name
        name = _FORMAT_BRACKET_TRAIL.sub('', name).strip()
        name = _FORMAT_PAREN_TRAIL.sub('', name).strip()
        if name == prev:
            break

    name = _CD_BITRATE_SLUG.sub('', name).strip()
    name = _CATALOG_TAIL.sub('', name).strip()

    dm = _DUPLICATE_YEAR.match(name)
    if dm:
        year_prefix = dm.group(1)
        rest = dm.group(3).strip().rstrip('-–').strip()
        if not meta.get("year"):
            meta["year"] = year_prefix
        name = f"{year_prefix} - {rest}" if '-' not in rest[:3] else f"{year_prefix}{rest}"
        name = re.sub(r"\s+", " ", name).strip()

    name, paren_year = _extract_mid_paren_year(name)
    if paren_year and not meta.get("year"):
        meta["year"] = paren_year

    name, bracket_year = _extract_mid_bracket_year(name)
    if bracket_year and not meta.get("year"):
        meta["year"] = bracket_year

    lm = _LABEL_4DASH.match(name)
    if lm:
        label = lm.group("label").strip()
        year = lm.group("year")
        artist = lm.group("artist").strip()
        album = lm.group("album").strip()
        known_labels = {lb.lower() for lb in (
            (cfg or {}).get("reference", {}).get("known_labels", []) or [])}
        label_is_label = (
            detect_label_as_albumartist(label) or label.lower() in known_labels
        )
        if label_is_label:
            meta["extracted_label"] = label
            if not meta.get("year"):
                meta["year"] = year
            name = f"{artist} - {year} - {album}"

    name = re.sub(r"\s+", " ", name).strip()
    name = name.rstrip("-– ").strip()
    name = re.sub(r'([a-z])(?:19|20)\d{2}\s*$', r'\1', name, flags=re.IGNORECASE).strip()
    name = name.rstrip("-– ").strip()
    name = re.sub(r"\s+", " ", name).strip()

    return name, meta


# ─────────────────────────────────────────────
# Garbage tag value detection
# ─────────────────────────────────────────────
_TLDS = ("com|net|org|info|biz|co|io|me|fm|tv|cc|us|uk|de|fr|es|it|nl|ru|br|mx"
         "|in|jp|cn|au|ca|ch|se|no|fi|dk|pl|cz")

_GARBAGE_URL_RE = re.compile(
    r"(^https?://)"
    r"|(^www\.)"
    rf"|(\b[\w\-]+\.({_TLDS})\b)",
    re.IGNORECASE
)

_MUSICAL_KEY_GARBAGE = frozenset({
    "a", "ab", "am", "b", "bb", "bbm", "bm", "c", "cb", "cm", "d", "db", "dm", "dbm",
    "e", "eb", "ebm", "em", "f", "fb", "fm", "g", "gb", "gbm", "gm",
    "abm", "c#", "c#m", "d#", "d#m", "f#", "f#m", "g#", "g#m", "a#", "a#m",
    "smg",
})


def is_garbage_tag_value(val: str) -> bool:
    """Return True if a tag value looks like a URL/domain or musical key — not a real artist/album."""
    if not val:
        return False
    v = val.strip()
    if _GARBAGE_URL_RE.search(v):
        return True
    if v.lower() in _MUSICAL_KEY_GARBAGE and len(v) <= 4:
        return True
    return False


def strip_trailing_domains(s: str) -> str:
    """Strip trailing URLs/domains from a name string."""
    s = re.sub(
        rf"(\s*[\(\[]\s*(https?:\/\/)?(www\.)?[\w\-]+(\.[\w\-]+)+\.({_TLDS})\s*[\)\]]\s*)$",
        "", s.strip(), flags=re.IGNORECASE)
    s = re.sub(
        rf"(\s*[-–—]\s*)?(https?:\/\/)?(www\.)?[\w\-]+(\.[\w\-]+)+\.({_TLDS})\s*$",
        "", s, flags=re.IGNORECASE)
    return re.sub(r"(\s*[-–—]\s*)?www\s*$", "", s, flags=re.IGNORECASE).strip()


# ─────────────────────────────────────────────
# Vote normalisation
# ─────────────────────────────────────────────
def normalize_for_vote(s: str, cfg: Dict[str, Any]) -> str:
    """Normalize a tag value for voting (lowercase, strip brackets, punctuation, etc.)."""
    nc = cfg.get("normalize", {})
    o = (s or "").strip()
    if nc.get("lower_case", True):
        o = o.lower()
    if nc.get("strip_bracketed_phrases_for_voting", True):
        o = re.sub(r"[\(\[].*?[\)\]]", "", o)
    if nc.get("strip_punctuation_for_voting", True):
        o = re.sub(r"[^\w\s]", " ", o)
    if nc.get("collapse_whitespace", True):
        o = re.sub(r"\s+", " ", o).strip()
    for suf in nc.get("strip_common_suffixes_for_voting", []) or []:
        if o.endswith(suf.lower()):
            o = o[:-len(suf)].strip()
    return o


# ─────────────────────────────────────────────
# Folder name heuristic parser
# ─────────────────────────────────────────────
def parse_folder_name_heuristic(folder_name: str,
                                cfg: Optional[Dict[str, Any]] = None,
                                ) -> Dict[str, Optional[str]]:
    """Parse a folder name into artist/album/year using heuristics."""
    result: Dict[str, Optional[str]] = {"artist": None, "album": None, "year": None}
    if cfg is None:
        cfg = {}

    cleaned, meta = apply_folder_pre_processor(folder_name, cfg)
    if meta.get("year"):
        result["year"] = meta["year"]

    cleaned = smart_title_case(cleaned, cfg)

    name = normalize_unicode(cleaned.strip())
    name = re.sub(r"\s*\[[A-Z0-9\.\s]+\]\s*$", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"_-_", " - ", name).replace("_", " ")
    name = re.sub(r"\s+", " ", name).strip()
    ym = re.search(r"\b(19\d{2}|20\d{2})\b", name)
    if ym and not result["year"]:
        result["year"] = ym.group(1)
    name_ny = re.sub(r"[\(\[]\s*(19\d{2}|20\d{2})\s*[\)\]]", "", name).strip()
    name_ny = re.sub(r"\b(19\d{2}|20\d{2})\b", "", name_ny).strip()
    name_ny = re.sub(r"\s+", " ", name_ny).strip().rstrip("-–").strip()
    m = re.match(r"^[\[\(]?(19\d{2}|20\d{2})[\]\)]?\s*[-–]\s*(.+?)\s*[-–]\s*(.+)$", name)
    if m:
        if not result["year"]:
            result["year"] = m.group(1)
        result["artist"] = smart_title_case(m.group(2).strip())
        result["album"] = smart_title_case(
            re.sub(r"\s*[\(\[](19\d{2}|20\d{2})[\)\]]\s*$", "", m.group(3)).strip())
        return result
    m2 = re.match(r"^(.+?)\s*[-–]\s*(.+)$", name_ny)
    if m2:
        result["artist"] = smart_title_case(m2.group(1).strip())
        result["album"] = smart_title_case(m2.group(2).strip().rstrip("-– "))
        return result
    if name_ny:
        result["album"] = smart_title_case(name_ny)
    return result
