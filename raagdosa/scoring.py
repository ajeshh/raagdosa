"""
RaagDosa scoring — 7-factor confidence score computation.

Layer 2: imports from core (L0), naming (L2 peer — only detect_garbage_name, detect_mojibake).
No side effects, no terminal output.
"""
from __future__ import annotations

import re
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from raagdosa.naming import detect_garbage_name, detect_mojibake


# ─────────────────────────────────────────────
# Helper utilities
# ─────────────────────────────────────────────
def parse_int_prefix(s: str) -> Optional[int]:
    m = re.match(r"^\s*(\d+)", s or "")
    return int(m.group(1)) if m else None


def string_similarity(a: str, b: str) -> float:
    a = re.sub(r"\s+", " ", a.strip().lower())
    b = re.sub(r"\s+", " ", b.strip().lower())
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    aset = set(a.split())
    bset = set(b.split())
    j = len(aset & bset) / max(1, len(aset | bset))
    pref = sum(1 for ca, cb in zip(a, b) if ca == cb)
    return max(j, pref / max(len(a), len(b)))


def compute_dominant(counter: Counter) -> Tuple[Optional[str], float, int]:
    if not counter:
        return None, 0.0, 0
    total = sum(counter.values())
    key, cnt = counter.most_common(1)[0]
    return key, (cnt / total if total else 0.0), cnt


def recover_display(norm_key: Optional[str],
                    raw_by_norm: Dict[str, Counter]) -> Optional[str]:
    if not norm_key:
        return None
    rc = raw_by_norm.get(norm_key)
    return rc.most_common(1)[0][0] if rc else norm_key


# ─────────────────────────────────────────────
# Track analysis
# ─────────────────────────────────────────────
_VINYL_RE = re.compile(r'^([A-Da-d])(\d{1,2})$')


def parse_vinyl_track(s: str) -> Optional[Tuple[str, int, int]]:
    """Parse vinyl side-track notation like A1, B2. Returns (side, num, absolute) or None."""
    m = _VINYL_RE.match((s or "").strip())
    if not m:
        return None
    side = m.group(1).upper()
    n = int(m.group(2))
    absolute = (ord(side) - ord('A')) * 8 + n
    return side, n, absolute


def detect_track_gaps(track_nums: List[int]) -> List[int]:
    """Return list of missing track numbers in the sequence."""
    if len(track_nums) < 2:
        return []
    s = sorted(set(track_nums))
    gaps = []
    for i in range(s[0], s[-1] + 1):
        if i not in set(s):
            gaps.append(i)
    return gaps


def detect_duplicate_track_numbers(track_nums: List[int]) -> List[int]:
    """Return list of track numbers that appear more than once."""
    c = Counter(track_nums)
    return [n for n, cnt in c.items() if cnt > 1]


# ─────────────────────────────────────────────
# Title quality
# ─────────────────────────────────────────────
def compute_meaningful_title_ratio(titles: List[str]) -> float:
    """Fraction of titles that look like real titles vs garbage/missing."""
    if not titles:
        return 0.5
    meaningful = 0
    for t in titles:
        if not t or len(t.strip()) < 2:
            continue
        g = detect_garbage_name(t)
        if not g and not detect_mojibake(t):
            meaningful += 1
    ratio = meaningful / len(titles)
    unique_titles = set(t.strip().lower() for t in titles if t and t.strip())
    if len(unique_titles) == 1 and len(titles) > 2:
        ratio = min(ratio, 0.30)
    return ratio


# ─────────────────────────────────────────────
# Filename-tag consistency
# ─────────────────────────────────────────────
_HASH_CHECKSUM_TAIL = re.compile(r'[-_][0-9a-f]{7,12}$', re.IGNORECASE)


def _parse_fn_artitle(stem: str) -> Tuple[Optional[str], Optional[str]]:
    """Quick filename stem → (artist, title) for consistency scoring."""
    s = re.sub(r'_-_', ' - ', stem).replace('_', ' ')
    s = _HASH_CHECKSUM_TAIL.sub('', s).strip()
    s = re.sub(r'^\d{1,3}/\d{1,3}\s*[-–—.]\s*', '', s)
    s = re.sub(r'^\d{1,3}\s*\.?\s*[-–—]?\s*', '', s)
    parts = [p.strip() for p in re.split(r'\s*[-–—]\s*', s) if p.strip()]

    if len(parts) == 1:
        return None, parts[0]
    if len(parts) == 2:
        return parts[0], parts[1]
    if len(parts) >= 3:
        if re.match(r'^\d{1,3}$', parts[2]) and len(parts) >= 4:
            return parts[0], parts[3]
        return parts[0], parts[-1]
    return None, parts[0] if parts else None


def compute_filename_tag_consistency(
    audio_files: List[Path],
    all_tags: List[Dict[str, Optional[str]]],
) -> float:
    """Score 0–1 measuring how well filename stems agree with tag content."""
    if not audio_files or not all_tags:
        return 0.5
    scores: List[float] = []
    for f, tags in zip(audio_files, all_tags):
        fn_artist, fn_title = _parse_fn_artitle(f.stem)
        tag_title = (tags.get("title") or "").strip()
        if fn_title and tag_title:
            scores.append(string_similarity(fn_title, tag_title))
        elif not fn_title and not tag_title:
            scores.append(0.5)
    return sum(scores) / len(scores) if scores else 0.5


# ─────────────────────────────────────────────
# Albumartist consistency
# ─────────────────────────────────────────────
def compute_albumartist_consistency(
    all_tags: List[Dict[str, Optional[str]]],
) -> float:
    """Fraction of tagged tracks sharing the dominant albumartist."""
    tagged = [t for t in all_tags if t.get("albumartist")]
    if not tagged:
        return 0.5
    c = Counter(t["albumartist"].strip().lower() for t in tagged)
    _, dom_share, _ = compute_dominant(c)
    return dom_share


# ─────────────────────────────────────────────
# Folder alignment
# ─────────────────────────────────────────────
def _tokenise_for_alignment(name: str, noise_tokens: Optional[Set[str]] = None) -> List[str]:
    """Tokenise a folder/proposed name for alignment comparison."""
    tokens = re.split(r'[^a-z0-9]+', name.lower())
    tokens = [t for t in tokens if t]
    cutoff = len(tokens)
    for i, t in enumerate(tokens):
        if re.fullmatch(r'(?:19|20)\d{2}', t):
            cutoff = i + 1
            break
    tokens = tokens[:cutoff]
    default_noise = {
        "web", "webflac", "webrip", "flac", "mp3", "aac", "ogg", "opus", "wav", "aiff",
        "cd", "cdda", "vinyl", "dvd", "lp", "ep", "320", "256", "192", "128", "v0", "v2", "vbr",
        "proper", "repack", "retail", "promo", "advance", "limited", "reissue", "remaster",
        "remastered", "deluxe", "expanded", "anniversary", "nfo", "sfv", "readnfo",
        "dirfix", "nfofix", "kbps", "khz", "lossless", "hq",
    }
    effective_noise = (noise_tokens or set()) | default_noise
    return [t for t in tokens if t not in effective_noise and len(t) > 1]


def compute_folder_alignment_bonus(
    folder_name: str,
    proposed_name: str,
    cfg: Optional[Dict[str, Any]] = None,
) -> float:
    """
    v2: Token-coverage alignment.
    0.0 = no overlap, 1.0 = all proposed tokens found in folder.
    """
    fn = unicodedata.normalize("NFC", folder_name.strip().lower())
    pn = unicodedata.normalize("NFC", proposed_name.strip().lower())
    if fn == pn:
        return 1.0

    noise_cfg: Set[str] = set()
    if cfg is not None:
        extra = [t.lower() for t in
                 cfg.get("reference", {}).get("folder_alignment_noise_tokens", [])
                 if isinstance(t, str)]
        noise_cfg = set(extra)

    folder_toks = set(_tokenise_for_alignment(folder_name, noise_cfg))
    proposed_toks = _tokenise_for_alignment(proposed_name, noise_cfg)

    if not proposed_toks:
        return 0.5

    def _coverage(p_toks: List[str]) -> float:
        if not p_toks:
            return 0.5
        hits = sum(1 for t in p_toks if t in folder_toks)
        return hits / len(p_toks)

    coverage = _coverage(proposed_toks)

    if " - " in proposed_name:
        _, rest = proposed_name.split(" - ", 1)
        rest_toks = _tokenise_for_alignment(rest, noise_cfg)
        rest_cov = _coverage(rest_toks)
        coverage = max(coverage, rest_cov)

    if not folder_toks:
        return 0.5

    return coverage


# ─────────────────────────────────────────────
# Main confidence computation
# ─────────────────────────────────────────────
def compute_confidence_factors(
    audio_files: List[Path],
    tagged: int,
    alb_share: float,
    aa_share: float,
    art_share: float,
    used_heuristic: bool,
    folder_name: str,
    proposed_name: str,
    all_tags: List[Dict[str, Optional[str]]],
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, float]:
    """
    Compute named confidence factors.
    Returns a dict of factor_name → value (0–1 each).
    """
    total = len(audio_files)
    factors: Dict[str, float] = {}

    factors["tag_coverage"] = tagged / total if total else 0.0

    if aa_share > 0:
        factors["dominance"] = alb_share * 0.60 + aa_share * 0.40
    else:
        factors["dominance"] = alb_share * 0.70 + art_share * 0.30

    titles = [t.get("title", "") for t in all_tags if t.get("title")]
    factors["title_quality"] = compute_meaningful_title_ratio(titles) if titles else 0.5

    factors["filename_consistency"] = compute_filename_tag_consistency(audio_files, all_tags)

    track_nums: List[int] = []
    for t in all_tags:
        raw = t.get("tracknumber") or ""
        vt = parse_vinyl_track(raw.split("/")[0].strip())
        n = vt[2] if vt else parse_int_prefix(raw)
        if n:
            track_nums.append(n)
    gaps = detect_track_gaps(track_nums)
    dupes = detect_duplicate_track_numbers(track_nums)
    gap_pen = min(0.30, len(gaps) * 0.06)
    dup_pen = min(0.20, len(dupes) * 0.07)
    factors["completeness"] = max(0.0, 1.0 - gap_pen - dup_pen)
    factors["track_gaps"] = len(gaps)
    factors["track_dupes"] = len(dupes)

    factors["aa_consistency"] = compute_albumartist_consistency(all_tags)

    factors["folder_alignment"] = compute_folder_alignment_bonus(
        folder_name, proposed_name, cfg if cfg is not None else {})

    return factors


def confidence_from_factors(factors: Dict[str, float], used_heuristic: bool) -> float:
    """Compute a single 0–1 confidence score from the factor dict."""
    weights = {
        "dominance": 0.40,
        "tag_coverage": 0.15,
        "title_quality": 0.12,
        "filename_consistency": 0.07,
        "completeness": 0.12,
        "aa_consistency": 0.06,
        "folder_alignment": 0.08,
    }
    score = sum(factors.get(k, 0.5) * w for k, w in weights.items())
    if used_heuristic:
        score *= 0.60
    return min(1.0, max(0.0, score))
