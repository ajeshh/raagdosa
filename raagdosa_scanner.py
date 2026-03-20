#!/usr/bin/env python3
"""
RaagDosa Tag Scanner — standalone training tool for learning tag patterns.

Scans music folders, logs every tag field, detects noise/anomalies,
proposes fixes with explanations, and builds a learning database.

NOT part of the main raagdosa.py pipeline.
Communicates with RaagDosa via YAML export only.

Usage:
  python raagdosa_scanner.py scan /path/to/music
  python raagdosa_scanner.py report
  python raagdosa_scanner.py proposals [--folder PATH]
  python raagdosa_scanner.py patterns
  python raagdosa_scanner.py export --output findings.yaml
  python raagdosa_scanner.py history
  python raagdosa_scanner.py undo <scan_id>
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import sys
import textwrap
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    from mutagen import File as MutagenFile
except ImportError:
    print("ERROR: mutagen is required. Install: pip install mutagen", file=sys.stderr)
    sys.exit(1)

try:
    import yaml
except ImportError:
    yaml = None

# ─────────────────────────────────────────────────────────────────
# Shared modules
# ─────────────────────────────────────────────────────────────────
from raagdosa_tags import (
    # Constants
    AUDIO_EXTENSIONS, TAG_KEY_MAP, NOISE_PATTERNS, BPM_PATTERNS,
    KEY_PATTERNS, KEY_PREFIX_PATTERN, FEAT_PATTERN, VS_PATTERN,
    ORIGINAL_MIX_PATTERN, FILENAME_NOISE_PATTERNS, SINGLES_KEYWORDS,
    MOJIBAKE_MAP, RISK_TIERS, RISK_THRESHOLDS,
    STRUCTURE_TYPES, CATCHALL_KEYWORDS, CRATE_KEYWORDS,
    FOLDER_PARSE_PATTERNS,
    # Functions
    normalize_unicode, mutagen_first, read_tags, detect_noise,
    clean_noise, detect_mojibake, extract_bpm_from_value,
    extract_key_from_value, detect_key_prefix_in_artist,
    strip_feat_from_artist, detect_filename_noise,
    normalize_artist, score_artist_spelling, compute_tag_completeness,
    looks_like_genre_or_org,
    # Folder intelligence
    FolderContext, analyze_folder_context, apply_folder_context,
)

# ─────────────────────────────────────────────────────────────────
# Scanner-only constants
# ─────────────────────────────────────────────────────────────────
VERSION = "2.1.0"

# Default log directory (alongside the scanner DB)
SCANNER_LOGS_DIR = Path("scanner_logs")

# Minimum reviews required before a rule can graduate to auto-apply
GRADUATION_THRESHOLDS = {
    "safe":        20,
    "moderate":    50,
    "destructive": 100,
}

# Source fingerprinting signals
SOURCE_SIGNALS = {
    "beatport":   {"comment": [r'(?i)beatport', r'(?i)purchased?\s+at\s+beatport'],
                   "encoder": [], "filename": [r'^\d{7,}_'], "txxx": ["BEATPORT_ID"]},
    "bandcamp":   {"comment": [r'(?i)bandcamp'],
                   "encoder": [], "filename": [r'^[\w\s]+ - [\w\s]+\.flac$'], "txxx": ["BANDCAMP"]},
    "itunes":     {"comment": [r'(?i)itunes'],
                   "encoder": [r'(?i)iTunes\s'], "filename": [r'^\d{2}\s'], "txxx": []},
    "djcity":     {"comment": [r'(?i)dj\s*city'],
                   "encoder": [], "filename": [r'(?i)\b(?:clean|dirty|intro|short)\b'], "txxx": []},
    "bpm_supreme":{"comment": [r'(?i)bpm\s*supreme'],
                   "encoder": [], "filename": [], "txxx": []},
    "soundcloud": {"comment": [r'(?i)soundcloud'],
                   "encoder": [r'(?i)yt-?dlp', r'(?i)youtube-?dl'],
                   "filename": [r'[\w-]{11,}'], "txxx": []},
}

# ─────────────────────────────────────────────────────────────────
# Database
# ─────────────────────────────────────────────────────────────────
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS scans (
    scan_id     TEXT PRIMARY KEY,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    root_path   TEXT NOT NULL,
    file_count  INTEGER DEFAULT 0,
    folder_count INTEGER DEFAULT 0,
    label       TEXT,
    status      TEXT DEFAULT 'running'
);

CREATE TABLE IF NOT EXISTS files (
    file_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id     TEXT NOT NULL REFERENCES scans(scan_id),
    file_path   TEXT NOT NULL,
    folder_path TEXT NOT NULL,
    filename    TEXT NOT NULL,
    extension   TEXT NOT NULL,
    file_size   INTEGER,
    duration_sec REAL,
    bitrate     INTEGER,
    sample_rate INTEGER,
    channels    INTEGER,
    container_type TEXT,
    id3_version TEXT,
    has_id3v1   INTEGER DEFAULT 0,
    has_id3v2   INTEGER DEFAULT 0,
    has_ape     INTEGER DEFAULT 0,
    has_artwork INTEGER DEFAULT 0,
    encoding    TEXT,
    UNIQUE(scan_id, file_path)
);

CREATE TABLE IF NOT EXISTS tags (
    tag_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id     INTEGER NOT NULL REFERENCES files(file_id),
    field_name  TEXT NOT NULL,
    raw_value   TEXT NOT NULL,
    value_length INTEGER,
    is_empty    INTEGER DEFAULT 0,
    UNIQUE(file_id, field_name)
);

CREATE TABLE IF NOT EXISTS noise_findings (
    finding_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id     INTEGER NOT NULL REFERENCES files(file_id),
    field_name  TEXT NOT NULL,
    noise_category TEXT NOT NULL,
    matched_pattern TEXT NOT NULL,
    matched_text TEXT NOT NULL,
    confidence  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS proposals (
    proposal_id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id     INTEGER NOT NULL REFERENCES files(file_id),
    scan_id     TEXT NOT NULL,
    field_name  TEXT NOT NULL,
    old_value   TEXT,
    new_value   TEXT NOT NULL,
    fix_type    TEXT NOT NULL,
    confidence  REAL NOT NULL,
    reason      TEXT NOT NULL,
    status      TEXT DEFAULT 'pending',
    applied_at  TEXT,
    undone_at   TEXT,
    UNIQUE(file_id, field_name, fix_type)
);

CREATE TABLE IF NOT EXISTS artist_variants (
    variant_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id     TEXT NOT NULL,
    raw_name    TEXT NOT NULL,
    normalized  TEXT NOT NULL,
    source_field TEXT NOT NULL,
    occurrences INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS dupe_signals (
    signal_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id_a   INTEGER NOT NULL,
    file_id_b   INTEGER NOT NULL,
    signal_type TEXT NOT NULL,
    signal_value TEXT,
    similarity  REAL,
    CHECK(file_id_a < file_id_b)
);

CREATE TABLE IF NOT EXISTS source_guesses (
    guess_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id     INTEGER NOT NULL REFERENCES files(file_id),
    source      TEXT NOT NULL,
    confidence  REAL NOT NULL,
    evidence    TEXT
);

CREATE TABLE IF NOT EXISTS folder_summaries (
    summary_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_id     TEXT NOT NULL,
    folder_path TEXT NOT NULL,
    file_count  INTEGER,
    artist_count INTEGER,
    dominant_artist TEXT,
    dominant_album TEXT,
    tag_completeness REAL,
    format_mix  TEXT,
    is_va       INTEGER DEFAULT 0,
    year_spread INTEGER DEFAULT 0,
    -- Folder intelligence (v2.0)
    structure_type TEXT,            -- artist_album, compilation, catchall, singles, etc.
    structure_confidence REAL,
    context_score REAL,             -- 0.0-1.0 folder trustworthiness
    parsed_artist TEXT,
    parsed_album TEXT,
    parsed_year INTEGER,
    inferred_artist TEXT,
    inferred_album TEXT,
    is_catchall INTEGER DEFAULT 0,
    is_crate INTEGER DEFAULT 0,
    evidence TEXT,                  -- JSON array of evidence strings
    UNIQUE(scan_id, folder_path)
);

-- Reviews: user feedback on proposals (accept/reject/skip)
CREATE TABLE IF NOT EXISTS reviews (
    review_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    proposal_id INTEGER NOT NULL REFERENCES proposals(proposal_id),
    verdict     TEXT NOT NULL CHECK(verdict IN ('accept', 'reject', 'skip')),
    reviewed_at TEXT NOT NULL,
    source_label TEXT,   -- source profile active when reviewed (e.g. "beatport")
    notes       TEXT     -- optional user comment on why
);

-- Learned model: aggregated hit rates per rule, per source
CREATE TABLE IF NOT EXISTS model_rules (
    rule_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    fix_type        TEXT NOT NULL,
    source_label    TEXT DEFAULT '_baseline_',  -- '_baseline_' = global, else source-specific
    risk_tier       TEXT NOT NULL,
    total_reviewed  INTEGER DEFAULT 0,
    total_accepted  INTEGER DEFAULT 0,
    total_rejected  INTEGER DEFAULT 0,
    hit_rate        REAL DEFAULT 0.0,           -- accepted / (accepted + rejected)
    learned_confidence REAL,                    -- replaces hardcoded confidence when graduated
    graduated       INTEGER DEFAULT 0,          -- 1 = enough data to auto-apply
    last_trained_at TEXT,
    UNIQUE(fix_type, source_label)
);

-- Source profiles: named groupings of scan labels
CREATE TABLE IF NOT EXISTS source_profiles (
    profile_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TEXT NOT NULL,
    scan_labels TEXT     -- comma-separated list of scan labels that map to this profile
);

CREATE INDEX IF NOT EXISTS idx_files_scan ON files(scan_id);
CREATE INDEX IF NOT EXISTS idx_files_folder ON files(folder_path);
CREATE INDEX IF NOT EXISTS idx_tags_file ON tags(file_id);
CREATE INDEX IF NOT EXISTS idx_tags_field ON tags(field_name);
CREATE INDEX IF NOT EXISTS idx_noise_file ON noise_findings(file_id);
CREATE INDEX IF NOT EXISTS idx_proposals_scan ON proposals(scan_id);
CREATE INDEX IF NOT EXISTS idx_proposals_status ON proposals(status);
CREATE INDEX IF NOT EXISTS idx_proposals_type ON proposals(fix_type);
CREATE INDEX IF NOT EXISTS idx_artist_scan ON artist_variants(scan_id);
CREATE INDEX IF NOT EXISTS idx_reviews_proposal ON reviews(proposal_id);
CREATE INDEX IF NOT EXISTS idx_reviews_source ON reviews(source_label);
CREATE INDEX IF NOT EXISTS idx_model_rules_type ON model_rules(fix_type);
CREATE INDEX IF NOT EXISTS idx_model_rules_source ON model_rules(source_label);
"""


# ─────────────────────────────────────────────────────────────────
# Session logging — write output to files like the main app
# ─────────────────────────────────────────────────────────────────
class TeeOutput:
    """Context manager that tees stdout to a log file."""

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_file = None
        self.original_stdout = None

    def __enter__(self):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = open(self.log_path, "w", encoding="utf-8")
        self.original_stdout = sys.stdout
        sys.stdout = self  # redirect stdout to self
        return self

    def __exit__(self, *args):
        sys.stdout = self.original_stdout
        if self.log_file:
            self.log_file.close()

    def write(self, text):
        self.original_stdout.write(text)
        self.log_file.write(text)

    def flush(self):
        self.original_stdout.flush()
        self.log_file.flush()


def get_scan_log_path(logs_dir: Path, scan_id: str, label: Optional[str] = None,
                      suffix: str = "") -> Path:
    """Generate a log file path for a scan session."""
    timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    label_part = f"_{label}" if label else ""
    suffix_part = f"_{suffix}" if suffix else ""
    return logs_dir / f"{timestamp}_{scan_id}{label_part}{suffix_part}.log"


class ScannerDB:
    """SQLite database for the tag scanner."""

    def __init__(self, db_path: str = "raagdosa_scanner.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA_SQL)
        self.conn.commit()

    def execute(self, sql: str, params=()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def executemany(self, sql: str, params_list) -> sqlite3.Cursor:
        return self.conn.executemany(sql, params_list)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()


# ─────────────────────────────────────────────────────────────────
# Audio file info (scanner-only — not tag reading)
# ─────────────────────────────────────────────────────────────────
def read_file_info(file_path: Path) -> Dict[str, Any]:
    """Read audio metadata (duration, bitrate, etc.) from a file."""
    info: Dict[str, Any] = {
        "file_size": file_path.stat().st_size,
        "duration_sec": None,
        "bitrate": None,
        "sample_rate": None,
        "channels": None,
        "container_type": None,
        "id3_version": None,
        "has_id3v1": 0,
        "has_id3v2": 0,
        "has_ape": 0,
        "has_artwork": 0,
        "encoding": None,
    }

    try:
        mf = MutagenFile(str(file_path))
    except Exception:
        return info

    if mf is None:
        return info

    # Audio info
    if hasattr(mf, "info") and mf.info:
        info["duration_sec"] = getattr(mf.info, "length", None)
        info["bitrate"] = getattr(mf.info, "bitrate", None)
        if info["bitrate"]:
            info["bitrate"] = int(info["bitrate"] / 1000)  # bps → kbps
        info["sample_rate"] = getattr(mf.info, "sample_rate", None)
        info["channels"] = getattr(mf.info, "channels", None)

    # Container type detection
    type_name = type(mf).__name__
    info["container_type"] = type_name

    # ID3 version detection (MP3 files)
    tags = mf.tags
    if tags:
        tag_type = type(tags).__name__
        if "ID3" in tag_type:
            info["has_id3v2"] = 1
            ver = getattr(tags, "version", None)
            if ver:
                info["id3_version"] = f"{ver[0]}.{ver[1]}"
        if "APE" in tag_type:
            info["has_ape"] = 1

        # Check for artwork
        for key in tags:
            if isinstance(key, str) and key.startswith("APIC"):
                info["has_artwork"] = 1
                break
        if hasattr(tags, "pictures") and tags.pictures:
            info["has_artwork"] = 1

    return info



def detect_source(tags: Dict[str, Optional[str]], filename: str) -> List[Tuple[str, float, str]]:
    """Guess the download source from tag signals.
    Returns list of (source, confidence, evidence)."""
    results = []

    for source, signals in SOURCE_SIGNALS.items():
        evidence = []
        score = 0.0

        # Check comment field
        comment = tags.get("comment", "") or ""
        for pat in signals["comment"]:
            if re.search(pat, comment, re.I):
                evidence.append(f"comment matches: {pat}")
                score += 0.4

        # Check encoder
        encoder = tags.get("encoder", "") or ""
        for pat in signals["encoder"]:
            if re.search(pat, encoder, re.I):
                evidence.append(f"encoder matches: {pat}")
                score += 0.3

        # Check filename
        for pat in signals["filename"]:
            if re.search(pat, filename, re.I):
                evidence.append(f"filename matches: {pat}")
                score += 0.2

        if score > 0.15:
            results.append((source, min(score, 1.0), "; ".join(evidence)))

    return results


# ─────────────────────────────────────────────────────────────────
# Folder intelligence imported from raagdosa_tags:
#   STRUCTURE_TYPES, CATCHALL_KEYWORDS, CRATE_KEYWORDS,
#   FOLDER_PARSE_PATTERNS, FolderContext,
#   analyze_folder_context, apply_folder_context
# ─────────────────────────────────────────────────────────────────






def generate_proposals(file_id: int, tags: Dict[str, Optional[str]],
                       filename: str, folder_name: str,
                       folder_ctx: Optional[FolderContext] = None) -> List[Dict[str, Any]]:
    """Generate tag fix proposals for a single file.
    Each proposal includes old_value, new_value, fix_type, confidence, reason."""
    proposals = []

    # 1. Title noise removal
    title = tags.get("title")
    if title:
        cleaned = title
        reasons = []
        for pat_str, category, conf in NOISE_PATTERNS.get("title", []):
            m = re.search(pat_str, cleaned, re.I)
            if m:
                reasons.append(f"strip {category}: '{m.group(0).strip()}'")
                cleaned = re.sub(pat_str, '', cleaned, flags=re.I).strip()

        # Whitespace cleanup
        cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
        if cleaned and cleaned != title:
            proposals.append({
                "field_name": "title",
                "old_value": title,
                "new_value": cleaned,
                "fix_type": "noise_removal",
                "confidence": 0.95,
                "reason": "; ".join(reasons) if reasons else "whitespace cleanup",
            })

    # 2. BPM extraction from title/artist/comment
    bpm_tag = tags.get("bpm")
    if not bpm_tag:
        for field in ["title", "artist", "comment"]:
            val = tags.get(field)
            if val:
                result = extract_bpm_from_value(val)
                if result:
                    bpm_val, conf, matched = result
                    proposals.append({
                        "field_name": "bpm",
                        "old_value": None,
                        "new_value": str(bpm_val),
                        "fix_type": "bpm_extraction",
                        "confidence": conf,
                        "reason": f"BPM '{matched}' found in {field} field, "
                                  f"extracted to BPM tag",
                    })
                    # Also propose cleaning the source field
                    cleaned_src = re.sub(re.escape(matched), '', val).strip()
                    cleaned_src = re.sub(r'\s{2,}', ' ', cleaned_src).strip()
                    cleaned_src = re.sub(r'^\s*[-–]\s*|\s*[-–]\s*$', '', cleaned_src).strip()
                    if cleaned_src and cleaned_src != val:
                        proposals.append({
                            "field_name": field,
                            "old_value": val,
                            "new_value": cleaned_src,
                            "fix_type": "bpm_cleanup",
                            "confidence": conf * 0.95,
                            "reason": f"cleaned BPM data '{matched}' from {field}",
                        })
                    break

    # 3. Key-prefixed artist detection (e.g., "10B - Katy Perry")
    # Must run BEFORE key extraction so we don't double-propose
    artist = tags.get("artist")
    key_from_prefix = None
    if artist:
        prefix_result = detect_key_prefix_in_artist(artist)
        if prefix_result:
            key_val, real_artist = prefix_result
            key_from_prefix = key_val
            proposals.append({
                "field_name": "artist",
                "old_value": artist,
                "new_value": real_artist,
                "fix_type": "key_prefix_strip",
                "confidence": 0.88,
                "reason": f"Camelot key '{key_val}' prefix stripped from artist; "
                          f"real artist is '{real_artist}'",
            })
            # Also extract the key if key tag is empty
            if not tags.get("key"):
                proposals.append({
                    "field_name": "key",
                    "old_value": None,
                    "new_value": key_val.upper(),
                    "fix_type": "key_extraction",
                    "confidence": 0.92,
                    "reason": f"key '{key_val}' extracted from key-prefixed artist name",
                })

    # 3b. Key extraction from title/artist/comment (skip if already found via prefix)
    key_tag = tags.get("key")
    if not key_tag and not key_from_prefix:
        for field in ["title", "artist", "comment"]:
            val = tags.get(field)
            if val:
                result = extract_key_from_value(val)
                if result:
                    key_val, notation, conf, matched = result
                    proposals.append({
                        "field_name": "key",
                        "old_value": None,
                        "new_value": key_val,
                        "fix_type": "key_extraction",
                        "confidence": conf,
                        "reason": f"key '{matched}' ({notation}) found in {field} field, "
                                  f"extracted to key tag",
                    })
                    break

    # 4. Comment cleanup
    comment = tags.get("comment")
    if comment:
        noise_found = detect_noise("comment", comment)
        if noise_found:
            cleaned = comment
            reasons = []
            for category, pat, matched, conf in noise_found:
                reasons.append(f"strip {category}: '{matched}'")
                cleaned = cleaned.replace(matched, "").strip()

            cleaned = re.sub(r'\s{2,}', ' ', cleaned).strip()
            if cleaned != comment:
                proposals.append({
                    "field_name": "comment",
                    "old_value": comment,
                    "new_value": cleaned if cleaned else "",
                    "fix_type": "comment_cleanup",
                    "confidence": min(c for _, _, _, c in noise_found),
                    "reason": "; ".join(reasons),
                })

    # 5. Mojibake detection in all text fields
    for field in ["title", "artist", "album_artist", "album"]:
        val = tags.get(field)
        if val:
            result = detect_mojibake(val)
            if result:
                repaired, conf = result
                proposals.append({
                    "field_name": field,
                    "old_value": val,
                    "new_value": repaired,
                    "fix_type": "encoding_repair",
                    "confidence": conf,
                    "reason": f"mojibake detected: '{val}' → '{repaired}'",
                })

    # 6. Whitespace issues in all fields
    for field in ["title", "artist", "album_artist", "album"]:
        val = tags.get(field)
        if val:
            cleaned = re.sub(r'\s{2,}', ' ', val).strip()
            if cleaned != val and field not in [p["field_name"] for p in proposals]:
                proposals.append({
                    "field_name": field,
                    "old_value": val,
                    "new_value": cleaned,
                    "fix_type": "whitespace",
                    "confidence": 1.0,
                    "reason": "extra whitespace removed",
                })

    # 7. Missing album_artist (set from artist, stripping feat. suffixes)
    if not tags.get("album_artist") and tags.get("artist"):
        artist_val = tags["artist"]
        # If we detected a key prefix, use the cleaned artist instead
        key_strip_prop = next((p for p in proposals
                               if p["fix_type"] == "key_prefix_strip"
                               and p["field_name"] == "artist"), None)
        if key_strip_prop:
            artist_val = key_strip_prop["new_value"]

        # Strip feat./ft. suffix — album_artist should be primary artist only
        primary_artist = strip_feat_from_artist(artist_val)
        if primary_artist:
            # Differentiate confidence based on quality signals
            aa_conf = 0.60  # base
            aa_reasons = []
            # Artist tag is clean (no encoding issues, no noise proposals)
            has_artist_issues = any(p["field_name"] == "artist" and p["fix_type"] in
                                   ("encoding_repair", "key_prefix_strip", "noise_removal")
                                   for p in proposals)
            if not has_artist_issues:
                aa_conf += 0.10
                aa_reasons.append("clean artist tag")
            # Feat. was stripped → we have a clear primary artist
            if primary_artist != artist_val:
                aa_conf += 0.05
                aa_reasons.append("stripped feat. info")
            # Title exists → file has decent tags overall
            if tags.get("title"):
                aa_conf += 0.05
                aa_reasons.append("has title")
            # Album exists → more confident about tag quality
            if tags.get("album"):
                aa_conf += 0.05
                aa_reasons.append("has album")

            proposals.append({
                "field_name": "album_artist",
                "old_value": None,
                "new_value": primary_artist,
                "fix_type": "fill_album_artist",
                "confidence": min(0.85, aa_conf),
                "reason": "album_artist empty, proposed from primary artist"
                          + (f" ({', '.join(aa_reasons)})" if aa_reasons else "")
                          + " (folder-level voting may override)",
            })

    # 8. Feat. separator normalization in artist tag
    artist_val = tags.get("artist")
    if artist_val:
        # Normalize various feat patterns to consistent "feat."
        # Requires trailing space to avoid matching word-internal 'ft' etc.
        normalized = re.sub(
            r'\b(?:feat\.?|ft\.?|featuring)\s',
            'feat. ',
            artist_val,
            flags=re.IGNORECASE)
        if normalized != artist_val:
            proposals.append({
                "field_name": "artist",
                "old_value": artist_val,
                "new_value": normalized,
                "fix_type": "feat_normalize",
                "confidence": 0.95,
                "reason": f"normalized featuring separator to 'feat.'",
            })

    # 9. "Original Mix" suffix stripping from title
    title_val = tags.get("title")
    if title_val:
        stripped = ORIGINAL_MIX_PATTERN.sub('', title_val).strip()
        # Don't strip if already proposed a noise_removal for title
        title_already_proposed = any(
            p["field_name"] == "title" and p["fix_type"] == "noise_removal"
            for p in proposals)
        if stripped != title_val and not title_already_proposed:
            proposals.append({
                "field_name": "title",
                "old_value": title_val,
                "new_value": stripped,
                "fix_type": "original_mix_strip",
                "confidence": 0.92,
                "reason": "stripped 'Original Mix' suffix (Beatport convention)",
            })

    # 10. Filename noise detection (URLs, YouTube IDs, converter tags)
    fn_noise = detect_filename_noise(filename)
    if fn_noise:
        reasons = [f"{cat}: '{matched}'" for cat, matched, _ in fn_noise]
        proposals.append({
            "field_name": "filename",
            "old_value": filename,
            "new_value": filename,  # flag only — no rename yet
            "fix_type": "filename_noise_flag",
            "confidence": max(c for _, _, c in fn_noise),
            "reason": f"filename contains noise: {'; '.join(reasons)}",
        })

    return proposals


# ─────────────────────────────────────────────────────────────────
# Scanner core
# ─────────────────────────────────────────────────────────────────
def scan_folder(root: Path, db: ScannerDB, label: Optional[str] = None,
                verbose: bool = False) -> str:
    """Scan a folder tree, log all tags and findings to the database.
    Returns scan_id."""
    scan_id = str(uuid.uuid4())[:12]
    now = dt.datetime.now().isoformat()

    db.execute("INSERT INTO scans (scan_id, started_at, root_path, label, status) "
               "VALUES (?, ?, ?, ?, 'running')",
               (scan_id, now, str(root), label))
    db.commit()

    # Discover audio files
    audio_files: List[Path] = []
    folders: Set[str] = set()
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            fp = Path(dirpath) / fn
            if fp.suffix.lower() in AUDIO_EXTENSIONS:
                audio_files.append(fp)
                folders.add(dirpath)

    total = len(audio_files)
    if verbose:
        print(f"  Found {total} audio files in {len(folders)} folders")

    # Process each file
    file_count = 0
    artist_counter: Dict[str, Counter] = defaultdict(Counter)  # folder → artist counts
    folder_tags: Dict[str, List[Dict]] = defaultdict(list)

    for i, fp in enumerate(audio_files):
        if verbose and (i + 1) % 50 == 0:
            print(f"  [{i+1}/{total}] {fp.parent.name}/{fp.name}")

        try:
            # Read tags
            tags = read_tags(fp)
            file_info = read_file_info(fp)
            folder_path = str(fp.parent)

            # Insert file record
            cur = db.execute(
                "INSERT INTO files (scan_id, file_path, folder_path, filename, "
                "extension, file_size, duration_sec, bitrate, sample_rate, channels, "
                "container_type, id3_version, has_id3v1, has_id3v2, has_ape, "
                "has_artwork, encoding) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (scan_id, str(fp), folder_path, fp.name,
                 fp.suffix.lower().lstrip("."),
                 file_info["file_size"], file_info["duration_sec"],
                 file_info["bitrate"], file_info["sample_rate"],
                 file_info["channels"], file_info["container_type"],
                 file_info["id3_version"], file_info["has_id3v1"],
                 file_info["has_id3v2"], file_info["has_ape"],
                 file_info["has_artwork"], file_info["encoding"]))
            file_id = cur.lastrowid

            # Insert tags
            for field_name, value in tags.items():
                db.execute(
                    "INSERT OR IGNORE INTO tags (file_id, field_name, raw_value, "
                    "value_length, is_empty) VALUES (?,?,?,?,?)",
                    (file_id, field_name, value, len(value), 0))

            # Detect noise in each field
            for field_name, value in tags.items():
                findings = detect_noise(field_name, value)
                for category, pattern, matched, conf in findings:
                    db.execute(
                        "INSERT INTO noise_findings (file_id, field_name, "
                        "noise_category, matched_pattern, matched_text, confidence) "
                        "VALUES (?,?,?,?,?,?)",
                        (file_id, field_name, category, pattern, matched, conf))

            # Generate proposals (folder context applied in second pass)
            proposals = generate_proposals(file_id, tags, fp.name, fp.parent.name)
            for prop in proposals:
                db.execute(
                    "INSERT OR IGNORE INTO proposals (file_id, scan_id, field_name, "
                    "old_value, new_value, fix_type, confidence, reason, status) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (file_id, scan_id, prop["field_name"],
                     prop["old_value"], prop["new_value"],
                     prop["fix_type"], prop["confidence"],
                     prop["reason"], "pending"))

            # Collect artist variants
            for field in ["artist", "album_artist"]:
                val = tags.get(field)
                if val:
                    norm = normalize_artist(val)
                    db.execute(
                        "INSERT INTO artist_variants (scan_id, raw_name, normalized, "
                        "source_field, occurrences) VALUES (?,?,?,?,1)",
                        (scan_id, val, norm, field))
                    artist_counter[folder_path][norm] += 1

            # Source fingerprinting
            source_guesses = detect_source(tags, fp.name)
            for source, conf, evidence in source_guesses:
                db.execute(
                    "INSERT INTO source_guesses (file_id, source, confidence, evidence) "
                    "VALUES (?,?,?,?)",
                    (file_id, source, conf, evidence))

            folder_tags[folder_path].append(tags)
            file_count += 1

        except Exception as e:
            if verbose:
                print(f"  ⚠ Error reading {fp.name}: {e}", file=sys.stderr)

        # Commit every 100 files
        if (i + 1) % 100 == 0:
            db.commit()

    # ── Folder intelligence pass ─────────────────────────────
    # Analyze each folder's structure, then enhance proposals with context
    folder_files: Dict[str, List[Tuple[int, Dict, str]]] = defaultdict(list)
    # Rebuild file→tags mapping from DB for context pass
    for folder_path in folders:
        rows = db.execute(
            "SELECT f.file_id, f.filename FROM files f "
            "WHERE f.scan_id = ? AND f.folder_path = ?",
            (scan_id, folder_path)).fetchall()
        for row in rows:
            fid = row["file_id"]
            tag_rows = db.execute(
                "SELECT field_name, raw_value FROM tags WHERE file_id = ?",
                (fid,)).fetchall()
            tags_dict = {r["field_name"]: r["raw_value"] for r in tag_rows}
            folder_files[folder_path].append((fid, tags_dict, row["filename"]))

    folder_contexts: Dict[str, FolderContext] = {}
    for folder_path, file_list in folder_files.items():
        file_tags_list = [t for _, t, _ in file_list]
        ctx = analyze_folder_context(folder_path, file_tags_list)
        folder_contexts[folder_path] = ctx

        # Second pass: enhance existing proposals + add folder-derived ones
        for fid, tags_dict, fname in file_list:
            # Get existing proposals for this file
            existing = db.execute(
                "SELECT proposal_id, field_name, old_value, new_value, fix_type, "
                "confidence, reason FROM proposals WHERE file_id = ? AND scan_id = ?",
                (fid, scan_id)).fetchall()

            existing_props = [{
                "field_name": r["field_name"],
                "old_value": r["old_value"],
                "new_value": r["new_value"],
                "fix_type": r["fix_type"],
                "confidence": r["confidence"],
                "reason": r["reason"],
                "proposal_id": r["proposal_id"],
            } for r in existing]

            # Apply folder context
            enhanced = apply_folder_context(existing_props, ctx, tags_dict, fname)

            # Update existing proposals with adjusted confidence
            for prop in enhanced:
                if "proposal_id" in prop:
                    db.execute(
                        "UPDATE proposals SET confidence = ?, reason = ? "
                        "WHERE proposal_id = ?",
                        (prop["confidence"], prop["reason"], prop["proposal_id"]))
                else:
                    # New folder-derived proposal
                    db.execute(
                        "INSERT OR IGNORE INTO proposals (file_id, scan_id, field_name, "
                        "old_value, new_value, fix_type, confidence, reason, status) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (fid, scan_id, prop["field_name"],
                         prop["old_value"], prop["new_value"],
                         prop["fix_type"], prop["confidence"],
                         prop["reason"], "pending"))

    db.commit()

    # Build folder summaries
    for folder_path, tag_list in folder_tags.items():
        artists = Counter()
        albums = Counter()
        genres = Counter()
        formats = Counter()
        years = set()
        completeness_scores = []

        for t in tag_list:
            if t.get("artist"):
                artists[normalize_artist(t["artist"])] += 1
            if t.get("album"):
                albums[t["album"].lower().strip()] += 1
            if t.get("genre"):
                genres[t["genre"]] += 1
            if t.get("year"):
                try:
                    years.add(int(re.search(r'\d{4}', t["year"]).group()))
                except (AttributeError, ValueError):
                    pass
            completeness_scores.append(compute_tag_completeness(t))

        dominant_artist = artists.most_common(1)[0][0] if artists else None
        dominant_album = albums.most_common(1)[0][0] if albums else None
        year_spread = max(years) - min(years) if len(years) > 1 else 0
        is_va = 1 if (len(artists) > 1 and
                      artists.most_common(1)[0][1] / len(tag_list) < 0.60) else 0
        avg_completeness = sum(completeness_scores) / len(completeness_scores) if completeness_scores else 0

        ext_counts = Counter(Path(str(fp)).suffix.lower() for fp in
                           [f for f in audio_files if str(f.parent) == folder_path])

        # Merge folder context if available
        ctx = folder_contexts.get(folder_path)

        db.execute(
            "INSERT OR IGNORE INTO folder_summaries (scan_id, folder_path, file_count, "
            "artist_count, dominant_artist, dominant_album, tag_completeness, "
            "format_mix, is_va, year_spread, structure_type, structure_confidence, "
            "context_score, parsed_artist, parsed_album, parsed_year, "
            "inferred_artist, inferred_album, is_catchall, is_crate, evidence) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (scan_id, folder_path, len(tag_list), len(artists),
             dominant_artist, dominant_album, round(avg_completeness, 3),
             json.dumps(dict(ext_counts)), is_va, year_spread,
             ctx.structure_type if ctx else None,
             ctx.structure_confidence if ctx else None,
             ctx.context_score if ctx else None,
             ctx.parsed_artist if ctx else None,
             ctx.parsed_album if ctx else None,
             ctx.parsed_year if ctx else None,
             ctx.inferred_artist if ctx else None,
             ctx.inferred_album if ctx else None,
             1 if ctx and ctx.is_catchall else 0,
             1 if ctx and ctx.is_crate else 0,
             json.dumps(ctx.evidence) if ctx else None))

    # ── Artist variant normalization pass ─────────────────────
    # Find artists with multiple spellings and propose normalizing to the most common
    variant_rows = db.execute(
        "SELECT normalized, raw_name, SUM(occurrences) as total "
        "FROM artist_variants WHERE scan_id = ? "
        "GROUP BY normalized, raw_name "
        "ORDER BY normalized, total DESC",
        (scan_id,)).fetchall()

    # Group by normalized form
    norm_groups: Dict[str, List[Tuple[str, int]]] = defaultdict(list)
    for row in variant_rows:
        norm_groups[row["normalized"]].append((row["raw_name"], row["total"]))

    for norm_key, variants in norm_groups.items():
        if len(variants) < 2:
            continue
        # Best spelling = highest quality score, with frequency as tiebreaker
        total_occurrences = sum(count for _, count in variants)
        best_spelling = max(
            variants,
            key=lambda v: (score_artist_spelling(v[0]), v[1])
        )[0]

        for raw_name, count in variants[1:]:
            if raw_name == best_spelling:
                continue
            # Find all files with this variant and create proposals
            file_rows = db.execute(
                "SELECT f.file_id, t.field_name FROM tags t "
                "JOIN files f ON t.file_id = f.file_id "
                "WHERE f.scan_id = ? AND t.field_name IN ('artist', 'album_artist') "
                "AND t.raw_value = ?",
                (scan_id, raw_name)).fetchall()

            for frow in file_rows:
                # Check if we already have a proposal for this field on this file
                existing = db.execute(
                    "SELECT 1 FROM proposals WHERE file_id = ? AND scan_id = ? "
                    "AND field_name = ? AND fix_type = 'artist_normalize'",
                    (frow["file_id"], scan_id, frow["field_name"])).fetchone()
                if existing:
                    continue

                conf = min(0.85, 0.50 + (total_occurrences / 100) * 0.35)
                db.execute(
                    "INSERT OR IGNORE INTO proposals (file_id, scan_id, field_name, "
                    "old_value, new_value, fix_type, confidence, reason, status) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (frow["file_id"], scan_id, frow["field_name"],
                     raw_name, best_spelling, "artist_normalize", round(conf, 4),
                     f"normalize '{raw_name}' → '{best_spelling}' "
                     f"({len(variants)} variants found, {total_occurrences} total occurrences)",
                     "pending"))

    db.commit()

    # Dupe signal detection (within this scan)
    _detect_dupe_signals(db, scan_id)

    # Finalize
    db.execute("UPDATE scans SET finished_at=?, file_count=?, folder_count=?, status='completed' "
               "WHERE scan_id=?",
               (dt.datetime.now().isoformat(), file_count, len(folders), scan_id))
    db.commit()

    return scan_id


def _detect_dupe_signals(db: ScannerDB, scan_id: str):
    """Find potential duplicates within a scan by ISRC, duration, and name."""
    # ISRC matches
    rows = db.execute(
        "SELECT t1.file_id AS fid_a, t2.file_id AS fid_b, t1.raw_value AS isrc "
        "FROM tags t1 "
        "JOIN tags t2 ON t1.raw_value = t2.raw_value AND t1.file_id < t2.file_id "
        "JOIN files f1 ON t1.file_id = f1.file_id "
        "JOIN files f2 ON t2.file_id = f2.file_id "
        "WHERE t1.field_name = 'isrc' AND t2.field_name = 'isrc' "
        "AND f1.scan_id = ? AND f2.scan_id = ?",
        (scan_id, scan_id)).fetchall()

    for row in rows:
        db.execute(
            "INSERT INTO dupe_signals (file_id_a, file_id_b, signal_type, "
            "signal_value, similarity) VALUES (?,?,?,?,?)",
            (row["fid_a"], row["fid_b"], "isrc_match", row["isrc"], 1.0))

    # Duration clustering (within 2 seconds, same title)
    rows = db.execute(
        "SELECT f1.file_id AS fid_a, f2.file_id AS fid_b, "
        "f1.duration_sec AS dur_a, f2.duration_sec AS dur_b "
        "FROM files f1 "
        "JOIN files f2 ON f1.file_id < f2.file_id "
        "JOIN tags t1 ON f1.file_id = t1.file_id AND t1.field_name = 'title' "
        "JOIN tags t2 ON f2.file_id = t2.file_id AND t2.field_name = 'title' "
        "WHERE f1.scan_id = ? AND f2.scan_id = ? "
        "AND f1.duration_sec IS NOT NULL AND f2.duration_sec IS NOT NULL "
        "AND ABS(f1.duration_sec - f2.duration_sec) < 2.0 "
        "AND LOWER(t1.raw_value) = LOWER(t2.raw_value)",
        (scan_id, scan_id)).fetchall()

    for row in rows:
        db.execute(
            "INSERT INTO dupe_signals (file_id_a, file_id_b, signal_type, "
            "signal_value, similarity) VALUES (?,?,?,?,?)",
            (row["fid_a"], row["fid_b"], "duration_title_match",
             f"{row['dur_a']:.1f}s vs {row['dur_b']:.1f}s", 0.90))


# ─────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────
def print_report(db: ScannerDB, scan_id: Optional[str] = None):
    """Print a summary report of scan findings."""
    if scan_id is None:
        row = db.execute("SELECT scan_id FROM scans ORDER BY started_at DESC LIMIT 1").fetchone()
        if not row:
            print("No scans found. Run: python raagdosa_scanner.py scan /path/to/music")
            return
        scan_id = row["scan_id"]

    scan = db.execute("SELECT * FROM scans WHERE scan_id=?", (scan_id,)).fetchone()
    if not scan:
        print(f"Scan {scan_id} not found.")
        return

    print(f"\n{'═' * 60}")
    print(f" Scan Report: {scan_id}")
    print(f" Root: {scan['root_path']}")
    print(f" Date: {scan['started_at'][:19]}")
    print(f" Files: {scan['file_count']}  |  Folders: {scan['folder_count']}")
    print(f"{'═' * 60}\n")

    # Tag completeness
    total_files = scan["file_count"]
    if total_files == 0:
        print("  No files scanned.")
        return

    fields = ["artist", "title", "album", "album_artist", "year", "genre",
              "bpm", "key", "label", "isrc", "comment"]
    print(" Tag Coverage:")
    for field in fields:
        count = db.execute(
            "SELECT COUNT(DISTINCT t.file_id) FROM tags t "
            "JOIN files f ON t.file_id = f.file_id "
            "WHERE f.scan_id=? AND t.field_name=?",
            (scan_id, field)).fetchone()[0]
        pct = (count / total_files * 100) if total_files else 0
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"   {field:15s}  {count:5d}/{total_files}  {bar}  {pct:5.1f}%")

    # Noise findings
    print(f"\n Noise Findings:")
    noise_rows = db.execute(
        "SELECT nf.noise_category, COUNT(*) as cnt "
        "FROM noise_findings nf "
        "JOIN files f ON nf.file_id = f.file_id "
        "WHERE f.scan_id=? "
        "GROUP BY nf.noise_category ORDER BY cnt DESC",
        (scan_id,)).fetchall()
    if noise_rows:
        for row in noise_rows:
            print(f"   {row['noise_category']:20s}  {row['cnt']:5d} occurrences")
    else:
        print("   No noise patterns detected.")

    # Proposals
    print(f"\n Fix Proposals:")
    prop_rows = db.execute(
        "SELECT fix_type, COUNT(*) as cnt, "
        "AVG(confidence) as avg_conf, status "
        "FROM proposals WHERE scan_id=? "
        "GROUP BY fix_type, status ORDER BY cnt DESC",
        (scan_id,)).fetchall()
    if prop_rows:
        for row in prop_rows:
            status_mark = {"pending": "○", "applied": "✓", "undone": "↩"}.get(row["status"], "?")
            print(f"   {status_mark} {row['fix_type']:20s}  {row['cnt']:5d} proposals  "
                  f"avg confidence: {row['avg_conf']:.2f}")
    else:
        print("   No fix proposals generated.")

    # Source distribution
    print(f"\n Source Detection:")
    src_rows = db.execute(
        "SELECT sg.source, COUNT(*) as cnt, AVG(sg.confidence) as avg_conf "
        "FROM source_guesses sg "
        "JOIN files f ON sg.file_id = f.file_id "
        "WHERE f.scan_id=? "
        "GROUP BY sg.source ORDER BY cnt DESC",
        (scan_id,)).fetchall()
    if src_rows:
        for row in src_rows:
            print(f"   {row['source']:15s}  {row['cnt']:5d} files  "
                  f"avg confidence: {row['avg_conf']:.2f}")
    else:
        print("   No source fingerprints detected.")

    # Dupe candidates
    dupe_count = db.execute(
        "SELECT COUNT(*) FROM dupe_signals ds "
        "JOIN files f ON ds.file_id_a = f.file_id "
        "WHERE f.scan_id=?",
        (scan_id,)).fetchone()[0]
    print(f"\n Duplicate Signals: {dupe_count} pairs found")

    # Folder Intelligence
    print(f"\n Folder Intelligence:")
    ctx_rows = db.execute(
        "SELECT folder_path, structure_type, context_score, parsed_artist, "
        "inferred_artist, is_catchall, is_crate, file_count "
        "FROM folder_summaries WHERE scan_id=? AND structure_type IS NOT NULL "
        "ORDER BY context_score ASC",
        (scan_id,)).fetchall()
    if ctx_rows:
        for row in ctx_rows:
            name = Path(row["folder_path"]).name
            stype = row["structure_type"] or "unknown"
            cscore = row["context_score"] or 0
            score_bar = "█" * int(cscore * 10) + "░" * (10 - int(cscore * 10))
            flags = []
            if row["is_catchall"]:
                flags.append("CATCHALL")
            if row["is_crate"]:
                flags.append("CRATE")
            if row["inferred_artist"]:
                flags.append(f"→ {row['inferred_artist']}")
            flag_str = f"  {' '.join(flags)}" if flags else ""
            print(f"   {score_bar} {cscore:.2f}  {stype:15s}  "
                  f"{name[:40]:40s}  ({row['file_count']} files){flag_str}")
    else:
        print("   No folder context data available.")

    # Artist variants
    variant_rows = db.execute(
        "SELECT normalized, COUNT(DISTINCT raw_name) as variants, "
        "GROUP_CONCAT(DISTINCT raw_name) as names "
        "FROM artist_variants WHERE scan_id=? "
        "GROUP BY normalized HAVING variants > 1 "
        "ORDER BY variants DESC LIMIT 15",
        (scan_id,)).fetchall()
    if variant_rows:
        print(f"\n Artist Name Variants (top 15):")
        for row in variant_rows:
            names = row["names"].split(",")
            print(f"   [{row['variants']} variants] {names[0]}")
            for n in names[1:]:
                print(f"      → {n}")

    print()


def print_proposals(db: ScannerDB, scan_id: Optional[str] = None,
                    folder: Optional[str] = None, fix_type: Optional[str] = None):
    """Print detailed fix proposals with explanations."""
    if scan_id is None:
        row = db.execute("SELECT scan_id FROM scans ORDER BY started_at DESC LIMIT 1").fetchone()
        if not row:
            print("No scans found.")
            return
        scan_id = row["scan_id"]

    sql = """
        SELECT p.*, f.file_path, f.filename, f.folder_path
        FROM proposals p
        JOIN files f ON p.file_id = f.file_id
        WHERE p.scan_id = ?
    """
    params: list = [scan_id]

    if folder:
        sql += " AND f.folder_path LIKE ?"
        params.append(f"%{folder}%")
    if fix_type:
        sql += " AND p.fix_type = ?"
        params.append(fix_type)

    sql += " ORDER BY f.folder_path, f.filename, p.field_name"

    rows = db.execute(sql, params).fetchall()
    if not rows:
        print("No proposals found.")
        return

    current_folder = None
    current_file = None

    print(f"\n{'═' * 60}")
    print(f" Fix Proposals — Scan {scan_id}")
    print(f"{'═' * 60}\n")

    for row in rows:
        # Folder header
        if row["folder_path"] != current_folder:
            current_folder = row["folder_path"]
            folder_name = Path(current_folder).name
            print(f"\n ┌─ {folder_name}")
            print(f" │")

        # File header
        if row["filename"] != current_file:
            current_file = row["filename"]
            status = {"pending": "○", "applied": "✓", "undone": "↩"}.get(row["status"], "?")
            print(f" │  {status} {current_file}")

        # Proposal detail
        conf_bar = "█" * int(row["confidence"] * 10) + "░" * (10 - int(row["confidence"] * 10))
        risk = RISK_TIERS.get(row["fix_type"], "moderate")
        threshold = RISK_THRESHOLDS.get(risk, 0.92)
        auto = "auto" if row["confidence"] >= threshold else "review"
        risk_label = {"safe": "SAFE", "moderate": "MOD", "destructive": "DEST"}.get(risk, "?")

        old_display = f'"{row["old_value"]}"' if row["old_value"] else "(empty)"
        new_display = f'"{row["new_value"]}"'

        print(f" │     {row['field_name']:15s}  - {old_display}")
        print(f" │     {'':15s}  + {new_display}")
        print(f" │     {'':15s}    {row['fix_type']} · {conf_bar} {row['confidence']:.2f} · {auto} [{risk_label}]")
        print(f" │     {'':15s}    reason: {row['reason']}")

    print(f" │")
    print(f" └─ {len(rows)} proposals total\n")


def print_patterns(db: ScannerDB, scan_id: Optional[str] = None):
    """Show recurring patterns discovered across scans."""
    if scan_id is None:
        # Analyze across all scans
        where_clause = ""
        params: tuple = ()
    else:
        where_clause = "WHERE f.scan_id = ?"
        params = (scan_id,)

    print(f"\n{'═' * 60}")
    print(f" Discovered Patterns")
    print(f"{'═' * 60}\n")

    # Most common noise in each field
    print(" Top Noise Patterns by Field:")
    for field in ["title", "comment", "artist", "album"]:
        rows = db.execute(f"""
            SELECT nf.noise_category, nf.matched_text, COUNT(*) as cnt
            FROM noise_findings nf
            JOIN files f ON nf.file_id = f.file_id
            {where_clause.replace('?', '?') if where_clause else ''}
            {'AND' if where_clause else 'WHERE'} nf.field_name = ?
            GROUP BY nf.noise_category, nf.matched_text
            ORDER BY cnt DESC LIMIT 5
        """, (*params, field)).fetchall()
        if rows:
            print(f"\n   [{field}]")
            for row in rows:
                print(f"     {row['cnt']:4d}x  {row['noise_category']:15s}  \"{row['matched_text']}\"")

    # Most common fix types (with learned model data if available)
    print(f"\n\n Top Fix Types:")
    rows = db.execute(f"""
        SELECT p.fix_type, COUNT(*) as cnt, AVG(p.confidence) as avg_conf,
               SUM(CASE WHEN p.status = 'accepted' THEN 1 ELSE 0 END) as accepted,
               SUM(CASE WHEN p.status = 'rejected' THEN 1 ELSE 0 END) as rejected
        FROM proposals p
        JOIN files f ON p.file_id = f.file_id
        {where_clause}
        GROUP BY p.fix_type ORDER BY cnt DESC
    """, params).fetchall()

    # Load model data for enrichment
    model_data = {}
    try:
        for mr in db.execute("SELECT * FROM model_rules").fetchall():
            key = (mr["fix_type"], mr["source_label"])
            model_data[key] = mr
    except sqlite3.OperationalError:
        pass  # table may not exist in older DBs

    for row in rows:
        risk = RISK_TIERS.get(row["fix_type"], "moderate")
        risk_label = {"safe": "SAFE", "moderate": "MOD ", "destructive": "DEST"}.get(risk, "?")
        line = f"   {row['cnt']:5d}  {row['fix_type']:25s}  [{risk_label}]  avg conf: {row['avg_conf']:.2f}"

        # Show review stats if any
        reviewed = (row["accepted"] or 0) + (row["rejected"] or 0)
        if reviewed > 0:
            hit = (row["accepted"] or 0) / reviewed
            line += f"  | reviewed: {reviewed} hit: {hit:.0%}"

        # Show model learned confidence if graduated
        baseline = model_data.get((row["fix_type"], "_baseline_"))
        if baseline and baseline["graduated"]:
            line += f"  | model: {baseline['learned_confidence']:.2f} GRADUATED"
        elif baseline:
            line += f"  | model: {baseline['learned_confidence']:.2f} ({baseline['total_reviewed']}/{GRADUATION_THRESHOLDS.get(risk, 50)})"

        print(line)

    # Folders with worst tag completeness
    print(f"\n\n Folders with Lowest Tag Completeness:")
    rows = db.execute(f"""
        SELECT folder_path, file_count, tag_completeness, dominant_artist, is_va
        FROM folder_summaries
        {'WHERE scan_id = ?' if scan_id else ''}
        ORDER BY tag_completeness ASC LIMIT 10
    """, (scan_id,) if scan_id else ()).fetchall()
    for row in rows:
        name = Path(row["folder_path"]).name
        va = " [VA]" if row["is_va"] else ""
        print(f"   {row['tag_completeness']:.2f}  {name[:50]:50s}  "
              f"({row['file_count']} files){va}")

    print()


def print_history(db: ScannerDB):
    """List all scans."""
    rows = db.execute(
        "SELECT scan_id, started_at, root_path, file_count, folder_count, "
        "label, status FROM scans ORDER BY started_at DESC").fetchall()

    if not rows:
        print("No scans found.")
        return

    print(f"\n{'═' * 60}")
    print(f" Scan History")
    print(f"{'═' * 60}\n")
    print(f" {'ID':12s}  {'Date':19s}  {'Files':>5s}  {'Folders':>7s}  {'Status':9s}  Root")
    print(f" {'─'*12}  {'─'*19}  {'─'*5}  {'─'*7}  {'─'*9}  {'─'*30}")
    for row in rows:
        root_short = str(Path(row["root_path"]).name)
        label = f" ({row['label']})" if row["label"] else ""
        print(f" {row['scan_id']:12s}  {row['started_at'][:19]}  "
              f"{row['file_count']:5d}  {row['folder_count']:7d}  "
              f"{row['status']:9s}  {root_short}{label}")
    print()


# ─────────────────────────────────────────────────────────────────
# Export
# ─────────────────────────────────────────────────────────────────
def export_findings(db: ScannerDB, scan_id: Optional[str] = None,
                    output_path: str = "scanner_findings.yaml",
                    min_confidence: float = 0.70):
    """Export scan findings as YAML for RaagDosa reference import."""
    if yaml is None:
        print("ERROR: pyyaml required for export. Install: pip install pyyaml",
              file=sys.stderr)
        return

    if scan_id is None:
        row = db.execute("SELECT scan_id FROM scans ORDER BY started_at DESC LIMIT 1").fetchone()
        if not row:
            print("No scans found.")
            return
        scan_id = row["scan_id"]

    scan = db.execute("SELECT * FROM scans WHERE scan_id=?", (scan_id,)).fetchone()

    export: Dict[str, Any] = {
        "scanner_version": VERSION,
        "generated_at": dt.datetime.now().isoformat(),
        "scan_id": scan_id,
        "root_path": scan["root_path"],
        "files_scanned": scan["file_count"],
    }

    # Noise patterns (grouped by field)
    noise_export: Dict[str, list] = {}
    rows = db.execute("""
        SELECT nf.field_name, nf.noise_category, nf.matched_pattern,
               nf.matched_text, COUNT(*) as cnt
        FROM noise_findings nf
        JOIN files f ON nf.file_id = f.file_id
        WHERE f.scan_id = ?
        GROUP BY nf.field_name, nf.noise_category, nf.matched_pattern
        HAVING cnt >= 3
        ORDER BY cnt DESC
    """, (scan_id,)).fetchall()
    for row in rows:
        field = row["field_name"]
        if field not in noise_export:
            noise_export[field] = []
        noise_export[field].append({
            "pattern": row["matched_pattern"],
            "category": row["noise_category"],
            "occurrences": row["cnt"],
            "example": row["matched_text"],
        })
    export["noise_patterns"] = noise_export

    # Artist alias candidates
    alias_rows = db.execute("""
        SELECT normalized, GROUP_CONCAT(DISTINCT raw_name) as names,
               COUNT(DISTINCT raw_name) as variant_count
        FROM artist_variants WHERE scan_id = ?
        GROUP BY normalized HAVING variant_count > 1
        ORDER BY variant_count DESC
    """, (scan_id,)).fetchall()
    aliases = []
    for row in alias_rows:
        names = list(set(row["names"].split(",")))
        # Pick the most common form as canonical
        canonical = max(names, key=len)  # longest form as canonical
        others = [n for n in names if n != canonical]
        if others:
            aliases.append({
                "canonical": canonical,
                "aliases": others,
                "confidence": 0.85,
            })
    export["artist_aliases"] = aliases

    # Tag completeness summary
    fields = ["artist", "title", "album", "album_artist", "year", "genre",
              "bpm", "key", "label", "isrc"]
    completeness: Dict[str, Any] = {"total_files": scan["file_count"]}
    field_stats = {}
    for field in fields:
        count = db.execute(
            "SELECT COUNT(DISTINCT t.file_id) FROM tags t "
            "JOIN files f ON t.file_id = f.file_id "
            "WHERE f.scan_id=? AND t.field_name=?",
            (scan_id, field)).fetchone()[0]
        total = scan["file_count"]
        field_stats[field] = {
            "present": count,
            "empty": total - count,
            "rate": round(count / total, 3) if total else 0,
        }
    completeness["fields"] = field_stats
    export["completeness_summary"] = completeness

    # Source distribution
    src_rows = db.execute("""
        SELECT sg.source, COUNT(*) as cnt
        FROM source_guesses sg
        JOIN files f ON sg.file_id = f.file_id
        WHERE f.scan_id = ?
        GROUP BY sg.source ORDER BY cnt DESC
    """, (scan_id,)).fetchall()
    export["source_summary"] = {row["source"]: row["cnt"] for row in src_rows}

    # Dupe candidates
    dupe_rows = db.execute("""
        SELECT ds.*, f1.file_path AS path_a, f2.file_path AS path_b
        FROM dupe_signals ds
        JOIN files f1 ON ds.file_id_a = f1.file_id
        JOIN files f2 ON ds.file_id_b = f2.file_id
        WHERE f1.scan_id = ?
    """, (scan_id,)).fetchall()
    dupes = []
    for row in dupe_rows:
        dupes.append({
            "file_a": row["path_a"],
            "file_b": row["path_b"],
            "signal_type": row["signal_type"],
            "signal_value": row["signal_value"],
            "similarity": row["similarity"],
        })
    export["dupe_candidates"] = dupes

    # Write YAML
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(export, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"\n  Exported to: {output_path}")
    print(f"  Noise patterns:   {sum(len(v) for v in noise_export.values())}")
    print(f"  Artist aliases:   {len(aliases)}")
    print(f"  Dupe candidates:  {len(dupes)}")
    print(f"\n  Import into RaagDosa: python raagdosa.py reference import {output_path}\n")


# ─────────────────────────────────────────────────────────────────
# Undo
# ─────────────────────────────────────────────────────────────────
def undo_scan(db: ScannerDB, scan_id: str):
    """Mark a scan's proposals as undone (no file changes to reverse yet)."""
    scan = db.execute("SELECT * FROM scans WHERE scan_id=?", (scan_id,)).fetchone()
    if not scan:
        print(f"Scan {scan_id} not found.")
        return

    applied = db.execute(
        "SELECT COUNT(*) FROM proposals WHERE scan_id=? AND status='applied'",
        (scan_id,)).fetchone()[0]

    if applied == 0:
        print(f"  No applied proposals to undo for scan {scan_id}.")
        # Mark all pending as undone
        db.execute("UPDATE proposals SET status='undone', undone_at=? WHERE scan_id=?",
                   (dt.datetime.now().isoformat(), scan_id))
        db.commit()
        print(f"  All pending proposals marked as undone.")
        return

    # For now, mark proposals as undone (actual tag restoration will come
    # when we implement apply)
    db.execute(
        "UPDATE proposals SET status='undone', undone_at=? "
        "WHERE scan_id=? AND status='applied'",
        (dt.datetime.now().isoformat(), scan_id))
    db.commit()
    print(f"  Undone {applied} applied proposals for scan {scan_id}.")
    print(f"  (Tag file restoration not yet implemented — proposals marked for tracking)")


# ─────────────────────────────────────────────────────────────────
# Review — accept/reject proposals to build training data
# ─────────────────────────────────────────────────────────────────
def review_proposals(db: ScannerDB, scan_id: Optional[str] = None,
                     folder: Optional[str] = None, fix_type: Optional[str] = None,
                     verdict: Optional[str] = None,
                     min_confidence: float = 0.0, max_confidence: float = 1.0,
                     risk: Optional[str] = None,
                     source_label: Optional[str] = None):
    """Bulk review proposals: accept or reject them to build training data.

    Non-interactive: use --accept or --reject to stamp matching proposals.
    Filter by folder, fix_type, confidence range, risk tier.
    """
    if verdict not in ("accept", "reject"):
        print("ERROR: must specify --accept or --reject", file=sys.stderr)
        return

    if scan_id is None:
        row = db.execute("SELECT scan_id FROM scans ORDER BY started_at DESC LIMIT 1").fetchone()
        if not row:
            print("No scans found.")
            return
        scan_id = row["scan_id"]

    # Resolve source label from scan's label if not given
    if source_label is None:
        scan_row = db.execute("SELECT label FROM scans WHERE scan_id=?", (scan_id,)).fetchone()
        if scan_row and scan_row["label"]:
            # Check if this label maps to a source profile
            profile = db.execute(
                "SELECT name FROM source_profiles WHERE scan_labels LIKE ?",
                (f"%{scan_row['label']}%",)).fetchone()
            source_label = profile["name"] if profile else scan_row["label"]

    # Build query for matching proposals
    sql = """
        SELECT p.proposal_id, p.fix_type, p.confidence, p.field_name,
               p.old_value, p.new_value, p.reason, f.filename, f.folder_path
        FROM proposals p
        JOIN files f ON p.file_id = f.file_id
        WHERE p.scan_id = ? AND p.status = 'pending'
        AND p.confidence >= ? AND p.confidence <= ?
    """
    params: list = [scan_id, min_confidence, max_confidence]

    if folder:
        sql += " AND f.folder_path LIKE ?"
        params.append(f"%{folder}%")
    if fix_type:
        sql += " AND p.fix_type = ?"
        params.append(fix_type)

    sql += " ORDER BY f.folder_path, f.filename"
    rows = db.execute(sql, params).fetchall()

    # Filter by risk tier in Python (since it's a lookup, not a DB column)
    if risk:
        rows = [r for r in rows if RISK_TIERS.get(r["fix_type"], "moderate") == risk]

    if not rows:
        print("No matching pending proposals found.")
        return

    now = dt.datetime.now().isoformat()
    reviewed = 0

    for row in rows:
        db.execute(
            "INSERT INTO reviews (proposal_id, verdict, reviewed_at, source_label) "
            "VALUES (?, ?, ?, ?)",
            (row["proposal_id"], verdict, now, source_label))
        new_status = "accepted" if verdict == "accept" else "rejected"
        db.execute("UPDATE proposals SET status = ? WHERE proposal_id = ?",
                   (new_status, row["proposal_id"]))
        reviewed += 1

    db.commit()

    risk_filter = f" [{risk}]" if risk else ""
    type_filter = f" type={fix_type}" if fix_type else ""
    conf_filter = f" conf={min_confidence:.2f}-{max_confidence:.2f}" if min_confidence > 0 or max_confidence < 1 else ""
    print(f"\n  {verdict.upper()}: {reviewed} proposals{type_filter}{risk_filter}{conf_filter}")
    print(f"  Source: {source_label or '(none)'}")
    print(f"  Scan: {scan_id}")
    print(f"\n  Run 'train' to update the model with these reviews.\n")


# ─────────────────────────────────────────────────────────────────
# Train — aggregate reviews into a learned model
# ─────────────────────────────────────────────────────────────────
def train_model(db: ScannerDB, source_label: Optional[str] = None):
    """Aggregate all reviews into learned model rules.

    Without --source: trains the _baseline_ model from ALL reviews.
    With --source: trains a source-specific overlay from reviews tagged with that source.
    """
    target_label = source_label or "_baseline_"
    now = dt.datetime.now().isoformat()

    # Get review stats grouped by fix_type
    if source_label:
        rows = db.execute("""
            SELECT p.fix_type,
                   COUNT(*) as total,
                   SUM(CASE WHEN r.verdict = 'accept' THEN 1 ELSE 0 END) as accepted,
                   SUM(CASE WHEN r.verdict = 'reject' THEN 1 ELSE 0 END) as rejected,
                   SUM(CASE WHEN r.verdict = 'skip' THEN 1 ELSE 0 END) as skipped
            FROM reviews r
            JOIN proposals p ON r.proposal_id = p.proposal_id
            WHERE r.source_label = ?
            GROUP BY p.fix_type
        """, (source_label,)).fetchall()
    else:
        # Baseline: all reviews regardless of source
        rows = db.execute("""
            SELECT p.fix_type,
                   COUNT(*) as total,
                   SUM(CASE WHEN r.verdict = 'accept' THEN 1 ELSE 0 END) as accepted,
                   SUM(CASE WHEN r.verdict = 'reject' THEN 1 ELSE 0 END) as rejected,
                   SUM(CASE WHEN r.verdict = 'skip' THEN 1 ELSE 0 END) as skipped
            FROM reviews r
            JOIN proposals p ON r.proposal_id = p.proposal_id
            GROUP BY p.fix_type
        """).fetchall()

    if not rows:
        print(f"No reviews found{' for source: ' + source_label if source_label else ''}.")
        print("Run 'review' first to accept/reject proposals.")
        return

    print(f"\n{'═' * 60}")
    print(f" Training Model: {target_label}")
    print(f"{'═' * 60}\n")

    for row in rows:
        fix_type = row["fix_type"]
        accepted = row["accepted"]
        rejected = row["rejected"]
        total_judged = accepted + rejected  # skip doesn't count
        risk = RISK_TIERS.get(fix_type, "moderate")
        grad_threshold = GRADUATION_THRESHOLDS.get(risk, 50)

        if total_judged == 0:
            hit_rate = 0.0
        else:
            hit_rate = accepted / total_judged

        # Determine graduation
        graduated = 1 if total_judged >= grad_threshold and hit_rate >= 0.90 else 0

        # Learned confidence: blend hardcoded default with observed hit rate
        # Weight shifts toward hit_rate as we get more data
        data_weight = min(total_judged / (grad_threshold * 2), 0.8)
        default_conf = RISK_THRESHOLDS.get(risk, 0.92)
        learned_conf = round(default_conf * (1 - data_weight) + hit_rate * data_weight, 4)

        # Upsert into model_rules
        db.execute("""
            INSERT INTO model_rules
                (fix_type, source_label, risk_tier, total_reviewed,
                 total_accepted, total_rejected, hit_rate, learned_confidence,
                 graduated, last_trained_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(fix_type, source_label) DO UPDATE SET
                total_reviewed = excluded.total_reviewed,
                total_accepted = excluded.total_accepted,
                total_rejected = excluded.total_rejected,
                hit_rate = excluded.hit_rate,
                learned_confidence = excluded.learned_confidence,
                graduated = excluded.graduated,
                last_trained_at = excluded.last_trained_at
        """, (fix_type, target_label, risk, total_judged, accepted, rejected,
              round(hit_rate, 4), learned_conf, graduated, now))

        # Output
        grad_mark = "GRADUATED" if graduated else f"{total_judged}/{grad_threshold}"
        risk_label = {"safe": "SAFE", "moderate": "MOD ", "destructive": "DEST"}.get(risk, "?")
        print(f"  {fix_type:25s}  [{risk_label}]  "
              f"hit: {hit_rate:.0%} ({accepted}/{total_judged})  "
              f"conf: {learned_conf:.2f}  {grad_mark}")

    db.commit()
    print(f"\n  Model updated: {target_label}")
    print(f"  Run 'model' to see current model state.\n")


# ─────────────────────────────────────────────────────────────────
# Model — show current learned model state
# ─────────────────────────────────────────────────────────────────
def print_model(db: ScannerDB, source_label: Optional[str] = None):
    """Print the current learned model: hit rates, graduation status, risk tiers."""
    if source_label:
        rows = db.execute(
            "SELECT * FROM model_rules WHERE source_label = ? ORDER BY fix_type",
            (source_label,)).fetchall()
        if not rows:
            print(f"No trained rules for source: {source_label}")
            return
    else:
        rows = db.execute(
            "SELECT * FROM model_rules ORDER BY source_label, fix_type").fetchall()
        if not rows:
            print("No trained model yet. Run: review → train")
            return

    print(f"\n{'═' * 60}")
    print(f" Learned Model")
    print(f"{'═' * 60}\n")
    print(f" {'Rule':25s}  {'Source':15s}  {'Risk':4s}  {'Hit Rate':>8s}  "
          f"{'Conf':>5s}  {'Reviews':>7s}  Status")
    print(f" {'─'*25}  {'─'*15}  {'─'*4}  {'─'*8}  {'─'*5}  {'─'*7}  {'─'*12}")

    current_source = None
    for row in rows:
        if row["source_label"] != current_source:
            current_source = row["source_label"]
            if current_source != "_baseline_":
                print()  # Visual separator between sources

        risk_label = {"safe": "SAFE", "moderate": "MOD", "destructive": "DEST"}.get(
            row["risk_tier"], "?")
        status = "GRADUATED" if row["graduated"] else "learning"
        grad_threshold = GRADUATION_THRESHOLDS.get(row["risk_tier"], 50)
        progress = f"({row['total_reviewed']}/{grad_threshold})" if not row["graduated"] else ""

        print(f" {row['fix_type']:25s}  {row['source_label']:15s}  "
              f"{risk_label:4s}  {row['hit_rate']:7.0%}   "
              f"{row['learned_confidence']:5.2f}  {row['total_reviewed']:7d}  "
              f"{status} {progress}")

    # Summary stats
    total_rules = len(rows)
    graduated = sum(1 for r in rows if r["graduated"])
    total_reviews = sum(r["total_reviewed"] for r in rows)
    sources = len(set(r["source_label"] for r in rows))

    print(f"\n  {total_rules} rules across {sources} source(s)")
    print(f"  {graduated} graduated, {total_rules - graduated} still learning")
    print(f"  {total_reviews} total reviews\n")


# ─────────────────────────────────────────────────────────────────
# Source profiles — manage named source groupings
# ─────────────────────────────────────────────────────────────────
def manage_source_profile(db: ScannerDB, action: str, name: Optional[str] = None,
                          description: Optional[str] = None,
                          labels: Optional[List[str]] = None):
    """Create, list, or update source profiles."""
    if action == "list":
        rows = db.execute("SELECT * FROM source_profiles ORDER BY name").fetchall()
        if not rows:
            print("No source profiles defined.")
            print("Create one: python raagdosa_scanner.py source create beatport "
                  "--labels 'bp march,bp april' --desc 'Beatport downloads'")
            return

        print(f"\n{'═' * 60}")
        print(f" Source Profiles")
        print(f"{'═' * 60}\n")
        for row in rows:
            label_list = row["scan_labels"] or "(no labels)"
            print(f"  {row['name']:20s}  {row['description'] or ''}")
            print(f"  {'':20s}  labels: {label_list}")

            # Show model status for this source
            model_row = db.execute(
                "SELECT COUNT(*) as rules, SUM(graduated) as grad, "
                "SUM(total_reviewed) as reviews "
                "FROM model_rules WHERE source_label = ?",
                (row["name"],)).fetchone()
            if model_row and model_row["rules"]:
                print(f"  {'':20s}  model: {model_row['rules']} rules, "
                      f"{model_row['grad'] or 0} graduated, "
                      f"{model_row['reviews'] or 0} reviews")
            print()
        return

    if action == "create" and name:
        now = dt.datetime.now().isoformat()
        label_str = ",".join(labels) if labels else ""
        try:
            db.execute(
                "INSERT INTO source_profiles (name, description, created_at, scan_labels) "
                "VALUES (?, ?, ?, ?)",
                (name, description, now, label_str))
            db.commit()
            print(f"  Created source profile: {name}")
            if label_str:
                print(f"  Labels: {label_str}")
        except sqlite3.IntegrityError:
            # Update existing
            db.execute(
                "UPDATE source_profiles SET description=?, scan_labels=? WHERE name=?",
                (description, label_str, name))
            db.commit()
            print(f"  Updated source profile: {name}")
        return

    if action == "add-labels" and name and labels:
        row = db.execute("SELECT scan_labels FROM source_profiles WHERE name=?",
                         (name,)).fetchone()
        if not row:
            print(f"Profile '{name}' not found.")
            return
        existing = set(row["scan_labels"].split(",")) if row["scan_labels"] else set()
        existing.update(labels)
        db.execute("UPDATE source_profiles SET scan_labels=? WHERE name=?",
                   (",".join(sorted(existing)), name))
        db.commit()
        print(f"  Updated labels for {name}: {','.join(sorted(existing))}")
        return


# ─────────────────────────────────────────────────────────────────
# Model export — portable YAML for the main app
# ─────────────────────────────────────────────────────────────────
def export_model(db: ScannerDB, output_path: str = "raagdosa_model.yaml"):
    """Export the learned model as a portable YAML file for raagdosa.py to consume."""
    if yaml is None:
        print("ERROR: pyyaml required. Install: pip install pyyaml", file=sys.stderr)
        return

    rows = db.execute("SELECT * FROM model_rules ORDER BY source_label, fix_type").fetchall()
    if not rows:
        print("No trained model to export. Run: review → train")
        return

    model: Dict[str, Any] = {
        "scanner_version": VERSION,
        "exported_at": dt.datetime.now().isoformat(),
        "risk_tiers": dict(RISK_TIERS),
        "risk_thresholds": dict(RISK_THRESHOLDS),
        "graduation_thresholds": dict(GRADUATION_THRESHOLDS),
    }

    # Group by source
    sources: Dict[str, Dict] = {}
    for row in rows:
        src = row["source_label"]
        if src not in sources:
            sources[src] = {"rules": {}}
        sources[src]["rules"][row["fix_type"]] = {
            "risk": row["risk_tier"],
            "hit_rate": row["hit_rate"],
            "learned_confidence": row["learned_confidence"],
            "graduated": bool(row["graduated"]),
            "total_reviewed": row["total_reviewed"],
        }

    model["baseline"] = sources.pop("_baseline_", {"rules": {}})
    model["sources"] = sources

    # Source profile metadata
    profiles = db.execute("SELECT * FROM source_profiles").fetchall()
    model["source_profiles"] = {
        row["name"]: {
            "description": row["description"],
            "scan_labels": row["scan_labels"].split(",") if row["scan_labels"] else [],
        }
        for row in profiles
    }

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.dump(model, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    baseline_rules = len(model["baseline"].get("rules", {}))
    source_count = len(model["sources"])
    graduated = sum(1 for src in [model["baseline"]] + list(model["sources"].values())
                    for r in src.get("rules", {}).values() if r.get("graduated"))

    print(f"\n  Model exported: {output_path}")
    print(f"  Baseline rules: {baseline_rules}")
    print(f"  Source overlays: {source_count}")
    print(f"  Graduated rules: {graduated}")
    print(f"\n  Load in raagdosa.py: model_path: {output_path}\n")


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        prog="raagdosa_scanner",
        description="RaagDosa Tag Scanner — learn patterns from your music library",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            training workflow:
              1. Scan:    %(prog)s scan ~/Music/Beatport --label "beatport"
              2. Review:  %(prog)s proposals  (inspect what it found)
              3. Accept:  %(prog)s review --accept --risk safe
              4. Reject:  %(prog)s review --reject --type encoding_repair --max-conf 0.85
              5. Train:   %(prog)s train  (builds baseline model)
              6. Train:   %(prog)s train --source beatport  (builds source overlay)
              7. Model:   %(prog)s model  (see learned hit rates)
              8. Export:   %(prog)s model-export -o raagdosa_model.yaml

            source profiles:
              %(prog)s source create beatport --labels "bp march" "bp april" --desc "Beatport downloads"
              %(prog)s source create soundcloud --desc "SoundCloud rips"
              %(prog)s source list

            other commands:
              %(prog)s report
              %(prog)s patterns
              %(prog)s export --output findings.yaml
              %(prog)s history
              %(prog)s undo abc123
        """))

    parser.add_argument("--db", default="raagdosa_scanner.db",
                        help="Path to scanner database (default: ./raagdosa_scanner.db)")
    parser.add_argument("--logs-dir", default="scanner_logs",
                        help="Directory for session log files (default: ./scanner_logs)")
    parser.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")

    sub = parser.add_subparsers(dest="command", help="Command to run")

    # scan
    p_scan = sub.add_parser("scan", help="Scan a folder tree and log all tag data")
    p_scan.add_argument("path", help="Root folder to scan")
    p_scan.add_argument("--label", "-l", help="Human label for this scan (used for source profiling)")
    p_scan.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    # report
    p_report = sub.add_parser("report", help="Print summary report")
    p_report.add_argument("--scan", help="Specific scan ID (default: latest)")
    p_report.add_argument("--output", "-o", help="Write report to file (also prints to stdout)")

    # proposals
    p_proposals = sub.add_parser("proposals", help="Show detailed fix proposals with reasons")
    p_proposals.add_argument("--scan", help="Specific scan ID (default: latest)")
    p_proposals.add_argument("--output", "-o", help="Write proposals to file (also prints to stdout)")
    p_proposals.add_argument("--folder", help="Filter by folder name")
    p_proposals.add_argument("--type", help="Filter by fix type")

    # review — accept/reject proposals
    p_review = sub.add_parser("review", help="Accept or reject proposals to train the model")
    p_review.add_argument("--scan", help="Specific scan ID (default: latest)")
    p_review.add_argument("--accept", action="store_true", help="Accept matching proposals")
    p_review.add_argument("--reject", action="store_true", help="Reject matching proposals")
    p_review.add_argument("--folder", help="Filter by folder name")
    p_review.add_argument("--type", help="Filter by fix type")
    p_review.add_argument("--risk", choices=["safe", "moderate", "destructive"],
                          help="Filter by risk tier")
    p_review.add_argument("--min-conf", type=float, default=0.0,
                          help="Minimum confidence to review")
    p_review.add_argument("--max-conf", type=float, default=1.0,
                          help="Maximum confidence to review")
    p_review.add_argument("--source", help="Source label for this review batch")

    # train — build model from reviews
    p_train = sub.add_parser("train", help="Aggregate reviews into the learned model")
    p_train.add_argument("--source", help="Train source-specific overlay (default: baseline)")

    # model — show model state
    p_model = sub.add_parser("model", help="Show current learned model state")
    p_model.add_argument("--source", help="Filter to specific source")

    # model-export — export portable model file
    p_mexport = sub.add_parser("model-export", help="Export learned model as YAML for raagdosa.py")
    p_mexport.add_argument("--output", "-o", default="raagdosa_model.yaml",
                           help="Output file path")

    # source — manage source profiles
    p_source = sub.add_parser("source", help="Manage source profiles")
    p_source.add_argument("action", choices=["list", "create", "add-labels"],
                          help="Action to perform")
    p_source.add_argument("name", nargs="?", help="Profile name")
    p_source.add_argument("--labels", nargs="+", help="Scan labels to associate")
    p_source.add_argument("--desc", help="Profile description")

    # patterns
    p_patterns = sub.add_parser("patterns", help="Show recurring patterns across scans")
    p_patterns.add_argument("--scan", help="Specific scan ID (default: all)")

    # export
    p_export = sub.add_parser("export", help="Export findings as YAML for RaagDosa import")
    p_export.add_argument("--output", "-o", default="scanner_findings.yaml",
                          help="Output file path")
    p_export.add_argument("--scan", help="Specific scan ID (default: latest)")
    p_export.add_argument("--min-confidence", type=float, default=0.70,
                          help="Minimum confidence for exported suggestions")

    # history
    sub.add_parser("history", help="List all scans")

    # undo
    p_undo = sub.add_parser("undo", help="Mark a scan's proposals as undone")
    p_undo.add_argument("scan_id", help="Scan ID to undo")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    db = ScannerDB(args.db)

    try:
        logs_dir = Path(args.logs_dir)

        if args.command == "scan":
            root = Path(args.path).expanduser().resolve()
            if not root.is_dir():
                print(f"ERROR: {root} is not a directory", file=sys.stderr)
                sys.exit(1)

            # Auto-label from folder name + date when not explicitly provided
            if not args.label:
                folder_name = root.name.replace(" ", "_")
                # For nested paths like SLSK/complete, use parent_child
                if len(folder_name) <= 3 or folder_name.lower() in ("music", "songs", "files", "downloads"):
                    folder_name = f"{root.parent.name}_{root.name}".replace(" ", "_")
                date_stamp = dt.datetime.now().strftime("%Y%m%d")
                args.label = f"{folder_name}_{date_stamp}"

            # Pre-generate scan_id so we can name the log file
            scan_id_preview = str(uuid.uuid4())[:12]
            log_path = get_scan_log_path(logs_dir, scan_id_preview, args.label, "scan")

            with TeeOutput(log_path):
                print(f"\n  Scanning: {root}")
                print(f"  Label:    {args.label}")
                scan_id = scan_folder(root, db, label=args.label, verbose=args.verbose)
                print(f"\n  Scan complete: {scan_id}")
                print(f"  View report:    python raagdosa_scanner.py report --scan {scan_id}")
                print(f"  View proposals: python raagdosa_scanner.py proposals --scan {scan_id}")
                print()

                # Auto-show summary
                print_report(db, scan_id)

                # Auto-show proposals
                print_proposals(db, scan_id)

            # Rename log to use actual scan_id
            actual_log = log_path.parent / log_path.name.replace(scan_id_preview, scan_id)
            if log_path != actual_log:
                log_path.rename(actual_log)
                log_path = actual_log

            print(f"\n  Session log: {log_path}\n")

        elif args.command == "report":
            if getattr(args, "output", None):
                with TeeOutput(Path(args.output)):
                    print_report(db, args.scan)
                print(f"\n  Report written to: {args.output}")
            else:
                print_report(db, args.scan)

        elif args.command == "proposals":
            if getattr(args, "output", None):
                with TeeOutput(Path(args.output)):
                    print_proposals(db, args.scan, folder=args.folder, fix_type=args.type)
                print(f"\n  Proposals written to: {args.output}")
            else:
                print_proposals(db, args.scan, folder=args.folder, fix_type=args.type)

        elif args.command == "review":
            if not args.accept and not args.reject:
                print("ERROR: specify --accept or --reject", file=sys.stderr)
                sys.exit(1)
            if args.accept and args.reject:
                print("ERROR: cannot accept and reject at the same time", file=sys.stderr)
                sys.exit(1)
            verdict = "accept" if args.accept else "reject"
            review_proposals(db, scan_id=args.scan, folder=args.folder,
                             fix_type=args.type, verdict=verdict,
                             min_confidence=args.min_conf,
                             max_confidence=args.max_conf,
                             risk=args.risk, source_label=args.source)

        elif args.command == "train":
            train_model(db, source_label=args.source)

        elif args.command == "model":
            print_model(db, source_label=getattr(args, "source", None))

        elif args.command == "model-export":
            export_model(db, output_path=args.output)

        elif args.command == "source":
            manage_source_profile(db, action=args.action, name=args.name,
                                  description=getattr(args, "desc", None),
                                  labels=getattr(args, "labels", None))

        elif args.command == "patterns":
            print_patterns(db, args.scan)

        elif args.command == "export":
            export_findings(db, args.scan, args.output, args.min_confidence)

        elif args.command == "history":
            print_history(db)

        elif args.command == "undo":
            undo_scan(db, args.scan_id)

    finally:
        db.close()


if __name__ == "__main__":
    main()
