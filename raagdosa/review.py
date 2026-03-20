"""
RaagDosa review — review routing, summary building, review constants.

Layer 5 (CLI-only per architecture). This module contains:
- Pure-logic routing functions (no terminal output)
- Review reason constants and summary builders
- Collision resolution
- Format suffix application

The interactive_review() and apply_folder_moves() functions remain in
raagdosa_main.py because they are deeply coupled to terminal I/O.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from raagdosa.core import FolderProposal, now_iso
from raagdosa.tags import normalize_unicode
from raagdosa.files import ensure_dir


# ─────────────────────────────────────────────
# Review reason descriptions
# ─────────────────────────────────────────────
REASON_DESCRIPTIONS: Dict[str, str] = {
    "low_confidence": "Confidence score below threshold",
    "generic_folder_name": "Folder name is too generic to classify",
    "unreadable_ratio_high": "Too many tracks have unreadable tags",
    "heuristic_fallback": "Name derived from heuristics (no usable tags)",
    "ep": "Detected as EP release",
    "mix_folder": "Detected as DJ mix or chart compilation",
    "djcrate_singles": "Detected as DJ crate (singles/loose tracks)",
    "djcrate_set": "Detected as DJ set prep crate (preserved intact)",
}

FACTOR_DESCRIPTIONS: Dict[str, str] = {
    "tag_coverage": "Tag readability",
    "dominance": "Album/artist vote consensus",
    "title_quality": "Song title confidence",
    "filename_consistency": "Filename ↔ tag alignment",
    "completeness": "Track numbering completeness",
    "aa_consistency": "Album-artist consistency",
    "folder_alignment": "Folder name ↔ proposed name",
}

REVIEW_REASON_PRESETS = [
    "va-misclass",
    "wrong-artist",
    "bad-tags",
    "incomplete-release",
    "duplicate",
    "not-music",
    "wrong-genre",
    "needs-research",
]

# v6.2: generic/vague folder names that are not album names — force review
GENERIC_FOLDER_NAMES = {
    "a", "music", "complete", "down tempo", "downtempo", "warm up", "warm-up",
    "underground house", "haifa club", "estray", "rare tracks", "new", "misc",
    "unsorted", "temp", "incoming", "downloads", "stuff", "tracks", "songs",
    "playlist", "mix", "mixes", "cd1", "cd2", "cd3", "disc 1", "disc 2",
    "electronica-downtempo", "world electro - ethno disco - global groove",
    "jazz house piano bar", "master pieces of electro", "remixes and other tracks",
}


# ─────────────────────────────────────────────
# Review summary builder
# ─────────────────────────────────────────────
def build_review_summary(reasons: List[str], factors: Dict[str, float],
                         confidence: float) -> str:
    """Build a human-readable summary explaining why a folder was routed the way it was."""
    parts = []
    for r in reasons:
        base = r.split(":")[0]
        if base in REASON_DESCRIPTIONS:
            parts.append(REASON_DESCRIPTIONS[base])
        elif r.startswith("duplicate"):
            parts.append(f"Duplicate: {r.split(':', 1)[1] if ':' in r else 'name collision'}")
        elif r.startswith("already_in_clean"):
            parts.append("Already exists in Clean library")
        else:
            parts.append(r.replace("_", " ").capitalize())
    weak = [f"{FACTOR_DESCRIPTIONS.get(k, k)} ({v:.0%})"
            for k, v in factors.items()
            if isinstance(v, float) and v < 0.5 and k in FACTOR_DESCRIPTIONS]
    if weak:
        parts.append("Weak: " + ", ".join(weak))
    return ". ".join(parts)


# ─────────────────────────────────────────────
# Format suffix
# ─────────────────────────────────────────────
def apply_format_suffix(name: str, cfg: Dict[str, Any],
                        extensions: Optional[Dict[str, int]]) -> str:
    """Append format tag (e.g. [FLAC]) to folder name based on config."""
    sfx = cfg.get("format_suffix", {})
    if not sfx.get("enabled", True) or not extensions:
        return name
    if sfx.get("only_if_all_same_extension", True) and len(extensions) == 1:
        ext1 = next(iter(extensions.keys()))
        ignore_ext = (sfx.get("ignore_extension", ".mp3") or ".mp3").strip().lower()
        if (ext1 and ext1 != ignore_ext
                and sfx.get("style", "brackets_upper") == "brackets_upper"):
            name = f"{name} [{ext1.lstrip('.').upper()}]"
    return name


# ─────────────────────────────────────────────
# Collision resolution
# ─────────────────────────────────────────────
def collision_resolve(dst: Path, policy: str, suffix_fmt: str) -> Optional[Path]:
    if not dst.exists():
        return dst
    if policy == "skip":
        return None
    n = 1
    while True:
        cand = Path(str(dst) + suffix_fmt.format(n=n))
        if not cand.exists():
            return cand
        n += 1


# ─────────────────────────────────────────────
# Proposal routing (pure logic)
# ─────────────────────────────────────────────
def route_proposal(
    p: FolderProposal,
    cfg: Dict[str, Any],
    seen_names: Counter,
    existing_clean: Set[str],
    manifest_entries: Set[str],
    review_albums: Path,
    dup_root: Path,
    mixes_root: Path,
    all_proposals: Optional[List] = None,
) -> FolderProposal:
    """Route a single proposal to clean/review/duplicate. Mutates p in place."""
    rr = cfg.get("review_rules", {})
    min_conf = float(rr.get("min_confidence_for_clean", 0.85))
    sc = cfg.get("scan", {})
    max_unread = float(sc.get("max_unreadable_track_ratio", 0.25))
    reasons: List[str] = []
    dest = "clean"

    if rr.get("route_questionable_to_review", True) and p.confidence < min_conf:
        dest = "review"
        reasons.append("low_confidence")

    _fn_norm = p.folder_name.strip().strip("_- ").lower()
    if _fn_norm in GENERIC_FOLDER_NAMES:
        if dest == "clean":
            dest = "review"
        reasons.append("generic_folder_name")
        p.confidence = min(p.confidence, min_conf - 0.05)

    seen_names[p.proposed_folder_name] += 1
    if rr.get("route_duplicates", True) and seen_names[p.proposed_folder_name] > 1:
        dest = "duplicate"
        if all_proposals:
            colliders = [q.folder_name for q in all_proposals
                         if q.proposed_folder_name == p.proposed_folder_name and q is not p]
            reasons.append(
                f"duplicate_in_run:{colliders[0][:40] if colliders else '?'}")
        else:
            reasons.append("duplicate_in_run")

    norm_prop = normalize_unicode(p.proposed_folder_name)
    if rr.get("route_cross_run_duplicates", True) and (
            norm_prop in existing_clean or norm_prop in manifest_entries):
        dest = "duplicate"
        reasons.append("already_in_clean")

    if p.decision.get("unreadable_ratio", 0.0) > max_unread:
        dest = "review"
        reasons.append("unreadable_ratio_high")

    if p.decision.get("used_heuristic", False):
        if dest == "clean":
            dest = "review"
        reasons.append("heuristic_fallback")

    if p.stats.format_duplicates:
        reasons.append(f"format_dupes({len(p.stats.format_duplicates)})")

    if p.decision.get("is_ep"):
        reasons.append("ep")

    if p.decision.get("is_mix") and dest == "clean":
        reasons.append("mix_folder")
        ensure_dir(mixes_root)
        p.target_path = str(mixes_root / p.proposed_folder_name)

    if p.decision.get("is_crate"):
        _ct = p.decision.get("crate_type", "singles")
        dest = "review"
        if _ct == "set":
            reasons.append("djcrate_set")
            _sets_folder = cfg.get("djcrates", {}).get("keep_intact_routing", "_Sets")
            p.target_path = str(review_albums.parent / _sets_folder / p.folder_name)
        else:
            reasons.append("djcrate_singles")

    p.destination = dest
    p.decision["route_reasons"] = reasons
    if reasons:
        p.decision["review_summary"] = build_review_summary(
            reasons, p.decision.get("confidence_factors", {}), p.confidence)

    if dest == "review" and not p.decision.get("is_crate"):
        p.target_path = str(review_albums / p.proposed_folder_name)
    elif (dest == "review" and p.decision.get("is_crate")
          and p.decision.get("crate_type") != "set"):
        p.target_path = str(review_albums / p.proposed_folder_name)
    elif dest == "duplicate":
        p.target_path = str(dup_root / p.proposed_folder_name)

    return p


# ─────────────────────────────────────────────
# Review sidecar
# ─────────────────────────────────────────────
def write_review_sidecar(folder_path: Path, proposal: FolderProposal,
                         session_id: str) -> None:
    """Write .raagdosa_review.json inside a Review folder with routing rationale."""
    sidecar = folder_path / ".raagdosa_review.json"
    data = {
        "session_id": session_id,
        "timestamp": now_iso(),
        "original_folder": proposal.folder_name,
        "proposed_name": proposal.proposed_folder_name,
        "confidence": proposal.confidence,
        "destination": proposal.destination,
        "route_reasons": proposal.decision.get("route_reasons", []),
        "review_summary": proposal.decision.get("review_summary", ""),
        "confidence_factors": {
            k: round(v, 3)
            for k, v in proposal.decision.get("confidence_factors", {}).items()
        },
        "artist": proposal.decision.get("albumartist_display", ""),
        "album": proposal.decision.get("dominant_album_display", ""),
        "is_va": proposal.decision.get("is_va", False),
        "user_notes": [],
    }
    try:
        sidecar.write_text(
            json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
