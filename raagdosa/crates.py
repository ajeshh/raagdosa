"""
RaagDosa crates — DJ crate detection, embedded release discovery.

Layer 4: imports from scoring (L2), naming (L2), artists (L2), files (L1),
         library (L3), session (L3). No terminal output.
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from raagdosa.scoring import detect_track_gaps
from raagdosa.naming import normalize_for_vote, is_garbage_tag_value
from raagdosa.artists import normalize_artist_name
from raagdosa.files import sanitize_name

import os
import shutil
import uuid

from raagdosa.core import now_iso, CrateTrackPlan, EmbeddedRelease
from raagdosa.files import (ensure_dir, append_jsonl, cleanup_empty_parents,
    _same_device, _restore_creation_date)
from raagdosa.tagreader import read_audio_tags
from raagdosa.library import _resolve_lib_cfg
from raagdosa.session import derive_clean_albums_root, derive_review_albums_root
from raagdosa.ui import C, out, err, status_tag, conf_color


# ─────────────────────────────────────────────
# Crate keyword defaults
# ─────────────────────────────────────────────
_CRATE_FOLDER_KW_DEFAULTS = [
    "singles", "unsorted", "to sort", "downloads", "incoming",
    "misc", "dump", "random", "temp", "new music", "promos", "edits",
    "tools", "bangers",
]


# ─────────────────────────────────────────────
# Crate helper functions
# ─────────────────────────────────────────────
def is_unknown_album(album: str, cfg: Dict[str, Any]) -> bool:
    """Return True if album tag is blank or matches an unknown-album placeholder."""
    if not album or not album.strip():
        return True
    dc = cfg.get("djcrates", {})
    patterns = [p.lower() for p in dc.get("unknown_album_patterns",
                                          ["unknown album", "untitled", ""])]
    return album.strip().lower() in patterns


def folder_matches_set_patterns(folder_name: str, cfg: Dict[str, Any]) -> bool:
    """Return True if folder name matches set/gig prep patterns."""
    dc = cfg.get("djcrates", {})
    patterns = dc.get("keep_intact_patterns", [])
    custom = dc.get("custom_set_patterns", [])
    for pat in patterns + custom:
        try:
            if re.search(pat, folder_name):
                return True
        except re.error:
            pass
    return False


def folder_matches_crate_keywords(folder_name: str, cfg: Dict[str, Any]) -> float:
    """Return 1.0 if folder name matches crate keywords, 0.0 otherwise."""
    dc = cfg.get("djcrates", {})
    kw_list = dc.get("folder_name_patterns", _CRATE_FOLDER_KW_DEFAULTS)
    fn_lower = folder_name.lower()
    for kw in kw_list:
        if kw.lower() in fn_lower:
            return 1.0
    for pat in dc.get("custom_crate_patterns", []):
        try:
            if re.search(pat, folder_name):
                return 1.0
        except re.error:
            pass
    return 0.0


# ─────────────────────────────────────────────
# DJ Crate detection
# ─────────────────────────────────────────────
def detect_djcrate(
    folder_name: str,
    albums_norm: Counter,
    track_artists_norm: Counter,
    all_tags: List[Dict[str, Optional[str]]],
    total_tracks: int,
    dom_alb_share: float,
    dom_alb_n: Optional[str],
    dom_aa_share: float,
    dom_aa_n: Optional[str],
    cfg: Dict[str, Any],
) -> Tuple[bool, str, float, str]:
    """
    Detect whether a folder is a DJ crate rather than a true VA compilation.
    Returns (is_crate, crate_type, crate_confidence, reason).
    """
    dc = cfg.get("djcrates", {})
    if not dc.get("enabled", True):
        return False, "", 0.0, "djcrates_disabled"

    min_tracks = int(dc.get("min_tracks", 3))
    if total_tracks < min_tracks:
        return False, "", 0.0, "too_few_tracks"

    # Learned veto patterns
    for veto_pat in dc.get("crate_veto_patterns", []):
        try:
            if re.search(veto_pat, folder_name):
                return False, "", 0.0, "learned_veto_pattern"
        except re.error:
            pass

    # Set prep detection
    if folder_matches_set_patterns(folder_name, cfg):
        return True, "set", 0.9, "set_prep_pattern_match"

    # Hard overrides: NOT a crate
    veto_threshold = float(dc.get("album_coherence_veto", 0.6))

    if dom_alb_share >= veto_threshold and dom_alb_n and not is_unknown_album(dom_alb_n, cfg):
        return False, "", 0.0, "album_coherence_veto"

    comp_flags = [t.get("compilation") for t in all_tags if t.get("compilation")]
    if total_tracks > 0:
        comp_share = len(comp_flags) / total_tracks
        if comp_share >= 0.7 and dom_alb_share >= 0.5:
            return False, "", 0.0, "compilation_flag_veto"

    _va_kws = {"various artists", "various", "va", "v/a", "v.a.", "v.a", "vvaa"}
    if dom_aa_n and dom_aa_n.lower() in _va_kws and dom_aa_share >= 0.8 and dom_alb_share >= 0.5:
        return False, "", 0.0, "properly_tagged_va"

    # Sequential track numbering veto
    track_nums: List[int] = []
    for t in all_tags:
        tn = t.get("tracknumber")
        if tn:
            m = re.match(r"(\d+)", str(tn))
            if m:
                try:
                    track_nums.append(int(m.group(1)))
                except ValueError:
                    pass
    if len(track_nums) >= total_tracks * 0.8 and total_tracks >= 3:
        sorted_nums = sorted(track_nums)
        unique_nums = sorted(set(track_nums))
        has_dupes = len(track_nums) != len(unique_nums)
        gaps = detect_track_gaps(unique_nums)
        if not has_dupes and not gaps and unique_nums == list(
                range(unique_nums[0], unique_nums[-1] + 1)):
            return False, "", 0.0, "sequential_tracks_veto"

    # Weighted scoring
    effective_unique = 0
    if total_tracks > 0:
        blank_count = sum(1 for t in all_tags if is_unknown_album(t.get("album", ""), cfg))
        real_albums = set()
        for t in all_tags:
            alb = (t.get("album") or "").strip()
            if alb and not is_unknown_album(alb, cfg):
                real_albums.add(normalize_for_vote(alb, cfg))
        effective_unique = len(real_albums) + blank_count
    album_diversity = min(1.0, effective_unique / max(total_tracks, 1))
    s_album_diversity = album_diversity * 0.35

    low_quality_count = 0
    fn_lower = folder_name.lower().strip()
    for t in all_tags:
        alb = (t.get("album") or "").strip()
        if is_unknown_album(alb, cfg):
            low_quality_count += 1
        elif alb.lower().strip() == fn_lower:
            low_quality_count += 1
    album_quality = low_quality_count / max(total_tracks, 1)
    s_album_quality = album_quality * 0.20

    if track_nums and total_tracks >= 3:
        gaps_list = detect_track_gaps(sorted(set(track_nums)))
        gap_ratio = len(gaps_list) / max(total_tracks, 1)
        dupe_count = len(track_nums) - len(set(track_nums))
        dupe_ratio = dupe_count / max(total_tracks, 1)
        no_tn_ratio = (total_tracks - len(track_nums)) / max(total_tracks, 1)
        track_incoherence = min(1.0, gap_ratio + dupe_ratio + no_tn_ratio)
    elif not track_nums:
        track_incoherence = 1.0
    else:
        track_incoherence = 0.5
    s_track_incoherence = track_incoherence * 0.15

    kw_match = folder_matches_crate_keywords(folder_name, cfg)
    s_folder_keywords = kw_match * 0.15

    if total_tracks > 0:
        comp_absent = (total_tracks - len(comp_flags)) / total_tracks
    else:
        comp_absent = 1.0
    s_comp_absent = comp_absent * 0.15

    total_score = (s_album_diversity + s_album_quality + s_track_incoherence
                   + s_folder_keywords + s_comp_absent)

    threshold = float(dc.get("detection_threshold", 0.55))
    known_labels = [l.lower() for l in cfg.get("reference", {}).get("known_labels", []) if l]
    if known_labels and dom_alb_n:
        if any(lab in dom_alb_n.lower() for lab in known_labels):
            threshold = 0.85

    if total_score >= threshold:
        reason_parts = []
        if s_album_diversity > 0.15:
            reason_parts.append(f"album_diversity({album_diversity:.2f})")
        if s_album_quality > 0.10:
            reason_parts.append(f"album_quality({album_quality:.2f})")
        if s_track_incoherence > 0.07:
            reason_parts.append(f"track_incoherence({track_incoherence:.2f})")
        if kw_match > 0:
            reason_parts.append("folder_keyword_match")
        if s_comp_absent > 0.10:
            reason_parts.append("no_compilation_flags")
        reason = ", ".join(reason_parts) if reason_parts else "score_above_threshold"
        return True, "singles", total_score, reason

    return False, "", total_score, "score_below_threshold"


# ─────────────────────────────────────────────
# Embedded release detection helpers
# ─────────────────────────────────────────────
def normalize_album_for_cluster(album: str, cfg: Dict[str, Any]) -> str:
    """Normalize album name for clustering: strip EP/LP suffix, lowercase, strip punctuation."""
    if not album:
        return ""
    a = album.strip().lower()
    a = re.sub(r'\s*[\[\(]\s*e\.?p\.?\s*[\]\)]\s*$', '', a, flags=re.I)
    a = re.sub(r'\s+e\.?p\.?\s*$', '', a, flags=re.I)
    a = re.sub(r'\s*[\[\(]\s*l\.?p\.?\s*[\]\)]\s*$', '', a, flags=re.I)
    a = re.sub(r'\s+l\.?p\.?\s*$', '', a, flags=re.I)
    a = re.sub(r'[^\w\s]', '', a).strip()
    a = re.sub(r'\s+', ' ', a)
    return a


def resolve_crate_collision(dst: Path) -> Path:
    """Resolve filename collision by appending a numeric suffix."""
    if not dst.exists():
        return dst
    n = 1
    while True:
        cand = dst.with_name(dst.stem + f" ({n})" + dst.suffix)
        if not cand.exists():
            return cand
        n += 1


# ── Crate explosion (extracted from monolith) ──────────────────

def detect_embedded_releases(
    audio_files:List[Path],
    cfg:Dict[str,Any],
    min_tracks:int=3
)->Tuple[List[EmbeddedRelease],Set[str]]:
    """
    Detect coherent album/EP releases hidden within a DJ crate.

    Groups tracks by (normalized_artist, normalized_album). Clusters with
    min_tracks+ tracks, non-placeholder album, and near-sequential track
    numbers are flagged as embedded releases.

    Returns (releases, release_file_paths) where release_file_paths is a set
    of str(Path) for quick membership checks.

    v9.0 Phase 3.
    """
    dc=cfg.get("djcrates",{})
    # Group tracks by (artist_norm, album_norm)
    groups:Dict[Tuple[str,str],List[Tuple[Path,Dict[str,Optional[str]]]]]={}
    for f in audio_files:
        tags=read_audio_tags(f,cfg)
        artist_raw=(tags.get("artist") or "").strip()
        album_raw=(tags.get("album") or "").strip()
        if not artist_raw or not album_raw: continue
        if is_unknown_album(album_raw,cfg): continue

        artist_n=normalize_for_vote(artist_raw,cfg)
        album_n=normalize_album_for_cluster(album_raw,cfg)
        if not artist_n or not album_n: continue
        groups.setdefault((artist_n,album_n),[]).append((f,tags))

    releases:List[EmbeddedRelease]=[]
    release_paths:Set[str]=set()

    for (artist_n,album_n),tracks_and_tags in groups.items():
        if len(tracks_and_tags)<min_tracks: continue

        # Extract track numbers
        track_nums:List[Tuple[int,Path,Dict]]=[]
        total_in_album:Optional[int]=None
        for f,tags in tracks_and_tags:
            tn_raw=tags.get("tracknumber","")
            if not tn_raw: continue
            tn_str=str(tn_raw).strip()
            # Handle "X/Y" format
            m=re.match(r"(\d+)\s*/\s*(\d+)",tn_str)
            if m:
                try:
                    num=int(m.group(1))
                    tot=int(m.group(2))
                    if total_in_album is None: total_in_album=tot
                    track_nums.append((num,f,tags))
                except ValueError: pass
                continue
            m2=re.match(r"(\d+)",tn_str)
            if m2:
                try: track_nums.append((int(m2.group(1)),f,tags))
                except ValueError: pass

        if len(track_nums)<min_tracks: continue

        # Check for near-sequential numbering (allow 1 gap)
        nums_sorted=sorted(set(n for n,_,_ in track_nums))
        if len(nums_sorted)<min_tracks: continue  # dupes reduced count below threshold

        # Compute gaps
        expected_range=list(range(nums_sorted[0],nums_sorted[-1]+1))
        gaps=[n for n in expected_range if n not in nums_sorted]
        max_allowed_gaps=max(1,len(nums_sorted)//4)  # allow ~25% gaps for partial albums

        if len(gaps)>max_allowed_gaps: continue  # too many gaps — not a coherent release

        # Check for duplicates (multiple tracks claiming same number)
        num_counts=Counter(n for n,_,_ in track_nums)
        if any(c>1 for c in num_counts.values()): continue  # dupes = not a clean release

        # Build the release
        # Get display forms from first track's tags
        first_tags=tracks_and_tags[0][1]
        artist_display=normalize_artist_name((first_tags.get("artist") or "").strip(),cfg) or artist_n
        album_display=(first_tags.get("album") or "").strip()

        is_partial=False
        if total_in_album and len(nums_sorted)<total_in_album:
            is_partial=True
        elif not total_in_album and gaps:
            is_partial=True

        release_files=[f for _,f,_ in sorted(track_nums)]
        release_nums=[n for n,_,_ in sorted(track_nums)]

        rel=EmbeddedRelease(
            artist=artist_n,artist_display=artist_display,
            album=album_n,album_display=album_display,
            tracks=release_files,track_numbers=release_nums,
            total_in_album=total_in_album,is_partial=is_partial)
        releases.append(rel)
        for f in release_files:
            release_paths.add(str(f))

    return releases,release_paths


def build_crate_explosion_plan(
    folder:Path,
    audio_files:List[Path],
    cfg:Dict[str,Any],
    profile:Dict[str,Any],
    source_root:Path,
    crate_confidence:float
)->Tuple[List[CrateTrackPlan],List[EmbeddedRelease]]:
    """
    Build per-track routing plan for a DJ crate.
    Singles route to {base}/{Artist}/Singles/{filename}.
    Embedded releases route to {base}/{Artist}/{Album}/NN - Title.ext.

    v9.0 Phase 2+3: explosion routing with embedded release detection.

    Returns (plans, embedded_releases).
    """
    lib=_resolve_lib_cfg(profile,cfg)
    singles_folder=lib.get("singles_folder","Singles")
    unknown_label=lib.get("unknown_artist_label","_Unknown")
    dc=cfg.get("djcrates",{})
    default_routing=dc.get("default_routing","review")

    # Determine base paths
    clean_albums=derive_clean_albums_root(profile,source_root)
    review_albums=derive_review_albums_root(profile,source_root)

    # Phase 3: detect embedded releases first
    embedded_releases,release_paths=detect_embedded_releases(audio_files,cfg)

    # Build a lookup: file path -> (release, track_number) for embedded release tracks
    _release_lookup:Dict[str,Tuple[EmbeddedRelease,int]]={}
    for rel in embedded_releases:
        for f,tn in zip(rel.tracks,rel.track_numbers):
            _release_lookup[str(f)]=(rel,tn)

    plans:List[CrateTrackPlan]=[]
    for f in sorted(audio_files):
        tags=read_audio_tags(f,cfg)
        artist_raw=(tags.get("artist") or "").strip()
        title_raw=(tags.get("title") or "").strip()

        # Determine artist folder
        if artist_raw:
            artist_clean=normalize_artist_name(artist_raw,cfg) or sanitize_name(artist_raw)
        else:
            artist_clean=None

        # Per-track confidence
        t_conf=0.0; t_reasons=[]
        if artist_raw: t_conf+=0.30
        else: t_reasons.append("no_artist")
        if title_raw: t_conf+=0.25
        else: t_reasons.append("no_title")
        if tags.get("genre"): t_conf+=0.10
        if tags.get("year"): t_conf+=0.05
        if tags.get("bpm"): t_conf+=0.05
        if artist_raw and not is_garbage_tag_value(artist_raw): t_conf+=0.15
        else: t_reasons.append("noisy_artist")
        if title_raw and not is_garbage_tag_value(title_raw): t_conf+=0.10
        else: t_reasons.append("noisy_title")
        t_conf=min(1.0,t_conf)

        # Route decision
        min_clean=float(cfg.get("review_rules",{}).get("min_confidence_for_clean",0.85))

        # Check if this track belongs to an embedded release
        _rel_info=_release_lookup.get(str(f))
        if _rel_info:
            rel,track_num=_rel_info
            # Embedded release: route as album track
            # Use album-style naming: NN - Title.ext
            album_display=sanitize_name(rel.album_display)
            if title_raw:
                # Zero-pad track number based on total tracks
                pad=2 if (rel.total_in_album or len(rel.tracks))<100 else 3
                track_name=f"{track_num:0{pad}d} - {sanitize_name(title_raw)}{f.suffix.lower()}"
            else:
                track_name=f.name

            # Embedded releases get a confidence boost — they have album coherence
            t_conf=min(1.0,t_conf+0.10)
            if default_routing=="review" or t_conf<min_clean:
                dest="review"; base=review_albums
            else:
                dest="clean"; base=clean_albums

            if artist_clean:
                target=base/artist_clean/album_display/track_name
            else:
                target=base/"_Unsorted"/album_display/track_name

            _partial_note=f" (partial {len(rel.tracks)}/{rel.total_in_album})" if rel.is_partial and rel.total_in_album else ""
            reason=f"embedded_release: {rel.album_display}{_partial_note}"
            plans.append(CrateTrackPlan(
                source_path=str(f),filename=f.name,
                artist=artist_clean,title=title_raw,
                target_path=str(target),destination=dest,
                confidence=t_conf,reason=reason,
                embedded_release=rel.album_display,track_number=track_num))
        else:
            # Regular single — route to Singles/
            if title_raw:
                track_name=sanitize_name(title_raw)+f.suffix.lower()
            else:
                track_name=f.name

            if default_routing=="review" or not artist_raw or not title_raw or t_conf<min_clean:
                dest="review"; base=review_albums
            else:
                dest="clean"; base=clean_albums

            if artist_clean:
                target=base/artist_clean/singles_folder/track_name
            else:
                target=base/"_Unsorted"/singles_folder/track_name

            reason=", ".join(t_reasons) if t_reasons else "ok"
            plans.append(CrateTrackPlan(
                source_path=str(f),filename=f.name,
                artist=artist_clean,title=title_raw,
                target_path=str(target),destination=dest,
                confidence=t_conf,reason=reason))

    return plans,embedded_releases


def apply_crate_explosion(
    folder:Path,
    plans:List[CrateTrackPlan],
    session_id:str,
    cfg:Dict[str,Any],
    dry_run:bool=False,
    source_root:Optional[Path]=None
)->List[Dict[str,Any]]:
    """
    Execute per-track moves for a crate explosion.
    Each track is moved individually to its computed target.
    Returns list of history entries for undo support.

    v9.0 Phase 2.
    """
    hist_path=Path(cfg["logging"]["history_log"])
    skip_path=Path(cfg["logging"]["skipped_log"])
    applied:List[Dict[str,Any]]=[]
    use_cs=bool(cfg.get("move",{}).get("use_checksum",False))

    for plan in plans:
        src=Path(plan.source_path)
        if not src.exists():
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,
                "type":"crate_track","reason":"missing_source","src":str(src)})
            continue

        dst=resolve_crate_collision(Path(plan.target_path))

        if dry_run:
            out(f"    {C.DIM}[dry-run]{C.RESET} {src.name}  →  {dst.parent.parent.name}/{dst.parent.name}/{dst.name}  "
                f"{status_tag(plan.destination)}  conf={conf_color(plan.confidence)}")
            continue

        ensure_dir(dst.parent)
        try:
            # Preserve timestamps
            src_stat=src.stat()
            if _same_device(src,dst):
                src.rename(dst)
            else:
                shutil.copy2(str(src),str(dst))
                src.unlink()
            # Restore timestamps
            try: os.utime(str(dst),(src_stat.st_atime,src_stat.st_mtime))
            except Exception: pass
            _restore_creation_date(src_stat,dst)
        except Exception as e:
            err(f"    ⛔ Track move failed ({src.name}): {e}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,
                "type":"crate_track","reason":f"move_failed:{e}","src":str(src)})
            continue

        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,
               "type":"crate_explode","move_type":"crate_explode",
               "original_path":str(src),"original_folder":str(folder),
               "target_path":str(dst),"destination":plan.destination,
               "confidence":plan.confidence,"artist":plan.artist,
               "embedded_release":plan.embedded_release,"track_number":plan.track_number,
               "source_crate":folder.name,"source_crate_path":str(folder)}
        append_jsonl(hist_path,entry)
        applied.append(entry)
        if plan.embedded_release:
            out(f"    MOVED {status_tag(plan.destination)} {C.DIM}{src.name}{C.RESET}  →  "
                f"{dst.parent.parent.name}/{dst.parent.name}/{dst.name}  {C.GREEN}[ALBUM]{C.RESET}  conf={conf_color(plan.confidence)}")
        else:
            out(f"    MOVED {status_tag(plan.destination)} {C.DIM}{src.name}{C.RESET}  →  "
                f"{dst.parent.parent.name}/{dst.parent.name}/{dst.name}  conf={conf_color(plan.confidence)}")

    # Clean up empty source folder after explosion
    if not dry_run and applied:
        # Check if source folder is now empty (only non-audio files may remain)
        remaining=[f for f in folder.iterdir() if f.is_file()]
        audio_exts=set(cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a",".wav",".aiff",".ogg",".opus"]))
        remaining_audio=[f for f in remaining if f.suffix.lower() in audio_exts]
        if not remaining_audio:
            out(f"    {C.DIM}Source folder empty after explosion{C.RESET}")
            # Move any leftover non-audio files (artwork, nfo, etc.) to a sidecar
            if remaining:
                out(f"    {C.DIM}{len(remaining)} non-audio file(s) remain in source{C.RESET}")
        if source_root:
            try: cleanup_empty_parents(folder,source_root)
            except Exception: pass

    return applied

