"""
RaagDosa Proposal — folder proposal building and routing.

Layer 5: imports from tagreader (L2), scanning (L4), scoring (L2), naming (L2),
         artists (L2), crates (L4), files (L1), config (L2), library (L3),
         session (L3), core (L0), ui (L0), tags (L1).
"""
from __future__ import annotations

import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from raagdosa.core import now_iso, FolderProposal, FolderStats
from raagdosa.files import ensure_dir, sanitize_name, write_json
from raagdosa.tags import normalize_unicode
from raagdosa.tagreader import read_audio_tags
from raagdosa.ui import C, VERBOSE, out
from raagdosa.config import load_folder_override
from raagdosa.naming import (
    normalize_for_vote,
    apply_title_case,
    detect_garbage_name,
    is_garbage_tag_value,
    strip_bracket_stack,
    strip_disc_indicator,
    strip_display_noise,
    strip_trailing_domains,
    sanitize_windows_reserved,
    parse_folder_name_heuristic,
)
_is_garbage_tag_value = is_garbage_tag_value  # alias for monolith compat
from raagdosa.artists import normalize_artist_name
from raagdosa.scoring import (
    compute_dominant,
    recover_display,
    compute_confidence_factors,
    confidence_from_factors,
)
from raagdosa.scanning import classify_folder_content, detect_va, pick_year, detect_format_dupes
from raagdosa.crates import detect_djcrate
from raagdosa.library import resolve_library_path
from raagdosa.session import derive_clean_albums_root


# ── helpers ─────────────────────────────────────────────────────

def lower(s:Optional[str])->str: return (s or "").strip().lower()


def _detect_label_as_albumartist(albumartist: str) -> bool:
    """
    True if the albumartist tag looks like a record label, not an artist name.
    Pattern confirmed in 37 folders (3.7%) from session 2 (Tropical Twista Records cluster).
    """
    if not albumartist:
        return False
    return bool(re.search(
        r'\b(Records?|Discos?|Recordings?|Label|Music\s+Group|Inc\.?|Ltd\.?|Disques?)\b',
        albumartist, re.IGNORECASE))


# ── review summary & routing ────────────────────────────────────

_REASON_DESCRIPTIONS:Dict[str,str]={
    "low_confidence":"Confidence score below threshold",
    "generic_folder_name":"Folder name is too generic to classify",
    "unreadable_ratio_high":"Too many tracks have unreadable tags",
    "heuristic_fallback":"Name derived from heuristics (no usable tags)",
    "ep":"Detected as EP release",
    "mix_folder":"Detected as DJ mix or chart compilation",
    "djcrate_singles":"Detected as DJ crate (singles/loose tracks)",
    "djcrate_set":"Detected as DJ set prep crate (preserved intact)",
}

_FACTOR_DESCRIPTIONS:Dict[str,str]={
    "tag_coverage":"Tag readability",
    "dominance":"Album/artist vote consensus",
    "title_quality":"Song title confidence",
    "filename_consistency":"Filename ↔ tag alignment",
    "completeness":"Track numbering completeness",
    "aa_consistency":"Album-artist consistency",
    "folder_alignment":"Folder name ↔ proposed name",
}

def _build_review_summary(reasons:List[str],factors:Dict[str,float],confidence:float)->str:
    """Build a human-readable summary explaining why a folder was routed the way it was."""
    parts=[]
    for r in reasons:
        base=r.split(":")[0]  # handle "duplicate_in_run:FolderName"
        if base in _REASON_DESCRIPTIONS:
            parts.append(_REASON_DESCRIPTIONS[base])
        elif r.startswith("duplicate"):
            parts.append(f"Duplicate: {r.split(':',1)[1] if ':' in r else 'name collision'}")
        elif r.startswith("already_in_clean"):
            parts.append("Already exists in Clean library")
        else:
            parts.append(r.replace("_"," ").capitalize())
    # Add weak factor details
    weak=[f"{_FACTOR_DESCRIPTIONS.get(k,k)} ({v:.0%})" for k,v in factors.items()
          if isinstance(v,float) and v<0.5 and k in _FACTOR_DESCRIPTIONS]
    if weak:
        parts.append("Weak: "+", ".join(weak))
    return ". ".join(parts)


def _write_review_sidecar(folder_path:Path,proposal,session_id:str)->None:
    """Write .raagdosa_review.json inside a Review folder with routing rationale."""
    sidecar=folder_path/".raagdosa_review.json"
    data={
        "session_id":session_id,
        "timestamp":now_iso(),
        "original_folder":proposal.folder_name,
        "proposed_name":proposal.proposed_folder_name,
        "confidence":proposal.confidence,
        "destination":proposal.destination,
        "route_reasons":proposal.decision.get("route_reasons",[]),
        "review_summary":proposal.decision.get("review_summary",""),
        "confidence_factors":{k:round(v,3) for k,v in proposal.decision.get("confidence_factors",{}).items()},
        "artist":proposal.decision.get("albumartist_display",""),
        "album":proposal.decision.get("dominant_album_display",""),
        "is_va":proposal.decision.get("is_va",False),
        "user_notes":[],
    }
    try: write_json(sidecar,data)
    except Exception as e:
        out(f"  {C.DIM}Could not write review sidecar: {e}{C.RESET}",level=VERBOSE)

# ─────────────────────────────────────────────
# v7.0 — Structured review reasons
# ─────────────────────────────────────────────
REVIEW_REASON_PRESETS=[
    "va-misclass",        # VA detection was wrong
    "wrong-artist",       # Artist name incorrect
    "bad-tags",           # Tags are unreliable
    "incomplete-release", # Missing tracks or partial download
    "duplicate",          # Already have this release elsewhere
    "not-music",          # Not an album (samples, stems, etc.)
    "wrong-genre",        # Genre classification wrong
    "needs-research",     # Need to check Discogs/MusicBrainz
]


_GENERIC_FOLDER_NAMES = {
    "a","music","complete","down tempo","downtempo","warm up","warm-up",
    "underground house","haifa club","estray","rare tracks","new","misc",
    "unsorted","temp","incoming","downloads","stuff","tracks","songs",
    "playlist","mix","mixes","cd1","cd2","cd3","disc 1","disc 2",
    "electronica-downtempo","world electro - ethno disco - global groove",
    "jazz house piano bar","master pieces of electro","remixes and other tracks",
}


def _apply_format_suffix(name:str,cfg:Dict[str,Any],extensions:Optional[Dict[str,int]])->str:
    """Append format tag (e.g. [FLAC]) to folder name based on config."""
    sfx=cfg.get("format_suffix",{})
    if not sfx.get("enabled",True) or not extensions:
        return name
    if sfx.get("only_if_all_same_extension",True) and len(extensions)==1:
        ext1=next(iter(extensions.keys()))
        if ext1 and ext1!=lower(sfx.get("ignore_extension",".mp3")) and sfx.get("style","brackets_upper")=="brackets_upper":
            name=f"{name} [{ext1.lstrip('.').upper()}]"
    return name


def _route_proposal(p:FolderProposal,cfg:Dict[str,Any],
                    seen_names:Counter,existing_clean:Set[str],
                    manifest_entries:Set[str],
                    review_albums:Path,dup_root:Path,mixes_root:Path,
                    all_proposals:Optional[List]=None)->FolderProposal:
    """Route a single proposal to clean/review/duplicate. Mutates p in place."""
    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    sc=cfg.get("scan",{}); max_unread=float(sc.get("max_unreadable_track_ratio",0.25))
    reasons:List[str]=[]; dest="clean"
    if rr.get("route_questionable_to_review",True) and p.confidence<min_conf:
        dest="review"; reasons.append("low_confidence")
    _fn_norm = p.folder_name.strip().strip("_- ").lower()
    if _fn_norm in _GENERIC_FOLDER_NAMES:
        if dest == "clean": dest = "review"
        reasons.append("generic_folder_name")
        p.confidence = min(p.confidence, min_conf - 0.05)
    seen_names[p.proposed_folder_name]+=1
    if rr.get("route_duplicates",True) and seen_names[p.proposed_folder_name]>1:
        dest="duplicate"
        if all_proposals:
            colliders=[q.folder_name for q in all_proposals if q.proposed_folder_name==p.proposed_folder_name and q is not p]
            reasons.append(f"duplicate_in_run:{colliders[0][:40] if colliders else '?'}")
        else:
            reasons.append("duplicate_in_run")
    norm_prop=normalize_unicode(p.proposed_folder_name)
    if rr.get("route_cross_run_duplicates",True) and (norm_prop in existing_clean or norm_prop in manifest_entries):
        dest="duplicate"; reasons.append("already_in_clean")
    if p.decision.get("unreadable_ratio",0.0)>max_unread:
        dest="review"; reasons.append("unreadable_ratio_high")
    if p.decision.get("used_heuristic",False):
        if dest=="clean": dest="review"
        reasons.append("heuristic_fallback")
    if p.stats.format_duplicates: reasons.append(f"format_dupes({len(p.stats.format_duplicates)})")
    if p.decision.get("is_ep"): reasons.append("ep")
    if p.decision.get("is_mix") and dest=="clean":
        reasons.append("mix_folder"); ensure_dir(mixes_root)
        p.target_path=str(mixes_root/p.proposed_folder_name)
    # v9.0: DJ crate routing — always Review in Phase 1
    if p.decision.get("is_crate"):
        _ct=p.decision.get("crate_type","singles")
        dest="review"
        if _ct=="set":
            reasons.append("djcrate_set")
            # Set prep crates go to Review/_Sets/FolderName
            _sets_folder=cfg.get("djcrates",{}).get("keep_intact_routing","_Sets")
            p.target_path=str(review_albums.parent/_sets_folder/p.folder_name)
        else:
            reasons.append("djcrate_singles")
    p.destination=dest; p.decision["route_reasons"]=reasons
    if reasons:
        p.decision["review_summary"]=_build_review_summary(reasons,p.decision.get("confidence_factors",{}),p.confidence)
    if dest=="review" and not p.decision.get("is_crate"):
        p.target_path=str(review_albums/p.proposed_folder_name)
    elif dest=="review" and p.decision.get("is_crate") and p.decision.get("crate_type")!="set":
        p.target_path=str(review_albums/p.proposed_folder_name)
    elif dest=="duplicate": p.target_path=str(dup_root/p.proposed_folder_name)
    return p


# ── garbage detection ──────────────────────────────────────────

_TLDS="com|net|org|info|biz|co|io|me|fm|tv|cc|us|uk|de|fr|es|it|nl|ru|br|mx|in|jp|cn|au|ca|ch|se|no|fi|dk|pl|cz"

# v6.2: URL/domain garbage detection for tag values
_GARBAGE_URL_RE=re.compile(
    r"(^https?://)"                        # starts with http(s)://
    r"|(^www\.)"                            # starts with www.
    rf"|(\b[\w\-]+\.({_TLDS})\b)",          # contains domain.tld
    re.IGNORECASE
)
# Musical key names that get mis-tagged as artist (e.g. key=Em stored in artist field)
_MUSICAL_KEY_GARBAGE=frozenset({
    "a","ab","am","b","bb","bbm","bm","c","cb","cm","d","db","dm","dbm",
    "e","eb","ebm","em","f","fb","fm","g","gb","gbm","gm",
    "abm","c#","c#m","d#","d#m","f#","f#m","g#","g#m","a#","a#m",
    "smg",  # scene-release group, not an artist
})


def _is_garbage_tag_value(val:str)->bool:
    """Return True if a tag value looks like a URL/domain or musical key — not a real artist/album."""
    if not val: return False
    v=val.strip()
    if _GARBAGE_URL_RE.search(v): return True
    if v.lower() in _MUSICAL_KEY_GARBAGE and len(v)<=4: return True
    return False


# ── folder proposal builder ────────────────────────────────────

def build_folder_proposal(folder:Path,audio_files:List[Path],source_root:Path,profile:Dict[str,Any],cfg:Dict[str,Any],force_album:bool=False)->Optional[FolderProposal]:
    # ── per-folder override ──────────────────────────────────────────────
    override=load_folder_override(folder)
    if override and override.get("skip"): return None

    albums_norm:Counter=Counter(); albumartists_norm:Counter=Counter(); track_artists_norm:Counter=Counter(); years:Counter=Counter()
    primary_artists_norm:Counter=Counter()  # v5.5.1: artist before feat/ft — for VA detection
    genres:Counter=Counter(); labels:Counter=Counter(); bpms:List[float]=[]; keys_raw:Counter=Counter()
    albums_raw:Dict[str,Counter]={}; albumartists_raw:Dict[str,Counter]={}; track_artists_raw:Dict[str,Counter]={}
    tracks_with_year=0; tagged=0; unreadable=0
    extensions:Counter=Counter(p.suffix.lower() for p in audio_files)
    all_tags:List[Dict[str,Optional[str]]]=[]

    for f in audio_files:
        tags=read_audio_tags(f,cfg)
        all_tags.append(tags)
        if all(v is None for v in tags.values()): unreadable+=1; continue
        alb_r=(tags.get("album") or "").strip(); aa_r=(tags.get("albumartist") or "").strip()
        art_r=(tags.get("artist") or "").strip(); yr_r=(tags.get("year") or "").strip()
        genre_r=(tags.get("genre") or "").strip()
        label_r=(tags.get("label") or "").strip()
        bpm_r=(tags.get("bpm") or "").strip()
        key_r=(tags.get("key") or "").strip()

        # v6.2: discard garbage tag values (URLs, domains, musical keys in artist field)
        if _is_garbage_tag_value(aa_r):  aa_r=""
        if _is_garbage_tag_value(art_r): art_r=""
        if _is_garbage_tag_value(alb_r): alb_r=""

        # Strip display noise and disc indicators from album for voting
        alb_r_clean=strip_disc_indicator(strip_display_noise(alb_r)) if alb_r else alb_r

        alb_n=normalize_for_vote(alb_r_clean,cfg) if alb_r_clean else ""
        aa_n =normalize_for_vote(aa_r,cfg)         if aa_r         else ""
        art_n=normalize_for_vote(art_r,cfg)        if art_r        else ""
        if alb_n: albums_norm[alb_n]+=1; albums_raw.setdefault(alb_n,Counter())[alb_r_clean or alb_r]+=1
        if aa_n:  albumartists_norm[aa_n]+=1; albumartists_raw.setdefault(aa_n,Counter())[aa_r]+=1
        if art_n:
            track_artists_norm[art_n]+=1; track_artists_raw.setdefault(art_n,Counter())[art_r]+=1
            # v5.5.1: extract primary artist (before feat/ft/featuring/&/vs) for VA detection
            _prim = re.split(r'\s+(?:feat\.?|ft\.?|featuring|&|and|vs\.?|x)\s+', art_n, maxsplit=1, flags=re.I)[0].strip()
            if _prim: primary_artists_norm[_prim]+=1
        if genre_r: genres[genre_r]+=1
        if label_r: labels[label_r]+=1
        if key_r: keys_raw[key_r]+=1
        if bpm_r:
            try: bpms.append(float(re.sub(r"[^\d.]","",bpm_r)))
            except ValueError: pass
        if yr_r:
            m=re.search(r"(\d{4})",yr_r)
            if m: years[m.group(1)]+=1; tracks_with_year+=1
        tagged+=1

    total=len(audio_files)
    dom_alb_n,alb_share,_=compute_dominant(albums_norm)
    dom_aa_n,aa_share,_  =compute_dominant(albumartists_norm)
    dom_art_n,art_share,_=compute_dominant(track_artists_norm)
    dom_alb=recover_display(dom_alb_n,albums_raw); dom_aa=recover_display(dom_aa_n,albumartists_raw)

    # Always parse folder name heuristic — needed for compound artist recovery (&/and joins)
    _h_parsed=parse_folder_name_heuristic(folder.name,cfg)
    _h_album=_h_parsed.get("album"); _h_artist=_h_parsed.get("artist"); _h_year=_h_parsed.get("year")

    used_heuristic=False
    if not dom_alb:
        if _h_album:
            dom_alb=_h_album; alb_share=0.50
        # Only take artist from folder name when we have no tags at all
        if tagged==0:
            dom_aa=dom_aa or _h_artist
            aa_share=aa_share or (0.50 if _h_artist else 0.0)
        used_heuristic=True
        if not tracks_with_year and _h_year: years[_h_year]=1; tracks_with_year=1

    # ── override: force name / artist ───────────────────────────────────
    if override:
        if override.get("album"):  dom_alb=str(override["album"])
        if override.get("artist") or override.get("albumartist"):
            dom_aa=str(override.get("albumartist") or override.get("artist"))
        if override.get("year"):
            try: years[str(int(override["year"]))]=total; tracks_with_year=total
            except (TypeError, ValueError): pass

    va_label=cfg.get("various_artists",{}).get("label","VA")

    # ── v9.0: DJ Crate detection (runs BEFORE VA detection) ──────────────
    # If folder is a personal DJ crate, skip VA detection entirely.
    # Sidecar override takes priority.
    is_crate=False; crate_type=""; crate_confidence=0.0; crate_reason=""
    _override_ft=override.get("folder_type","") if override else ""
    if _override_ft in ("crate_singles","crate_set"):
        is_crate=True
        crate_type="set" if _override_ft=="crate_set" else "singles"
        crate_confidence=1.0; crate_reason="sidecar_override"
    elif _override_ft not in ("album","va","mix","ep","") and not force_album:
        pass  # unknown override type — fall through to auto-detection
    elif not force_album:
        is_crate,crate_type,crate_confidence,crate_reason=detect_djcrate(
            folder.name,albums_norm,track_artists_norm,all_tags,total,
            alb_share,dom_alb_n,aa_share,dom_aa_n,cfg)

    # ── VA detection ──────────────────────────────────────────────────────
    # CRITICAL: pass the Counter (not .keys()) so ratio = unique/total tracks.
    # Passing list(Counter.keys()) gives a 1-element list for any single-artist
    # album, yielding ratio=1/1=1.0 ≥ 0.5 → false VA for every real album.
    # v5.5.1: use primary_artists_norm (feat/ft stripped) for VA detection —
    # prevents "Artist feat. X", "Artist feat. Y" from each counting as unique.
    # v9.0: skip VA detection if folder is a DJ crate
    is_va = False if is_crate else detect_va(dom_aa_n or "", primary_artists_norm, cfg)

    # v6.2: folder-name VA prefix override — if folder explicitly starts with
    # "VA -", "VA_", "va -" etc., trust that signal and force VA detection.
    # This rescues cases where tags have garbage/wrong albumartist but the
    # user (or uploader) named the folder correctly.
    _fn_stripped = folder.name.strip()
    _fn_va_prefix = re.match(r'^(?:va|v\.?a\.?)\s*[-_–—]', _fn_stripped, re.I)
    _va_forced_by_folder = False
    if _fn_va_prefix and not force_album:
        is_va = True
        _va_forced_by_folder = True

    # v6.1: force_album override — user says this is not VA
    if force_album:
        is_va = False
        # If albumartist was a VA keyword, derive artist from track-level tags instead
        _va_kws_early = {m.lower() for m in cfg.get("various_artists",{}).get("albumartist_matches",[])}
        if dom_aa_n and dom_aa_n in _va_kws_early and dom_art_n:
            dom_aa   = recover_display(dom_art_n, {}) or dom_art_n
            dom_aa_n = dom_art_n
            aa_share = art_share

    # Pre-compute the set of known VA keywords for safeguard checks
    _va_kws = {m.lower() for m in cfg.get("various_artists",{}).get("albumartist_matches",[])}

    # v6.2: Safeguards A–I can un-flag VA, but NEVER when VA was forced by
    # an explicit "VA -" folder-name prefix — that's the strongest signal we have.
    # Safeguard A: Strong single-artist signal overrides VA heuristic.
    # If albumartist AND artist tags BOTH point to the same name at ≥75%
    # dominance, this is definitively a single-artist album — not VA.
    # Guard: only fires when the albumartist is NOT itself a VA keyword.
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and dom_art_n:
        if dom_aa_n == dom_art_n and aa_share >= 0.75 and art_share >= 0.75:
            is_va = False

    # Safeguard A2: albumartist alone at very high confidence with no
    # counter-evidence — but ONLY for non-VA albumartist tags.
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and aa_share >= 0.90:
        if dom_art_n and art_share >= 0.75 and dom_art_n == dom_aa_n:
            is_va = False
        elif not dom_art_n:
            # albumartist present & non-VA, artist tag absent → trust albumartist
            is_va = False

    # Safeguard B: Folder name contains the dominant albumartist name.
    # "Artist - Album" or "Year - Artist - Album" folder + AA tag = single artist.
    # Guard: albumartist must not be a VA keyword.
    # v6.1: lowered threshold from 0.80→0.60, added contains check (not just startswith).
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and aa_share >= 0.60:
        fn_norm = normalize_unicode(folder.name.strip().lower())
        aa_norm_str = normalize_unicode((dom_aa or dom_aa_n or "").strip().lower())
        if aa_norm_str and len(aa_norm_str) >= 3 and aa_norm_str in fn_norm:
            is_va = False

    # Safeguard C: No albumartist tag, but artist tag is dominant and
    # folder name contains that artist.
    # v6.1: lowered threshold from 0.90→0.70, added contains check.
    if is_va and not _va_forced_by_folder and not dom_aa_n and dom_art_n and art_share >= 0.70:
        fn_norm = normalize_unicode(folder.name.strip().lower())
        art_norm_str = normalize_unicode(dom_art_n.strip().lower())
        if art_norm_str and len(art_norm_str) >= 3 and art_norm_str in fn_norm:
            is_va = False

    # Safeguard E (v5.5.1): No albumartist tag, but dominant artist covers
    # all tracks — regardless of folder naming.  If every single track
    # credits the SAME primary artist, this is definitively single-artist.
    # This rescues "Year - Album" or album-only folder names where
    # Safeguard C cannot match on folder prefix.
    if is_va and not _va_forced_by_folder and not dom_aa_n and dom_art_n and art_share >= 0.95:
        is_va = False

    # Safeguard F (v5.5.1): albumartist present, not a VA keyword,
    # dominant artist present and matches — even without folder alignment.
    # Covers cases where albumartist and artist agree but aren't identical
    # strings (e.g. "artist feat. X" vs "artist").
    if is_va and not _va_forced_by_folder and dom_aa_n and dom_aa_n not in _va_kws and dom_art_n:
        # Extract primary artist (before feat/ft/featuring) for comparison
        _prim_aa = re.split(r'\s+(?:feat\.?|ft\.?|featuring)\s+', dom_aa_n, maxsplit=1, flags=re.I)[0].strip()
        _prim_art = re.split(r'\s+(?:feat\.?|ft\.?|featuring)\s+', dom_art_n, maxsplit=1, flags=re.I)[0].strip()
        if _prim_aa == _prim_art and aa_share >= 0.70 and art_share >= 0.70:
            is_va = False

    # Safeguard G (v6.1): Parent folder name contains the dominant artist.
    # Structure like "Artist Name/2024 - Album" or "Artist Name/Album Title"
    # means the parent is an artist folder — this is not VA.
    if is_va and not _va_forced_by_folder and (dom_aa_n or dom_art_n):
        _parent_name = normalize_unicode(folder.parent.name.strip().lower())
        _check_artist = dom_aa_n if (dom_aa_n and dom_aa_n not in _va_kws) else dom_art_n
        _check_share = aa_share if (dom_aa_n and dom_aa_n not in _va_kws) else art_share
        if _check_artist and _parent_name and _check_share >= 0.60:
            _art_norm = normalize_unicode(_check_artist.strip().lower())
            if _art_norm and (_parent_name == _art_norm or _parent_name.startswith(_art_norm)):
                is_va = False

    # Safeguard H (v6.1): Sibling folders share the same dominant artist.
    # If the parent contains multiple album subfolders and sibling folder names
    # contain the same artist, this is an artist discography — not VA.
    if is_va and not _va_forced_by_folder and (dom_aa_n or dom_art_n):
        _check_artist_h = dom_aa_n if (dom_aa_n and dom_aa_n not in _va_kws) else dom_art_n
        _check_share_h = aa_share if (dom_aa_n and dom_aa_n not in _va_kws) else art_share
        if _check_artist_h and _check_share_h >= 0.60:
            _art_norm_h = normalize_unicode(_check_artist_h.strip().lower())
            try:
                _siblings = [d.name for d in folder.parent.iterdir() if d.is_dir() and d != folder]
                if _siblings and _art_norm_h:
                    _sibling_match = sum(1 for s in _siblings if _art_norm_h in normalize_unicode(s.strip().lower()))
                    if _sibling_match >= 1:
                        is_va = False
            except OSError as e:
                out(f"  {C.DIM}Could not scan siblings: {e}{C.RESET}",level=VERBOSE)

    # Safeguard I (v6.1): Remix/remixed album — track artists are remixers,
    # not evidence of VA.  If the album name or folder name contains remix
    # indicators, the albumartist (or folder-derived artist) is the real artist.
    # Remixers inflate the unique artist ratio but don't make it a compilation.
    if is_va and not _va_forced_by_folder:
        _remix_pat = re.compile(r'\b(remix(?:ed|es)?|rmx|reworks?)\b', re.I)
        _alb_is_remix = bool(dom_alb and _remix_pat.search(dom_alb))
        _fn_is_remix = bool(_remix_pat.search(folder.name))
        if _alb_is_remix or _fn_is_remix:
            # If we have an albumartist that isn't a VA keyword, trust it
            if dom_aa_n and dom_aa_n not in _va_kws:
                is_va = False
            # Or if we can parse the artist from the folder name
            elif not dom_aa_n:
                _parsed_fn = parse_folder_name_heuristic(folder.name)
                _fn_artist = _parsed_fn.get("artist")
                if _fn_artist:
                    dom_aa = _fn_artist
                    dom_aa_n = normalize_for_vote(_fn_artist, cfg)
                    aa_share = 0.60
                    is_va = False

    # Safeguard D (v4.3): albumartist tag is a label name, not an artist.
    # Pattern confirmed in 37 folders (3.7%) in session 2.
    # Re-parse using dominant track artist instead.
    if not _va_forced_by_folder and dom_aa_n and _detect_label_as_albumartist(dom_aa or dom_aa_n or ""):
        # Folder has pattern "Label - Artist - Album" or "Label - Album"
        # Check if track artist gives us a cleaner single-artist signal
        if dom_art_n and art_share >= 0.70:
            dom_aa   = recover_display(dom_art_n, {}) or dom_art_n
            dom_aa_n = dom_art_n
            aa_share = art_share
            is_va    = detect_va(dom_aa_n, primary_artists_norm, cfg)

    # ── folder content type ─────────────────────────────────────────────
    folder_type=classify_folder_content(audio_files,folder.name,all_tags,cfg)
    is_mix=(folder_type=="mix")
    is_ep =(folder_type=="ep")
    # v9.0: crate classification overrides VA
    if is_crate:
        folder_type="crate_set" if crate_type=="set" else "crate_singles"
    elif is_va and folder_type not in ("mix",): folder_type="va"

    _feat_pat=re.compile(r'\s+(?:feat\.?|ft\.?|featuring)\s+.+$',re.I)
    if is_va:
        artist_for_folder:Optional[str]=va_label
    elif dom_aa:
        # Strip feat. collaborator from albumartist — main artist owns the folder,
        # feat. credit stays in the album name if it appears there.
        _prim_aa=_feat_pat.sub("",dom_aa).strip() or dom_aa
        artist_for_folder=normalize_artist_name(_prim_aa,cfg)
    elif cfg.get("decision",{}).get("allow_artist_fallback",True):
        raw_art=recover_display(dom_art_n,track_artists_raw) or dom_art_n
        _prim_art=_feat_pat.sub("",raw_art or "").strip() or (raw_art or "")
        artist_for_folder=normalize_artist_name(_prim_art,cfg) or None
        # Compound artist recovery: if folder name has "A and B" / "A & B" and tag artist
        # is one of the parts (tags split credits per track), prefer the compound name.
        if artist_for_folder and _h_artist:
            _split_pat=re.compile(r'\s+(?:and|&)\s+',re.I)
            _h_parts=_split_pat.split(_h_artist)
            if len(_h_parts)>=2:
                _art_low=normalize_unicode(artist_for_folder.lower().strip())
                _matches=[normalize_unicode(p.lower().strip())==_art_low or _art_low in normalize_unicode(p.lower()) for p in _h_parts]
                if any(_matches):
                    # One of the compound parts matches the tag artist → use full compound name
                    # Normalise "and" → "&" for cleaner folder names
                    _compound=_split_pat.sub(" & ",_h_artist)
                    artist_for_folder=normalize_artist_name(_compound,cfg) or artist_for_folder
    else:
        artist_for_folder=None

    # v9.0: crates don't need album/artist for proposal — they use folder name
    if is_crate:
        if not dom_alb: dom_alb=folder.name
        if not artist_for_folder: artist_for_folder=va_label
    elif not dom_alb or not artist_for_folder:
        return None

    # ── apply title-case fix to album name ──────────────────────────────
    dom_alb=apply_title_case(dom_alb,cfg)
    # Strip residual display noise from voted album name
    dom_alb=strip_display_noise(dom_alb)
    # Strip garbage bracket stack from album name
    garbage=detect_garbage_name(dom_alb)
    if garbage: dom_alb=strip_bracket_stack(dom_alb)

    # ── strip URL/domain that leaked into voted album name ───────────────
    dom_alb=strip_trailing_domains(dom_alb)
    # If the album name IS a bare domain/URL (nothing left after stripping), keep original
    if not dom_alb.strip():
        dom_alb=apply_title_case(recover_display(dom_alb_n,albums_raw) or dom_alb_n or "",cfg)

    year_val,year_meta=pick_year(years,tracks_with_year,max(total,1) if used_heuristic else total,cfg)

    # v7.0: If year came only from heuristic (no tag agreement) and was low confidence,
    # drop it — better to have no year than a wrong year in the folder name.
    if year_val and used_heuristic and year_meta.get("agreement",1.0)<0.70:
        year_val=None; year_meta={"included":False,"reason":"heuristic_low_agreement"}

    fmt=cfg.get("format",{})
    # Strip trailing EP/E.P./LP suffixes from album name before adding [EP] label
    # to prevent doubling like "Ashen EP [EP]"
    _ep_stripped_alb = dom_alb
    if is_ep and fmt.get("label_eps",True):
        # Strip both bare "EP"/"E.P." and bracketed "[EP]"/"(EP)" from album name end
        _ep_stripped_alb = re.sub(r'\s*[\[\(]\s*E\.?P\.?\s*[\]\)]\s*$|\s+E\.?P\.?\s*$', '', dom_alb, flags=re.I).strip()
    ep_suffix=" [EP]" if is_ep and fmt.get("label_eps",True) else ""
    # v7.0: Strip year from album name entirely, then place it at the end via pattern.
    # This prevents years appearing in the wrong position inside the album name
    # (e.g. "2023 Album Name" → "Artist - 2023 Album Name" instead of "Artist - Album Name (2023)").
    _alb_for_fmt = _ep_stripped_alb + ep_suffix
    if year_val:
        # Remove year in any position: "(2023)", "[2023]", bare "2023", leading "2023 - "
        _yr_str = str(year_val)
        _alb_for_fmt = re.sub(r'\s*[\(\[]\s*' + _yr_str + r'\s*[\)\]]\s*', ' ', _alb_for_fmt).strip()
        _alb_for_fmt = re.sub(r'(?:^|\s+)' + _yr_str + r'(?:\s+|$)', ' ', _alb_for_fmt).strip()
        _alb_for_fmt = re.sub(r'^\s*' + _yr_str + r'\s*[-–—]\s*', '', _alb_for_fmt).strip()
        _alb_for_fmt = _alb_for_fmt.strip(' -–—')
        if not _alb_for_fmt:
            _alb_for_fmt = _ep_stripped_alb + ep_suffix  # safety: don't blank out the name
    pat_key = "pattern_with_year" if year_val else "pattern_no_year"
    pat=fmt.get(pat_key,"{albumartist} - {album}")
    proposed=pat.format(albumartist=artist_for_folder,album=_alb_for_fmt,year=year_val or "")
    proposed=sanitize_name(proposed,repl=fmt.get("replace_illegal_chars_with"," - "))
    proposed=sanitize_windows_reserved(proposed)

    proposed=_apply_format_suffix(proposed,cfg,dict(extensions))

    is_flac_only=set(extensions.keys())=={".flac"}
    # Genre/label: plurality vote (raw display form — normalization happens in resolve_library_path)
    dom_genre,_,_=compute_dominant(genres)
    dom_label,_,_=compute_dominant(labels)
    dom_key,_,_=compute_dominant(keys_raw)
    # BPM: use median of all tracks (more robust than mode for continuous values)
    avg_bpm:Optional[float]=None
    if bpms:
        sorted_bpms=sorted(bpms)
        mid=len(sorted_bpms)//2
        avg_bpm=sorted_bpms[mid] if len(sorted_bpms)%2 else (sorted_bpms[mid-1]+sorted_bpms[mid])/2
    clean_albums=derive_clean_albums_root(profile,source_root)
    target_dir=resolve_library_path(clean_albums,artist_for_folder,dom_alb,year_val,
                                     is_flac_only,is_va,False,is_mix,cfg,
                                     profile=profile,genre=dom_genre,
                                     bpm=avg_bpm,key=dom_key,label=dom_label)

    # v7.0: Multi-disc grouping — if source folder name contains a disc indicator
    # (CD1, CD2, Disc 1, etc.), nest it as a subfolder under the album folder.
    # This groups CD1 + CD2 under one parent: Artist/Album/CD1/, Artist/Album/CD2/
    _disc_folder_pat=re.compile(r'(?:disc|cd|disk)\s*(\d+)',re.I)
    _disc_m=_disc_folder_pat.search(folder.name)
    if _disc_m:
        disc_label=f"CD{_disc_m.group(1)}"
        target_dir=target_dir/disc_label
        decision_extra_disc=disc_label
    else:
        decision_extra_disc=None

    # ── named confidence factors ─────────────────────────────────────────
    conf_factors=compute_confidence_factors(
        audio_files,tagged,alb_share,aa_share,art_share,
        used_heuristic,folder.name,proposed,all_tags,cfg)

    # v6.2: VA-prefix scene-named folders get penalised on folder_alignment
    # because scene naming ("VA-Title-CAT-WEB-2023-GRP") never matches the
    # proposed clean name. Boost alignment to 0.5 minimum for confirmed VA.
    if is_va and _fn_va_prefix and conf_factors.get("folder_alignment",0) < 0.5:
        conf_factors["folder_alignment"] = 0.5

    confidence=confidence_from_factors(conf_factors,used_heuristic)

    # Override confidence boost from .raagdosa
    if override and override.get("confidence_boost"):
        confidence=min(1.0,confidence+float(override["confidence_boost"]))

    fmt_dupes=detect_format_dupes(audio_files)
    decision={
        "dominant_album":dom_alb_n,"dominant_album_display":dom_alb,"dominant_album_share":alb_share,
        "dominant_albumartist":dom_aa_n,"dominant_albumartist_display":dom_aa,"dominant_albumartist_share":aa_share,
        "dominant_artist":dom_art_n,"dominant_artist_share":art_share,
        "is_va":is_va,"is_mix":is_mix,"is_ep":is_ep,"folder_type":folder_type,
        "is_crate":is_crate,"crate_type":crate_type,"crate_confidence":crate_confidence,"crate_reason":crate_reason,
        "albumartist_display":artist_for_folder,"year":year_val,"year_meta":year_meta,
        "genre":dom_genre,"label":dom_label,"bpm":avg_bpm,"key":dom_key,
        "unreadable_ratio":(unreadable/total) if total else 0.0,
        "used_heuristic":used_heuristic,"is_flac_only":is_flac_only,
        "garbage_reasons":garbage,"confidence_factors":conf_factors,
        "disc_subfolder":decision_extra_disc,
    }
    # v9.0: crates always route to Review in Phase 1
    _dest="clean"
    if is_crate:
        _dest="review"
        # For crates, use the crate confidence as the overall confidence
        confidence=crate_confidence
    stats=FolderStats(tracks_total=total,tracks_tagged=tagged,tracks_unreadable=unreadable,extensions=dict(extensions),format_duplicates=fmt_dupes)
    return FolderProposal(folder_path=str(folder),folder_name=folder.name,proposed_folder_name=proposed,
                          target_path=str(target_dir),destination=_dest,confidence=float(confidence),decision=decision,stats=stats)


# ── multi-disc detection ──────────────────────────────────────

def folder_is_multidisc(files:List[Path],cfg:Dict[str,Any])->bool:
    discs:set=set()
    for f in files:
        dn=(read_audio_tags(f,cfg).get("discnumber") or ""); d=parse_int_prefix(dn) if dn else None
        if d:
            discs.add(d)
        else:
            # Detect disc-compound filename prefix: 101→disc1, 213→disc2 track13
            m=re.match(r"^(\d{3})",f.stem.strip())
            if m:
                raw=int(m.group(1))
                if 100<=raw<=999 and raw%100>=1:
                    discs.add(raw//100)
    return len(discs)>1

