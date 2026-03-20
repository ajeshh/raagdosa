"""RaagDosa commands — all cmd_* and profile_* functions."""
from __future__ import annotations

import argparse, csv, dataclasses, datetime as dt, json, os, platform, re, shutil
import signal, sys, uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import yaml
except Exception:
    yaml = None

try:
    from mutagen import File as MutagenFile
except Exception:
    MutagenFile = None

# ── Package imports ──────────────────────────────────────────────────
from raagdosa import APP_VERSION
from raagdosa.ui import (C, out, err, warn, ok_msg, NORMAL, VERBOSE, QUIET,
    set_verbosity, status_tag, conf_color, Progress)
from raagdosa.core import (now_iso, make_session_id, FolderProposal, fp_from_dict,
    register_stop_handler, should_stop)
from raagdosa.files import (sanitize_name, ensure_dir, is_hidden_file, list_audio_files,
    read_json, write_json, append_jsonl, iter_jsonl, safe_move_folder,
    cleanup_empty_parents)
from raagdosa.config import (read_yaml, write_yaml, load_folder_override,
    load_config_with_paths, _validate_config)
from raagdosa.tags import normalize_unicode
from raagdosa.session import (setup_logging_paths, validate_config, read_manifest,
    manifest_has, manifest_set_last_run, manifest_get_last_run, manifest_add,
    ensure_roots, derive_wrapper_root, derive_clean_root, derive_review_root,
    derive_clean_albums_root, find_dj_databases, rotate_log_if_needed,
    validate_proposal_paths,
    resolve_log_paths_from_active_profile as _resolve_log_paths_from_active_profile,
    resolve_last_session as _resolve_last_session,
    load_last_session)
from raagdosa.library import resolve_library_path, BUILTIN_TEMPLATES, _resolve_lib_cfg
from raagdosa.scoring import (string_similarity, parse_vinyl_track, parse_int_prefix,
    detect_track_gaps, detect_duplicate_track_numbers)
from raagdosa.naming import smart_title_case, normalize_for_vote
from raagdosa.artists import normalize_artist_name, artists_are_same
from raagdosa.pipeline import (resolve_perf_settings, detect_recommended_tier,
    folder_mtime, build_skip_sets)
from raagdosa.tracks import classify_folder_for_tracks, build_track_filename
from raagdosa.review import collision_resolve

# ── Constants ────────────────────────────────────────────────────────

_TEMPLATE_EXAMPLES:Dict[str,List[str]]={
    "standard":["Bicep/","  Isles (2021)/","  Isles Deluxe (2022)/","Floating Points/","  Crush (2019)/"],
    "dated":["Aphex Twin/","  1994 - Selected Ambient Works Vol II/","  2001 - Drukqs/","Burial/","  2007 - Untrue/"],
    "flat":["Bicep - Isles/","Burial - Untrue/","Four Tet - There Is Love In You/"],
    "bpm":["120-124 BPM/","  Bicep - Isles/","125-129 BPM/","  Amelie Lens - Higher/","_Unsorted/","  Unknown - Untitled/"],
    "genre-bpm":["House/","  120-124/","    Bicep - Isles/","Techno/","  130-134/","    Charlotte de Witte - Doppler/"],
    "genre-bpm-key":["Melodic Techno/","  125-129/","    8A/","      Tale Of Us - Afterlife 001/"],
    "genre":["Electronic/","  Bicep/","    Isles (2021)/","Jazz/","  Kamasi Washington/","    The Epic (2015)/","_Unsorted/","  Unknown Artist/","    Untitled/"],
    "label":["Warp Records/","  Aphex Twin - Selected Ambient Works/","4AD/","  Cocteau Twins - Heaven or Las Vegas/","_Unsorted/","  Unknown - Demo/"],
    "decade":["1990s/","  Drum & Bass/","    Goldie - Timeless/","2020s/","  Electronic/","    Bicep - Isles/"],
}

_CATCHALL_FOLDER_NAMES = {
    "_singles","_unsorted","_inbox","_dump","sort","still sort","unzip",
    "staging","tempo","new music","macbook clean","dupes from tuneup",
    "chroma download",
}

_AUDIO_EXTS_TREE = {".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma",".aac",".alac",".ape"}
_NON_AUDIO_EXTS_TREE = {".jpg",".jpeg",".png",".gif",".nfo",".txt",".sfk",".m3u",".m3u8",".cue",".pdf",".log",".url"}


# ── Helpers ──────────────────────────────────────────────────────────

def _parse_since(val:Optional[str],cfg:Dict[str,Any])->Optional[dt.datetime]:
    if not val: return None
    if val=="last_run": return manifest_get_last_run(cfg)
    try: return dt.datetime.fromisoformat(val)
    except Exception: err(f"Cannot parse --since '{val}'. Use ISO date or 'last_run'."); sys.exit(1)


def _resolve_genre_roots(cfg: Dict[str, Any], cli_roots: Optional[List[str]] = None) -> Set[str]:
    """Return effective set of genre root folder names (CLI override + config persistent list)."""
    roots: Set[str] = set()
    for item in cfg.get("genre_roots", []) or []:
        if isinstance(item, str): roots.add(item)
        elif isinstance(item, dict) and item.get("name"): roots.add(item["name"])
    if cli_roots:
        for r in cli_roots: roots.add(r.strip())
    return roots


# ── Tree helpers ─────────────────────────────────────────────────────

def _tree_walk(
    root: Path, base: Path, lines: List[str],
    audio_only: bool, max_depth: Optional[int], current_depth: int,
    skip_exts: Optional[set] = None, skip_folders: Optional[set] = None,
) -> None:
    if max_depth is not None and current_depth > max_depth:
        return
    try:
        entries = sorted(root.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    except PermissionError:
        return
    for entry in entries:
        rel = str(entry.relative_to(base))
        if entry.is_dir():
            if skip_folders and entry.name in skip_folders: continue
            lines.append(rel + "/")
            _tree_walk(entry, base, lines, audio_only, max_depth, current_depth + 1,
                       skip_exts=skip_exts, skip_folders=skip_folders)
        elif entry.is_file():
            if entry.name.startswith("._"): continue
            if skip_exts and entry.suffix.lower() in skip_exts: continue
            if audio_only and entry.suffix.lower() not in _AUDIO_EXTS_TREE: continue
            lines.append(rel)


def _cmd_tree_diff(trees_dir: Path, name_a: str, name_b: str) -> None:
    """Show what appeared and disappeared between two tree snapshots."""
    def _find_snap(name: str) -> Optional[Path]:
        sd = trees_dir / name
        if sd.is_dir():
            txts = sorted(sd.glob("*.txt"))
            if txts: return txts[0]
        exact = trees_dir / name
        if exact.exists(): return exact
        exact_txt = trees_dir / f"{name}.txt"
        if exact_txt.exists(): return exact_txt
        for d in sorted(trees_dir.iterdir()):
            if d.is_dir() and name.lower() in d.name.lower():
                txts = sorted(d.glob("*.txt"))
                if txts: return txts[0]
        matches = sorted(trees_dir.glob(f"*{name}*.txt"))
        return matches[0] if matches else None
    pa = _find_snap(name_a); pb = _find_snap(name_b)
    if not pa: err(f"Snapshot not found: {name_a}"); sys.exit(1)
    if not pb: err(f"Snapshot not found: {name_b}"); sys.exit(1)
    lines_a = set(pa.read_text(encoding="utf-8").splitlines()) - set()
    lines_b = set(pb.read_text(encoding="utf-8").splitlines())
    lines_a = {l for l in lines_a if not l.startswith("#") and l.strip()}
    lines_b = {l for l in lines_b if not l.startswith("#") and l.strip()}
    added = sorted(lines_b - lines_a)
    removed = sorted(lines_a - lines_b)
    out(f"\n{C.CYAN}Tree diff:{C.RESET} {pa.name} → {pb.name}")
    out(f"  {C.GREEN}+{len(added)} added{C.RESET}   {C.RED}-{len(removed)} removed{C.RESET}\n")
    for l in removed: out(f"{C.RED}− {l}{C.RESET}")
    for l in added:   out(f"{C.GREEN}+ {l}{C.RESET}")


# ── Catchall helpers ─────────────────────────────────────────────────

def _clean_catchall_stem(stem: str) -> str:
    s = re.sub(r"^\[.*?\]\s*", "", stem).strip()
    s = re.sub(r"^\(.*?\)\s*", "", s).strip()
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    _camelot_pat = re.compile(r"^(1[0-2]|[1-9])[ABab]\s*[-–.\s]")
    _openkey_pat = re.compile(r"^(1[0-2]|[1-9])[dDmM]\s*[-–.\s]")
    if not _camelot_pat.match(s) and not _openkey_pat.match(s):
        s = re.sub(r"^(\d{1,3}[a-zA-Z]?[\s.\-]+)", "", s).strip()
    else:
        s = re.sub(r"^(\d{1,3}[\s.\-]+)(?=\d)", "", s).strip()
    s = re.sub(r"^([A-Z]\d{1,2}[\s.\-]+)(?=[A-Z])", "", s).strip()
    s = re.sub(r"[-–]\s*[-–]", "-", s).strip()
    s = re.sub(r"^\s*[-–]\s*", "", s).strip()
    return s


def _parse_artist_from_stem(stem: str) -> Optional[str]:
    m3 = re.match(r"^(.+?)\s*[-–]\s*\1\s*[-–]\s*(.+)$", stem, re.I)
    if m3:
        candidate = m3.group(1).strip()
        if len(candidate) > 1 and not candidate.isdigit():
            return candidate
    m2 = re.match(r"^(.+?)\s*[-–]\s*(.+?)\s*[-–]\s*(.+)$", stem)
    if m2:
        candidate = m2.group(1).strip()
        if len(candidate) <= 3 and (candidate.isdigit() or re.match(r"^\d+[a-zA-Z]$", candidate)):
            candidate = m2.group(2).strip()
        if len(candidate) > 1 and not candidate.isdigit():
            return candidate
    m = re.match(r"^(.+?)\s*[-–]\s*(.+)$", stem)
    if m:
        candidate = m.group(1).strip()
        if len(candidate) > 1 and not candidate.isdigit():
            return candidate
    return None


def _extract_catchall_artist(path: Path, cfg: Dict[str, Any]) -> str:
    if MutagenFile is not None:
        try:
            mf = MutagenFile(str(path), easy=True)
            if mf and mf.tags:
                t = mf.tags
                for key in ["albumartist", "artist"]:
                    v = t.get(key)
                    if isinstance(v, list): v = v[0] if v else None
                    if v and str(v).strip():
                        val = str(v).strip().replace("_", " ")
                        return val
                title_v = t.get("title")
                if isinstance(title_v, list): title_v = title_v[0] if title_v else None
                if title_v:
                    cleaned = _clean_catchall_stem(str(title_v).strip())
                    parsed = _parse_artist_from_stem(cleaned)
                    if parsed:
                        return smart_title_case(parsed)
        except Exception as e:
            out(f"  {C.DIM}Tag read failed: {e}{C.RESET}",level=VERBOSE)
    stem = _clean_catchall_stem(path.stem)
    parsed = _parse_artist_from_stem(stem)
    if parsed:
        return smart_title_case(parsed)
    return "_Unknown"


def _build_catchall_track_name(path: Path, artist: str, cfg: Dict[str, Any]) -> str:
    from raagdosa.tagreader import read_audio_tags
    tags = read_audio_tags(path, cfg)
    title_raw = (tags.get("title") or "").strip()
    artist_raw = (tags.get("artist") or "").strip()
    if title_raw: title_raw = title_raw.replace("_", " ")
    if artist_raw: artist_raw = artist_raw.replace("_", " ")
    display_artist = artist_raw or artist
    if not title_raw:
        stem = _clean_catchall_stem(path.stem)
        m3 = re.match(r"^(.+?)\s*[-–]\s*\1\s*[-–]\s*(.+)$", stem, re.I)
        if m3:
            title_raw = m3.group(2).strip()
        else:
            m2 = re.match(r"^(.+?)\s*[-–]\s*(.+?)\s*[-–]\s*(.+)$", stem)
            if m2:
                title_raw = m2.group(3).strip()
            else:
                m = re.match(r"^(.+?)\s*[-–]\s*(.+)$", stem)
                if m:
                    title_raw = m.group(2).strip()
                else:
                    title_raw = stem
    _art_lower = (display_artist or "").strip().lower()
    _title_check = title_raw.strip()
    _title_m = re.match(r"^(.+?)\s*[-–]\s*(.+)$", _title_check)
    if _title_m and _art_lower and _title_m.group(1).strip().lower() == _art_lower:
        title_raw = _title_m.group(2).strip()
    display_artist = smart_title_case(sanitize_name(display_artist), cfg)
    title_clean = smart_title_case(sanitize_name(title_raw), cfg)
    tc = cfg.get("title_cleanup", {})
    if tc.get("enabled", True):
        for phrase in tc.get("strip_trailing_phrases", []):
            _pat = re.compile(r"[\s\-–]*" + re.escape(phrase) + r"\s*$", re.IGNORECASE)
            title_clean = _pat.sub("", title_clean).strip()
    if not title_clean:
        title_clean = path.stem
    return f"{display_artist} - {title_clean}{path.suffix.lower()}"


# ── Crate scan helpers ───────────────────────────────────────────────

def _scan_folder_for_crate_signals(folder:Path,cfg:Dict[str,Any],min_tracks:int)->Optional[Dict[str,Any]]:
    from raagdosa.tagreader import read_audio_tags
    from raagdosa.crates import is_unknown_album as _is_unknown_album, folder_matches_crate_keywords as _folder_matches_crate_keywords, folder_matches_set_patterns as _folder_matches_set_patterns
    audio_exts={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}
    audio_files=[f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in audio_exts]
    if len(audio_files)<min_tracks: return None
    has_subfolders=any(d.is_dir() and not d.name.startswith(".") for d in folder.iterdir())
    sample=audio_files[:20]
    tags=[]
    for af in sample:
        try: tags.append(read_audio_tags(af,cfg))
        except Exception: pass
    if len(tags)<min_tracks: return None
    albums:Counter=Counter()
    blank_count=0
    for t in tags:
        alb=(t.get("album") or "").strip()
        if not alb or _is_unknown_album(alb,cfg):
            blank_count+=1
        else:
            albums[normalize_for_vote(alb,cfg)]+=1
    effective_unique=len(albums)+blank_count
    album_diversity=effective_unique/max(len(tags),1)
    dom_alb_share=albums.most_common(1)[0][1]/len(tags) if albums else 0.0
    dom_alb_name=albums.most_common(1)[0][0] if albums else None
    track_nums:List[int]=[]
    for t in tags:
        tn=t.get("tracknumber")
        if tn:
            m=re.match(r"(\d+)",str(tn))
            if m:
                try: track_nums.append(int(m.group(1)))
                except ValueError: pass
    sequential=False
    if len(track_nums)>=len(tags)*0.8 and len(tags)>=3:
        unique_nums=sorted(set(track_nums))
        has_dupes=len(track_nums)!=len(unique_nums)
        if not has_dupes and unique_nums==list(range(unique_nums[0],unique_nums[-1]+1)):
            sequential=True
    comp_count=sum(1 for t in tags if t.get("compilation")=="1")
    comp_share=comp_count/len(tags) if tags else 0.0
    is_likely_crate=(
        album_diversity>=0.4
        and dom_alb_share<0.6
        and not sequential
        and not (comp_share>=0.7 and dom_alb_share>=0.5)
    )
    kw_match=_folder_matches_crate_keywords(folder.name,cfg)>0
    set_match=_folder_matches_set_patterns(folder.name,cfg)
    if kw_match or set_match:
        is_likely_crate=True
    if not is_likely_crate: return None
    return {
        "path":folder,"name":folder.name,"total_tracks":len(audio_files),"sampled":len(tags),
        "album_diversity":album_diversity,"dom_alb_share":dom_alb_share,"dom_alb_name":dom_alb_name,
        "sequential":sequential,"has_subfolders":has_subfolders,"comp_share":comp_share,
        "kw_match":kw_match,"set_match":set_match,
    }


def _extract_naming_patterns(crate_infos:List[Dict[str,Any]])->List[Dict[str,Any]]:
    patterns:List[Dict[str,Any]]=[]
    names=[c["name"] for c in crate_infos]
    suffix_groups:Dict[str,List[Dict[str,Any]]]={}
    for c in crate_infos:
        n=c["name"]
        for sep in [" - ","_"," "]:
            parts=n.rsplit(sep,1)
            if len(parts)==2 and len(parts[1])>=3:
                suffix=parts[1].lower()
                suffix_groups.setdefault(suffix,[]).append(c)
    for suffix,members in suffix_groups.items():
        if len(members)>=2:
            crate_type="crate_set" if any(m["set_match"] for m in members) else "crate_singles"
            regex_pat=rf".*[\s_\-]{re.escape(suffix)}$"
            patterns.append({
                "suffix":suffix,"regex":regex_pat,"type":crate_type,
                "matches":members,"count":len(members),
            })
    prefix_groups:Dict[str,List[Dict[str,Any]]]={}
    for c in crate_infos:
        n=c["name"]
        for sep in [" - ","_"," "]:
            parts=n.split(sep,1)
            if len(parts)==2 and len(parts[0])>=3:
                prefix=parts[0].lower()
                prefix_groups.setdefault(prefix,[]).append(c)
    for prefix,members in prefix_groups.items():
        if len(members)>=2:
            crate_type="crate_set" if any(m["set_match"] for m in members) else "crate_singles"
            regex_pat=rf"^{re.escape(prefix)}[\s_\-].*"
            existing_match_sets=[set(m["name"] for m in p["matches"]) for p in patterns]
            member_names=set(m["name"] for m in members)
            if member_names not in existing_match_sets:
                patterns.append({
                    "prefix":prefix,"regex":regex_pat,"type":crate_type,
                    "matches":members,"count":len(members),
                })
    patterned_names:Set[str]=set()
    for p in patterns:
        for m in p["matches"]: patterned_names.add(m["name"])
    standalone=[c for c in crate_infos if c["name"] not in patterned_names]
    return patterns


# ═══════════════════════════════════════════════════════════════════
# Profile CRUD
# ═══════════════════════════════════════════════════════════════════

def profile_list(cfg:Dict[str,Any])->None:
    for n in cfg.get("profiles",{}).keys():
        mark=f"  {C.GREEN}* active{C.RESET}" if n==cfg.get("active_profile") else ""
        out(f"  {n}{mark}")

def profile_show(cfg:Dict[str,Any],name:str)->None:
    p=cfg.get("profiles",{}).get(name)
    if not p: err("No such profile."); return
    out(json.dumps(p,indent=2))

def profile_add(cfg_path:Path,cfg:Dict[str,Any],name:str,source:str,clean_mode:str,clean_folder:str,review_folder:str,template:Optional[str]=None)->None:
    cfg.setdefault("profiles",{})
    if name in cfg["profiles"]: raise ValueError("Profile already exists.")
    prof:Dict[str,Any]={"source_root":source,"clean_mode":clean_mode,"clean_folder_name":clean_folder,
                         "review_folder_name":review_folder,"clean_albums_folder_name":"Albums",
                         "clean_tracks_folder_name":"Tracks","review_albums_folder_name":"Albums",
                         "duplicates_folder_name":"Duplicates","orphans_folder_name":"Orphans"}
    if template:
        tpl=BUILTIN_TEMPLATES.get(template)
        if tpl:
            prof["library"]={"template":tpl["template"]}
            out(f"  Template: {template} ({tpl['name']})")
            if tpl.get("requires"):
                out(f"  {C.DIM}Requires tags: {', '.join(tpl['requires'])}{C.RESET}")
        else:
            prof["library"]={"template":template}
            out(f"  Template: {template}")
    cfg["profiles"][name]=prof
    write_yaml(cfg_path,cfg); out(f"Added profile: {name}")

def profile_set(cfg_path:Path,cfg:Dict[str,Any],name:str,source:Optional[str],clean_mode:Optional[str],clean_folder:Optional[str],review_folder:Optional[str],template:Optional[str]=None)->None:
    prof=cfg.get("profiles",{}).get(name)
    if not prof: raise ValueError("No such profile.")
    if source:        prof["source_root"]=source
    if clean_mode:    prof["clean_mode"]=clean_mode
    if clean_folder:  prof["clean_folder_name"]=clean_folder
    if review_folder: prof["review_folder_name"]=review_folder
    if template:
        tpl=BUILTIN_TEMPLATES.get(template)
        if tpl:
            prof.setdefault("library",{})["template"]=tpl["template"]
            out(f"  Template set to: {template} ({tpl['name']})")
            if tpl.get("requires"):
                out(f"  {C.DIM}Requires tags: {', '.join(tpl['requires'])}{C.RESET}")
        else:
            prof.setdefault("library",{})["template"]=template
            out(f"  Template set to: {template}")
    write_yaml(cfg_path,cfg); out(f"Updated profile: {name}")

def profile_delete(cfg_path:Path,cfg:Dict[str,Any],name:str)->None:
    if name not in cfg.get("profiles",{}): raise ValueError("No such profile.")
    del cfg["profiles"][name]
    if cfg.get("active_profile")==name: cfg["active_profile"]=None
    write_yaml(cfg_path,cfg); out(f"Deleted: {name}")

def profile_use(cfg_path:Path,cfg:Dict[str,Any],name:str)->None:
    if name not in cfg.get("profiles",{}): raise ValueError("No such profile.")
    cfg["active_profile"]=name; write_yaml(cfg_path,cfg); out(f"Active profile: {name}")


# ═══════════════════════════════════════════════════════════════════
# CLEAN commands — only need package imports
# ═══════════════════════════════════════════════════════════════════


def cmd_verify(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root,create=False); clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    manifest=read_manifest(cfg); mf_entries=manifest.get("entries",{})
    issues:List[str]=[]; ok_count=0
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa verify — {profile_name}{C.RESET}\n{'═'*60}")
    out(f"\nChecking {len(mf_entries)} manifest entries...")
    for name in mf_entries:
        candidates=list(clean_albums.rglob(name)) if clean_albums.exists() else []
        if not candidates: issues.append(f"MANIFEST_MISSING_ON_DISK: '{name}'")
        else: ok_count+=1
    out("Checking disk vs manifest...")
    if clean_albums.exists():
        for d in clean_albums.rglob("*"):
            if not d.is_dir(): continue
            if any(f.suffix.lower() in exts for f in d.iterdir() if f.is_file()):
                if normalize_unicode(d.name) not in mf_entries: issues.append(f"NOT_IN_MANIFEST: {d}")
    out("Checking for empty folders...")
    if clean_albums.exists():
        for d in clean_albums.rglob("*"):
            if d.is_dir() and not any(d.iterdir()): issues.append(f"EMPTY_FOLDER: {d}")
    allowed={e.lower() for e in cfg.get("track_rename",{}).get("allowed_extensions",[".mp3",".flac",".m4a"])}
    expected=re.compile(r"^\d{2,3}\s*[-–]\s*.+\.\w+$"); unclean=0
    if clean_albums.exists():
        for f in clean_albums.rglob("*"):
            if f.is_file() and f.suffix.lower() in allowed and not is_hidden_file(f) and not expected.match(f.name): unclean+=1
    out(f"\n{C.BOLD}Results:{C.RESET}")
    out(f"  Manifest OK:            {ok_count}")
    out(f"  Unclean track names:    {unclean}  {C.DIM}(run 'raagdosa tracks' to fix){C.RESET}")
    if issues:
        out(f"\n{C.YELLOW}Issues ({len(issues)}):{C.RESET}")
        for i in issues[:50]: out(f"  {C.YELLOW}⚠{C.RESET}  {i}")
        if len(issues)>50: out(f"  ... and {len(issues)-50} more")
    else: ok_msg("No issues found — library looks healthy.")
    log_root=Path(cfg.get("logging",{}).get("root_dir","logs")); ensure_dir(log_root)
    vpath=log_root/f"verify_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    vpath.write_text("\n".join([f"RaagDosa v{APP_VERSION} — Verify",f"Profile: {profile_name}",f"Date: {now_iso()}",f"Issues: {len(issues)}",""]+issues if issues else [f"RaagDosa v{APP_VERSION} — Verify",f"Profile: {profile_name}",f"Date: {now_iso()}","No issues."]),encoding="utf-8")
    out(f"\n{C.DIM}Report: {vpath}{C.RESET}")


def cmd_reference(cfg_path:Path,cfg:Dict[str,Any],action:str,
                  import_path:Optional[str]=None,section:Optional[str]=None,
                  export_path:Optional[str]=None)->None:
    """Handle `raagdosa reference list|import|export` commands."""
    ref=cfg.get("reference",{})

    if action=="list":
        aliases=ref.get("artist_aliases",{}) or {}
        labels=ref.get("known_labels",[]) or []
        va_prefixes=ref.get("va_rescue_prefixes",[]) or []
        noise=ref.get("noise_patterns",[]) or []
        out(f"\n{C.BOLD}Musical Reference{C.RESET}")
        out(f"{'─'*50}")
        out(f"  Artist aliases:     {len(aliases)}")
        out(f"  Known labels:       {len(labels)}")
        out(f"  VA rescue prefixes: {len(va_prefixes)}")
        out(f"  Noise patterns:     {len(noise)}")
        if aliases:
            out(f"\n  {C.DIM}Artist aliases (first 20):{C.RESET}")
            for i,(k,v) in enumerate(sorted(aliases.items(),key=lambda x:x[1].lower())):
                if i>=20:
                    out(f"  {C.DIM}  ... and {len(aliases)-20} more{C.RESET}"); break
                out(f"    {k:30s} → {v}")
        if labels:
            out(f"\n  {C.DIM}Known labels:{C.RESET}")
            for lb in labels[:15]:
                out(f"    {lb}")
            if len(labels)>15: out(f"    {C.DIM}... and {len(labels)-15} more{C.RESET}")

    elif action=="export":
        # Export reference section to a shareable YAML file
        export_data={
            "raagdosa_reference_version":1,
            "exported_at":now_iso(),
        }
        if section:
            if section in ref:
                export_data[section]=ref[section]
            else:
                err(f"Unknown section: {section}. Available: {', '.join(ref.keys())}"); return
        else:
            export_data.update(ref)
        out_path=Path(export_path) if export_path else Path("reference_export.yaml")
        write_yaml(out_path,export_data)
        aliases_n=len(export_data.get("artist_aliases",{}))
        labels_n=len(export_data.get("known_labels",[]))
        out(f"  {C.GREEN}Exported:{C.RESET} {out_path}")
        out(f"  {aliases_n} aliases, {labels_n} labels")
        out(f"  Share this file with the community!")

    elif action=="import":
        if not import_path:
            err("Usage: raagdosa reference import <file.yaml>"); return
        imp_path=Path(import_path)
        if not imp_path.exists():
            err(f"File not found: {imp_path}"); return
        imp_data=read_yaml(imp_path)
        # Strip metadata keys
        for meta_key in ("raagdosa_reference_version","exported_at"):
            imp_data.pop(meta_key,None)
        # Merge each section
        added=0; conflicts=0; source=f"imported:{imp_path.name}:{now_iso()[:10]}"
        # Artist aliases
        if "artist_aliases" in imp_data:
            existing=ref.get("artist_aliases",{}) or {}
            incoming=imp_data["artist_aliases"] or {}
            for k,v in incoming.items():
                if k.lower() in {ek.lower() for ek in existing}:
                    # Check if canonical differs
                    existing_canonical=next((ev for ek,ev in existing.items() if ek.lower()==k.lower()),None)
                    if existing_canonical and existing_canonical!=v:
                        warn(f"Conflict: '{k}' → yours: '{existing_canonical}' vs import: '{v}'")
                        choice=input(f"    Keep yours (y) or use import (n)? [y/N]: ").strip().lower()
                        if choice!="n":
                            conflicts+=1; continue
                    else:
                        continue  # identical, skip
                existing[k]=v; added+=1
            ref["artist_aliases"]=existing
        # Known labels
        if "known_labels" in imp_data:
            existing_labels=set(ref.get("known_labels",[]) or [])
            for lb in (imp_data["known_labels"] or []):
                if lb not in existing_labels:
                    existing_labels.add(lb); added+=1
            ref["known_labels"]=sorted(existing_labels)
        # VA rescue prefixes
        if "va_rescue_prefixes" in imp_data:
            existing_va=set(ref.get("va_rescue_prefixes",[]) or [])
            for p in (imp_data["va_rescue_prefixes"] or []):
                if p not in existing_va:
                    existing_va.add(p); added+=1
            ref["va_rescue_prefixes"]=sorted(existing_va)
        # Noise patterns
        if "noise_patterns" in imp_data:
            existing_noise=ref.get("noise_patterns",[]) or []
            existing_pats={n.get("pattern","") if isinstance(n,dict) else n for n in existing_noise}
            for n in (imp_data["noise_patterns"] or []):
                pat=n.get("pattern","") if isinstance(n,dict) else n
                if pat and pat not in existing_pats:
                    existing_noise.append(n); added+=1
            ref["noise_patterns"]=existing_noise

        # Write back to config
        cfg["reference"]=ref
        write_yaml(cfg_path,cfg)
        out(f"\n  {C.GREEN}Imported:{C.RESET} {added} new entries from {imp_path.name}")
        if conflicts: out(f"  {C.YELLOW}Conflicts:{C.RESET} {conflicts} kept your version")
        out(f"  Source: {source}")
    else:
        err(f"Unknown reference action: {action}. Use: list, import, export")


def cmd_learn(cfg_path:Path,cfg:Dict[str,Any],session_id:Optional[str])->None:
    _resolve_log_paths_from_active_profile(cfg)
    sdir=Path(cfg["logging"]["session_dir"]); sessions:List[Path]=[]
    if session_id:
        sp=sdir/session_id/"proposals.json"
        if sp.exists(): sessions=[sp]
    else:
        if sdir.exists():
            all_s=sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True)
            sessions=[s/"proposals.json" for s in all_s[:5] if (s/"proposals.json").exists()]
    if not sessions: out("No sessions found."); return
    review_albums:List[str]=[]; review_reasons:Counter=Counter()
    low_conf:List[str]=[]; heuristic:List[str]=[]; unclean_suffixes:Counter=Counter()
    for sp in sessions:
        try: payload=read_json(sp)
        except Exception: continue
        for fp in payload.get("folder_proposals",[]):
            if fp.get("destination")!="review": continue
            name=fp.get("folder_name",""); reasons=fp.get("decision",{}).get("route_reasons",[])
            review_albums.append(name)
            for r in reasons: review_reasons[r]+=1
            if "low_confidence" in reasons: low_conf.append(name)
            if "heuristic_fallback" in reasons: heuristic.append(name)
            for b in re.findall(r"[\(\[]([\w\s]+)[\)\]]",name):
                bl=b.strip().lower()
                if bl not in {k.lower() for k in cfg.get("normalize",{}).get("strip_common_suffixes_for_voting",[]) or []}:
                    unclean_suffixes[bl]+=1
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa learn{C.RESET}\n{'═'*60}")
    out(f"Sessions analysed: {len(sessions)} | Review folders: {len(review_albums)}")
    if not review_albums: ok_msg("No Review folders — config looks well-tuned."); return
    out(f"\n{C.BOLD}Review reasons:{C.RESET}")
    for reason,count in review_reasons.most_common(): out(f"  {count:3d}×  {reason}")
    suggestions:List[Tuple[str,str,Any]]=[]
    if len(low_conf)>5:
        cur=float(cfg.get("review_rules",{}).get("min_confidence_for_clean",0.85))
        prop_conf=max(0.70,cur-0.05)
        suggestions.append(("review_rules.min_confidence_for_clean",f"Lower from {cur} → {prop_conf} ({len(low_conf)} folders hit this threshold)",prop_conf))
    new_suf=[(s,c) for s,c in unclean_suffixes.most_common(10) if c>=2]
    if new_suf:
        suggestions.append(("normalize.strip_common_suffixes_for_voting",f"Add {len(new_suf)} suffix(es) seen in Review names: "+", ".join(f"'{s}'({c}×)" for s,c in new_suf[:5]),[s for s,_ in new_suf]))
    if len(heuristic)>3:
        suggestions.append(("NOTE",f"{len(heuristic)} zero-tag folders — consider running MusicBrainz Picard on them first.",None))
    if not suggestions: ok_msg("No actionable suggestions."); return
    out(f"\n{C.BOLD}Suggestions:{C.RESET}")
    for i,(key,desc,_) in enumerate(suggestions,1): out(f"\n  [{i}] {C.CYAN}{key}{C.RESET}\n      {desc}")
    out("\nApply which? (e.g. 1,2  or  all  or  none): ",level=NORMAL)
    ans=input("  → ").strip().lower()
    if ans in ("","none","n"): out("No changes."); return
    chosen=set(range(1,len(suggestions)+1)) if ans=="all" else {int(x.strip()) for x in ans.split(",") if x.strip().isdigit()}
    applied_changes:List[str]=[]
    for i,(key,_,val) in enumerate(suggestions,1):
        if i not in chosen or val is None: continue
        if key=="review_rules.min_confidence_for_clean":
            cfg.setdefault("review_rules",{})["min_confidence_for_clean"]=val; applied_changes.append(f"{key} → {val}")
        elif key=="normalize.strip_common_suffixes_for_voting":
            ex=cfg.setdefault("normalize",{}).setdefault("strip_common_suffixes_for_voting",[])
            for s in val:
                if s not in ex: ex.append(s)
            applied_changes.append(f"Added {len(val)} suffix(es) to {key}")
    if applied_changes:
        write_yaml(cfg_path,cfg); out(f"\n{C.GREEN}Config updated:{C.RESET}")
        for c in applied_changes: out(f"  ✓ {c}")
        out(f"\n{C.DIM}Saved: {cfg_path}{C.RESET}")
    else: out("No changes applied.")


def cmd_dump_tree(
    cfg: Dict[str, Any],
    profile_name: str,
    out_path: str,
    include_clean: bool = False,
    include_review: bool = False,
    include_logs: bool = False,
    folders_only: bool = False,
    files_only: bool = False,
) -> None:
    profiles = cfg.get("profiles", {})
    if profile_name not in profiles:
        raise ValueError(f"Unknown profile: {profile_name}")

    profile = profiles[profile_name]
    source_root = Path(profile["source_root"]).expanduser().resolve()
    roots = ensure_roots(profile, source_root,create=False)

    if not source_root.exists():
        raise FileNotFoundError(f"source_root missing: {source_root}")

    clean_root = roots["clean_root"].resolve()
    review_root = roots["review_root"].resolve()
    logs_cfg = Path(cfg.get("logging", {}).get("root_dir", "logs"))
    logs_root = (source_root / logs_cfg).resolve() if not logs_cfg.is_absolute() else logs_cfg.resolve()

    out_file = Path(out_path).expanduser().resolve()
    ensure_dir(out_file.parent)

    def _is_under(path: Path, parent: Path) -> bool:
        try:
            path.resolve().relative_to(parent.resolve())
            return True
        except Exception:
            return False

    lines: List[str] = []

    for root, dirs, files in os.walk(source_root):
        root_path = Path(root).resolve()

        # Prune excluded top-level generated areas
        pruned_dirs = []
        for d in dirs:
            child = (root_path / d).resolve()

            if not include_clean and _is_under(child, clean_root):
                continue
            if not include_review and _is_under(child, review_root):
                continue
            if not include_logs and _is_under(child, logs_root):
                continue

            pruned_dirs.append(d)

        dirs[:] = sorted(pruned_dirs)
        files = sorted(files)

        # Skip hidden macOS noise files in output
        rel_root = root_path.relative_to(source_root)
        depth = 0 if str(rel_root) == "." else len(rel_root.parts)

        if not files_only:
            rel_str = "." if str(rel_root) == "." else rel_root.as_posix()
            lines.append(f"DIR|depth={depth}|path={rel_str}")

        if folders_only:
            continue

        for fname in files:
            fpath = root_path / fname

            if is_hidden_file(fpath):
                continue

            if not include_clean and _is_under(fpath, clean_root):
                continue
            if not include_review and _is_under(fpath, review_root):
                continue
            if not include_logs and _is_under(fpath, logs_root):
                continue

            rel_file = fpath.relative_to(source_root).as_posix()
            file_depth = len(Path(rel_file).parts) - 1
            ext = fpath.suffix.lower() or ""
            lines.append(f"FILE|depth={file_depth}|ext={ext}|path={rel_file}")

    out_file.write_text("\n".join(lines) + "\n", encoding="utf-8")

    out(f"\n{C.BOLD}raagdosa dump-tree — {profile_name}{C.RESET}")
    out(f"Source: {source_root}")
    out(f"Wrote:  {out_file}")
    out(f"Lines:  {len(lines)}")


def cmd_orphans(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root,create=False)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    orphans:List[Path]=[]
    for root_key in ("clean_albums","review_albums"):
        base=roots[root_key]
        if not base.exists(): continue
        # Files directly in the Albums/ root (no subfolder)
        for p in base.iterdir():
            if p.is_file() and p.suffix.lower() in exts and not is_hidden_file(p):
                orphans.append(p)
        # Files in artist-level folder (one level deep) but no album subfolder
        for artist_dir in base.iterdir():
            if not artist_dir.is_dir(): continue
            for p in artist_dir.iterdir():
                if p.is_file() and p.suffix.lower() in exts and not is_hidden_file(p):
                    orphans.append(p)
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa orphans — {profile_name}{C.RESET}\n{'═'*60}")
    if not orphans:
        ok_msg("No orphan audio files found — library looks tidy.")
        return
    out(f"\n{C.YELLOW}Found {len(orphans)} orphan file(s):{C.RESET}")
    for p in sorted(orphans): out(f"  {C.DIM}{p}{C.RESET}")
    out(f"\n{C.DIM}Orphans can be moved to '{profile.get('orphans_folder_name','Orphans')}' manually or by adding them to an album folder.{C.RESET}")


def cmd_artists(cfg:Dict[str,Any],profile_name:str,list_mode:bool,find_query:Optional[str])->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root,create=False); clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa artists — {profile_name}{C.RESET}\n{'═'*60}")

    if not clean_albums.exists(): out("Clean/Albums not found."); return
    lib=cfg.get("library",{}); template=lib.get("template","{artist}/{album}")

    # Collect artist directories (top-level subdirs of clean_albums, excluding _* special folders)
    if "{artist}" in template:
        # Artist folders are direct children
        artist_dirs=sorted([d for d in clean_albums.iterdir() if d.is_dir() and not d.name.startswith("_")])
    else:
        # Flat template — read albumartist from tags
        artist_dirs=[]

    if find_query:
        q=find_query.strip().lower()
        matches=[(d,string_similarity(d.name.lower(),q)) for d in artist_dirs]
        matches=sorted([(d,s) for d,s in matches if s>=0.3],key=lambda x:x[1],reverse=True)
        if not matches: out(f"No artists matching '{find_query}'."); return
        out(f"\nMatches for '{find_query}':")
        for d,score in matches[:20]:
            albums=len([x for x in d.iterdir() if x.is_dir()]) if d.is_dir() else 0
            out(f"  {C.GREEN}{d.name:<40}{C.RESET}  {score:.0%} match  {albums} album(s)  {C.DIM}{d}{C.RESET}")
    else:
        if not list_mode: out(f"\n{len(artist_dirs)} artist(s) in Clean:"); list_mode=True
        for d in artist_dirs:
            albums=len([x for x in d.iterdir() if x.is_dir()]) if d.is_dir() else 0
            tracks=sum(len(list_audio_files(x,exts)) for x in d.rglob("*") if x.is_dir())
            out(f"  {d.name:<45}  {albums:3d} album(s)  {tracks:4d} tracks")
        out(f"\n{C.DIM}Total: {len(artist_dirs)} artists{C.RESET}")


def cmd_review_list(cfg:Dict[str,Any],profile_name:str,older_than_days:Optional[int])->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root,create=False); review_albums=roots["review_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa review-list — {profile_name}{C.RESET}\n{'═'*60}")
    if not review_albums.exists(): out("Review/Albums not found — empty."); return

    now=dt.datetime.now()
    cutoff=now-dt.timedelta(days=older_than_days) if older_than_days else None

    # Build a lookup from session proposals for confidence/reason/origin data
    conf_map:Dict[str,float]={}; reason_map:Dict[str,str]={}
    origin_map:Dict[str,str]={}; detail_map:Dict[str,Dict[str,Any]]={}
    sdir=Path(cfg["logging"]["session_dir"])
    if sdir.exists():
        for sd in sorted(sdir.iterdir(),key=lambda p:p.name,reverse=True)[:10]:
            pf=sd/"proposals.json"
            if not pf.exists(): continue
            try:
                pl=read_json(pf)
                for fp in pl.get("folder_proposals",[]):
                    nm=fp.get("proposed_folder_name","")
                    if nm and nm not in conf_map:
                        conf_map[nm]=float(fp.get("confidence",0.0))
                        dec=fp.get("decision",{})
                        reason_map[nm]=", ".join(dec.get("route_reasons",[]))
                        origin_map[nm]=fp.get("folder_name","")
                        detail_map[nm]={"artist":dec.get("albumartist_display",""),
                                        "is_va":dec.get("is_va",False),
                                        "folder_type":dec.get("folder_type",""),
                                        "heuristic":dec.get("used_heuristic",False)}
            except Exception: continue

    folders=sorted([d for d in review_albums.rglob("*") if d.is_dir()
                    and any(f.suffix.lower() in exts for f in d.iterdir() if f.is_file())],
                   key=lambda d:folder_mtime(d))

    if cutoff:
        folders=[d for d in folders if dt.datetime.fromtimestamp(folder_mtime(d))<=cutoff]

    if not folders: out(f"No Review folders{' older than '+str(older_than_days)+' days' if older_than_days else ''}."); return
    out("")
    for d in folders:
        mtime=dt.datetime.fromtimestamp(folder_mtime(d))
        age=(now-mtime).days
        conf=conf_map.get(d.name)
        reason=reason_map.get(d.name,"")
        origin=origin_map.get(d.name,"")
        detail=detail_map.get(d.name,{})
        conf_s=conf_color(conf) if conf else f"{C.DIM}n/a{C.RESET}"
        age_col=C.RED if age>60 else (C.YELLOW if age>14 else C.DIM)
        # v6.1: Show FROM → TO transformation so user can evaluate the proposal
        if origin and origin != d.name:
            out(f"  {C.DIM}{origin}{C.RESET}")
            out(f"  → {C.BOLD}{d.name}{C.RESET}  {conf_s}  {age_col}{age}d{C.RESET}")
        else:
            out(f"  {C.BOLD}{d.name}{C.RESET}  {conf_s}  {age_col}{age}d{C.RESET}")
        # Show key decision context
        flags=[]
        if detail.get("artist"): flags.append(f"artist={detail['artist']}")
        if detail.get("is_va"): flags.append("VA")
        if detail.get("folder_type") and detail["folder_type"] not in ("album",): flags.append(detail["folder_type"])
        if detail.get("heuristic"): flags.append("heuristic")
        if reason: flags.append(reason)
        if flags:
            out(f"    {C.DIM}{' | '.join(flags)}{C.RESET}")
        out("")
    out(f"{C.DIM}Total: {len(folders)} folder(s) in Review{C.RESET}")


def cmd_diff(cfg:Dict[str,Any],session_a:str,session_b:str)->None:
    sdir=Path(cfg["logging"]["session_dir"])
    def _load(sid:str)->Dict[str,Any]:
        # Support "last" / "prev" shortcuts
        all_s=sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True) if sdir.exists() else []
        if sid=="last" and all_s: sid=all_s[0].name
        elif sid=="prev" and len(all_s)>1: sid=all_s[1].name
        p=sdir/sid/"proposals.json"
        if not p.exists(): err(f"Session not found: {sid}"); sys.exit(1)
        pl=read_json(p); pl["_session_id"]=sid; return pl
    pa=_load(session_a); pb=_load(session_b)
    props_a={fp["proposed_folder_name"]:fp for fp in pa.get("folder_proposals",[])}
    props_b={fp["proposed_folder_name"]:fp for fp in pb.get("folder_proposals",[])}
    names_a=set(props_a); names_b=set(props_b)
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa diff{C.RESET}")
    out(f"  A: {pa['_session_id']}  ({len(props_a)} proposals)")
    out(f"  B: {pb['_session_id']}  ({len(props_b)} proposals)\n{'─'*60}")
    only_a=sorted(names_a-names_b); only_b=sorted(names_b-names_a); common=sorted(names_a&names_b)
    if only_a:
        out(f"\n{C.YELLOW}Only in A ({len(only_a)}):{C.RESET}")
        for n in only_a: out(f"  {C.YELLOW}{n}{C.RESET}")
    if only_b:
        out(f"\n{C.CYAN}Only in B ({len(only_b)}):{C.RESET}")
        for n in only_b: out(f"  {C.CYAN}{n}{C.RESET}")
    changed_dest=[]; changed_conf=[]
    for n in common:
        fa=props_a[n]; fb=props_b[n]
        if fa.get("destination")!=fb.get("destination"):
            changed_dest.append((n,fa.get("destination"),fb.get("destination"),fa.get("confidence",0),fb.get("confidence",0)))
        elif abs(float(fa.get("confidence",0))-float(fb.get("confidence",0)))>0.05:
            changed_conf.append((n,float(fa.get("confidence",0)),float(fb.get("confidence",0))))
    if changed_dest:
        out(f"\n{C.BOLD}Destination changes ({len(changed_dest)}):{C.RESET}")
        for n,da,db,ca,cb in changed_dest:
            out(f"  {n[:50]:<50}  {da}→{db}  conf {ca:.2f}→{cb:.2f}")
    if changed_conf:
        out(f"\n{C.BOLD}Confidence changes >5% ({len(changed_conf)}):{C.RESET}")
        for n,ca,cb in changed_conf:
            delta=cb-ca; sym="↑" if delta>0 else "↓"
            out(f"  {n[:50]:<50}  {conf_color(ca)} {sym} {conf_color(cb)}")
    if not only_a and not only_b and not changed_dest and not changed_conf:
        ok_msg("Sessions are identical.")

# ─────────────────────────────────────────────
# report
# ─────────────────────────────────────────────


def cmd_status(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root,create=False)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    def count_in(path:Path)->Tuple[int,int]:
        if not path.exists(): return 0,0
        folders=[p for p in path.rglob("*") if p.is_dir() and any(f.suffix.lower() in exts for f in p.iterdir() if f.is_file())]
        return len(folders),sum(len(list_audio_files(f,exts)) for f in folders)
    cf,ct=count_in(roots["clean_albums"]); rf,rt=count_in(roots["review_albums"]); df,dt_n=count_in(roots["duplicates"])
    manifest=read_manifest(cfg); committed=len(manifest.get("entries",{})); last_run=manifest.get("last_run")
    out(f"\n{C.BOLD}{'═'*50}{C.RESET}\n{C.BOLD}RaagDosa Status — {profile_name}{C.RESET}\n{'═'*50}")
    src_exists=source_root.exists()
    out(f"Source:              {source_root}  ({C.GREEN+'exists'+C.RESET if src_exists else C.RED+'MISSING'+C.RESET})")
    out(f"\n{C.GREEN}Clean/Albums:{C.RESET}        {cf:4d} folders  {ct:5d} tracks")
    out(f"{C.YELLOW}Review/Albums:{C.RESET}       {rf:4d} folders  {rt:5d} tracks")
    out(f"{C.RED}Review/Duplicates:{C.RESET}   {df:4d} folders  {dt_n:5d} tracks")
    out(f"\nManifest entries:    {committed}"); out(f"Last run:            {last_run or 'never'}")
    clean_str=str(roots["clean_root"].resolve()); review_str=str(roots["review_root"].resolve())
    min_t=int(cfg.get("scan",{}).get("min_tracks",3)); pending=0
    if source_root.exists():
        for root,dirs,files in os.walk(source_root):
            rp=str(Path(root).resolve())
            if rp.startswith(clean_str) or rp.startswith(review_str): dirs[:]=[] ; continue
            if sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_t: pending+=1
    out(f"Pending in source:   {pending} folder(s)")
    lr_dt=manifest_get_last_run(cfg)
    if lr_dt and source_root.exists():
        new_since=sum(1 for root,dirs,files in os.walk(source_root)
                      if dt.datetime.fromtimestamp(folder_mtime(Path(root)))>=lr_dt
                      and sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_t)
        out(f"New since last run:  {new_since} folder(s)  {C.DIM}(use --since last_run){C.RESET}")
    dj=cfg.get("dj_safety",{})
    if dj.get("detect_dj_databases",True) and source_root.exists():
        dbs=find_dj_databases(source_root)
        if dbs: warn(f"DJ databases: {', '.join(dbs)}")
        else: ok_msg("No DJ databases detected.")
    try:
        stat=shutil.disk_usage(str(roots["clean_albums"].parent))
        out(f"\nDisk (dest):         {stat.free/1024**3:.1f} GB free / {stat.total/1024**3:.1f} GB total")
    except Exception: pass
    out()

# ─────────────────────────────────────────────
# init
# ─────────────────────────────────────────────


def cmd_init(cfg_path:Path)->None:
    paths_file=cfg_path.parent/"paths.local.yaml"
    is_first_run=not cfg_path.exists()
    is_adding_profile=cfg_path.exists() and paths_file.exists()

    out(f"\n{C.BOLD}RaagDosa v{APP_VERSION} — Setup{C.RESET}")
    out(f"{'═'*50}")

    if is_adding_profile:
        out(f"\n  Existing setup detected. Adding a new profile.\n")
        paths_data=read_yaml(paths_file)
        existing_profiles=list((paths_data.get("profiles") or {}).keys())
        if existing_profiles:
            out(f"  Current profiles: {', '.join(existing_profiles)}")
    else:
        out("")
        paths_data={}

    # ── 1. Source folder ─────────────────────────────────────────────────
    source=input("  Source folder (where your unsorted music lives):\n  → ").strip()
    if not source: err("Source folder is required."); sys.exit(1)
    source_path=Path(source).expanduser().resolve()
    if not source_path.exists():
        out(f"\n  {C.YELLOW}Warning: {source_path} does not exist yet.{C.RESET}")
        if input("  Continue anyway? [Y/n] ").strip().lower() in ("n","no"):
            out("Aborted."); return

    # ── 2. Profile name ──────────────────────────────────────────────────
    out(f"\n  Profiles let you run RaagDosa on different source folders independently.")
    out(f"  {C.DIM}Examples: bandcamp, beatport, promos, soulseek, vinyl-rips{C.RESET}")
    default_name=source_path.name.lower().replace(" ","_")
    # Avoid colliding with existing profiles
    existing=set((paths_data.get("profiles") or {}).keys())
    if default_name in existing:
        n=2
        while f"{default_name}_{n}" in existing: n+=1
        default_name=f"{default_name}_{n}"
    profile_name=input(f"\n  Profile name [{default_name}]: ").strip() or default_name
    profile_name=profile_name.lower().replace(" ","_")

    # ── 3. Clean mode ────────────────────────────────────────────────────
    _src_short=source_path.name
    out(f"\n  {C.BOLD}Where should sorted output go?{C.RESET}")
    out(f"    [1] Inside source  — {_src_short}/raagdosa/Clean/  {C.DIM}(default, most setups){C.RESET}")
    out(f"    [2] Next to source — {source_path.parent.name}/raagdosa/Clean/  {C.DIM}(keeps source untouched){C.RESET}")
    out(f"    [?] Show folder tree examples")
    mode_choice=input("\n  Choose [1]: ").strip()
    if mode_choice=="?":
        out(f"\n    {C.CYAN}[1] Inside source folder{C.RESET}")
        out(f"        {_src_short}/")
        out(f"        ├── (your music files)")
        out(f"        └── raagdosa/")
        out(f"            ├── Clean/Albums/   ← sorted, high confidence")
        out(f"            └── Review/Albums/  ← needs your eyes\n")
        out(f"    {C.CYAN}[2] Next to source folder{C.RESET}")
        out(f"        {source_path.parent.name}/")
        out(f"        ├── {_src_short}/        ← source (untouched)")
        out(f"        └── raagdosa/")
        out(f"            ├── Clean/Albums/")
        out(f"            └── Review/Albums/\n")
        mode_choice=input("  Choose [1]: ").strip()
    clean_mode="inside_parent" if mode_choice=="2" else "inside_root"

    # ── 4. Write paths.local.yaml ────────────────────────────────────────
    if clean_mode=="inside_parent":
        wrapper=source_path.parent/"raagdosa"
    else:
        wrapper=source_path/"raagdosa"

    # ── 5. Write paths.local.yaml ────────────────────────────────────────
    if "profiles" not in paths_data:
        paths_data["profiles"]={}
    paths_data["profiles"][profile_name]={"source_root":str(source_path),"clean_mode":clean_mode}
    paths_data["active_profile"]=profile_name
    write_yaml(paths_file,paths_data)

    # ── 6. Write config.yaml (first run only) ────────────────────────────
    if is_first_run:
        new_cfg={
            "app":{"name":"RaagDosa","version":APP_VERSION},
            "profiles":{profile_name:{
                "wrapper_folder_name":"raagdosa",
                "clean_folder_name":"Clean","review_folder_name":"Review",
                "clean_albums_folder_name":"Albums","clean_tracks_folder_name":"Tracks",
                "review_albums_folder_name":"Albums","duplicates_folder_name":"Duplicates","orphans_folder_name":"Orphans"}},
            "library":{"template":"{artist}/{album}","flac_segregation":False,"singles_folder":"Singles","va_folder":"_Various Artists","unknown_artist_label":"_Unknown"},
            "artist_normalization":{"enabled":True,"the_prefix":"keep-front","normalize_hyphens":True,"fuzzy_dedup_threshold":0.92,"unicode_map":{},"aliases":{}},
            "scan":{"audio_extensions":[".mp3",".flac",".m4a",".aiff",".wav"],"leaf_folders_only":True,"min_tracks":3,"max_unreadable_track_ratio":0.25,"follow_symlinks":False,
                    "skip_sidecar_extensions":[".sfk",".asd",".reapeaks",".pkf",".db",".lrc"],
                    "skip_system_folders":["__MACOSX","__macosx"]},
            "ep_detection":{"enabled":True,"min_tracks":2,"max_tracks":6},
            "ignore":{"ignore_folder_names":["Singles","One-Offs","Dump","_dump","Clean","Review","raagdosa"]},
            "tags":{"album_keys":["album"],"albumartist_keys":["albumartist","album_artist","album artist"],
                    "artist_keys":["artist"],"title_keys":["title"],"tracknumber_keys":["tracknumber","track"],
                    "discnumber_keys":["discnumber","disc"],"year_keys_prefer":["originaldate","date","year"],
                    "bpm_keys":["bpm","tbpm"],"key_keys":["initialkey","key","tkey"]},
            "normalize":{"lower_case":True,"strip_whitespace":True,"collapse_whitespace":True,
                         "strip_punctuation_for_voting":True,"strip_bracketed_phrases_for_voting":True,
                         "strip_common_suffixes_for_voting":["deluxe edition","expanded edition","remaster","remastered","anniversary edition","bonus tracks","explicit","special edition"]},
            "fuzzy":{"enabled":True,"similarity_threshold":0.88,"prompt_threshold":0.75},
            "decision":{"album_dominance_threshold":0.75,"allow_artist_fallback":True,"require_confirmation":True,"auto_approve_above":0.92,"interactive_below":0.92},
            "various_artists":{"label":"VA","albumartist_matches":["various artists","various","va","v/a"],"enable_heuristics":True,"unique_artist_ratio_above":0.75},
            "year":{"enabled":True,"allowed_range":{"min":1900,"max":2030},"require_presence_ratio":0.50,"agreement_threshold":0.70},
            "format":{"pattern_no_year":"{albumartist} - {album}","pattern_with_year":"{albumartist} - {album} ({year})","replace_illegal_chars_with":" - ","trim_trailing_dots_spaces":True},
            "format_suffix":{"enabled":True,"only_if_all_same_extension":True,"ignore_extension":".mp3","style":"brackets_upper"},
            "review_rules":{"min_confidence_for_clean":0.85,"route_duplicates":True,"route_cross_run_duplicates":True,"route_questionable_to_review":True,"route_heuristic_to_review":True},
            "move":{"enabled":True,"on_collision":"suffix","suffix_format":" ({n})","use_checksum":False},
            "dj_safety":{"detect_dj_databases":True,"warn_on_dj_databases":True,"halt_on_dj_databases":False,
                         "database_patterns":["export.pdb","database2","rekordbox.xml","_Serato_","Serato Scratch","Serato DJ","PIONEER"]},
            "track_rename":{"enabled":True,"scope":"clean_only","allowed_extensions":[".mp3",".flac",".m4a"],"skip_extensions":[".wav",".aiff"],
                            "patterns":{"album":"{disc_prefix}{track:02d} - {title}{mix_suffix}{ext}","various":"{disc_prefix}{track:02d} - {artist} - {title}{mix_suffix}{ext}","mixed":"{artist} - {title}{mix_suffix}{ext}"},
                            "disc":{"enabled":True,"only_if_multi_disc":True,"format":"{disc}-"},
                            "track_numbers":{"required_for_album":True,"required_for_various":True,"pad_width":2,"fallback_to_filename_order":False}},
            "artists":{"feature_handling":{"enabled":True,"style":"keep_in_artist","normalize_tokens":True}},
            "mix_info":{"enabled":True,"detect_keywords":["remix","edit","mix","version","rework","bootleg","dub","original mix","extended mix","extended version","club mix","radio edit","instrumental","acapella","vip","flip"],"style":"parenthetical"},
            "title_cleanup":{"enabled":True,"strip_trailing_domains":True,"strip_trailing_handles":True,
                             "strip_trailing_phrases":["official video","official music video","official audio","lyrics","lyric video","free download","download","uploaded by","ripped by","encoded by","320kbps","lossless","hi-res","soundcloud","youtube","bandcamp","spotify","prod. by","prod by","clip","teaser","preview"],
                             "keep_parenthetical_if_contains":["live","remaster","edit","mix","version","instrumental","acapella","demo","mono","stereo","original","extended","club","radio","vip","dub","feat"],
                             "normalize":{"replace_underscores":True,"collapse_whitespace":True,"trim_dots_spaces":True}},
            "logging":{"root_dir":"logs","session_dir":"logs/sessions","history_log":"logs/history.jsonl",
                       "skipped_log":"logs/skipped.jsonl","track_history_log":"logs/track-history.jsonl",
                       "track_skipped_log":"logs/track-skipped.jsonl","write_human_report":True,"report_filename":"report.txt","rotate_log_max_mb":10.0},
            "undo":{"allow_undo_by_id":True,"allow_undo_by_original_path":True,"allow_undo_by_session":True,"allow_undo_by_folder":True}}
        write_yaml(cfg_path,new_cfg)
        out(f"  {C.GREEN}✓ Config written: {cfg_path}{C.RESET}")
    elif not is_adding_profile:
        # config.yaml exists but no paths.local.yaml — just created paths.local.yaml
        out(f"  {C.DIM}config.yaml already exists — not modified.{C.RESET}")

    # ── 7. Summary + next steps ──────────────────────────────────────────
    out(f"\n  {C.GREEN}✓ Profile '{profile_name}' ready{C.RESET}")
    out(f"    Source:  {source_path}")
    out(f"    Output:  {wrapper}/Clean/  and  {wrapper}/Review/")
    out(f"\n  {C.BOLD}Next steps:{C.RESET}")
    out(f"    raagdosa doctor                        ← verify setup")
    out(f"    raagdosa go --profile {profile_name} --dry-run  ← preview")
    out(f"    raagdosa go --profile {profile_name}            ← do it")
    if is_first_run:
        out(f"\n  {C.DIM}config.yaml has sensible defaults. Tweak later if needed.{C.RESET}")
    out(f"  {C.DIM}Run 'raagdosa init' again to add another source folder.{C.RESET}\n")

# ─────────────────────────────────────────────
# High-level flows
# ─────────────────────────────────────────────


def cmd_sessions(cfg:Dict[str,Any],last:int=20)->None:
    """List recent sessions with move counts and timestamps."""
    _resolve_log_paths_from_active_profile(cfg)
    hist_path=Path(cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
    if not hist: out("No session history found."); return
    # Build ordered session summary
    seen:Dict[str,Dict]={}
    order:List[str]=[]
    for h in hist:
        sid=h.get("session_id",""); ts=h.get("timestamp","")
        dest=h.get("destination","")
        orig=h.get("original_folder_name","")
        if sid not in seen:
            seen[sid]={"first_ts":ts,"last_ts":ts,"total":0,"clean":0,"review":0,"examples":[]}
            order.append(sid)
        s=seen[sid]; s["last_ts"]=ts; s["total"]+=1
        if dest=="clean": s["clean"]+=1
        elif dest=="review": s["review"]+=1
        if len(s["examples"])<3 and orig: s["examples"].append(orig)
    recent=order[-last:]
    out(f"\n{C.BOLD}Recent sessions ({len(recent)} of {len(order)} total):{C.RESET}\n")
    for sid in reversed(recent):
        s=seen[sid]
        out(f"  {C.BOLD}{sid}{C.RESET}")
        out(f"    {s['total']} moves  ·  {C.GREEN}{s['clean']} clean{C.RESET}  ·  {C.YELLOW}{s['review']} review{C.RESET}  ·  {s['first_ts'][:16]}")
        if s["examples"]:
            out(f"    e.g. {', '.join(s['examples'][:3])}")
        out("")
    out(f"  To undo a session:  raagdosa undo --session <session_id>")
    out(f"  To undo last:       raagdosa undo --session last")



def cmd_history(cfg:Dict[str,Any],last:int,session:Optional[str],match:Optional[str],tracks:bool)->None:
    _resolve_log_paths_from_active_profile(cfg)
    hist_path=Path(cfg["logging"]["track_history_log"] if tracks else cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
    if session=="last": session=_resolve_last_session(hist)
    if session: hist=[h for h in hist if h.get("session_id")==session]
    if match:   hist=[h for h in hist if match in h.get("original_path","")+" "+h.get("target_path","")]
    hist=hist[-last:] if last and len(hist)>last else hist
    session_counts:Counter=Counter()
    for h in hist:
        session_counts[h.get("session_id","")]+=1
        src=Path(h.get("original_path","")).name; dst=Path(h.get("target_path","")).name
        out(f"  {h.get('action_id')}  {h.get('timestamp')}  {src}  →  {dst}")
    out(f"\nTotal: {len(hist)}")
    for sid,cnt in session_counts.most_common(): out(f"  {sid}: {cnt} action(s)")



def cmd_undo(cfg:Dict[str,Any],action_id:Optional[str],session_id:Optional[str],from_path:Optional[str],tracks:bool,folder:Optional[str])->None:
    """
    Undo folder moves or track renames.

    --folder <name>  works for BOTH folder-level and track-level undo:
      • Without --tracks: undoes all folder moves where the original path
        contains <name> as a path component  (targets a source folder by name).
      • With --tracks:    undoes all track renames inside clean folder <name>.

    Examples:
      raagdosa undo --folder "Burial - Untold" --session last
      raagdosa undo --tracks --folder "Burial - Untold"
      raagdosa undo --session 2026-03-08_14-30_incoming_test
    """
    _resolve_log_paths_from_active_profile(cfg)
    hist_path=Path(cfg["logging"]["track_history_log"] if tracks else cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
    if not hist: err("No history."); return
    selected:List[Dict[str,Any]]=[]
    # Handle -1/-2/... shorthand: resolve to the Nth-most-recent session
    if session_id and re.match(r'^-\d+$',session_id):
        n=abs(int(session_id))  # -1 → 1, -2 → 2
        all_sids=list(dict.fromkeys(h.get("session_id") for h in hist if h.get("session_id")))
        if n<=len(all_sids):
            session_id=all_sids[-n]  # -1 = last, -2 = second-to-last
            out(f"  Undo target: session {session_id}")
        else:
            err(f"Only {len(all_sids)} session(s) in history."); return
        hist=iter_jsonl(hist_path)  # re-read after exhausting iterator
    if session_id=="last": session_id=_resolve_last_session(hist)
    if action_id:    selected=[h for h in hist if h.get("action_id")==action_id]
    elif session_id: selected=[h for h in hist if h.get("session_id")==session_id]
    elif from_path:  selected=[h for h in hist if from_path in h.get("original_path","")]
    elif folder:
        if tracks:
            # Track-level: match by folder field stored in track history
            selected=[h for h in hist if h.get("folder")==folder]
        else:
            # Folder-level: match if folder name appears as a path component
            selected=[h for h in hist if
                      folder==Path(h.get("original_path","")).name or
                      folder in h.get("original_path","")]
    else:
        # Interactive picker — show last session's moves and let user pick
        last_sid=_resolve_last_session(hist)
        if not last_sid: err("No history found."); return
        session_hist=sorted([h for h in hist if h.get("session_id")==last_sid],
                            key=lambda x:x.get("timestamp",""))
        if not session_hist: err("No moves in last session."); return
        out(f"\n{C.BOLD}Last session: {last_sid}{C.RESET}")
        out(f"  {'#':<4} {'Folder':<55} {'Dest'}")
        out(f"  {'─'*70}")
        for i,h in enumerate(session_hist,1):
            name=Path(h.get("original_path","")).name or h.get("action_id","")
            dest=h.get("destination","?")
            dest_col=C.GREEN if dest=="clean" else C.YELLOW
            out(f"  {i:<4} {name:<55} {dest_col}{dest}{C.RESET}")
        out(f"\n  Enter number(s) to undo (e.g. 3  or  1,3,5  or  all), or Enter to cancel:")
        try:
            raw=input("  > ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            raw=""
        if not raw: out("Cancelled."); return
        if raw=="all":
            selected=session_hist
        else:
            idxs=[]
            for tok in re.split(r"[,\s]+",raw):
                try: idxs.append(int(tok)-1)
                except ValueError: pass
            selected=[session_hist[i] for i in idxs if 0<=i<len(session_hist)]
        if not selected: out("Nothing selected."); return
    if not selected: err("No matches."); return
    selected=sorted(selected,key=lambda x:x.get("timestamp",""),reverse=True); undone=0
    _cleanup_dirs: Set[Path] = set()
    for h in selected:
        src=Path(h["target_path"]); dst=Path(h["original_path"])
        if not src.exists(): out(f"  SKIP (missing): {src.name}"); continue
        if dst.exists():
            n=1
            while True:
                cand=dst.with_name(dst.stem+f" (UNDO{n})"+dst.suffix)
                if not cand.exists(): dst=cand; break
                n+=1
        try:
            ensure_dir(dst.parent)
            if tracks:
                src.rename(dst)
            elif h.get("type") in ("crate_explode","catchall"):
                # v9.0: crate explosion / catchall undo — move individual file back
                shutil.move(str(src),str(dst))
                _cleanup_dirs.add(src.parent)
                _cleanup_dirs.add(src.parent.parent)
            else:
                shutil.move(str(src),str(dst))
            undone+=1; out(f"  UNDONE: {h.get('action_id')}  {src.name}  →  {dst.name}")
        except Exception as e: err(f"  FAILED: {h.get('action_id')} — {e}")
    # Clean up empty dirs left behind (deepest first so parents become empty too)
    for d in sorted(_cleanup_dirs, key=lambda p: len(p.parts), reverse=True):
        try:
            if d.exists() and not any(d.iterdir()):
                d.rmdir()
                out(f"  REMOVED empty: {d.name}/")
        except Exception: pass
    out(f"\nUndo complete. Reverted: {undone}")


def cmd_template_list(cfg:Dict[str,Any])->None:
    """List all builtin templates and show which one is active per profile."""
    active_profile=cfg.get("active_profile","")
    active_tpl=""
    if active_profile:
        prof=cfg.get("profiles",{}).get(active_profile,{})
        lib=_resolve_lib_cfg(prof,cfg)
        active_tpl=lib.get("template","{artist}/{album}")

    out(f"\n{C.BOLD}Library Templates{C.RESET}")
    out(f"{'  ID':<18}{'Name':<22}{'Pattern':<45}{'Tags'}")
    out(f"  {'─'*16}  {'─'*20}  {'─'*43}  {'─'*15}")
    for tid,t in BUILTIN_TEMPLATES.items():
        marker=""
        if t["template"]==active_tpl:
            marker=f" {C.GREEN}← active{C.RESET}"
        reqs=", ".join(t.get("requires",[])) or "—"
        out(f"  {tid:<16}  {t['name']:<20}  {t['template']:<43}  {reqs}{marker}")
    out(f"\n  {C.DIM}Set on a profile: raagdosa profile set <name> --template <id>{C.RESET}")
    out(f"  {C.DIM}Or in config.yaml under profiles.<name>.library.template{C.RESET}\n")


def cmd_template_show(cfg:Dict[str,Any],name:str)->None:
    """Show details and example tree for a template."""
    t=BUILTIN_TEMPLATES.get(name)
    if not t:
        # Check if it matches an active profile's custom template
        err(f"Unknown template: {name}")
        out(f"  Available: {', '.join(BUILTIN_TEMPLATES.keys())}")
        return
    out(f"\n{C.BOLD}Template: {name}{C.RESET} — {t['name']}")
    out(f"Pattern:  {t['template']}")
    out(f"{t['description']}")
    if t.get("note"):
        out(f"{C.YELLOW}Note:{C.RESET} {t['note']}")
    reqs=t.get("requires",[])
    if reqs:
        out(f"\n{C.BOLD}Required tags:{C.RESET} {', '.join(reqs)}")
        out(f"  {C.DIM}Albums missing these tags will go to the fallback folder (_Unsorted).{C.RESET}")
    else:
        out(f"\n{C.BOLD}Required tags:{C.RESET} none (works with any metadata)")
    examples=_TEMPLATE_EXAMPLES.get(name,[])
    if examples:
        out(f"\n{C.BOLD}Example tree:{C.RESET}  Clean/Albums/")
        for line in examples:
            out(f"  {C.CYAN}{line}{C.RESET}")
    out("")


def cmd_genre(cfg_path: Path, cfg: Dict[str, Any], action: str, name: Optional[str] = None) -> None:
    """Manage persistent genre root declarations."""
    roots_key = "genre_roots"

    if action == "list":
        roots = cfg.get(roots_key, []) or []
        if not roots:
            out(f"{C.DIM}No genre roots declared. Use: raagdosa genre add <FolderName>{C.RESET}")
            return
        out(f"{C.CYAN}Persistent genre roots:{C.RESET}")
        for r in sorted(str(x) for x in roots):
            out(f"  • {r}")
        return

    if action == "clear":
        cfg[roots_key] = []
        write_yaml(cfg_path, cfg)
        ok_msg("Cleared all genre roots.")
        return

    if action in ("add", "remove", "show"):
        if not name:
            err(f"'genre {action}' requires a folder name argument.")
            sys.exit(1)
        current: List[str] = [str(x) for x in (cfg.get(roots_key, []) or [])]

        if action == "show":
            if name in current:
                out(f"{C.GREEN}'{name}' IS a declared genre root.{C.RESET}")
            else:
                out(f"{C.DIM}'{name}' is NOT a declared genre root.{C.RESET}")
            return

        if action == "add":
            if name in current:
                warn(f"'{name}' is already a genre root.")
            else:
                current.append(name)
                cfg[roots_key] = current
                write_yaml(cfg_path, cfg)
                ok_msg(f"Added genre root: {name}")
            return

        if action == "remove":
            if name not in current:
                warn(f"'{name}' not found in genre roots.")
            else:
                current.remove(name)
                cfg[roots_key] = current
                write_yaml(cfg_path, cfg)
                ok_msg(f"Removed genre root: {name}")
            return

    err(f"Unknown genre action: {action}")
    sys.exit(1)


def cmd_tree(
    cfg: Dict[str, Any],
    path_str: str,
    audio_only: bool = False,
    depth: Optional[int] = None,
    list_mode: bool = False,
    diff_a: Optional[str] = None,
    diff_b: Optional[str] = None,
) -> None:
    """
    `raagdosa tree <path>`
    Each run creates its own subfolder inside logs/trees/:
      logs/trees/<FolderName>_YYYY-MM-DD_HH-MM/
        <FolderName>_YYYY-MM-DD_HH-MM.txt
    This keeps every snapshot self-contained and easy to locate by name + date.
    """
    trees_dir = Path(cfg.get("logging", {}).get("root_dir", "logs")) / "trees"
    ensure_dir(trees_dir)

    if list_mode:
        # Show subdirectories (one per snapshot run)
        subdirs = sorted(
            [d for d in trees_dir.iterdir() if d.is_dir()],
            key=lambda d: d.stat().st_mtime, reverse=True
        )
        # Also pick up legacy flat .txt files
        flat_txts = [f for f in trees_dir.glob("*.txt")]
        if not subdirs and not flat_txts:
            out(f"{C.DIM}No tree snapshots yet. Run: raagdosa tree <path>{C.RESET}")
            return
        out(f"{C.CYAN}Saved tree snapshots ({len(subdirs) + len(flat_txts)}):{C.RESET}")
        for sd in subdirs:
            try:
                txt = next(sd.glob("*.txt"))
                lines = txt.read_text(encoding="utf-8").splitlines()
                file_count = sum(1 for l in lines if not l.startswith("#") and l.strip() and not l.endswith("/"))
                created = dt.datetime.fromtimestamp(sd.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
                out(f"  {sd.name:<55}  {created}  {file_count:>6} files  → {txt.name}")
            except StopIteration:
                out(f"  {sd.name}  (empty)")
        for f in sorted(flat_txts, reverse=True):
            created = dt.datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
            out(f"  {f.name:<55}  {created}  (legacy flat)")
        return

    if diff_a and diff_b:
        _cmd_tree_diff(trees_dir, diff_a, diff_b)
        return

    if not path_str:
        err("Provide a path: raagdosa tree <path>")
        sys.exit(1)

    scan_path = Path(path_str).expanduser().resolve()
    if not scan_path.exists():
        err(f"Path not found: {scan_path}")
        sys.exit(1)

    # Build tree lines
    skip_exts, skip_folders = build_skip_sets(cfg)
    lines: List[str] = []
    _tree_walk(scan_path, scan_path, lines, audio_only=audio_only, max_depth=depth, current_depth=0,
               skip_exts=skip_exts, skip_folders=skip_folders)

    # Build snapshot folder name: FolderName_YYYY-MM-DD_HH-MM
    ts = dt.datetime.now().strftime("%Y-%m-%d_%H-%M")
    base_name = f"{scan_path.name}_{ts}"
    snap_dir = trees_dir / base_name
    # Handle same-minute duplicate
    if snap_dir.exists():
        for i in range(2, 100):
            candidate = trees_dir / f"{base_name}_{i}"
            if not candidate.exists():
                snap_dir = candidate
                base_name = snap_dir.name
                break
    ensure_dir(snap_dir)

    out_path = snap_dir / f"{base_name}.txt"
    header = [
        f"# RaagDosa tree snapshot",
        f"# Path:    {scan_path}",
        f"# Date:    {dt.datetime.now().isoformat(timespec='seconds')}",
        f"# Options: audio_only={audio_only} depth={depth}",
        f"# Files:   {sum(1 for l in lines if not l.startswith('#') and l.strip() and not l.endswith('/'))}",
        "",
    ]
    out_path.write_text("\n".join(header + lines), encoding="utf-8")
    file_count = sum(1 for l in lines if not l.endswith("/") and l.strip())
    folder_count = sum(1 for l in lines if l.endswith("/"))
    ok_msg(f"Tree saved → {snap_dir.name}/")
    out(f"  {file_count:,} files  |  {folder_count:,} folders  |  {out_path}")


def cmd_help()->None:
    """Print a well-organised command reference grouped by workflow."""
    _groups=[
        ("Getting started",[
            ("init",            "Guided setup — creates config.yaml + paths.local.yaml"),
            ("doctor",          "Verify config, dependencies, disk space, DJ databases"),
            ("status",          "Library overview (folder counts, sizes)"),
        ]),
        ("Core workflow",[
            ("go",              "Scan + triage + move + rename  (the main command)"),
            ("go --dry-run",    "Preview what would happen — nothing moves"),
            ("go -i",           "Interactive mode — review each folder one by one"),
            ("go -i --sort confidence","Interactive, hardest folders first"),
            ("scan",            "Scan only → saves proposals.json"),
            ("apply",           "Apply a saved proposals.json"),
            ("folders",         "Folder pass only (no track rename)"),
            ("tracks",          "Track rename pass only"),
            ("resume",          "Resume an interrupted session"),
        ]),
        ("Inspect & debug",[
            ("show <folder>",   "Deep-dive a single folder (scores, tags, routing)"),
            ("show <folder> --tracks","Include per-track rename preview"),
            ("report",          "View session report (txt/csv/html)"),
            ("sessions",        "List recent sessions with move counts"),
            ("history",         "Show move/rename history"),
            ("tree <path>",     "Snapshot a directory tree"),
            ("tree --diff A B", "Diff two snapshots"),
            ("compare A B",     "Compare two folders side by side"),
            ("diff A B",        "Diff two session reports"),
        ]),
        ("Library management",[
            ("artists --list",  "List all artists in Clean library"),
            ("artists --find Q","Fuzzy-find an artist"),
            ("review-list",     "Summarise Review folder contents"),
            ("review-promote",  "Re-evaluate a Review folder (VA → album)"),
            ("clean-report",    "Stats and health report for Clean library"),
            ("verify",          "Audit Clean library health"),
            ("orphans",         "Find loose audio files in Clean/Review"),
            ("extract",         "Extract tracks from a VA/mix folder"),
            ("catchall <path>", "Group loose files in a dump folder by artist"),
        ]),
        ("Learning & config",[
            ("learn",           "Suggest config improvements from past sessions"),
            ("learn-crates",    "Discover crate naming patterns from a folder tree"),
            ("genre add/list/remove","Manage genre root declarations"),
            ("profile list/add/set", "Manage source profiles"),
            ("template list/show",   "Browse library organisation templates"),
            ("reference",       "Manage musical reference (aliases, labels, patterns)"),
            ("cache",           "Manage tag cache (status/clear/evict)"),
        ]),
        ("Tags",[
            ("tags status",          "Show tag proposal summary by risk tier"),
            ("tags review",          "Interactively review pending proposals"),
            ("tags review --auto",   "Auto-accept safe proposals above threshold"),
            ("tags apply",           "Apply accepted proposals to audio files"),
            ("tags apply --dry-run", "Preview tag changes without writing"),
            ("tags undo --last",     "Revert the most recent tag apply session"),
        ]),
        ("Undo",[
            ("undo --session last",  "Undo the last session"),
            ("undo --id <ID>",       "Undo a specific move by action ID"),
            ("undo --folder <name>", "Undo all moves for a folder"),
        ]),
    ]
    out(f"\n{C.BOLD}RaagDosa v{APP_VERSION} — Command Reference{C.RESET}")
    out(f"{'═'*55}")
    for group_name,commands in _groups:
        out(f"\n  {C.BOLD}{group_name}{C.RESET}")
        for cmd,desc in commands:
            out(f"    {C.CYAN}{cmd:<28}{C.RESET} {desc}")
    out(f"\n  {C.DIM}Use 'raagdosa <command> --help' for detailed options.{C.RESET}")
    out(f"  {C.DIM}Use '--verbose' with any command for extra detail.{C.RESET}\n")



# ═══════════════════════════════════════════════════════════════════
# MONOLITH-dependent commands — lazy imports from raagdosa_main
# ═══════════════════════════════════════════════════════════════════

def cmd_show(cfg:Dict[str,Any],folder_path:str,profile_name:str,show_tracks:bool=False)->None:
    from raagdosa.tagreader import read_audio_tags
    from raagdosa.proposal import build_folder_proposal, folder_is_multidisc
    from raagdosa.crates import build_crate_explosion_plan
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    folder=Path(folder_path).expanduser().resolve()
    if not folder.exists(): err(f"Folder not found: {folder}"); sys.exit(1)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    audio_files=list_audio_files(folder,exts)
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa show: {folder}{C.RESET}\n{'═'*60}")
    out(f"Audio files: {len(audio_files)}")
    if not audio_files: out("  None found."); return

    # Check .raagdosa override
    ov=load_folder_override(folder)
    if ov: out(f"\n{C.CYAN}Override file (.raagdosa):{C.RESET} {ov}")

    out(f"\n{C.BOLD}Tags (first 8 files):{C.RESET}")
    for f in sorted(audio_files)[:8]:
        tags=read_audio_tags(f,cfg); has=any(v for k,v in tags.items() if k not in ("bpm","key") and v)
        out(f"  {C.DIM}{f.name}{C.RESET}")
        if has: out(f"    {', '.join(f'{k}={v!r}' for k,v in tags.items() if v)}")
        else: out(f"    {C.YELLOW}(no tags){C.RESET}")
    if len(audio_files)>8: out(f"  ... and {len(audio_files)-8} more")
    prop=build_folder_proposal(folder,audio_files,source_root,profile,cfg)
    if not prop:
        out(f"\n{C.RED}⛔ Could not build a proposal — insufficient metadata and heuristic failed.{C.RESET}")
        out("  Tips:\n  • Tag files with album/albumartist (use MusicBrainz Picard or beets)\n  • Rename folder to 'Artist - Album' for heuristic parsing")
        return
    d=prop.decision
    out(f"\n{C.BOLD}Proposal:{C.RESET}")
    out(f"  Proposed name:      {C.GREEN}{prop.proposed_folder_name}{C.RESET}")
    out(f"  Target path:        {prop.target_path}")
    out(f"  Confidence:         {conf_color(prop.confidence)}")
    _ft_extra=""
    if d.get("is_ep"): _ft_extra+="  [EP]"
    if d.get("is_mix"): _ft_extra+="  [MIX]"
    if d.get("is_crate"): _ft_extra+=f"  [CRATE: {d.get('crate_type','singles')}]"
    out(f"  Folder type:        {d.get('folder_type','album')}{_ft_extra}")
    out(f"  album tag:          '{d.get('dominant_album_display')}' ({d.get('dominant_album_share',0):.0%} dominance)")
    out(f"  albumartist tag:    '{d.get('dominant_albumartist_display')}' ({d.get('dominant_albumartist_share',0):.0%} dominance)")
    out(f"  year:               {d.get('year') or 'not included'}")
    out(f"  VA:                 {'yes' if d.get('is_va') else 'no'}")
    if d.get("is_crate"):
        out(f"  DJ Crate:           {d.get('crate_type','singles')} (confidence: {d.get('crate_confidence',0):.2f})")
        out(f"  Crate reason:       {d.get('crate_reason','')}")
        # v9.0: show explosion preview for singles crates
        if d.get("crate_type")=="singles" and cfg.get("djcrates",{}).get("explode_to_artist_folders",False):
            _audio_exts=set(e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a",".wav",".aiff",".ogg",".opus"]))
            _show_files=sorted([f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in _audio_exts])
            if _show_files:
                _plans,_embedded=build_crate_explosion_plan(folder,_show_files,cfg,profile,source_root,d.get("crate_confidence",0.5))
                _unique=len(set(tp.artist or "_Unsorted" for tp in _plans))
                _emb_count=sum(1 for tp in _plans if tp.embedded_release)
                _sin_count=len(_plans)-_emb_count
                out(f"\n{C.BOLD}Explosion preview:{C.RESET}  {len(_plans)} tracks → {_unique} artist folders")
                if _embedded:
                    out(f"  {C.GREEN}Embedded releases:{C.RESET}")
                    for rel in _embedded:
                        _partial=f" (partial {len(rel.tracks)}/{rel.total_in_album})" if rel.is_partial and rel.total_in_album else ""
                        out(f"    {rel.artist_display} - {rel.album_display} ({len(rel.tracks)} tracks){_partial}")
                    out(f"  Remaining singles: {_sin_count}")
                for tp in _plans[:10]:
                    _art=tp.artist or "_Unsorted"
                    if tp.embedded_release:
                        _dest=f"{_art}/{tp.embedded_release}/{tp.track_number:02d} - {(tp.title or tp.filename)[:25]}"
                    else:
                        _dest=f"{_art}/Singles/{(tp.title or tp.filename)[:30]}"
                    out(f"    {tp.filename[:40]:40s} → {_dest}  conf={conf_color(tp.confidence)}")
                if len(_plans)>10:
                    out(f"    {C.DIM}... and {len(_plans)-10} more{C.RESET}")
    out(f"  FLAC only:          {'yes' if d.get('is_flac_only') else 'no'}")
    out(f"  heuristic:          {'yes (no tags found)' if d.get('used_heuristic') else 'no'}")

    # Confidence factor breakdown
    cf=d.get("confidence_factors",{})
    if cf:
        out(f"\n{C.BOLD}Confidence factors:{C.RESET}")
        factor_labels={"dominance":"Vote dominance","tag_coverage":"Tag coverage",
                       "title_quality":"Title quality","filename_consistency":"Filename/tag consistency",
                       "completeness":"Track completeness","aa_consistency":"Albumartist consistency",
                       "folder_alignment":"Folder name alignment"}
        for key,label in factor_labels.items():
            v=cf.get(key,0.0)
            bar="█"*int(v*20)+"░"*(20-int(v*20))
            col=C.GREEN if v>=0.85 else (C.YELLOW if v>=0.65 else C.RED)
            out(f"  {label:<30} {col}[{bar}] {v:.2f}{C.RESET}")
        gaps=int(cf.get("track_gaps",0)); dupes=int(cf.get("track_dupes",0))
        if gaps:  warn(f"Track number gaps: {gaps} missing")
        if dupes: warn(f"Duplicate track numbers: {dupes}")

    if d.get("garbage_reasons"): warn(f"Garbage flags on album name: {', '.join(d['garbage_reasons'])}")
    if prop.stats.format_duplicates: warn(f"Format duplicates: {', '.join(prop.stats.format_duplicates)}")
    rr_cfg=cfg.get("review_rules",{}); min_conf=float(rr_cfg.get("min_confidence_for_clean",0.85))
    in_mf=manifest_has(cfg,prop.proposed_folder_name)
    reasons:List[str]=[]; route="clean"
    if prop.confidence<min_conf: route="review"; reasons.append(f"low_confidence ({prop.confidence:.2f}<{min_conf})")
    if in_mf: route="duplicate"; reasons.append("already_in_manifest")
    out(f"\n{C.BOLD}Routing:{C.RESET}  {status_tag(route)}  {', '.join(reasons) or 'all good'}")
    if route=="review":
        out(f"\n{C.YELLOW}Tips to reach Clean:{C.RESET}")
        out(f"  • Ensure consistent album/albumartist tags across all tracks")
        out(f"  • Or lower review_rules.min_confidence_for_clean below {prop.confidence:.2f} in config")

    # --tracks: show per-track proposed renames
    if show_tracks:
        out(f"\n{C.BOLD}Track rename preview:{C.RESET}")
        allowed={e.lower() for e in cfg.get("track_rename",{}).get("allowed_extensions",[".mp3",".flac",".m4a"])}
        tr_files=[p for p in sorted(audio_files) if p.suffix.lower() in allowed]
        if not tr_files: out("  No renamable tracks found.")
        else:
            cls=classify_folder_for_tracks(prop.decision,cfg)
            disc_multi=folder_is_multidisc(tr_files,cfg)
            for f in tr_files:
                tags=read_audio_tags(f,cfg)
                new_name,conf,reason,meta=build_track_filename(cls,tags,f,cfg,prop.decision,disc_multi,total_tracks=len(tr_files))
                if new_name:
                    changed=normalize_unicode(new_name)!=normalize_unicode(f.name)
                    sym=f"{C.GREEN}→{C.RESET}" if changed else f"{C.DIM}={C.RESET}"
                    out(f"  {C.DIM}{f.name:<50}{C.RESET} {sym} {new_name}  {conf_color(conf)}")
                else:
                    out(f"  {C.DIM}{f.name:<50}{C.RESET} {C.YELLOW}SKIP ({reason}){C.RESET}")



def cmd_learn_crates(cfg_path:Path,cfg:Dict[str,Any],scan_path:str,min_tracks:int=3)->None:
    """Scan a directory tree for crate-like folders and learn naming patterns."""
    root=Path(scan_path)
    if not root.exists():
        err(f"Path does not exist: {scan_path}"); return
    if not root.is_dir():
        err(f"Not a directory: {scan_path}"); return

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa learn-crates{C.RESET}")
    out(f"{'═'*60}")
    out(f"Scanning: {root}")
    out(f"Min tracks: {min_tracks}\n")

    # Walk directory tree — only look at leaf-ish folders (those containing audio)
    crate_candidates:List[Dict[str,Any]]=[]
    folder_count=0
    for dirpath,dirnames,filenames in os.walk(str(root)):
        # Skip hidden dirs
        dirnames[:]=[d for d in dirnames if not d.startswith(".")]
        dp=Path(dirpath)
        folder_count+=1
        if folder_count%50==0: out(f"  Scanned {folder_count} folders...",level=VERBOSE)
        result=_scan_folder_for_crate_signals(dp,cfg,min_tracks)
        if result:
            crate_candidates.append(result)

    if not crate_candidates:
        out(f"\nNo crate-like folders found in {folder_count} folders scanned.")
        return

    out(f"\n{C.GREEN}Found {len(crate_candidates)} crate-like folder(s){C.RESET} in {folder_count} folders scanned.\n")

    # Extract naming patterns
    patterns=_extract_naming_patterns(crate_candidates)

    # Show grouped patterns first
    added_crate:List[str]=[]
    added_set:List[str]=[]

    if patterns:
        out(f"{C.BOLD}Discovered naming patterns:{C.RESET}\n")
        for i,pat in enumerate(patterns,1):
            label=pat.get("suffix") or pat.get("prefix","?")
            ptype="set prep" if pat["type"]=="crate_set" else "singles"
            out(f"  {C.CYAN}Pattern {i}: *{label}*{C.RESET}  ({ptype}, {pat['count']} matches)")
            out(f"  Regex: {pat['regex']}")
            for m in pat["matches"][:5]:
                out(f"    {m['name']}/  ({m['total_tracks']} tracks, album diversity {m['album_diversity']:.0%})")
            if len(pat["matches"])>5:
                out(f"    ... and {len(pat['matches'])-5} more")
            out(f"\n  [a] Add to config  [s] Skip  [e] Edit pattern")
            ans=input("  → ").strip().lower()
            if ans=="a":
                if pat["type"]=="crate_set":
                    added_set.append(pat["regex"])
                else:
                    added_crate.append(pat["regex"])
                out(f"  {C.GREEN}✓ Added{C.RESET}\n")
            elif ans=="e":
                out(f"  Current regex: {pat['regex']}")
                new_pat=input("  New regex: ").strip()
                if new_pat:
                    # Validate regex
                    try:
                        re.compile(new_pat)
                        if pat["type"]=="crate_set":
                            added_set.append(new_pat)
                        else:
                            added_crate.append(new_pat)
                        out(f"  {C.GREEN}✓ Added custom pattern{C.RESET}\n")
                    except re.error as e:
                        out(f"  {C.RED}Invalid regex: {e}{C.RESET}\n")
                else:
                    out(f"  Skipped.\n")
            else:
                out(f"  Skipped.\n")

    # Show standalone crates (not matching any pattern)
    patterned_names:Set[str]=set()
    for p in patterns:
        for m in p["matches"]: patterned_names.add(m["name"])
    standalone=[c for c in crate_candidates if c["name"] not in patterned_names]

    if standalone:
        out(f"\n{C.BOLD}Standalone crate folders (no shared pattern):{C.RESET}\n")
        for c in standalone:
            ptype="set prep" if c["set_match"] else "singles"
            out(f"  {c['name']}/  ({c['total_tracks']} tracks, album diversity {c['album_diversity']:.0%}, {ptype})")
        out(f"\n  These folders were detected as crates but don't share a naming pattern.")
        out(f"  They'll be detected automatically by the scoring engine during scan.\n")

    # Persist to config
    if added_crate or added_set:
        dc=cfg.setdefault("djcrates",{})
        if added_crate:
            existing=dc.setdefault("custom_crate_patterns",[])
            for p in added_crate:
                if p not in existing: existing.append(p)
        if added_set:
            existing=dc.setdefault("custom_set_patterns",[])
            for p in added_set:
                if p not in existing: existing.append(p)
        write_yaml(cfg_path,cfg)
        out(f"\n{C.GREEN}Config updated:{C.RESET}")
        if added_crate: out(f"  Added {len(added_crate)} pattern(s) to djcrates.custom_crate_patterns")
        if added_set: out(f"  Added {len(added_set)} pattern(s) to djcrates.custom_set_patterns")
        out(f"\n{C.DIM}Saved: {cfg_path}{C.RESET}")
    elif patterns or standalone:
        out("No patterns added to config.")
    out("")


def cmd_review_promote(cfg:Dict[str,Any],profile_name:str,folder_query:str,dry_run:bool=False,artist_override:Optional[str]=None)->None:
    """
    Re-evaluate a Review folder as a single-artist album (force_album=True).
    If the new proposal passes confidence, move it to Clean/.
    The user can optionally provide --artist to force the artist name.
    """
    from raagdosa.proposal import build_folder_proposal
    from raagdosa.session import manifest_add
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root,create=not dry_run)
    review_albums=roots["review_albums"]; clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa review promote{C.RESET}\n{'═'*60}")

    if not review_albums.exists():
        out("Review/Albums not found."); return

    # Find the folder — exact match first, then fuzzy
    folder_query_p=Path(folder_query).expanduser().resolve()
    target:Optional[Path]=None
    if folder_query_p.exists() and folder_query_p.is_dir():
        target=folder_query_p
    else:
        # Search in Review/Albums for matching name
        candidates=[d for d in review_albums.rglob("*") if d.is_dir()
                    and any(f.suffix.lower() in exts for f in d.iterdir() if f.is_file())]
        # Exact name match
        for d in candidates:
            if d.name == folder_query:
                target=d; break
        # Fuzzy substring match
        if not target:
            q_low=folder_query.lower()
            matches=[d for d in candidates if q_low in d.name.lower()]
            if len(matches)==1:
                target=matches[0]
            elif len(matches)>1:
                out(f"  Multiple matches for '{folder_query}':")
                for m in matches[:10]:
                    out(f"    {m.name}")
                out(f"  Be more specific."); return

    if not target:
        err(f"Could not find '{folder_query}' in Review."); return

    audio_files=list_audio_files(target,exts)
    if not audio_files:
        err(f"No audio files in {target.name}"); return

    # If user provides --artist, write a temporary .raagdosa override
    _tmp_override=False
    override_path=target/".raagdosa"
    if artist_override and not override_path.exists():
        try:
            override_path.write_text(f"albumartist: {artist_override}\n",encoding="utf-8")
            _tmp_override=True
        except Exception: pass

    # Build original proposal (as-is) for comparison
    prop_original=build_folder_proposal(target,audio_files,source_root,profile,cfg)

    # Build new proposal with force_album=True
    prop_new=build_folder_proposal(target,audio_files,source_root,profile,cfg,force_album=True)

    # Clean up temp override
    if _tmp_override:
        try: override_path.unlink()
        except Exception: pass

    if not prop_new:
        err(f"Could not build a proposal for {target.name}"); return

    # Show comparison
    out(f"\n{C.BOLD}Current (VA):{C.RESET}")
    if prop_original:
        out(f"  {prop_original.proposed_folder_name}  conf={conf_color(prop_original.confidence)}")
        out(f"  artist: {prop_original.decision.get('albumartist_display','?')}  VA={prop_original.decision.get('is_va')}")
    else:
        out(f"  {C.DIM}(no proposal){C.RESET}")

    out(f"\n{C.BOLD}Re-evaluated (album):{C.RESET}")
    out(f"  {C.GREEN}{prop_new.proposed_folder_name}{C.RESET}  conf={conf_color(prop_new.confidence)}")
    out(f"  artist: {prop_new.decision.get('albumartist_display','?')}  VA={prop_new.decision.get('is_va')}")

    # Route the new proposal
    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    new_dest="clean" if prop_new.confidence>=min_conf else "review"
    reasons=[]
    if prop_new.confidence<min_conf:
        reasons.append(f"low_confidence ({prop_new.confidence:.2f}<{min_conf})")
    if prop_new.decision.get("used_heuristic"):
        reasons.append("heuristic_fallback")
        if new_dest=="clean": new_dest="review"

    if new_dest=="clean":
        new_target=resolve_library_path(
            clean_albums,prop_new.decision.get("albumartist_display",""),
            prop_new.decision.get("dominant_album_display",""),
            prop_new.decision.get("year"),
            prop_new.decision.get("is_flac_only",False),False,False,
            prop_new.decision.get("is_mix",False),cfg,
            profile=profile,genre=prop_new.decision.get("genre"),
            bpm=prop_new.decision.get("bpm"),key=prop_new.decision.get("key"),
            label=prop_new.decision.get("label"))
        out(f"\n  {C.GREEN}→ Promotes to Clean:{C.RESET} {new_target}")

        if dry_run:
            out(f"  {C.DIM}[dry-run] No files moved.{C.RESET}")
        else:
            confirm=input(f"\n  Move to Clean? [y/N] ").strip().lower()
            if confirm=="y":
                ensure_dir(new_target.parent)
                dst=collision_resolve(new_target,"suffix"," ({n})")
                if dst is None:
                    err("Collision — cannot resolve target."); return
                try:
                    move_method,_=safe_move_folder(target,dst)
                    session_id=make_session_id(profile_name,str(source_root))
                    action_id=uuid.uuid4().hex[:10]
                    entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,
                           "type":"folder","subtype":"review_promote",
                           "original_path":str(target),"original_folder_name":target.name,
                           "target_path":str(dst),"target_folder_name":dst.name,
                           "destination":"clean","confidence":prop_new.confidence,
                           "decision":prop_new.decision,"move_method":move_method}
                    hist_path=Path(cfg["logging"]["history_log"])
                    append_jsonl(hist_path,entry)
                    manifest_add(cfg,dst.name,entry)
                    ok_msg(f"Promoted: {target.name} → {dst}")
                except Exception as e:
                    err(f"Move failed: {e}")
            else:
                out("  Skipped.")
    else:
        out(f"\n  {C.YELLOW}Still routes to Review:{C.RESET} {', '.join(reasons)}")
        out(f"  Tips:")
        out(f"  • Use --artist 'Artist Name' to force the artist")
        out(f"  • Or tag the files with proper albumartist/artist tags")
        out(f"  • Or lower review_rules.min_confidence_for_clean below {prop_new.confidence:.2f}")


def cmd_cache(cfg:Dict[str,Any],action:str)->None:
    from raagdosa.tagreader import TagCache, reset_tag_cache
    cache_path=Path(cfg.get("logging",{}).get("root_dir","logs"))/"tag_cache.json"
    out(f"\n{C.BOLD}{'═'*50}{C.RESET}\n{C.BOLD}raagdosa cache{C.RESET}\n{'═'*50}")
    if action=="status":
        if not cache_path.exists():
            out("No tag cache found. It will be created on the next scan.")
            return
        try:
            raw=read_json(cache_path)
            entries=raw.get("entries",raw) if isinstance(raw,dict) else {}
            saved=raw.get("saved","unknown") if isinstance(raw,dict) else "unknown"
            size_kb=cache_path.stat().st_size/1024
            out(f"  Path:     {cache_path}")
            out(f"  Entries:  {len(entries):,}")
            out(f"  Size:     {size_kb:.0f} KB")
            out(f"  Saved:    {saved}")
            out(f"\n{C.DIM}Warm scans skip mutagen for cached files — near-zero tag-read cost.{C.RESET}")
        except Exception as e:
            err(f"Could not read cache: {e}")
    elif action=="clear":
        if not cache_path.exists(): out("No cache to clear."); return
        if input("Clear tag cache? This forces a full re-read on next scan. [y/N] ").strip().lower()!="y":
            out("Aborted."); return
        cache_path.unlink(); reset_tag_cache(); ok_msg("Tag cache cleared.")
    elif action=="evict":
        tc=TagCache(cache_path)
        removed=tc.evict_missing()
        tc.save()
        ok_msg(f"Evicted {removed} stale entries.")
    else:
        err(f"Unknown cache action: {action}")



def cmd_clean_report(cfg:Dict[str,Any],profile_name:str)->None:
    from raagdosa.tagreader import read_audio_tags
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    setup_logging_paths(cfg, profile, source_root)
    roots=ensure_roots(profile,source_root,create=False); clean_albums=roots["clean_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa clean-report — {profile_name}{C.RESET}\n{'═'*60}")
    if not clean_albums.exists(): out("Clean/Albums not found."); return

    album_dirs:List[Path]=[]; total_tracks=0; ext_counts:Counter=Counter()
    tagged_count=0; untagged_count=0; issues:List[str]=[]

    for d in sorted(clean_albums.rglob("*")):
        if not d.is_dir(): continue
        files=list_audio_files(d,exts)
        if not files: continue
        album_dirs.append(d)
        total_tracks+=len(files)
        for f in files:
            ext_counts[f.suffix.lower()]+=1
            tags=read_audio_tags(f,cfg)
            if any(v for k,v in tags.items() if k not in ("bpm","key") and v): tagged_count+=1
            else: untagged_count+=1
        # Detect issues in this album folder
        track_nums:List[int]=[]
        for f in files:
            tags=read_audio_tags(f,cfg)
            vt=parse_vinyl_track((tags.get("tracknumber") or "").split("/")[0].strip())
            n=vt[2] if vt else parse_int_prefix(tags.get("tracknumber") or "")
            if n: track_nums.append(n)
        if detect_track_gaps(track_nums): issues.append(f"track_gaps: {d.name}")
        if detect_duplicate_track_numbers(track_nums): issues.append(f"duplicate_track_nums: {d.name}")

    out(f"\n{C.BOLD}Summary:{C.RESET}")
    out(f"  Album folders:       {len(album_dirs)}")
    out(f"  Total tracks:        {total_tracks}")
    out(f"  Tagged tracks:       {C.GREEN}{tagged_count}{C.RESET}")
    out(f"  Untagged tracks:     {C.RED}{untagged_count}{C.RESET}")
    out(f"\n{C.BOLD}Format breakdown:{C.RESET}")
    for ext,cnt in sorted(ext_counts.items(),key=lambda x:x[1],reverse=True):
        pct=cnt/total_tracks*100 if total_tracks else 0
        bar="█"*int(pct/2)
        out(f"  {ext:<8} {cnt:5d} tracks  {C.DIM}[{bar:<50}] {pct:.0f}%{C.RESET}")
    if issues:
        out(f"\n{C.YELLOW}Issues found ({len(issues)}):{C.RESET}")
        for i in issues[:30]: out(f"  {C.YELLOW}⚠{C.RESET}  {i}")
        if len(issues)>30: out(f"  … and {len(issues)-30} more")
    else:
        ok_msg("No issues detected in Clean library.")

# ─────────────────────────────────────────────
# extract --by-artist — split VA/mix folder
# ─────────────────────────────────────────────


def cmd_extract_by_artist(cfg:Dict[str,Any],profile_name:str,folder_path:str,dry_run:bool)->None:
    from raagdosa.tagreader import read_audio_tags
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root,create=not dry_run)
    folder=Path(folder_path).expanduser().resolve()
    if not folder.exists(): err(f"Folder not found: {folder}"); sys.exit(1)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    audio_files=list_audio_files(folder,exts)
    if not audio_files: err("No audio files found."); sys.exit(1)

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa extract --by-artist: {folder.name}{C.RESET}\n{'═'*60}")

    by_artist:Dict[str,List[Path]]={}
    for f in sorted(audio_files):
        tags=read_audio_tags(f,cfg)
        artist=(tags.get("artist") or "").strip() or "_Unknown"
        artist=normalize_artist_name(artist,cfg)
        by_artist.setdefault(artist,[]).append(f)

    if len(by_artist)<=1: out("Only one artist found — nothing to extract."); return
    out(f"\nWould extract {len(by_artist)} artist group(s) into Clean/Albums:\n")
    clean_albums=roots["clean_albums"]
    for artist,files in sorted(by_artist.items()):
        dest=clean_albums/sanitize_name(artist)/"Singles"
        out(f"  {C.GREEN}{artist:<40}{C.RESET}  {len(files):3d} track(s)  →  {dest}")
    if dry_run: out(f"\n{C.DIM}[dry-run] No files moved.{C.RESET}"); return
    if input("\nProceed? [y/N] ").strip().lower()!="y": out("Aborted."); return
    moved=0
    for artist,files in by_artist.items():
        dest=clean_albums/sanitize_name(artist)/"Singles"; ensure_dir(dest)
        for f in files:
            target=dest/f.name
            if target.exists(): target=dest/(f.stem+" (extracted)"+f.suffix)
            try: shutil.move(str(f),str(target)); moved+=1
            except Exception as e: err(f"  Failed {f.name}: {e}")
    ok_msg(f"Extracted {moved} tracks into artist folders.")

# ─────────────────────────────────────────────
# compare --folder — diff two folders
# ─────────────────────────────────────────────


def cmd_compare_folders(cfg:Dict[str,Any],folder_a:str,folder_b:str)->None:
    from raagdosa.tagreader import read_audio_tags
    fa=Path(folder_a).expanduser().resolve(); fb=Path(folder_b).expanduser().resolve()
    for p,label in ((fa,"A"),(fb,"B")):
        if not p.exists(): err(f"Folder {label} not found: {p}"); sys.exit(1)
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    fa_files={f.name for f in list_audio_files(fa,exts)}
    fb_files={f.name for f in list_audio_files(fb,exts)}
    out(f"\n{C.BOLD}{'═'*60}{C.RESET}")
    out(f"{C.BOLD}raagdosa compare{C.RESET}")
    out(f"  A: {fa}  ({len(fa_files)} tracks)")
    out(f"  B: {fb}  ({len(fb_files)} tracks)\n{'─'*60}")
    only_a=sorted(fa_files-fb_files); only_b=sorted(fb_files-fa_files); common=sorted(fa_files&fb_files)
    out(f"\n{C.GREEN}Common ({len(common)}):{C.RESET}")
    for n in common[:20]: out(f"  {n}")
    if len(common)>20: out(f"  … and {len(common)-20} more")
    if only_a:
        out(f"\n{C.YELLOW}Only in A ({len(only_a)}):{C.RESET}")
        for n in only_a: out(f"  {C.YELLOW}{n}{C.RESET}")
    if only_b:
        out(f"\n{C.CYAN}Only in B ({len(only_b)}):{C.RESET}")
        for n in only_b: out(f"  {C.CYAN}{n}{C.RESET}")
    # Tag comparison on common files
    if common:
        out(f"\n{C.BOLD}Tag comparison (first 5 common):{C.RESET}")
        for name in sorted(common)[:5]:
            ta=read_audio_tags(fa/name,cfg); tb=read_audio_tags(fb/name,cfg)
            diffs=[k for k in ("album","albumartist","artist","title","tracknumber","year") if (ta.get(k) or "")!=(tb.get(k) or "")]
            if diffs:
                out(f"  {C.DIM}{name}{C.RESET}")
                for k in diffs: out(f"    {k}: A={ta.get(k)!r}  B={tb.get(k)!r}")
            else:
                out(f"  {C.DIM}{name}{C.RESET}  {C.GREEN}tags match{C.RESET}")

# ─────────────────────────────────────────────
# diff — compare two session reports
# ─────────────────────────────────────────────


def cmd_report(cfg:Dict[str,Any],session_id:Optional[str],fmt:str)->None:
    from raagdosa.moves import _write_session_reports
    sdir=Path(cfg["logging"]["session_dir"])
    if not session_id:
        sessions=sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True) if sdir.exists() else []
        if not sessions: err("No sessions found."); sys.exit(1)
        session_id=sessions[0].name
    session_dir=sdir/session_id
    if not session_dir.exists(): err(f"Session not found: {session_id}"); sys.exit(1)
    fname={"txt":"report.txt","csv":"report.csv","html":"report.html"}.get(fmt,"report.txt")
    target=session_dir/fname
    if not target.exists():
        pp=session_dir/"proposals.json"
        if not pp.exists(): err(f"proposals.json not found for session {session_id}"); sys.exit(1)
        payload=read_json(pp); proposals=[fp_from_dict(fp) for fp in payload.get("folder_proposals",[])]
        _write_session_reports(payload.get("session_id",session_id),payload.get("profile",""),
                               Path(payload.get("source_root",".")),proposals,session_dir,cfg)
    out(f"\n{C.DIM}{target}{C.RESET}\n")
    if fmt=="txt": out(target.read_text(encoding="utf-8"))
    else: out(f"Open: {target}")

# ─────────────────────────────────────────────
# status
# ─────────────────────────────────────────────


def cmd_scan(cfg_path:Path,cfg:Dict[str,Any],profile:str,out_path:Optional[str],since:Optional[str],genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,session_name:str="")->str:
    from raagdosa.moves import scan_folders
    sid,sdir,_=scan_folders(cfg,profile,since=_parse_since(since,cfg),genre_roots=genre_roots,itunes_mode=itunes_mode,session_name=session_name)
    if out_path: shutil.copyfile(str(sdir/"proposals.json"),out_path)
    return sid



def cmd_apply(cfg:Dict[str,Any],proposals_path:Path,interactive:bool,auto_above:Optional[float],dry_run:bool)->None:
    from raagdosa.moves import apply_folder_moves, rename_tracks_in_clean_folder
    payload=read_json(proposals_path); session_id=payload.get("session_id") or make_session_id()
    raw_props=payload.get("folder_proposals",[]); profile_name=payload.get("profile","")
    profiles=cfg.get("profiles",{})
    if profile_name in profiles:
        prof=profiles[profile_name]; source_root=Path(prof["source_root"]).expanduser()
        roots=ensure_roots(prof,source_root,create=not dry_run)
        viols=validate_proposal_paths(raw_props,[roots["clean_albums"],roots["review_albums"],roots["duplicates"]])
        if viols: err("⛔ Path validation failed:"); [err(f"  {v}") for v in viols]; sys.exit(1)
    folder_props=[fp_from_dict(fp) for fp in raw_props]
    applied=apply_folder_moves(cfg,folder_props,interactive=interactive,auto_above=auto_above,dry_run=dry_run,session_id=session_id)
    trc=cfg.get("track_rename",{})
    if trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"):
        for a in applied:
            if a.get("destination")=="clean":
                rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),a.get("decision",{}),interactive=interactive,dry_run=dry_run,session_id=session_id)



def cmd_go(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,since:Optional[str],perf_tier:Optional[str]=None,genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,review_threshold:Optional[float]=None,sort_by:str="name",force:bool=False,auto_above:Optional[float]=None,session_name:str="")->None:
    """
    v7.1 default path: scan all → triage dashboard → bulk-approve → interactive review.
    --force: bypass triage, use original streaming pipeline (nuclear option).
    --interactive / -i: bypass triage, review all folders 1-by-1 in streaming mode.
    --auto-above FLOAT: override auto_approve_threshold for this run.
    """
    from raagdosa.tagreader import _get_tag_cache
    from raagdosa.moves import scan_folders
    from raagdosa.orchestration import _run_core, _interactive_streaming, _run_triage
    from raagdosa.pipeline import build_skip_sets
    if force:
        # --force: original _run_core streaming behaviour, no triage
        out(f"\n{C.YELLOW}--force: bypassing triage. Processing all folders without confirmation.{C.RESET}")
        _run_core(cfg_path,cfg,profile,interactive=False,dry_run=dry_run,since_str=since,
                  perf_tier=perf_tier,genre_roots=genre_roots,itunes_mode=itunes_mode)
        return

    if interactive:
        # --interactive: original streaming 1-by-1 mode, no triage
        _interactive_streaming(cfg,profile,dry_run=dry_run,since_str=since,
                               genre_roots=genre_roots,itunes_mode=itunes_mode,
                               threshold=review_threshold,sort_by=sort_by)
        return

    # ── v7.1 Triage workflow ──────────────────────────────────────────────
    for lk in ["history_log","track_history_log"]:
        lp=Path(cfg.get("logging",{}).get(lk,""))
        if lp.name: rotate_log_if_needed(lp,float(cfg.get("logging",{}).get("rotate_log_max_mb",10.0)))
    register_stop_handler()

    # Resolve profile
    profiles=cfg.get("profiles",{}); profile_obj=profiles.get(profile,{})
    source_root=Path(profile_obj.get("source_root","")).expanduser()
    if not source_root.exists():
        err(f"source_root not found: {source_root}"); sys.exit(3)

    setup_logging_paths(cfg,profile_obj,source_root)
    build_skip_sets(cfg)  # validate config (result used internally by orchestration)

    session_id=make_session_id()
    session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)

    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    out(f"{C.BOLD}Scanning:{C.RESET}  {source_root}")
    if dry_run: out(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")

    _get_tag_cache(cfg)

    # Full scan — builds all proposals before any moves
    since_dt=_parse_since(since,cfg)
    _,_,proposals=scan_folders(cfg,profile,since=since_dt,genre_roots=genre_roots,itunes_mode=itunes_mode,session_name=session_name)

    if not proposals:
        out(f"  {C.DIM}No candidates found.{C.RESET}"); manifest_set_last_run(cfg); return

    applied=_run_triage(cfg,profile,session_id,proposals,source_root,dry_run,auto_threshold=auto_above)

    if _tag_cache is not None: _tag_cache.save()
    manifest_set_last_run(cfg)

    if applied:
        print(f"\n{'━'*66}")
        clean_n=sum(1 for a in applied if a.get("destination")=="clean")
        rev_n  =sum(1 for a in applied if a.get("destination")=="review")
        parts=[]
        if clean_n: parts.append(f"{C.GREEN}{clean_n} → Clean{C.RESET}")
        if rev_n:   parts.append(f"{C.YELLOW}{rev_n} → Review{C.RESET}")
        skip_n=len(proposals)-len(applied)
        if skip_n:  parts.append(f"{C.DIM}{skip_n} skipped{C.RESET}")
        print(f"  {C.BOLD}DONE{C.RESET}  ·  {'  ·  '.join(parts)}")
        print(f"  Undo:  raagdosa undo --session {session_id}")
        print(f"{'━'*66}\n")



def cmd_folders_only(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,since:Optional[str],genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,sort_by:str="name")->None:
    from raagdosa.moves import scan_folders, apply_folder_moves
    from raagdosa.orchestration import _interactive_streaming
    if interactive:
        # Use streaming interactive — same one-at-a-time approach
        _interactive_streaming(cfg,profile,dry_run=dry_run,since_str=since,
                               genre_roots=genre_roots,itunes_mode=itunes_mode,sort_by=sort_by)
    else:
        register_stop_handler(); sid,_,proposals=scan_folders(cfg,profile,since=_parse_since(since,cfg),genre_roots=genre_roots,itunes_mode=itunes_mode)
        applied=apply_folder_moves(cfg,proposals,interactive=False,auto_above=None,dry_run=dry_run,session_id=sid)
        out(f"Folders applied: {len(applied)}"); manifest_set_last_run(cfg)



def cmd_tracks_only(cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool)->None:
    from raagdosa.proposal import build_folder_proposal
    from raagdosa.moves import rename_tracks_in_clean_folder
    profiles=cfg.get("profiles",{})
    if profile not in profiles: raise ValueError(f"Unknown profile: {profile}")
    prof=profiles[profile]; source_root=Path(prof["source_root"]).expanduser()
    setup_logging_paths(cfg, prof, source_root)
    build_skip_sets(cfg)  # validate skip config
    roots=ensure_roots(prof,source_root,create=not dry_run); clean_albums=roots["clean_albums"]
    sid=make_session_id(profile); out(f"Session: {sid} (tracks-only)"); register_stop_handler()
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    for folder in sorted([p for p in clean_albums.rglob("*") if p.is_dir()]):
        if should_stop(): break
        af=list_audio_files(folder,exts)
        if not af: continue
        prop=build_folder_proposal(folder,af,source_root,prof,cfg)
        decision=prop.decision if prop else {"dominant_album_share":0.0,"is_va":False}
        rename_tracks_in_clean_folder(cfg,folder,decision,interactive=interactive,dry_run=dry_run,session_id=sid)



def cmd_resume(cfg:Dict[str,Any],session_id:str,interactive:bool,dry_run:bool)->None:
    from raagdosa.moves import apply_folder_moves, rename_tracks_in_clean_folder
    _resolve_log_paths_from_active_profile(cfg)
    sdir=Path(cfg["logging"]["session_dir"]); pp=sdir/session_id/"proposals.json"
    if not pp.exists(): err(f"No proposals.json for session: {session_id}"); sys.exit(1)
    hist=iter_jsonl(Path(cfg["logging"]["history_log"]))
    already={h.get("original_path") for h in hist if h.get("session_id")==session_id}
    payload=read_json(pp); raw=payload.get("folder_proposals",[])
    remaining=[fp_from_dict(fp) for fp in raw if fp.get("folder_path") not in already]
    out(f"Resume {session_id} | already: {len(already)} | remaining: {len(remaining)}")
    if not remaining: out("Nothing left."); return
    register_stop_handler()
    applied=apply_folder_moves(cfg,remaining,interactive=interactive,auto_above=None,dry_run=dry_run,session_id=session_id)
    trc=cfg.get("track_rename",{})
    if trc.get("enabled",True):
        for a in applied:
            if a.get("destination")=="clean":
                rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),a.get("decision",{}),interactive=interactive,dry_run=dry_run,session_id=session_id)

# ─────────────────────────────────────────────
# History + undo
# ─────────────────────────────────────────────


def cmd_doctor(cfg_path:Path,cfg:Dict[str,Any])->None:
    from raagdosa.tagreader import read_audio_tags
    from raagdosa.pipeline import build_skip_sets as _build_skip_sets_fn
    is_ok=True
    out(f"\n{C.BOLD}RaagDosa v{APP_VERSION} — Doctor{C.RESET}")
    out(f"Python {sys.version.split()[0]}  |  {platform.system()} {platform.release()}")
    out(f"Config:  {cfg_path}  ({'exists' if cfg_path.exists() else C.RED+'MISSING'+C.RESET})")
    out(f"PyYAML:  {'✓' if yaml else C.RED+'✗ pip install pyyaml'+C.RESET}")
    out(f"Mutagen: {'✓' if MutagenFile else C.RED+'✗ pip install mutagen'+C.RESET}")
    warns=validate_config(cfg, APP_VERSION)
    if warns:
        out(f"\n{C.YELLOW}Config warnings:{C.RESET}"); [warn(w) for w in warns]; is_ok=False
    else: ok_msg("Config valid")
    prof_name=cfg.get("active_profile")
    if not prof_name or prof_name not in cfg.get("profiles",{}):
        err(f"active_profile '{prof_name}' not found."); sys.exit(1)
    prof=cfg["profiles"][prof_name]; source_root=Path(prof["source_root"]).expanduser()
    out(f"\nProfile: {prof_name}")
    out(f"Source:  {source_root}  ({'exists' if source_root.exists() else C.RED+'MISSING'+C.RESET})")
    lib=cfg.get("library",{}); out(f"Template: {lib.get('template','{artist}/{album}')}  FLAC-seg: {lib.get('flac_segregation',False)}")
    art_norm=cfg.get("artist_normalization",{})
    out(f"The-prefix: {art_norm.get('the_prefix','keep-front')}  Aliases: {len(art_norm.get('aliases',{}) or {})} defined")
    if source_root.exists():
        # v5.5 — resolve logging and skip sets before anything that might use them
        setup_logging_paths(cfg, prof, source_root)
        _build_skip_sets_fn(cfg)  # validate skip config parses OK
        roots=ensure_roots(prof,source_root,create=False)
        wrapper=derive_wrapper_root(prof,source_root)
        out(f"Wrapper: {wrapper}")
        try:
            s=shutil.disk_usage(str(source_root)); fgb=s.free/1024**3
            out(f"Disk:    {fgb:.1f} GB free / {s.total/1024**3:.1f} GB total")
            if fgb<5.0: warn("Less than 5 GB free.")
        except Exception: pass
        dj=cfg.get("dj_safety",{})
        if dj.get("detect_dj_databases",True):
            dbs=find_dj_databases(source_root, cfg)
            if dbs: warn(f"DJ databases: {', '.join(dbs)}"); out(f"  halt_on_dj_databases = {dj.get('halt_on_dj_databases',False)}")
            else: ok_msg("No DJ databases detected.")
        if MutagenFile:
            exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
            cr=str(roots["clean_root"]); test_file:Optional[Path]=None
            for root,_,files in os.walk(source_root):
                if str(Path(root)).startswith(cr): continue
                for f in files:
                    if Path(f).suffix.lower() in exts: test_file=Path(root)/f; break
                if test_file: break
            if test_file:
                try:
                    r=read_audio_tags(test_file,cfg); has=any(v for k,v in r.items() if k not in ("bpm","key") and v)
                    out(f"Mutagen: {'✓ tags readable' if has else C.YELLOW+'⚠ no tags'+C.RESET} ({test_file.name})")
                except Exception as e: err(f"Mutagen test failed: {e}")
    log_root=Path(cfg.get("logging",{}).get("root_dir","logs"))
    out(f"Logs:    {log_root}")
    try:
        ensure_dir(log_root); t=log_root/".write_test"; t.write_text("ok",encoding="utf-8"); t.unlink(); ok_msg(f"Logs writable: {log_root}")
    except Exception as e: err(f"Logs not writable: {log_root} — {e}"); is_ok=False
    # Performance tier
    recommended=detect_recommended_tier()
    configured=cfg.get("performance",{}).get("tier","medium")
    perf=resolve_perf_settings(cfg)
    out(f"\n{C.BOLD}Performance{C.RESET}")
    out(f"  CPU cores:       {os.cpu_count() or '?'}")
    out(f"  Configured tier: {C.CYAN}{configured}{C.RESET}")
    out(f"  Recommended:     {C.GREEN}{recommended}{C.RESET}")
    if recommended!=configured:
        out(f"  {C.DIM}Tip: set performance.tier: {recommended} in config.yaml, or use --performance {recommended}{C.RESET}")
    out(f"  Active:          workers={perf['workers']}  lookahead={perf['lookahead']}  sleep_copy={perf['sleep_copy_ms']}ms (copy-path only)")
    out(f"\n{'✓ Doctor complete.' if is_ok else C.YELLOW+'⚠ Doctor complete — see warnings.'+C.RESET}")


def cmd_catchall(
    cfg: Dict[str, Any],
    path_str: str,
    profile_name: str,
    dry_run: bool = False,
    genre_roots: Optional[List[str]] = None,
) -> None:
    """
    Process a flat dump folder: route loose files into Artist/Singles/ or Artist/Album/.
    Every track gets an artist folder. Tracks sharing an album tag are grouped together.
    Cross-references with Clean/ to propose merges for known artists.
    All moves are sessioned and undoable via `raagdosa undo --session <id>`.
    """
    cc = cfg.get("catchall", {})
    cross_ref = bool(cc.get("cross_reference_clean", True))

    # Auto-recognise catchall folder names
    extra_names = [n.lower() for n in cc.get("folder_names", []) or []]
    catchall_names = _CATCHALL_FOLDER_NAMES | set(extra_names)

    scan_path = Path(path_str).expanduser().resolve()
    if not scan_path.exists():
        err(f"Path not found: {scan_path}")
        sys.exit(1)

    is_known_catchall = scan_path.name.lower() in catchall_names
    out(f"\n{C.CYAN}Catchall:{C.RESET} {scan_path}" +
        (f"  {C.DIM}(auto-recognised){C.RESET}" if is_known_catchall else ""))

    exts = {e.lower() for e in cfg.get("scan", {}).get("audio_extensions", [".mp3", ".flac", ".m4a"])}

    # Session setup — logs go to app log folder, not source folder
    session_id = make_session_id("catchall", str(scan_path))
    session_dir = Path(cfg["logging"]["session_dir"]) / session_id
    if not dry_run:
        ensure_dir(session_dir)
    hist_path = Path(cfg["logging"]["history_log"])

    # Collect loose audio files (non-recursive — catchall is a flat dump)
    audio_files: List[Path] = []
    for f in scan_path.iterdir():
        if f.is_file() and f.suffix.lower() in exts and not f.name.startswith("._"):
            audio_files.append(f)

    if not audio_files:
        warn(f"No audio files found in {scan_path}")
        return

    out(f"  Found {len(audio_files)} audio files")

    # ── Read tags once for all files ──────────────────────────────────
    file_meta: Dict[Path, Dict[str, str]] = {}
    for af in audio_files:
        artist = _extract_catchall_artist(af, cfg)
        album = ""
        if MutagenFile:
            try:
                mf = MutagenFile(str(af), easy=True)
                if mf and mf.tags:
                    alb = mf.tags.get("album")
                    if isinstance(alb, list): alb = alb[0] if alb else None
                    if alb and str(alb).strip():
                        album = str(alb).strip()
            except Exception:
                pass
        file_meta[af] = {"artist": artist, "album": album}

    # ── Group by artist (fuzzy — merge connector variants) ───────────
    by_artist: Dict[str, List[Path]] = {}
    _artist_canonical: Dict[str, str] = {}  # normalized key → chosen display name
    for af, meta in file_meta.items():
        raw = meta["artist"]
        # Find existing group that matches this artist
        matched_key = None
        for existing_key in by_artist:
            if artists_are_same(raw, existing_key, cfg):
                matched_key = existing_key
                break
        if matched_key:
            by_artist[matched_key].append(af)
        else:
            by_artist[raw] = [af]

    # ── Cross-reference with Clean/ for merge proposals ───────────────
    clean_root: Optional[Path] = None
    if cross_ref and profile_name in cfg.get("profiles", {}):
        prof = cfg["profiles"][profile_name]
        source_root = Path(prof["source_root"]).expanduser()
        clean_root = derive_clean_root(prof, source_root)

    # ── Build per-file routing plans ──────────────────────────────────
    # Each file → Artist/Singles/ or Artist/Album/
    proposals: List[Dict[str, Any]] = []
    artist_stats: Dict[str, Dict[str, int]] = {}  # artist → {albums: N, singles: N}

    for artist, files in sorted(by_artist.items()):
        artist_clean = sanitize_name(normalize_artist_name(artist, cfg) or artist)
        artist_dir = scan_path / artist_clean

        # Sub-group by album within this artist
        album_groups: Dict[str, List[Path]] = {}  # album → files
        singles: List[Path] = []
        for f in files:
            alb = file_meta[f]["album"]
            if alb:
                album_groups.setdefault(alb, []).append(f)
            else:
                singles.append(f)

        # Albums need ≥2 tracks sharing the same album tag to be treated as albums
        confirmed_albums: Dict[str, List[Path]] = {}
        for alb, alb_files in album_groups.items():
            if len(alb_files) >= 2:
                confirmed_albums[alb] = alb_files
            else:
                # Single track with album tag → treat as single
                singles.extend(alb_files)

        a_stats = {"albums": 0, "singles": 0}

        # Build album proposals
        for alb, alb_files in sorted(confirmed_albums.items()):
            alb_clean = sanitize_name(smart_title_case(alb, cfg))
            dst = artist_dir / alb_clean
            file_plans = []
            for f in alb_files:
                new_name = _build_catchall_track_name(f, artist, cfg)
                file_plans.append({"source": str(f), "target_name": new_name})
            proposals.append({
                "type": "catchall_album",
                "artist": artist,
                "artist_clean": artist_clean,
                "album": alb,
                "album_clean": alb_clean,
                "files": file_plans,
                "count": len(alb_files),
                "destination": str(dst),
            })
            a_stats["albums"] += 1
            out(f"  {C.BLUE}[ALBUM ]{C.RESET}  {artist_clean}/{alb_clean}/ ({len(alb_files)} tracks)")

        # Build singles proposals → Artist/Singles/
        if singles:
            dst = artist_dir / "Singles"
            file_plans = []
            for f in singles:
                new_name = _build_catchall_track_name(f, artist, cfg)
                file_plans.append({"source": str(f), "target_name": new_name})
            proposals.append({
                "type": "catchall_singles",
                "artist": artist,
                "artist_clean": artist_clean,
                "files": file_plans,
                "count": len(singles),
                "destination": str(dst),
            })
            a_stats["singles"] = len(singles)

        artist_stats[artist_clean] = a_stats

        # Check if artist already exists in Clean/
        merge_target: Optional[Path] = None
        if clean_root and clean_root.exists():
            for existing in clean_root.rglob("*"):
                if existing.is_dir() and artists_are_same(existing.name, artist, cfg):
                    merge_target = existing
                    break

        total = len(files)
        alb_count = a_stats["albums"]
        sin_count = a_stats["singles"]
        parts = []
        if alb_count: parts.append(f"{alb_count} album{'s' if alb_count>1 else ''}")
        if sin_count: parts.append(f"{sin_count} single{'s' if sin_count>1 else ''}")
        detail = ", ".join(parts)

        if merge_target:
            out(f"  {C.YELLOW}[MERGE?]{C.RESET}  {artist_clean} ({total} tracks: {detail}) → {C.DIM}{merge_target}{C.RESET}")
        elif not confirmed_albums:
            out(f"  {C.GREEN}[ARTIST]{C.RESET}  {artist_clean}/Singles/ ({total} tracks)")

    if dry_run:
        out(f"\n  {C.DIM}--dry-run: no changes made. {len(proposals)} proposals.{C.RESET}")
        # Show rename preview per proposal
        for prop in proposals:
            dst_short = Path(prop["destination"]).relative_to(scan_path)
            out(f"  {C.DIM}→ {dst_short}/{C.RESET}")
            for fp in prop["files"][:3]:
                src_name = Path(fp["source"]).name
                if src_name != fp["target_name"]:
                    out(f"    {C.DIM}{src_name[:40]:<40} → {fp['target_name']}{C.RESET}")
                else:
                    out(f"    {C.DIM}{src_name}{C.RESET}")
            if len(prop["files"]) > 3:
                out(f"    {C.DIM}... and {len(prop['files']) - 3} more{C.RESET}")
        return

    # ── Apply moves with history logging ──────────────────────────────
    applied: List[Dict[str, Any]] = []
    created_dirs: Set[str] = set()
    dupe_count = 0
    for prop in proposals:
        dst_path = Path(prop["destination"])
        for fp in prop["files"]:
            f = Path(fp["source"])
            if not f.exists():
                continue
            try:
                ensure_dir(dst_path)
                created_dirs.add(str(dst_path))
                tgt = dst_path / fp["target_name"]
                # Collision resolution — detect true duplicates vs name clashes
                if tgt.exists():
                    # Check if same file size (likely duplicate)
                    if f.stat().st_size == tgt.stat().st_size:
                        dupe_count += 1
                        out(f"  {C.YELLOW}[DUPE?]{C.RESET}  {f.name} — same size as existing {tgt.name}, skipped")
                        continue
                    n = 1
                    while True:
                        cand = tgt.with_name(tgt.stem + f" ({n})" + tgt.suffix)
                        if not cand.exists():
                            tgt = cand
                            break
                        n += 1
                # Preserve timestamps
                src_stat = f.stat()
                f.rename(tgt)
                try:
                    os.utime(str(tgt), (src_stat.st_atime, src_stat.st_mtime))
                except Exception:
                    pass

                action_id = uuid.uuid4().hex[:10]
                entry = {
                    "action_id": action_id,
                    "timestamp": now_iso(),
                    "session_id": session_id,
                    "type": "catchall",
                    "move_type": "catchall",
                    "original_path": str(f),
                    "original_folder": str(scan_path),
                    "target_path": str(tgt),
                    "artist": prop.get("artist", "_Unknown"),
                    "album": prop.get("album", ""),
                    "proposal_type": prop["type"],
                }
                append_jsonl(hist_path, entry)
                applied.append(entry)
            except Exception as e:
                warn(f"Could not move {f.name}: {e}")

    # Save proposals to session log folder (not source folder)
    if applied:
        proposals_out = session_dir / "catchall_proposals.json"
        write_json(proposals_out, {
            "raagdosa_version": APP_VERSION,
            "generated_at": now_iso(),
            "session_id": session_id,
            "source_folder": str(scan_path),
            "proposals": proposals,
            "created_dirs": list(created_dirs),
        })

    moved_n = len(applied)
    renamed_n = sum(1 for a in applied if Path(a["original_path"]).name != Path(a["target_path"]).name)
    unique_artists = len(artist_stats)
    out(f"\n  {C.GREEN}✓ {moved_n} tracks moved{C.RESET} → {unique_artists} artist folder{'s' if unique_artists!=1 else ''}" +
        (f", {renamed_n} renamed" if renamed_n else "") +
        (f", {C.YELLOW}{dupe_count} duplicates skipped{C.RESET}" if dupe_count else ""))
    out(f"  {C.DIM}Session: {session_id}{C.RESET}")
    out(f"  {C.DIM}Undo: raagdosa undo --session {session_id}{C.RESET}")


# ─────────────────────────────────────────────
# help — grouped command reference
# ─────────────────────────────────────────────

