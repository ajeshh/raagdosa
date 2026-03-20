"""
RaagDosa Orchestration — top-level run modes.

Layer 7: imports from interactive (L7), moves (L6), pipeline (L6),
         proposal (L5), session (L3), files (L1), core (L0), ui (L0).

Provides:
  _run_core              — streaming batch pipeline (v3.81)
  _interactive_streaming — streaming interactive review (v7.0)
  _run_triage            — triage dashboard workflow (v7.1)
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import os
import queue as _queue
import re
import shutil
import sys
import threading as _threading
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from raagdosa import APP_VERSION
from raagdosa.core import (
    FolderProposal, now_iso, should_stop, make_session_id,
    register_stop_handler,
)
from raagdosa.files import (
    ensure_dir, safe_move_folder, append_jsonl, get_folder_size,
    cleanup_empty_parents, list_audio_files, write_json, is_hidden_file,
    _same_device,
)
from raagdosa.tags import normalize_unicode
from raagdosa.tagreader import read_audio_tags, _get_tag_cache
from raagdosa.ui import (
    C, VERBOSE, out, err, warn, ok_msg,
    status_tag, conf_color, human_size,
    Progress, SizeProgress, open_in_finder,
)
import raagdosa.ui as ui_module
from raagdosa.config import folder_matches_ignore
from raagdosa.session import (
    setup_logging_paths, ensure_roots, manifest_add, manifest_set_last_run,
    manifest_get_last_run, read_manifest, rotate_log_if_needed,
    derive_clean_root, derive_review_root,
    derive_clean_albums_root, derive_review_albums_root,
    derive_wrapper_root,
)
from raagdosa.library import resolve_library_path
from raagdosa.review import collision_resolve
from raagdosa.tracks import classify_folder_for_tracks, build_track_filename
from raagdosa.pipeline import (
    resolve_perf_settings, build_skip_sets, folder_mtime,
    compare_with_existing, resolve_genre_roots,
)
from raagdosa.proposal import (
    build_folder_proposal, _route_proposal, _write_review_sidecar,
    _build_review_summary, _apply_format_suffix, folder_is_multidisc,
)
from raagdosa.moves import (
    _write_session_reports, rename_tracks_in_clean_folder,
    _parse_since, classify_artifacts, move_artifacts_to_quarantine,
    merge_missing_tracks,
)
from raagdosa.interactive import (
    _display_folder_card, _display_tracks, _edit_track_title,
    _interactive_action_help, _prompt_review_note,
    interactive_review,
    _triage_proposals, _show_triage_dashboard,
    _prompt_triage_action, _bulk_approve_auto,
)


def _run_core(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,
               since_str:Optional[str]=None,perf_tier:Optional[str]=None,
               genre_roots:Optional[List[str]]=None,itunes_mode:bool=False)->None:
    """
    True per-album streaming pipeline (v3.81).

    Architecture:
      - Walk source → queue of individual candidate folders
      - Scanner thread pre-reads tags for `lookahead` folders ahead
      - Main thread: for each folder: route → artifact quarantine → apply move
        → track rename → duplicate resolution → immediately continue to next
      - First album moves within seconds of starting on any library size
      - Performance tier controls workers, lookahead, and copy-path sleep
    """
    import queue as _queue, threading as _threading

    for lk in ["history_log","track_history_log"]:
        lp=Path(cfg.get("logging",{}).get(lk,""))
        if lp.name: rotate_log_if_needed(lp,float(cfg.get("logging",{}).get("rotate_log_max_mb",10.0)))
    register_stop_handler()

    profiles=cfg.get("profiles",{}); profile_obj=profiles[profile]
    source_root=Path(profile_obj["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    # v5.5 — resolve log paths into wrapper folder and merge skip-sets from config
    setup_logging_paths(cfg, profile_obj, source_root)
    skip_exts, skip_folder_names = build_skip_sets(cfg)

    roots=ensure_roots(profile_obj,source_root,create=not dry_run)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    clean_root_str =str(roots["clean_root"].resolve())+os.sep
    review_root_str=str(roots["review_root"].resolve())+os.sep

    # ── Performance settings ──────────────────────────────────────
    perf=resolve_perf_settings(cfg, perf_tier)
    workers     =perf["workers"]
    lookahead   =perf["lookahead"]
    sleep_copy  =perf["sleep_copy_ms"]/1000.0

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])
    since=_parse_since(since_str,cfg)

    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    max_unread=float(sc.get("max_unreadable_track_ratio",0.25))
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder
    dc=cfg.get("duplicates",{}); do_dup_compare=bool(dc.get("compare_before_routing",True))
    do_merge    =bool(dc.get("merge_missing_tracks",True))
    flac_coex   =dc.get("flac_mp3_coexistence","keep_both")
    do_artifacts=bool(cfg.get("artifacts",{}).get("enabled",True))
    trc=cfg.get("track_rename",{})
    do_tracks=(trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"))

    # Session setup
    session_id=make_session_id()
    session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)
    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    tier_name=perf_tier or cfg.get("performance",{}).get("tier","medium")
    out(f"{C.DIM}Performance: {tier_name}  workers={workers}  lookahead={lookahead}  sleep={perf['sleep_copy_ms']}ms{C.RESET}",level=VERBOSE)

    # Pre-load existing Clean names for cross-run dedup
    seen_names:Counter=Counter()
    existing_clean:Set[str]=set()
    existing_clean_paths:Dict[str,Path]={}    # name → actual path (for comparison)
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir():
                    n=normalize_unicode(item.name)
                    existing_clean.add(n)
                    existing_clean_paths[n]=item
        except Exception: pass
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())

    # ── Phase 1: collect candidate paths ──────────────────────────
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[];continue
        if leaf_only and dirs: continue
        if sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_tracks: candidates.append(rp)
    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
        out(f"  --since: {len(candidates)} folders modified after {since.strftime('%Y-%m-%d %H:%M')}",level=VERBOSE)
    candidates=[rp for rp in candidates if not folder_matches_ignore(rp.name,ignore_patterns)]
    total_n=len(candidates)
    out(f"{C.DIM}Candidates: {total_n} folder(s){C.RESET}",level=VERBOSE)

    # ── Pre-flight: confirmation gate + disk space check ──
    if total_n and not dry_run:
        total_bytes=sum(get_folder_size(c) for c in candidates)
        out(f"\n  {C.BOLD}Ready to process {total_n} folder(s) ({human_size(total_bytes)}){C.RESET}")
        try:
            dest_path=clean_albums if clean_albums.exists() else clean_albums.parent
            free_bytes=shutil.disk_usage(dest_path).free
            if total_bytes > free_bytes * 0.9:
                out(f"  {C.YELLOW}Low disk space! Need ~{human_size(total_bytes)}, only {human_size(free_bytes)} free.{C.RESET}")
        except OSError:
            pass
        try:
            answer=input(f"  Continue? [y/N] ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            answer=""
        if answer != "y":
            out(f"  {C.DIM}Cancelled.{C.RESET}"); return

    _get_tag_cache(cfg)

    # ── Streaming pipeline ─────────────────────────────────────────
    # Queue carries individual FolderProposal|None (sentinel)
    # lookahead controls queue maxsize — scanner runs this many folders ahead
    scanner_queue:_queue.Queue=_queue.Queue(maxsize=max(1,lookahead))
    all_proposals:List[FolderProposal]=[]
    total_clean=0; total_review=0; total_dup=0; total_merged=0; total_artifacts=0; total_failed=0
    prog=Progress(total_n,"Processing")

    def _scan_one(rp:Path)->Optional[FolderProposal]:
        if should_stop(): return None
        audio_files=list_audio_files(rp,exts,follow_sym)
        if len(audio_files)<min_tracks: prog.tick(rp.name); return None
        return build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)

    def _scanner_worker():
        # Pre-read tags for each folder; push individual proposals to queue
        if workers>1 and total_n>1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures={pool.submit(_scan_one,rp):rp for rp in candidates}
                # Submit in order; yield in completion order
                for fut in as_completed(futures):
                    if should_stop(): break
                    try:
                        prop=fut.result()
                        scanner_queue.put(prop)   # None = skip, main thread handles
                    except Exception as e:
                        err(f"  scan error: {e}"); scanner_queue.put(None)
        else:
            for rp in candidates:
                if should_stop(): scanner_queue.put(None); break
                scanner_queue.put(_scan_one(rp))
        scanner_queue.put("DONE")
        _tc = _get_tag_cache(cfg)
        if _tc is not None:
            _tc.save()
            out(f"  Tag cache: {_tc.size} entries saved",level=VERBOSE)

    def _route_one(p:FolderProposal)->FolderProposal:
        """Route a single proposal. Updates seen_names."""
        reasons:List[str]=[]; dest="clean"
        if rr.get("route_questionable_to_review",True) and p.confidence<min_conf:
            dest="review"; reasons.append("low_confidence")
        seen_names[p.proposed_folder_name]+=1
        if rr.get("route_duplicates",True) and seen_names[p.proposed_folder_name]>1:
            dest="duplicate"; reasons.append("duplicate_in_run")
        norm_prop=normalize_unicode(p.proposed_folder_name)
        is_existing_clean=(norm_prop in existing_clean or norm_prop in manifest_entries)
        if rr.get("route_cross_run_duplicates",True) and is_existing_clean:
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
        p.destination=dest; p.decision["route_reasons"]=reasons
        if reasons:
            p.decision["review_summary"]=_build_review_summary(reasons,p.decision.get("confidence_factors",{}),p.confidence)
        if dest=="review":      p.target_path=str(review_albums/p.proposed_folder_name)
        elif dest=="duplicate": p.target_path=str(dup_root/p.proposed_folder_name)
        return p

    def _handle_duplicate(p:FolderProposal)->str:
        """
        Intelligent duplicate handling. Returns final outcome label.
        Mutates p.destination and p.decision in place.
        """
        if not do_dup_compare: return "duplicate"
        norm_prop=normalize_unicode(p.proposed_folder_name)
        ex_path=existing_clean_paths.get(norm_prop)
        if ex_path is None or not ex_path.exists(): return "duplicate"

        inc_path=Path(p.folder_path)
        # Read tags for both sides (use cache)
        inc_audio=list_audio_files(inc_path,exts,follow_sym)
        ex_audio =list_audio_files(ex_path, exts,follow_sym)
        inc_tags=[read_audio_tags(f,cfg) for f in inc_audio]
        ex_tags =[read_audio_tags(f,cfg) for f in ex_audio]

        result=compare_with_existing(inc_path,ex_path,inc_tags,ex_tags,cfg)
        outcome=result["outcome"]
        p.decision["dup_compare"]=result
        p.decision["dup_compare"]["missing_in_existing"]=[str(f) for f in result.get("missing_in_existing",[])]

        if outcome=="missing_tracks" and do_merge:
            missing=result.get("missing_in_existing",[])
            copied=merge_missing_tracks(missing,ex_path,cfg,session_id,dry_run=dry_run)
            p.decision["merged_tracks"]=copied
            p.destination="merged"; p.decision["route_reasons"].append(f"merged_{len(copied)}_tracks")
            # Re-run track rename on the now-augmented existing folder
            if do_tracks and not dry_run:
                rename_tracks_in_clean_folder(cfg,ex_path,p.decision,interactive=False,
                                               dry_run=False,session_id=session_id)
            return "merged"

        if outcome=="format_upgrade":
            if flac_coex=="keep_both" or flac_coex=="prefer_flac":
                if lib.get("flac_segregation",False):
                    # Route to FLAC sub-folder
                    artist_dir=ex_path.parent
                    flac_dest=artist_dir/"FLAC"/p.proposed_folder_name
                    p.target_path=str(flac_dest)
                    p.destination="clean"; p.decision["route_reasons"].append("flac_segregation")
                    return "flac_upgrade"
            p.decision["route_reasons"].append("format_upgrade")
            return "format_upgrade_review"

        if outcome=="lower_quality_mp3":
            p.decision["route_reasons"].append("lower_quality_mp3")
            return "lower_quality_mp3"

        p.decision["route_reasons"].append(outcome)
        return outcome

    # Scanner starts immediately
    scanner_thread=_threading.Thread(target=_scanner_worker,daemon=True)
    scanner_thread.start()

    folder_idx=0
    hist_path=Path(cfg.get("logging",{}).get("history_log","logs/history.jsonl"))

    while True:
        item=scanner_queue.get()
        if item=="DONE": break
        if item is None:
            # Either below min_tracks or scan error — just tick
            prog.tick(""); continue
        if should_stop():
            out(f"\n{C.YELLOW}Stop requested.{C.RESET}"); break

        p:FolderProposal=item
        folder_idx+=1
        prog.tick(p.folder_name)
        p=_route_one(p)
        src_path=Path(p.folder_path)

        # ── Artifact quarantine (before move) ──────────────────
        artifact_count=0
        if do_artifacts and src_path.exists():
            artifacts=classify_artifacts(src_path,cfg)
            artifact_count=move_artifacts_to_quarantine(
                artifacts,p.folder_name,cfg,profile_obj,source_root,dry_run,session_id)
            if artifact_count:
                p.decision["artifacts_quarantined"]=artifact_count
                total_artifacts+=artifact_count

        # ── Duplicate resolution ────────────────────────────────
        dup_outcome=None
        if p.destination=="duplicate":
            dup_outcome=_handle_duplicate(p)
            if dup_outcome=="merged":
                total_merged+=1
                out(f"  [{folder_idx}/{total_n}]  {C.CYAN}MERGED{C.RESET} ⟳  {p.folder_name}  "
                    f"({len(p.decision.get('merged_tracks',[]))} tracks added)")
                all_proposals.append(p); continue   # no folder move needed
            if dup_outcome=="flac_upgrade":
                p.destination="clean"   # will move to FLAC subfolder

        # ── Apply folder move ───────────────────────────────────
        if p.destination in ("clean","review","duplicate"):
            target=Path(p.target_path); ensure_dir(target.parent)
            if dry_run:
                same=_same_device(src_path,target)
                out(f"  [{folder_idx}/{total_n}]  {C.DIM}[dry-run][{'rename' if same else 'copy'}]{C.RESET}"
                    f"  {status_tag(p.destination)}  {p.folder_name}  →  {target.name}"
                    f"  conf={conf_color(p.confidence)}")
            elif src_path.exists():
                try:
                    move_method,move_elapsed=safe_move_folder(src_path,target)
                    if sleep_copy>0 and move_method=="copy":
                        import time as _time; _time.sleep(sleep_copy)
                    # Log action
                    action_id=uuid.uuid4().hex[:10]
                    entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,
                           "type":"folder","original_path":str(src_path),
                           "original_parent":str(src_path.parent),"original_folder_name":p.folder_name,
                           "target_path":str(target),"target_parent":str(target.parent),
                           "target_folder_name":target.name,"destination":p.destination,
                           "confidence":p.confidence,"decision":p.decision,
                           "move_method":move_method}
                    append_jsonl(hist_path,entry)
                    if p.destination=="clean":
                        manifest_add(cfg,target.name,{"original_path":str(src_path),
                                                       "confidence":p.confidence,"session_id":session_id})
                        existing_clean.add(normalize_unicode(target.name))
                        existing_clean_paths[normalize_unicode(target.name)]=target
                    elif p.destination=="review":
                        _write_review_sidecar(target,p,session_id)
                    method_tag=f"  {C.DIM}[{move_method} {move_elapsed*1000:.0f}ms]{C.RESET}" if ui_module._verbosity>=VERBOSE else ""
                    art_tag=f"  {C.DIM}[{artifact_count} artifacts quarantined]{C.RESET}" if artifact_count else ""
                    out(f"  [{folder_idx}/{total_n}]  MOVED {status_tag(p.destination)}"
                        f"  {C.DIM}{p.folder_name}{C.RESET}  →  {target.name}"
                        f"  conf={conf_color(p.confidence)}{method_tag}{art_tag}")
                    try: cleanup_empty_parents(src_path,source_root)
                    except Exception: pass
                    # Track rename immediately after move
                    if do_tracks and p.destination=="clean":
                        rename_tracks_in_clean_folder(cfg,target,p.decision,
                                                       interactive=interactive,dry_run=dry_run,
                                                       session_id=session_id)
                except RuntimeError as e:
                    err(f"  [{folder_idx}/{total_n}]  ⛔ FAILED  {p.folder_name}: {e}")
                    total_failed+=1

        # Tally
        if   p.destination=="clean":     total_clean+=1
        elif p.destination=="review":    total_review+=1
        elif p.destination in ("duplicate","merged"): total_dup+=1
        all_proposals.append(p)

    prog.done()
    scanner_thread.join(timeout=5)
    _tc = _get_tag_cache(cfg)
    if _tc is not None: _tc.save()

    # Session report
    payload={"app":cfg.get("app",{}),"session_id":session_id,"timestamp":now_iso(),
             "profile":profile,"source_root":str(source_root),
             "since":since.isoformat() if since else None,
             "folder_proposals":[dataclasses.asdict(p) for p in all_proposals]}
    write_json(session_dir/"proposals.json",payload)
    _write_session_reports(session_id,profile,source_root,all_proposals,session_dir,cfg)

    merged_tag=f"  {C.CYAN}Merged: {total_merged}{C.RESET}" if total_merged else ""
    fail_tag=f" | {C.RED}Failed: {total_failed}{C.RESET}" if total_failed else ""
    art_tag=f"  {C.DIM}Artifacts quarantined: {total_artifacts}{C.RESET}" if total_artifacts else ""
    out(f"\n{C.BOLD}Results:{C.RESET}   {len(all_proposals)} processed | "
        f"{C.GREEN}Clean: {total_clean}{C.RESET} | {C.YELLOW}Review: {total_review}{C.RESET} | "
        f"{C.RED}Dupes: {total_dup}{C.RESET}{merged_tag}{fail_tag}")
    if total_failed:
        out(f"  {C.YELLOW}⚠ {total_failed} folder(s) failed. Successfully moved folders can be undone:{C.RESET}")
        out(f"  raagdosa undo --session {session_id}")
    if art_tag: out(art_tag)
    out(f"{C.DIM}Reports:   {session_dir}/report.{{txt,csv,html}}{C.RESET}")
    manifest_set_last_run(cfg)



def _interactive_streaming(cfg:Dict[str,Any],profile_name:str,dry_run:bool=False,
                           since_str:Optional[str]=None,genre_roots:Optional[List[str]]=None,
                           itunes_mode:bool=False,threshold:Optional[float]=None,
                           sort_by:str="name")->None:
    """
    v7.0: Streaming interactive mode — scan one folder at a time, display the review
    card, wait for user action, then move to the next folder. No parallel scanning.
    This avoids the multitask/worker clash with interactive prompts.
    """
    register_stop_handler()
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile_obj=profiles[profile_name]; source_root=Path(profile_obj["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    setup_logging_paths(cfg, profile_obj, source_root)
    skip_exts, skip_folder_names = build_skip_sets(cfg)
    effective_genre_roots = resolve_genre_roots(cfg, genre_roots)
    roots=ensure_roots(profile_obj,source_root,create=not dry_run)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    wrapper_root_str=str(derive_wrapper_root(profile_obj,source_root).resolve())+os.sep
    clean_root_str =str(derive_clean_root(profile_obj,source_root).resolve())+os.sep
    review_root_str=str(derive_review_root(profile_obj,source_root).resolve())+os.sep

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])
    # skip_folder_names already set by build_skip_sets above
    # skip_exts already set by build_skip_sets above

    since=_parse_since(since_str,cfg)
    _get_tag_cache(cfg)

    # Session
    session_id=make_session_id(profile_name, str(source_root))
    session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)

    # Pre-load existing Clean names for dedup
    existing_clean:Set[str]=set()
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir(): existing_clean.add(normalize_unicode(item.name))
        except Exception: pass
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder
    seen_names:Counter=Counter()

    # Collect candidates (fast walk, no tag reading)
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        if rp_str.startswith(wrapper_root_str): dirs[:]=[];continue
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[];continue
        dirs[:] = [d for d in dirs if d not in skip_folder_names]
        if effective_genre_roots and rp.name in effective_genre_roots and rp != source_root: continue
        if leaf_only and dirs: continue
        audio_count = sum(1 for f in files
                          if Path(f).suffix.lower() in exts
                          and Path(f).suffix.lower() not in skip_exts
                          and not f.startswith("._"))
        if audio_count >= min_tracks: candidates.append(rp)
    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
    candidates=[rp for rp in candidates if not folder_matches_ignore(rp.name,ignore_patterns)]

    # Sort candidates based on user preference
    _conf_sort=sort_by in ("confidence","confidence-desc")
    if sort_by=="date-created":
        candidates.sort(key=lambda p:p.stat().st_birthtime if hasattr(p.stat(),"st_birthtime") else p.stat().st_ctime)
        sort_label="date created"
    elif sort_by=="date-modified":
        candidates.sort(key=lambda p:p.stat().st_mtime)
        sort_label="date modified"
    elif _conf_sort:
        sort_label="confidence (hardest first)" if sort_by=="confidence" else "confidence (easiest first)"
    else:
        # Default: name — symbols first, then numbers, then letters (natural sort)
        candidates.sort(key=lambda p:p.name.lower())
        sort_label="name"

    total=len(candidates)
    if total==0:
        out(f"\n  {C.DIM}No candidate folders found.{C.RESET}"); return

    # Confidence sort requires pre-scanning all folders to get scores
    _prescan_proposals:Optional[Dict[str,FolderProposal]]=None
    if _conf_sort:
        out(f"\n  Pre-scanning {total} folders for confidence sort...")
        _prescan_list:List[Tuple[Path,FolderProposal]]=[]
        _prog=Progress(total,"Pre-scanning")
        for rp in candidates:
            audio_files=list_audio_files(rp,exts,follow_sym)
            if len(audio_files)>=min_tracks:
                p=build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)
                if p:
                    _route_proposal(p,cfg,Counter(),existing_clean,manifest_entries,
                                    review_albums,dup_root,mixes_root)
                    _prescan_list.append((rp,p))
            _prog.tick(rp.name)
        _prog.done()
        # Sort by confidence
        reverse=sort_by=="confidence-desc"
        _prescan_list.sort(key=lambda x:x[1].confidence,reverse=reverse)
        candidates=[rp for rp,_ in _prescan_list]
        total=len(candidates)

    # Session header
    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}RAAGDOSA v{APP_VERSION}  ·  Interactive Review  ·  Session {session_id[:12]}{C.RESET}")
    print(f"{'─'*66}")
    print(f"  Candidates: {total} folder(s)  ·  Sort: {sort_label}")
    if threshold: print(f"  Threshold: only review folders below {threshold:.2f}")
    if dry_run: print(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")
    print(f"  Press ? for help at any prompt.")
    print(f"{'═'*66}")

    # Reuse the interactive_review action loop internals
    mc=cfg.get("move",{}); policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})")
    use_cs=bool(mc.get("use_checksum",False))
    hist_path=Path(cfg["logging"]["history_log"]); skip_path=Path(cfg["logging"]["skipped_log"])
    session_notes_path=session_dir/"review_notes.jsonl"

    # ── Tracking (moves happen immediately, these track what was done) ──
    applied:List[Dict[str,Any]]=[]   # successful move entries
    skipped_count=0
    moved_clean=0; moved_review=0
    held:List[Tuple[int,Path]]=[]    # user said [f] — review again later
    auto_approved=0; stopped=False

    def _execute_move(p_:FolderProposal,action:str="approved",reason:str="")->bool:
        """Execute a single folder move immediately. Returns True on success."""
        nonlocal moved_clean, moved_review
        if dry_run:
            dest_label=f"Review/{p_.proposed_folder_name}" if p_.destination=="review" else p_.proposed_folder_name
            out(f"  {C.DIM}[dry-run]{C.RESET} → {dest_label}  {status_tag(p_.destination)}")
            return True
        src_=Path(p_.folder_path); dst_=Path(p_.target_path)
        if not src_.exists(): return False
        dst2=collision_resolve(dst_,policy,sfmt)
        if dst2 is None:
            warn(f"Collision skip: {dst_.name}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src_)})
            return False
        ensure_dir(dst2.parent)
        try:
            move_method,_=safe_move_folder(src_,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"  Move failed: {src_.name}: {e}"); return False
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src_),"original_parent":str(src_.parent),"original_folder_name":src_.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p_.destination,"confidence":p_.confidence,"decision":p_.decision,
               "move_method":move_method,"interactive_action":action}
        if reason: entry["user_note"]=reason
        append_jsonl(hist_path,entry); applied.append(entry)
        if p_.destination=="clean":
            manifest_add(cfg,dst2.name,{"original_path":str(src_),"confidence":p_.confidence,"session_id":session_id})
            existing_clean.add(normalize_unicode(dst2.name))
            moved_clean+=1
        elif p_.destination=="review":
            _write_review_sidecar(dst2,p_,session_id)
            moved_review+=1
        if reason:
            ensure_dir(session_notes_path.parent)
            append_jsonl(session_notes_path,{"folder":src_.name,"action":action,"note":reason,"timestamp":now_iso(),"confidence":p_.confidence})
        try: cleanup_empty_parents(src_,source_root)
        except Exception: pass
        return True

    def _re_derive_target(p_:FolderProposal)->None:
        """Re-derive target path from current decision state."""
        ca=derive_clean_albums_root(profile_obj,source_root)
        art=p_.decision.get("albumartist_display","Unknown")
        new_t=resolve_library_path(ca,art,p_.decision.get("dominant_album_display",""),
                                   p_.decision.get("year"),p_.decision.get("is_flac_only",False),
                                   p_.decision.get("is_va",False),False,p_.decision.get("is_mix",False),
                                   cfg,profile_obj,genre=p_.decision.get("genre"),
                                   bpm=p_.decision.get("bpm"),key=p_.decision.get("key"),
                                   label=p_.decision.get("label"))
        # Re-apply format suffix (e.g. [FLAC]) — resolve_library_path doesn't add it
        folder_name=_apply_format_suffix(new_t.name,cfg,p_.stats.extensions if p_.stats else None)
        # Re-apply disc subfolder if present
        disc_sub=p_.decision.get("disc_subfolder")
        if disc_sub:
            new_t=new_t/disc_sub
        p_.proposed_folder_name=folder_name; p_.target_path=str(new_t); p_.destination="clean"

    def _run_review_pass(items:List[Tuple[int,Path]],pass_label:str="Review"):
        """Run the interactive review loop over a list of (idx, candidate_path) tuples."""
        nonlocal auto_approved, stopped, skipped_count
        for idx,rp in items:
            if should_stop() or stopped:
                out(f"\n{C.YELLOW}Stopped.{C.RESET}"); stopped=True; break

            # ── Scan this one folder ──────────────────────────────
            audio_files=list_audio_files(rp,exts,follow_sym)
            if len(audio_files)<min_tracks: continue
            p=build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)
            if p is None: continue

            # ── Route it ──────────────────────────────────────────
            _route_proposal(p,cfg,seen_names,existing_clean,manifest_entries,
                            review_albums,dup_root,mixes_root)

            # ── Threshold filter: auto-accept high-confidence folders ──
            if threshold is not None and p.confidence>=threshold:
                _execute_move(p,"auto_approved")
                auto_approved+=1; continue

            # ── Display card + track preview by default ─────────
            _display_folder_card(idx,total,p)
            _display_tracks(p,cfg)

            # Build track file list for e<N> track-title editing
            _tf_exts={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}
            _stream_track_files=sorted([f for f in rp.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if rp.exists() else []

            # Running tally
            tally=f"{C.DIM}{moved_clean} moved · {moved_review} review · {skipped_count} skipped{C.RESET}"

            while True:
                if p.confidence<0.50:
                    print(f"  {C.DIM}z:move  x:reject  c:skip  space:tracks  e:edit  o:finder  R:rescan  ?:more  ({tally}){C.RESET}")
                else:
                    print(f"  z:move  x:reject  c:skip  space:tracks  e:edit  o:finder  R:rescan  ?:more  ({tally})")
                try:
                    _raw=input(f"  > ").strip()
                    choice=_raw if _raw in ("R",) else _raw.lower()
                except (EOFError,KeyboardInterrupt):
                    choice="q"

                if choice in ("","z","y"):
                    # Confidence gate — warn but always allow explicit user override
                    if p.confidence<0.50:
                        confirm=input(f"  {C.RED}Very low confidence ({p.confidence:.2f}).{C.RESET} Force move anyway? [y/N]: ").strip().lower()
                        if confirm not in ("y","yes","z"): continue
                    elif p.confidence<0.70:
                        confirm=input(f"  {C.YELLOW}Low confidence ({p.confidence:.2f}).{C.RESET} Move anyway? [y/n]: ").strip().lower()
                        if confirm not in ("y","yes","z"): continue
                    if _execute_move(p):
                        out(f"  {C.GREEN}✓ Moved{C.RESET} → {p.proposed_folder_name}")
                    break

                elif choice in ("x","d","r"):
                    # Reject — move to Review/ immediately with optional reason
                    reason=_prompt_review_note()
                    p.destination="review"
                    p.decision["route_reasons"]=p.decision.get("route_reasons",[])+["user_rejected"]
                    p.decision["user_review_note"]=reason
                    review_target=review_albums/p.proposed_folder_name
                    p.target_path=str(review_target)
                    if _execute_move(p,"rejected",reason):
                        reason_str=f"  ({reason})" if reason else ""
                        out(f"  {C.YELLOW}→ Review/{C.RESET}{reason_str}")
                    break

                elif choice in ("c","k","s"):
                    skipped_count+=1
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                                            "reason":"user_skipped","src":str(Path(p.folder_path)),"interactive_action":"skipped"})
                    out(f"  {C.DIM}→ Skipped{C.RESET}"); break

                elif choice=="e":
                    # e = album title edit (e<N> = track title edit — separate operations)
                    current_album=p.decision.get("dominant_album_display","")
                    print(f"  {C.DIM}Current:{C.RESET} {current_album}")
                    new_album=input(f"  New album title (Enter to cancel): ").strip()
                    if not new_album: continue
                    p.decision["dominant_album_display"]=new_album
                    p.decision["override_type"]=p.decision.get("override_type","")+"edit_title"
                    _re_derive_target(p)
                    print(f"  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Album: {C.BOLD}{new_album}{C.RESET}")
                    print(f"    {C.BOLD}→ {p.proposed_folder_name}{C.RESET}")

                elif choice.startswith("e") and len(choice)>1 and choice[1:].strip().isdigit():
                    # e<N> = edit individual track title tag (separate from album title)
                    track_num=int(choice[1:].strip())
                    if 1<=track_num<=len(_stream_track_files):
                        _edit_track_title(_stream_track_files[track_num-1],cfg)
                        print(f"  {C.DIM}Press space to refresh track view.{C.RESET}")
                    else:
                        print(f"  {C.DIM}Track {track_num} not found (folder has {len(_stream_track_files)} tracks).{C.RESET}")

                elif choice=="a":
                    current=p.decision.get("albumartist_display","--")
                    print(f"  {C.DIM}Current:{C.RESET} {current}")
                    new_artist=input(f"  New artist name: ").strip()
                    if not new_artist: continue
                    p.decision["albumartist_display"]=new_artist
                    p.decision["is_va"]=False
                    p.decision["override_type"]="set_artist"
                    p.decision["override_original"]=current
                    _re_derive_target(p)
                    print(f"  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Artist: {C.BOLD}{new_artist}{C.RESET}  (was: {current})")
                    print(f"    {C.BOLD}→ {p.proposed_folder_name}{C.RESET}")

                elif choice=="v":
                    was_va=p.decision.get("is_va",False)
                    if was_va:
                        new_artist=input(f"  Artist name: ").strip()
                        if not new_artist: continue
                        p.decision["is_va"]=False; p.decision["albumartist_display"]=new_artist
                        p.decision["override_type"]="unmark_va"
                    else:
                        p.decision["is_va"]=True
                        p.decision["albumartist_display"]="Various Artists"
                        p.decision["override_type"]="mark_va"
                    _re_derive_target(p)
                    va_label="VA" if p.decision["is_va"] else p.decision["albumartist_display"]
                    print(f"  {C.CYAN}→ {va_label}{C.RESET}  TO: {p.proposed_folder_name}")

                elif choice=="f":
                    held.append((idx,rp))
                    out(f"  {C.YELLOW}→ Flagged{C.RESET}"); break

                elif choice=="u":
                    # In-session undo picker
                    if not applied:
                        print(f"  {C.DIM}Nothing moved yet this session.{C.RESET}"); continue
                    print(f"\n  {C.BOLD}Moves this session:{C.RESET}")
                    print(f"  {'#':<4} {'Folder':<55} {'Dest'}")
                    print(f"  {'─'*66}")
                    for ui,uh in enumerate(applied,1):
                        uname=Path(uh.get("original_path","")).name or uh.get("action_id","")
                        udest=uh.get("destination","?")
                        ucol=C.GREEN if udest=="clean" else C.YELLOW
                        print(f"  {ui:<4} {uname:<55} {ucol}{udest}{C.RESET}")
                    print(f"\n  Enter number(s) to undo (e.g. 3  or  1,3), or Enter to cancel:")
                    try:
                        uraw=input(f"  > ").strip().lower()
                    except (EOFError,KeyboardInterrupt):
                        uraw=""
                    if not uraw: continue
                    uidxs=[]
                    for tok in re.split(r"[,\s]+",uraw):
                        try: uidxs.append(int(tok)-1)
                        except ValueError: pass
                    for ui in sorted(set(uidxs),reverse=True):
                        if not 0<=ui<len(applied): continue
                        uh=applied[ui]
                        usrc=Path(uh["target_path"]); udst=Path(uh["original_path"])
                        if not usrc.exists():
                            print(f"  {C.RED}SKIP (missing):{C.RESET} {usrc.name}"); continue
                        try:
                            shutil.move(str(usrc),str(udst))
                            applied.pop(ui)
                            print(f"  {C.CYAN}UNDONE:{C.RESET} {usrc.name}")
                        except Exception as ue:
                            print(f"  {C.RED}FAILED:{C.RESET} {ue}")

                elif choice==" ":
                    _display_tracks(p,cfg)

                elif choice=="o":
                    open_in_finder(rp)

                elif choice in ("R",) or (choice in ("rescan",)):
                    # Rescan folder after manual changes in Finder
                    if not rp.exists():
                        print(f"  {C.RED}Folder no longer exists.{C.RESET}"); break
                    print(f"  {C.CYAN}Rescanning...{C.RESET}")
                    audio_files=list_audio_files(rp,exts,follow_sym)
                    if len(audio_files)<min_tracks:
                        print(f"  {C.YELLOW}Only {len(audio_files)} audio file(s) remain — below minimum ({min_tracks}).{C.RESET}")
                        print(f"  {C.DIM}Skip this folder? [y/N]{C.RESET}")
                        skip_confirm=input(f"  > ").strip().lower()
                        if skip_confirm in ("y","yes"):
                            skipped_count+=1; break
                        continue
                    new_p=build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)
                    if new_p is None:
                        print(f"  {C.YELLOW}Folder no longer qualifies after rescan.{C.RESET}"); break
                    _route_proposal(new_p,cfg,seen_names,existing_clean,manifest_entries,
                                    review_albums,dup_root,mixes_root)
                    # Replace current proposal
                    p=new_p
                    # Rebuild track file list
                    _stream_track_files=sorted([f for f in rp.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if rp.exists() else []
                    # Re-display
                    _display_folder_card(idx,total,p)
                    _display_tracks(p,cfg)
                    print(f"  {C.CYAN}─ RESCANNED ─{C.RESET}  {len(audio_files)} tracks  ·  conf {conf_color(p.confidence)}  → {p.destination.title()}")

                elif choice=="q":
                    stopped=True
                    out(f"\n{C.YELLOW}Stopped.{C.RESET}"); break

                elif choice=="?":
                    _interactive_action_help()

                else:
                    print(f"  {C.DIM}Unknown key. Press ? for help.{C.RESET}")

    # ══════════════════════════════════════════════════════════════
    # PASS 1: Main review
    # ══════════════════════════════════════════════════════════════
    main_items=[(idx,rp) for idx,rp in enumerate(candidates,1)]
    _run_review_pass(main_items, "Main review")

    # ══════════════════════════════════════════════════════════════
    # Held queue — opt-in, not automatic
    # ══════════════════════════════════════════════════════════════
    if held and not stopped:
        print(f"\n{'─'*66}")
        print(f"  {len(held)} folder(s) held. Review now? [y/n]")
        try:
            review_held=input(f"  ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            review_held="n"
        if review_held in ("y","yes"):
            _run_review_pass(held, "Held review")

    # ══════════════════════════════════════════════════════════════
    # End-of-session summary
    # ══════════════════════════════════════════════════════════════
    _tc = _get_tag_cache(cfg)
    if _tc is not None: _tc.save()

    print(f"\n{'━'*66}")
    parts=[]
    if moved_clean or auto_approved:
        clean_total=moved_clean+auto_approved
        parts.append(f"{C.GREEN}{clean_total} → Clean{C.RESET}")
        if auto_approved: parts[-1]+=f" {C.DIM}({auto_approved} auto){C.RESET}"
    if moved_review: parts.append(f"{C.YELLOW}{moved_review} → Review{C.RESET}")
    if skipped_count: parts.append(f"{C.DIM}{skipped_count} skipped{C.RESET}")
    n_held=len([h for h in held if isinstance(h,tuple)])
    if n_held: parts.append(f"{C.YELLOW}{n_held} held{C.RESET}")
    if parts:
        print(f"  {C.BOLD}DONE{C.RESET}  ·  {'  ·  '.join(parts)}")
    else:
        print(f"  {C.DIM}Nothing processed.{C.RESET}")
    if applied:
        print(f"  Undo:  raagdosa undo --session {session_id}")
    print(f"{'━'*66}")
    manifest_set_last_run(cfg)



def _run_triage(
    cfg:Dict[str,Any],
    profile:str,
    session_id:str,
    proposals:List[FolderProposal],
    source_root:Path,
    dry_run:bool,
    auto_threshold:Optional[float]=None,
)->List[Dict[str,Any]]:
    """
    v7.1 triage workflow:
      1. Split proposals into AUTO / HOLD
      2. Show triage dashboard
      3. Bulk-approve AUTO tier (requires YES confirmation)
      4. Hand HOLD tier to interactive_review()
    Returns all applied move entries.
    """
    rr=cfg.get("review_rules",{})
    thresh=auto_threshold or float(rr.get("auto_approve_threshold",rr.get("min_confidence_for_clean",0.90)))
    thresh=max(thresh,float(rr.get("min_confidence_for_clean",0.85)))  # never below clean floor

    triage=_triage_proposals(proposals,thresh)
    _show_triage_dashboard(triage,session_id,profile,thresh,dry_run)

    action=_prompt_triage_action(triage)
    applied:List[Dict[str,Any]]=[]

    if action=="quit":
        out(f"\n  {C.DIM}Quit. Nothing moved. Session {session_id[:12]} preserved.{C.RESET}")
        out(f"  Resume: raagdosa resume {session_id}")
        return []

    if action=="auto" and triage["auto"]:
        applied+=_bulk_approve_auto(triage,cfg,session_id,source_root,dry_run)
        # Track renames for auto-approved clean moves
        trc=cfg.get("track_rename",{})
        if trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"):
            for a in applied:
                if a.get("destination")=="clean" and not dry_run:
                    try:
                        rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),
                                                      a.get("decision",{}),
                                                      interactive=False,dry_run=False,session_id=session_id)
                    except Exception: pass

    # Interactive review for HOLD tier (or all folders if action=="review")
    review_queue=proposals if action=="review" else triage["hold"]
    if review_queue:
        print(f"\n{'═'*66}")
        tier_label="ALL" if action=="review" else f"HOLD ({len(review_queue)})"
        print(f"  {C.BOLD}Interactive Review — {tier_label}{C.RESET}  ·  Sorted by confidence (lowest first)")
        print(f"{'═'*66}")
        hold_applied=interactive_review(cfg,review_queue,session_id,dry_run=dry_run,source_root=source_root)
        applied+=hold_applied
        # Track renames for individually approved clean moves
        trc=cfg.get("track_rename",{})
        if trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"):
            for a in hold_applied:
                if a.get("destination")=="clean" and not dry_run:
                    try:
                        rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),
                                                      a.get("decision",{}),
                                                      interactive=False,dry_run=False,session_id=session_id)
                    except Exception: pass
    return applied

