"""
RaagDosa Interactive — folder-by-folder review UI.

Layer 7: imports from ui (L0), core (L0), files (L1), tags (L1),
         tagreader (L2), naming (L2), config (L2), session (L3),
         library (L3), tracks (L5), proposal (L5).

Provides:
  _conf_bar, _factor_bar       — confidence bar renderers
  _diff_old_highlight          — diff highlighting for old→new names
  _display_folder_card         — interactive review card
  _display_tracks              — track listing with rename preview
  _edit_track_title            — edit a single track's title tag
  _interactive_action_help     — key reference
  _prompt_review_note          — review reason prompt
  interactive_review           — v7.0 interactive folder review loop
  _triage_proposals            — split proposals into tiers
  _show_tier_detail            — paginated tier listing
  _show_triage_dashboard       — 3-tier triage summary
  _prompt_triage_action        — triage action prompt
  _bulk_approve_auto           — bulk approve HIGH tier
"""
from __future__ import annotations

import difflib
import re
import shutil
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from raagdosa import APP_VERSION
from raagdosa.core import (
    FolderProposal, now_iso, should_stop,
)
from raagdosa.files import (
    ensure_dir, safe_move_folder, append_jsonl, get_folder_size,
    cleanup_empty_parents, list_audio_files,
)
from raagdosa.tags import MutagenFile, normalize_unicode
from raagdosa.tagreader import read_audio_tags, _get_tag_cache
from raagdosa.ui import (
    C, VERBOSE, _IS_TTY, _HAS_READCHAR,
    out, err, warn, ok_msg,
    status_tag, conf_color, human_size, read_key, open_in_finder,
)
import raagdosa.ui as ui_module
from raagdosa.session import (
    ensure_roots, manifest_add,
    derive_clean_albums_root, derive_review_albums_root,
)
from raagdosa.library import resolve_library_path
from raagdosa.review import collision_resolve
from raagdosa.tracks import classify_folder_for_tracks, build_track_filename
from raagdosa.proposal import (
    build_folder_proposal, _route_proposal, _write_review_sidecar,
    _build_review_summary, _apply_format_suffix, _FACTOR_DESCRIPTIONS,
    REVIEW_REASON_PRESETS, folder_is_multidisc,
)


def _conf_bar(c:float,width:int=20)->str:
    """Render a confidence bar: ████████░░░░ 0.75"""
    filled=int(c*width); empty=width-filled
    if c>=0.80: color=C.GREEN
    elif c>=0.50: color=C.YELLOW
    else: color=C.RED
    return f"{color}{'█'*filled}{'░'*empty}{C.RESET}  {c:.2f}"

def _factor_bar(name:str,val:float,width:int=20)->str:
    """Render a single factor bar line."""
    desc=_FACTOR_DESCRIPTIONS.get(name,name)
    filled=int(val*width); empty=width-filled
    if val>=0.80: color=C.GREEN
    elif val>=0.50: color=C.YELLOW
    else: color=C.RED
    return f"    {desc:28s} {color}{'█'*filled}{'░'*empty}{C.RESET}  {val:.2f}"

def _display_folder_card(idx:int,total:int,p:FolderProposal)->None:
    """Print the interactive review card for one folder."""
    d=p.decision; factors=d.get("confidence_factors",{})
    src_name=p.folder_name; dst_name=p.proposed_folder_name
    dest_upper=p.destination.upper()
    dest_color=C.GREEN if p.destination=="clean" else (C.YELLOW if p.destination=="review" else C.RED)

    # ── Header ────────────────────────────────────────────────
    print(f"\n{'━'*70}  [{idx}/{total}]")

    # ── Confidence + route (top line, instant gut read) ───────
    print(f"  {_conf_bar(p.confidence)}   {dest_color}{dest_upper}{C.RESET}")

    # ── Warning flags (only if problems exist) ────────────────
    reasons=d.get("route_reasons",[])
    if reasons:
        flags=" · ".join(reasons)
        flag_color=C.RED if p.confidence<0.60 else C.YELLOW
        print(f"  {flag_color}▸ {flags}{C.RESET}")

    # ── FROM / TO (the decision point) ────────────────────────
    print()
    print(f"  {C.DIM}FROM  {src_name}{C.RESET}")
    if src_name!=dst_name:
        print(f"    {C.BOLD}→ {dst_name}{C.RESET}")
    else:
        print(f"    {C.DIM}→ (unchanged){C.RESET}")

    # ── Metadata (compact, glanceable) ────────────────────────
    artist=d.get("albumartist_display","--")
    album=d.get("dominant_album_display","")
    year=d.get("year","")
    va_flag=f"  {C.CYAN}VA{C.RESET}" if d.get("is_va") else ""
    mix_flag=f"  {C.MAGENTA}MIX{C.RESET}" if d.get("is_mix") else ""
    ep_flag=f"  {C.CYAN}EP{C.RESET}" if d.get("is_ep") else ""
    ext_str=", ".join(f"{k.upper()} {v}" for k,v in (p.stats.extensions or {}).items()) or "--"
    year_str=f"  {C.DIM}({year}){C.RESET}" if year else ""
    tagged=d.get("tagged_count",p.stats.tracks_total); total_t=p.stats.tracks_total
    tag_ratio=f"{tagged}/{total_t}" if tagged!=total_t else str(total_t)
    genre=d.get("genre","")
    genre_str=f"  {C.DIM}{genre}{C.RESET}" if genre else ""
    album_str=f"  {C.DIM}-  {album}{C.RESET}" if album else ""
    # Per-folder size
    folder_bytes=get_folder_size(Path(p.folder_path)) if Path(p.folder_path).exists() else 0
    size_str=f"  ·  {human_size(folder_bytes)}" if folder_bytes else ""
    print(f"\n  {C.BOLD}{artist}{C.RESET}{album_str}{ep_flag}{va_flag}{mix_flag}{year_str}")
    print(f"  {C.DIM}{tag_ratio} tracks  ·  {ext_str}{size_str}{genre_str}{C.RESET}")

    # ── Album vs VA barometer ─────────────────────────────────
    dom_art_share=float(d.get("dominant_artist_share",1.0))
    is_va=bool(d.get("is_va",False))
    _BAR=16
    def _va_bar(share:float)->str:
        # share = dominant artist share (high = album, low = VA)
        album_fill=int(share*_BAR); va_fill=_BAR-album_fill
        album_col=C.GREEN if share>=0.75 else (C.YELLOW if share>=0.40 else C.RED)
        va_col=C.RED if share>=0.75 else (C.YELLOW if share>=0.40 else C.GREEN)
        album_bar=f"{album_col}{'█'*album_fill}{C.RESET}{C.DIM}{'░'*va_fill}{C.RESET}"
        va_bar=f"{C.DIM}{'░'*album_fill}{C.RESET}{va_col}{'█'*va_fill}{C.RESET}"
        return (f"  {C.BOLD}ALBUM{C.RESET} {album_bar} {share:.0%}  ·  "
                f"{C.BOLD}VA{C.RESET} {va_bar} {1-share:.0%}  "
                f"{C.DIM}(current: {'VA' if is_va else 'Album'}  ·  v to toggle){C.RESET}")
    print(f"\n{_va_bar(dom_art_share)}")

    # ── Factor bars (compact) ─────────────────────────────────
    display_factors=["tag_coverage","dominance","title_quality","filename_consistency",
                     "completeness","aa_consistency","folder_alignment"]
    shown=[f for f in display_factors if f in factors and isinstance(factors[f],float)]
    if shown:
        print()
        for f in shown:
            print(_factor_bar(f,factors[f]))

    # ── Review summary ────────────────────────────────────────
    summary=d.get("review_summary","")
    if summary:
        print(f"\n  {C.DIM}{summary}{C.RESET}")

    print(f"{'━'*70}")

def _diff_old_highlight(old:str, new:str)->str:
    """Return old string with removed/changed portions highlighted in red, rest dimmed.
    Used to make the 'was:' line scannable at a glance."""
    sm=difflib.SequenceMatcher(None, old, new, autojunk=False)
    out=C.DIM
    for op,i1,i2,_j1,_j2 in sm.get_opcodes():
        chunk=old[i1:i2]
        if op=="equal":
            out+=chunk
        elif op in ("replace","delete"):
            # Un-dim and color the removed portion so it pops against the dim baseline
            out+=C.RESET+C.RED+chunk+C.RESET+C.DIM
    out+=C.RESET
    return out

def _display_tracks(p:FolderProposal,cfg:Optional[Dict[str,Any]]=None)->Optional[str]:
    """Show track listing with rename preview for current folder.
    For folders with >30 tracks, shows pages of 20 with n/p navigation.
    Returns 'approve' if user presses b to batch-approve from within the view."""
    folder=Path(p.folder_path)
    if not folder.exists():
        print(f"  {C.RED}Folder not found on disk.{C.RESET}")
        return
    exts={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}
    files=sorted([f for f in folder.iterdir() if f.suffix.lower() in exts],key=lambda f:f.name)
    if not files:
        print(f"  {C.DIM}No audio files found.{C.RESET}")
        return

    # If cfg is available, compute rename previews
    rename_map:Dict[str,str]={}
    classification=""
    if cfg is not None:
        try:
            classification=classify_folder_for_tracks(p.decision,cfg)
            disc_multi=folder_is_multidisc(files,cfg)
            for f in files:
                tags=read_audio_tags(f,cfg)
                new_name,conf,reason,meta=build_track_filename(classification,tags,f,cfg,p.decision,disc_multi,total_tracks=len(files))
                if new_name and normalize_unicode(new_name)!=normalize_unicode(f.name):
                    rename_map[f.name]=new_name
        except Exception:
            pass  # fall back to simple listing

    type_label=f"  {C.DIM}[{classification}]{C.RESET}" if classification else ""
    changes=len(rename_map)
    unchanged=len(files)-changes

    def _print_track(i:int, f:Path)->None:
        new_name=rename_map.get(f.name)
        if new_name:
            # New name is the main event — left-aligned, normal weight
            print(f"   {i+1:>2}  {new_name}")
            # Old name below: dim with removed portions highlighted red
            old_hl=_diff_old_highlight(f.name, new_name)
            print(f"       {C.DIM}was:{C.RESET} {old_hl}")
        else:
            print(f"   {C.DIM}{i+1:>2}  {f.name}{C.RESET}")

    BATCH=20
    if len(files)>30:
        # Large folder — paginate in batches of 20; b to batch-approve
        total_pages=(len(files)+BATCH-1)//BATCH
        page_idx=0
        while True:
            start=page_idx*BATCH; end=min(start+BATCH,len(files))
            print(f"\n  TRACKS ({len(files)} files){type_label}  ·  {C.GREEN}{changes} rename(s){C.RESET}  {C.DIM}{unchanged} unchanged  [Batch {page_idx+1}/{total_pages}]{C.RESET}")
            print(f"  {'─'*60}")
            for i,f in enumerate(files[start:end],start=start):
                _print_track(i,f)
            print(f"  {'─'*60}")
            print(f"  n:next-batch  p:prev-batch  {C.GREEN}b:approve-folder{C.RESET}  Enter:back")
            try:
                resp=input(f"  > ").strip().lower()
            except (KeyboardInterrupt,EOFError):
                break
            if resp=="n":
                page_idx=min(page_idx+1,total_pages-1)
            elif resp=="p":
                page_idx=max(page_idx-1,0)
            elif resp=="b":
                return "approve"
            else:
                break
    else:
        print(f"\n  TRACKS ({len(files)} files){type_label}  ·  {C.GREEN}{changes} rename(s){C.RESET}  {C.DIM}{unchanged} unchanged{C.RESET}")
        print(f"  {'─'*60}")
        page=30
        shown=min(len(files),page)
        for i,f in enumerate(files[:shown]):
            _print_track(i,f)
        if len(files)>shown:
            remaining=len(files)-shown
            try:
                resp=input(f"   {C.DIM}... and {remaining} more  [Enter to see all, any key to skip]{C.RESET} ").strip()
                if not resp:
                    for i,f in enumerate(files[shown:],start=shown):
                        _print_track(i,f)
            except (KeyboardInterrupt,EOFError):
                print()
        print(f"  {'─'*60}")
    return None

def _edit_track_title(f:Path,cfg:Dict[str,Any])->Optional[str]:
    """Edit the title tag of a single audio file interactively.
    Returns the new title if saved, None if cancelled."""
    if MutagenFile is None:
        print(f"  {C.RED}mutagen not available — cannot edit tags.{C.RESET}")
        return None
    tags=read_audio_tags(f,cfg)
    current=tags.get("title") or ""
    print(f"\n  {'─'*50}")
    print(f"  File:  {f.name}")
    print(f"  Title: {C.BOLD}{current or '(none)'}{C.RESET}")
    try:
        new_title=input(f"  New title (Enter to cancel): ").strip()
    except (KeyboardInterrupt,EOFError):
        print(); return None
    if not new_title:
        print(f"  {C.DIM}Cancelled.{C.RESET}"); return None
    try:
        mf=MutagenFile(str(f),easy=True)
        if mf is None:
            print(f"  {C.RED}Cannot open file for writing.{C.RESET}"); return None
        if mf.tags is None:
            mf.add_tags()
        mf.tags["title"]=[new_title]
        mf.save()
        # Update tag cache so rename preview reflects the change immediately
        cache=_get_tag_cache(cfg)
        if cache is not None:
            updated=dict(tags); updated["title"]=new_title
            cache.set(f,updated)
        print(f"  {C.GREEN}✓ Title saved: {new_title}{C.RESET}")
        return new_title
    except Exception as ex:
        print(f"  {C.RED}Write failed: {ex}{C.RESET}"); return None

def _interactive_action_help()->None:
    """Print the action key reference."""
    print(f"\n  {'─'*50}")
    print(f"  INTERACTIVE REVIEW — KEYS")
    print(f"  {'─'*50}")
    print(f"  {C.BOLD}── Decide ──{C.RESET}")
    print(f"  z  or  Enter    Move to Clean/")
    print(f"  x               Reject → Review/ (with reason)")
    print(f"  c               Skip (leave in source)")
    print(f"  f               Flag (review again at end)")
    print(f"  {C.BOLD}── Fix ──{C.RESET}")
    print(f"  e               Edit album title  (renames the folder)")
    print(f"  e<N>            Edit track N title tag  (e.g. e3, e12)  — separate from album")
    print(f"  a               Set artist name")
    print(f"  v               Toggle VA status")
    print(f"  {C.BOLD}── Inspect ──{C.RESET}")
    print(f"  o               Open folder in Finder")
    print(f"  R               Rescan folder (after manual changes in Finder)")
    print(f"  space / b       Track rename preview  (b within view to approve folder)")
    print(f"  u               Undo a move made this session")
    print(f"  ?               This help")
    print(f"  {'─'*50}")
    print(f"  q               Quit")
    print(f"  {'─'*50}")

def _prompt_review_note()->str:
    """Prompt for a review reason. Shows presets, allows custom text or blank."""
    print(f"\n  {C.DIM}Reason?{C.RESET}")
    for i,r in enumerate(REVIEW_REASON_PRESETS,1):
        print(f"    {i}. {r}")
    print(f"    {len(REVIEW_REASON_PRESETS)+1}. Custom note")
    print(f"    {C.DIM}Enter = no reason{C.RESET}")
    while True:
        choice=input(f"  Reason (1-{len(REVIEW_REASON_PRESETS)+1}, text, or Enter): ").strip()
        if not choice: return ""  # allow blank — no reason
        try:
            idx=int(choice)
            if 1<=idx<=len(REVIEW_REASON_PRESETS):
                return REVIEW_REASON_PRESETS[idx-1]
            elif idx==len(REVIEW_REASON_PRESETS)+1:
                note=input("  Custom note: ").strip()
                if note: return note
                continue
        except ValueError:
            return choice  # free text

def interactive_review(cfg:Dict[str,Any],proposals:List[FolderProposal],
                       session_id:str,dry_run:bool=False,
                       threshold:Optional[float]=None,
                       source_root:Optional[Path]=None)->List[Dict[str,Any]]:
    """
    v7.0: Interactive folder-by-folder review mode.
    Sort by confidence ascending (worst first), display card, prompt for action.
    Returns list of applied move entries (same format as apply_folder_moves).
    """
    mc=cfg.get("move",{})
    policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})")
    use_cs=bool(mc.get("use_checksum",False))
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    hist_path=Path(cfg["logging"]["history_log"]); skip_path=Path(cfg["logging"]["skipped_log"])

    # Filter by threshold if specified
    if threshold is not None:
        proposals=[p for p in proposals if p.confidence<threshold]
        if not proposals:
            out(f"\n  {C.GREEN}All folders above threshold {threshold:.2f}. Nothing to review.{C.RESET}")
            return []

    # Sort by confidence ascending — hardest decisions first
    proposals.sort(key=lambda p:p.confidence)

    total=len(proposals)
    applied:List[Dict[str,Any]]=[]
    skipped=0; sent_to_review=0; overrides=0

    # Session header
    clean_n=sum(1 for p in proposals if p.destination=="clean")
    rev_n=sum(1 for p in proposals if p.destination=="review")
    # Pre-compute per-folder sizes for progress tracking
    _review_sizes:Dict[str,int]={}
    for p in proposals:
        _review_sizes[p.folder_path]=get_folder_size(Path(p.folder_path)) if Path(p.folder_path).exists() else 0
    _review_total_bytes=sum(_review_sizes.values())
    _review_bytes_done=0
    readchar_note=f"  {C.DIM}(single-keypress mode){C.RESET}" if _HAS_READCHAR and _IS_TTY else ""
    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}RAAGDOSA v{APP_VERSION}  ·  Interactive Review  ·  Session {session_id[:12]}{C.RESET}")
    print(f"{'─'*66}")
    print(f"  Folders: {total} ({human_size(_review_total_bytes)})  ·  {C.GREEN}Clean: {clean_n}{C.RESET}  ·  {C.YELLOW}Review: {rev_n}{C.RESET}")
    print(f"  Sorted by confidence (lowest first)")
    if dry_run: print(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")
    print(f"  Press ? for help at any prompt.{readchar_note}")
    print(f"{'═'*66}")

    # Session notes file for learning
    session_notes_path=Path(cfg["logging"]["session_dir"])/session_id/"review_notes.jsonl"

    for idx,p in enumerate(proposals,1):
        if should_stop():
            out(f"\n{C.YELLOW}Stopped. {len(applied)}/{total} applied.{C.RESET}")
            break

        src=Path(p.folder_path); dst=Path(p.target_path)
        if not src.exists():
            _review_bytes_done+=_review_sizes.get(p.folder_path,0); continue

        _display_folder_card(idx,total,p)
        _display_tracks(p,cfg)

        # Build track file list for e<N> edit shortcut
        _tf_exts={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}
        track_files=sorted([f for f in src.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if src.exists() else []

        # Progress line — size-weighted percentage
        _cur_bytes=_review_sizes.get(p.folder_path,0)
        size_pct=int(_review_bytes_done/_review_total_bytes*100) if _review_total_bytes else 0
        dest_color=C.GREEN if p.destination=="clean" else C.YELLOW
        print(f"  {C.DIM}[{idx}/{total}] {size_pct}% by size{C.RESET}  ·  {p.proposed_folder_name}  ({conf_color(p.confidence)} → {dest_color}{p.destination.title()}{C.RESET})")

        # Action loop — stays on this folder until an action advances
        while True:
            if _HAS_READCHAR and _IS_TTY:
                action_labels=f"  z:move  x:reject  c:skip  b:tracks  e:edit  v:va  o:finder  R:rescan  q:quit  ?:help"
            else:
                action_labels=f"  z:move  x:reject  c:skip  space/b:tracks  e:album-title  e<N>:track-title  a:artist  v:va  o:finder  R:rescan  q:quit  ?:help"
            print(action_labels)
            choice=read_key("  > ")

            if choice=="":
                # Empty enter — require explicit choice
                print(f"  {C.YELLOW}Press z to move, x to reject, c to skip, ? for help{C.RESET}")
                continue
            if choice in ("z","y","yes"):
                # Approve — proceed with the proposed move
                dst2=collision_resolve(dst,policy,sfmt)
                if dst2 is None:
                    warn(f"Collision skip: {dst.name}")
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)})
                    break
                if dry_run:
                    out(f"  {C.DIM}[dry-run]{C.RESET} {src.name}  →  {dst2.name}  {status_tag(p.destination)}")
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)})
                    break
                ensure_dir(dst2.parent)
                try:
                    move_method,move_elapsed=safe_move_folder(src,dst2,use_checksum=use_cs)
                except RuntimeError as e:
                    err(f"  Move failed: {e}")
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)})
                    break
                action_id=uuid.uuid4().hex[:10]
                entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                       "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
                       "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
                       "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
                       "move_method":move_method,"interactive_action":"approved"}
                append_jsonl(hist_path,entry); applied.append(entry)
                if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
                elif p.destination=="review": _write_review_sidecar(dst2,p,session_id)
                out(f"  {C.GREEN}✓ Approved{C.RESET} → {dst2.name}")
                if source_root:
                    try: cleanup_empty_parents(src,source_root)
                    except Exception: pass
                break

            elif choice in ("c","k","s"):
                # Skip — leave in source
                reason_text=input(f"  Reason (Enter to skip): ").strip()
                skip_entry={"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                            "reason":"user_skipped","src":str(src),"interactive_action":"skipped"}
                if reason_text:
                    skip_entry["user_note"]=reason_text
                    ensure_dir(session_notes_path.parent)
                    append_jsonl(session_notes_path,{"folder":src.name,"action":"skipped","note":reason_text,"timestamp":now_iso(),"confidence":p.confidence})
                append_jsonl(skip_path,skip_entry)
                skipped+=1
                out(f"  {C.DIM}→ Skipped{C.RESET}")
                break

            elif choice in ("x","d","r"):
                # Send to Review with required note
                note=_prompt_review_note()
                # Re-route to review
                profile_name=cfg.get("active_profile","incoming")
                profiles=cfg.get("profiles",{})
                profile_obj=profiles.get(profile_name,{})
                sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                review_albums=derive_review_albums_root(profile_obj,sr)
                review_dst=review_albums/p.proposed_folder_name
                review_dst2=collision_resolve(review_dst,policy,sfmt)
                if review_dst2 is None:
                    warn(f"Review collision skip: {review_dst.name}")
                    break
                p.destination="review"; p.decision["route_reasons"]=p.decision.get("route_reasons",[])+["user_review"]
                p.decision["user_review_note"]=note
                if dry_run:
                    out(f"  {C.DIM}[dry-run]{C.RESET} → Review: {note}")
                    break
                ensure_dir(review_dst2.parent)
                try:
                    move_method,move_elapsed=safe_move_folder(src,review_dst2,use_checksum=use_cs)
                except RuntimeError as e:
                    err(f"  Move failed: {e}"); break
                action_id=uuid.uuid4().hex[:10]
                entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                       "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
                       "target_path":str(review_dst2),"target_parent":str(review_dst2.parent),"target_folder_name":review_dst2.name,
                       "destination":"review","confidence":p.confidence,"decision":p.decision,
                       "move_method":move_method,"interactive_action":"sent_to_review","user_note":note}
                append_jsonl(hist_path,entry); applied.append(entry)
                _write_review_sidecar(review_dst2,p,session_id)
                # Log note for learning
                ensure_dir(session_notes_path.parent)
                append_jsonl(session_notes_path,{"folder":src.name,"action":"sent_to_review","note":note,"timestamp":now_iso(),"confidence":p.confidence})
                sent_to_review+=1
                out(f"  {C.YELLOW}→ Review:{C.RESET} {note}")
                if source_root:
                    try: cleanup_empty_parents(src,source_root)
                    except Exception: pass
                break

            elif choice=="a":
                # Set artist override
                current=p.decision.get("albumartist_display","--")
                print(f"  Current artist: {current}")
                new_artist=input(f"  New artist name: ").strip()
                if not new_artist: continue
                # Update proposal
                p.decision["albumartist_display"]=new_artist
                p.decision["is_va"]=False
                p.decision["override_type"]="set_artist"
                p.decision["override_original"]=current
                # Re-derive destination path
                profile_name=cfg.get("active_profile","incoming")
                profiles=cfg.get("profiles",{})
                profile_obj=profiles.get(profile_name,{})
                sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                clean_albums=derive_clean_albums_root(profile_obj,sr)
                new_target=resolve_library_path(clean_albums,new_artist,p.decision.get("dominant_album_display",""),
                                               p.decision.get("year"),p.decision.get("is_flac_only",False),
                                               False,False,p.decision.get("is_mix",False),cfg,profile_obj,
                                               genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                               key=p.decision.get("key"),label=p.decision.get("label"))
                p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                p.target_path=str(new_target); p.destination="clean"
                overrides+=1
                print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                print(f"  Artist: {C.BOLD}{new_artist}{C.RESET}  (was: {current})   VA: No")
                print(f"    TO:   {new_target.name}")
                print(f"  ROUTE: {C.GREEN}CLEAN{C.RESET}")
                # Stay in action loop — user can approve or adjust further

            elif choice=="v":
                # Toggle VA status
                was_va=p.decision.get("is_va",False)
                if was_va:
                    # VA → single artist: need artist name
                    new_artist=input(f"  Artist name: ").strip()
                    if not new_artist: continue
                    p.decision["is_va"]=False
                    p.decision["albumartist_display"]=new_artist
                    p.decision["override_type"]="unmark_va"
                    overrides+=1
                    # Re-derive path
                    profile_name=cfg.get("active_profile","incoming")
                    profiles=cfg.get("profiles",{})
                    profile_obj=profiles.get(profile_name,{})
                    sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                    clean_albums=derive_clean_albums_root(profile_obj,sr)
                    new_target=resolve_library_path(clean_albums,new_artist,p.decision.get("dominant_album_display",""),
                                                   p.decision.get("year"),p.decision.get("is_flac_only",False),
                                                   False,False,p.decision.get("is_mix",False),cfg,profile_obj,
                                                   genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                                   key=p.decision.get("key"),label=p.decision.get("label"))
                    p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                    p.target_path=str(new_target); p.destination="clean"
                    print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Artist: {C.BOLD}{new_artist}{C.RESET}   VA→Album  (tracks will rename as: NN - Title)")
                    print(f"    TO:   {p.proposed_folder_name}")
                    print(f"  {C.DIM}Press space/b to preview updated track renames.{C.RESET}")
                else:
                    # Single artist → VA
                    old_artist=p.decision.get("albumartist_display","--")
                    p.decision["is_va"]=True
                    p.decision["albumartist_display"]="Various Artists"
                    p.decision["override_type"]="mark_va"
                    overrides+=1
                    profile_name=cfg.get("active_profile","incoming")
                    profiles=cfg.get("profiles",{})
                    profile_obj=profiles.get(profile_name,{})
                    sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                    clean_albums=derive_clean_albums_root(profile_obj,sr)
                    new_target=resolve_library_path(clean_albums,"Various Artists",p.decision.get("dominant_album_display",""),
                                                   p.decision.get("year"),p.decision.get("is_flac_only",False),
                                                   True,False,p.decision.get("is_mix",False),cfg,profile_obj,
                                                   genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                                   key=p.decision.get("key"),label=p.decision.get("label"))
                    p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                    p.target_path=str(new_target)
                    print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                    print(f"  Album→VA  (was: {old_artist})  tracks will rename as: NN - Artist - Title")
                    print(f"    TO:   {new_target.name}")
                    print(f"  {C.DIM}Press space/b to preview updated track renames.{C.RESET}")

            elif choice=="e":
                # Edit the album/folder title (e = album edit, e<N> = track title edit)
                current_album=p.decision.get("dominant_album_display","")
                print(f"  {C.DIM}Current album:{C.RESET} {current_album}")
                new_album=input(f"  New album title (Enter to cancel): ").strip()
                if not new_album: continue
                p.decision["dominant_album_display"]=new_album
                p.decision["override_type"]=p.decision.get("override_type","")+"edit_title"
                profile_name=cfg.get("active_profile","incoming")
                profiles=cfg.get("profiles",{}); profile_obj=profiles.get(profile_name,{})
                sr=source_root or Path(profile_obj.get("source_root","")).expanduser()
                clean_albums=derive_clean_albums_root(profile_obj,sr)
                new_target=resolve_library_path(clean_albums,p.decision.get("albumartist_display",""),new_album,
                                               p.decision.get("year"),p.decision.get("is_flac_only",False),
                                               p.decision.get("is_va",False),False,p.decision.get("is_mix",False),cfg,profile_obj,
                                               genre=p.decision.get("genre"),bpm=p.decision.get("bpm"),
                                               key=p.decision.get("key"),label=p.decision.get("label"))
                p.proposed_folder_name=_apply_format_suffix(new_target.name,cfg,p.stats.extensions if p.stats else None)
                p.target_path=str(new_target); p.destination="clean"
                overrides+=1
                print(f"\n  {C.CYAN}─ UPDATED ─{C.RESET}")
                print(f"  Album: {C.BOLD}{new_album}{C.RESET}  (was: {current_album})")
                print(f"    TO:   {new_target.name}")
                print(f"  ROUTE: {C.GREEN}CLEAN{C.RESET}")

            elif choice.startswith("e") and len(choice)>1 and choice[1:].strip().isdigit():
                # e<N> — edit individual track title tag (separate from album title)
                track_num=int(choice[1:].strip())
                if 1<=track_num<=len(track_files):
                    _edit_track_title(track_files[track_num-1],cfg)
                    print(f"  {C.DIM}Press space/b to refresh track view.{C.RESET}")
                else:
                    print(f"  {C.DIM}Track {track_num} not found (folder has {len(track_files)} tracks).{C.RESET}")

            elif choice in (" ","b"):
                trk_result=_display_tracks(p,cfg)
                if trk_result=="approve":
                    # User approved from batch track view — execute move inline
                    dst2=collision_resolve(dst,policy,sfmt)
                    if dst2 is None:
                        warn(f"Collision skip: {dst.name}")
                        append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)})
                        break
                    if dry_run:
                        out(f"  {C.DIM}[dry-run]{C.RESET} {src.name}  →  {dst2.name}  {status_tag(p.destination)}")
                        append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)})
                        break
                    ensure_dir(dst2.parent)
                    try:
                        move_method,move_elapsed=safe_move_folder(src,dst2,use_checksum=use_cs)
                    except RuntimeError as e:
                        err(f"  Move failed: {e}")
                        append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)})
                        break
                    action_id=uuid.uuid4().hex[:10]
                    entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
                           "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
                           "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
                           "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
                           "move_method":move_method,"interactive_action":"approved"}
                    append_jsonl(hist_path,entry); applied.append(entry)
                    if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
                    elif p.destination=="review": _write_review_sidecar(dst2,p,session_id)
                    out(f"  {C.GREEN}✓ Batch-approved{C.RESET} → {dst2.name}")
                    if source_root:
                        try: cleanup_empty_parents(src,source_root)
                        except Exception: pass
                    break

            elif choice=="o":
                open_in_finder(src)

            elif choice in ("R",) or (not _HAS_READCHAR and choice in ("rescan",)):
                # Rescan folder after manual changes (e.g. moved tracks in Finder)
                if not src.exists():
                    print(f"  {C.RED}Folder no longer exists.{C.RESET}"); break
                print(f"  {C.CYAN}Rescanning...{C.RESET}")
                new_audio=list_audio_files(src,exts,False)
                min_trk=int(cfg.get("scan",{}).get("min_tracks",3))
                if len(new_audio)<min_trk:
                    print(f"  {C.YELLOW}Only {len(new_audio)} audio file(s) remain — below minimum ({min_trk}).{C.RESET}")
                    print(f"  {C.DIM}Skip this folder? [y/N]{C.RESET}")
                    skip_confirm=input(f"  > ").strip().lower()
                    if skip_confirm in ("y","yes"):
                        skipped+=1; break
                    continue
                profile_name_r=cfg.get("active_profile","incoming")
                profile_obj_r=cfg.get("profiles",{}).get(profile_name_r,{})
                sr_r=source_root or Path(profile_obj_r.get("source_root","")).expanduser()
                new_p=build_folder_proposal(src,new_audio,sr_r,profile_obj_r,cfg)
                if new_p is None:
                    print(f"  {C.YELLOW}Folder no longer qualifies after rescan.{C.RESET}"); break
                # Determine routing based on confidence
                rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
                if new_p.confidence<min_conf:
                    new_p.destination="review"
                    review_albums_r=derive_review_albums_root(profile_obj_r,sr_r)
                    new_p.target_path=str(review_albums_r/new_p.proposed_folder_name)
                # Replace the current proposal in-place
                p.stats=new_p.stats; p.confidence=new_p.confidence; p.decision=new_p.decision
                p.proposed_folder_name=new_p.proposed_folder_name; p.target_path=new_p.target_path
                p.destination=new_p.destination
                dst=Path(p.target_path)
                # Rebuild track file list
                track_files=sorted([f for f in src.iterdir() if f.suffix.lower() in _tf_exts],key=lambda f:f.name) if src.exists() else []
                # Re-display
                _display_folder_card(idx,total,p)
                _display_tracks(p,cfg)
                print(f"  {C.CYAN}─ RESCANNED ─{C.RESET}  {len(new_audio)} tracks  ·  conf {conf_color(p.confidence)}  → {p.destination.title()}")

            elif choice=="q":
                print(f"\n{'═'*66}")
                print(f"  Stop processing?")
                print(f"  Completed: {len(applied)} approved · {skipped} skipped · {sent_to_review} to review")
                print(f"  Remaining: {total-idx} folders will stay in source.")
                print(f"  Moves already made will NOT be undone.")
                print(f"{'─'*66}")
                confirm=input(f"  Confirm stop? [y/N]: ").strip().lower()
                if confirm in ("y","yes"):
                    out(f"\n  Session ended. {len(applied)} moved · {skipped} skipped · {total-idx} remaining.")
                    out(f"  Undo: raagdosa undo --session {session_id}")
                    return applied
                # Otherwise continue with this folder

            elif choice=="?":
                _interactive_action_help()

            else:
                print(f"  {C.DIM}Unknown action. Press ? for help.{C.RESET}")

        # Track bytes for size-weighted progress
        _review_bytes_done+=_cur_bytes

    # Session summary
    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}SESSION COMPLETE{C.RESET}")
    print(f"{'─'*66}")
    approved_clean=sum(1 for a in applied if a.get("destination")=="clean")
    approved_review=sum(1 for a in applied if a.get("destination")=="review")
    print(f"  Approved to Clean:   {C.GREEN}{approved_clean}{C.RESET} folders")
    print(f"  Sent to Review:      {C.YELLOW}{approved_review}{C.RESET} folders")
    print(f"  Skipped:             {skipped} folders")
    if overrides: print(f"  Overrides applied:   {overrides}")
    print(f"{'─'*66}")
    print(f"  Undo:  raagdosa undo --session {session_id}")
    print(f"  Log:   raagdosa history --session {session_id}")
    print(f"{'═'*66}\n")

    return applied

def _triage_proposals(proposals:List[FolderProposal],auto_threshold:float)->Dict[str,List[FolderProposal]]:
    """
    Split proposals into HIGH / MID / PROB tiers.

    HIGH:  conf >= auto_threshold AND destination == 'clean' AND no review-forcing flags
    PROB:  destination == 'review' OR has FORCE_HOLD flags
    MID:   everything else (dest=clean, conf < auto_threshold, no force-hold)

    Keys 'auto' and 'hold' are kept as aliases for backward compat.
    """
    FORCE_HOLD={"duplicate_in_run","already_in_clean","heuristic_fallback","unreadable_ratio_high"}
    high:List[FolderProposal]=[]; mid:List[FolderProposal]=[]; prob:List[FolderProposal]=[]
    for p in proposals:
        reasons=set(p.decision.get("route_reasons",[]))
        is_force_hold=bool(reasons & FORCE_HOLD)
        if p.confidence>=auto_threshold and p.destination=="clean" and not is_force_hold:
            high.append(p)
        elif p.destination=="review" or is_force_hold:
            prob.append(p)
        else:
            mid.append(p)
    high.sort(key=lambda p:p.confidence,reverse=True)
    mid.sort(key=lambda p:p.confidence,reverse=True)
    prob.sort(key=lambda p:p.confidence)  # worst first
    return {"high":high,"mid":mid,"prob":prob,"auto":high,"hold":mid+prob}

def _show_tier_detail(tier_name:str,proposals:List[FolderProposal])->None:
    """Show all proposals in a tier, paginated at 20."""
    if not proposals:
        print(f"\n  {C.DIM}No folders in {tier_name} tier.{C.RESET}"); return
    PAGE=20; total=len(proposals); offset=0
    color={"HIGH":C.GREEN,"MID":C.YELLOW,"PROB":C.RED}.get(tier_name,C.DIM)
    while True:
        chunk=proposals[offset:offset+PAGE]
        print(f"\n  {color}── {tier_name} tier — {total} folders ──{C.RESET}  (showing {offset+1}–{offset+len(chunk)})")
        for p in chunk:
            reasons=p.decision.get("route_reasons",[])
            reason_str=("  ~"+",".join(reasons[:2])) if reasons else ""
            dest=f"[{p.destination}]"
            name=p.proposed_folder_name[:54]
            print(f"  {color}{p.confidence:.2f}{C.RESET}  {name}  {C.DIM}{dest}{reason_str}{C.RESET}")
        remaining=total-(offset+len(chunk))
        if remaining<=0: print(f"\n  {C.DIM}(end of list){C.RESET}"); break
        try:
            key=input(f"  {C.DIM}n=next {remaining} · Enter=done > {C.RESET}").strip().lower()
        except (EOFError,KeyboardInterrupt): break
        if key=="n": offset+=PAGE
        else: break

def _show_triage_dashboard(triage:Dict[str,List[FolderProposal]],session_id:str,profile:str,auto_threshold:float,dry_run:bool)->None:
    """Print the 3-tier triage summary dashboard."""
    high=triage["high"]; mid=triage["mid"]; prob=triage["prob"]
    total=len(high)+len(mid)+len(prob)

    BAR=32
    def _bar(n:int,tot:int)->str:
        if not tot: return "░"*BAR
        filled=int(n/tot*BAR)
        return "█"*filled+"░"*(BAR-filled)
    def _pct(n:int)->int: return int(n/total*100) if total else 0

    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}RAAGDOSA v{APP_VERSION}  ·  Triage  ·  Session {session_id[:12]}{C.RESET}")
    print(f"{'─'*66}")
    print(f"  Profile: {profile}   Scanned: {total} folders   Auto-approve ≥ {auto_threshold:.2f}")
    if dry_run: print(f"  {C.YELLOW}DRY RUN — nothing will be moved{C.RESET}")
    print(f"{'─'*66}")
    print(f"  {C.GREEN}HIGH{C.RESET}   {len(high):>3}  {_bar(len(high),total)}  {_pct(len(high)):>3}%  conf ≥ {auto_threshold:.2f} → Clean   {C.DIM}[h]{C.RESET}")
    print(f"  {C.YELLOW}MID{C.RESET}    {len(mid):>3}  {_bar(len(mid),total)}  {_pct(len(mid)):>3}%  conf < {auto_threshold:.2f} → Review  {C.DIM}[m]{C.RESET}")
    print(f"  {C.RED}PROB{C.RESET}   {len(prob):>3}  {_bar(len(prob),total)}  {_pct(len(prob)):>3}%  flagged → Review           {C.DIM}[p]{C.RESET}")
    print(f"{'─'*66}")

    # HIGH sample
    if high:
        n=min(6,len(high))
        print(f"\n  {C.GREEN}HIGH — top {n} of {len(high)}{C.RESET}  (sorted by confidence)")
        for p in high[:n]:
            print(f"  {C.GREEN}{p.confidence:.2f}{C.RESET}  {p.proposed_folder_name[:54]}")
        if len(high)>n: print(f"  {C.DIM}… {len(high)-n} more  (h to list all){C.RESET}")

    # MID sample
    if mid:
        n=min(4,len(mid))
        print(f"\n  {C.YELLOW}MID — top {n} of {len(mid)}{C.RESET}  (sorted by confidence)")
        for p in mid[:n]:
            print(f"  {C.YELLOW}{p.confidence:.2f}{C.RESET}  {p.proposed_folder_name[:54]}")
        if len(mid)>n: print(f"  {C.DIM}… {len(mid)-n} more  (m to list all){C.RESET}")

    # PROB sample
    if prob:
        n=min(4,len(prob))
        print(f"\n  {C.RED}PROB — first {n} of {len(prob)}{C.RESET}  (worst first)")
        for p in prob[:n]:
            reasons=p.decision.get("route_reasons",[])
            reason_str=("  ~"+",".join(reasons[:2])) if reasons else ""
            print(f"  {C.RED}{p.confidence:.2f}{C.RESET}  {p.folder_name[:46]}{C.DIM}{reason_str}{C.RESET}")
        if len(prob)>n: print(f"  {C.DIM}… {len(prob)-n} more  (p to list all){C.RESET}")

    print(f"\n{'─'*66}")
    if high:
        print(f"  a:bulk-approve({len(high)})   r:review-all-{total}   q:quit   h/m/p:list-tier   ?:help")
    else:
        print(f"  {C.DIM}(no auto-approvable folders){C.RESET}   r:review-all-{total}   q:quit   h/m/p:list-tier   ?:help")
    print(f"{'═'*66}")

def _prompt_triage_action(triage:Dict[str,List[FolderProposal]])->str:
    """Read the triage action from the user. Returns 'auto', 'review', or 'quit'."""
    high=triage["high"]; total=len(triage["high"])+len(triage["mid"])+len(triage["prob"])
    while True:
        try:
            raw=input(f"  > ").strip().lower()
        except (EOFError,KeyboardInterrupt):
            return "quit"
        if raw=="a" and high:
            return "auto"
        elif raw=="r":
            return "review"
        elif raw in ("q","quit"):
            return "quit"
        elif raw=="h":
            _show_tier_detail("HIGH",triage["high"])
        elif raw=="m":
            _show_tier_detail("MID",triage["mid"])
        elif raw=="p":
            _show_tier_detail("PROB",triage["prob"])
        elif raw=="?":
            print(f"  a  — bulk-approve the {len(high)} HIGH-confidence folders → Clean")
            print(f"  r  — review all {total} folders 1-by-1")
            print(f"  h  — list all HIGH tier folders (conf ≥ threshold, → Clean)")
            print(f"  m  — list all MID tier folders (conf < threshold, → Review)")
            print(f"  p  — list all PROB tier folders (flagged / low confidence)")
            print(f"  q  — quit without moving anything")
        else:
            print(f"  {C.DIM}Unknown. Press ? for help.{C.RESET}")

def _bulk_approve_auto(
    triage:Dict[str,List[FolderProposal]],
    cfg:Dict[str,Any],
    session_id:str,
    source_root:Optional[Path],
    dry_run:bool,
)->List[Dict[str,Any]]:
    """
    Show the bulk-approve confirmation gate and execute AUTO tier moves.
    Requires the user to type YES. Returns list of applied move entries.
    """
    auto=triage["auto"]
    mc=cfg.get("move",{}); policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})"); use_cs=bool(mc.get("use_checksum",False))
    hist_path=Path(cfg["logging"]["history_log"])

    print(f"\n{'═'*66}")
    print(f"  {C.BOLD}BULK APPROVE — confirm{C.RESET}")
    print(f"{'─'*66}")
    print(f"  {len(auto)} folders  →  Clean/")
    print(f"  Move mode: {'dry-run' if dry_run else 'enabled'}")
    print(f"  Undo:  raagdosa undo --session {session_id}")
    print(f"{'─'*66}")
    print(f"  Type {C.BOLD}YES{C.RESET} to confirm, or Enter to cancel: ",end="",flush=True)
    try:
        confirm=input("").strip()
    except (EOFError,KeyboardInterrupt):
        confirm=""

    if confirm!="YES":
        out(f"  {C.DIM}Cancelled. Returning all AUTO folders to manual review.{C.RESET}")
        triage["hold"]=auto+triage["hold"]
        triage["auto"]=[]
        return []

    print(f"{'─'*66}")
    applied:List[Dict[str,Any]]=[]
    skip_path=Path(cfg["logging"]["skipped_log"])

    for p in auto:
        src=Path(p.folder_path); dst=Path(p.target_path)
        if not src.exists():
            print(f"  {C.DIM}SKIP (gone):{C.RESET} {p.folder_name}"); continue
        dst2=collision_resolve(dst,policy,sfmt)
        if dst2 is None:
            warn(f"  Collision skip: {dst.name}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)})
            continue
        if dry_run:
            out(f"  {C.DIM}[dry-run]{C.RESET} {src.name[:52]}  →  {dst2.name[:30]}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)})
            continue
        ensure_dir(dst2.parent)
        try:
            move_method,_=safe_move_folder(src,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"  FAILED: {e}  ({src.name})"); continue
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
               "move_method":move_method,"interactive_action":"bulk_approved"}
        append_jsonl(hist_path,entry); applied.append(entry)
        manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
        print(f"  {C.GREEN}✓{C.RESET} {dst2.name[:60]}")
        if source_root:
            try: cleanup_empty_parents(src,source_root)
            except Exception: pass

    print(f"{'═'*66}")
    return applied
