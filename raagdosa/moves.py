"""
RaagDosa Moves — folder move execution, track renaming, session reports.

Layer 6: imports from proposal (L5), crates (L4), tagreader (L2), tracks (L5),
         review (L4), files (L1), session (L3), core (L0), ui (L0).

Contains the I/O-heavy execution functions that actually move files.
"""
from __future__ import annotations

import csv
import html
import os
import datetime as dt
import re
import shutil
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from raagdosa import APP_VERSION
from raagdosa.core import (now_iso, FolderProposal, FolderStats,
    CrateTrackPlan, EmbeddedRelease, should_stop)
from raagdosa.files import (ensure_dir, safe_move_folder, append_jsonl,
    is_hidden_file, get_folder_size, check_folder_locked, check_path_length,
    cleanup_empty_parents, _same_device, _restore_creation_date)
from raagdosa.tags import normalize_unicode
from raagdosa.tagreader import read_audio_tags
from raagdosa.ui import C, VERBOSE, out, err, warn, ok_msg, status_tag, conf_color, SizeProgress
from raagdosa.config import write_yaml
from raagdosa.session import (find_dj_databases, manifest_add, manifest_get_last_run, ensure_roots)
from raagdosa.review import collision_resolve
from raagdosa.proposal import _write_review_sidecar
from raagdosa.crates import build_crate_explosion_plan, apply_crate_explosion
from raagdosa.tracks import classify_folder_for_tracks, build_track_filename
from raagdosa.naming import normalise_extension
from raagdosa.proposal import folder_is_multidisc


# ── session reports ────────────────────────────────────────────

def _write_session_reports(session_id:str,profile_name:str,source_root:Path,proposals:List[FolderProposal],session_dir:Path,cfg:Dict[str,Any])->None:
    if not cfg.get("logging",{}).get("write_human_report",True): return
    cn=sum(1 for p in proposals if p.destination=="clean")
    rn=sum(1 for p in proposals if p.destination=="review")
    dn=sum(1 for p in proposals if p.destination=="duplicate")

    # TXT
    lines=[f"RaagDosa v{APP_VERSION} — Session Report","="*70,
           f"Session:  {session_id}",f"Profile:  {profile_name}",f"Source:   {source_root}",f"Date:     {now_iso()}","",
           f"SUMMARY:  {len(proposals)} total | CLEAN {cn} | REVIEW {rn} | DUPES {dn}","",
           f"{'Status':<9} {'Conf':>6}  {'Original Folder':<45}  Proposed Name","-"*110]
    for p in proposals:
        rr=", ".join(p.decision.get("route_reasons",[]))
        tag={"clean":"CLEAN","review":"REVIEW","duplicate":"DUPE"}.get(p.destination,p.destination.upper())
        heur=" [heuristic]" if p.decision.get("used_heuristic") else ""
        va_flag=" [VA]" if p.decision.get("is_va") else ""
        art_info=f"  artist={p.decision.get('albumartist_display','')}" if p.decision.get("albumartist_display") else ""
        lines.append(f"{tag:<9} {p.confidence:>6.2f}  {p.folder_name[:45]:<45}  → {p.proposed_folder_name}{heur}{va_flag}{art_info}{('  ['+rr+']') if rr else ''}")
        for fd in (p.stats.format_duplicates or []):
            lines.append(f"{'':>20}  ⚠ format dupe: {fd}")
    (session_dir/"report.txt").write_text("\n".join(lines),encoding="utf-8")

    # CSV
    with (session_dir/"report.csv").open("w",newline="",encoding="utf-8") as f:
        w=csv.writer(f)
        w.writerow(["status","confidence","original_folder","proposed_name","target_path","track_count",
                    "tagged_count","unreadable_count","extensions","route_reasons","heuristic","format_duplicates"])
        for p in proposals:
            w.writerow([p.destination,f"{p.confidence:.4f}",p.folder_name,p.proposed_folder_name,p.target_path,
                        p.stats.tracks_total,p.stats.tracks_tagged,p.stats.tracks_unreadable,
                        "|".join(sorted(p.stats.extensions.keys())),"|".join(p.decision.get("route_reasons",[])),
                        "yes" if p.decision.get("used_heuristic") else "no","|".join(p.stats.format_duplicates or [])])

    # HTML
    rows=""
    for p in proposals:
        rr=", ".join(p.decision.get("route_reasons",[])); heur=" <span class='h'>[heuristic]</span>" if p.decision.get("used_heuristic") else ""
        dc={"clean":"c","review":"r","duplicate":"d"}.get(p.destination,"")
        cc="hi" if p.confidence>=0.90 else ("mi" if p.confidence>=0.75 else "lo")
        rows+=(f'<tr class="{dc}"><td class="st">{p.destination.upper()}</td>'
               f'<td class="cf {cc}">{p.confidence:.2f}</td><td>{html.escape(p.folder_name)}</td>'
               f'<td>{html.escape(p.proposed_folder_name)}{heur}</td><td>{p.stats.tracks_total}</td>'
               f'<td>{html.escape("|".join(sorted(p.stats.extensions.keys())))}</td>'
               f'<td class="rr">{html.escape(rr)}</td></tr>\n')
    html_out=f"""<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<title>RaagDosa {html.escape(session_id)}</title><style>
body{{font-family:'JetBrains Mono','Courier New',monospace;background:#1a1a1a;color:#e0e0e0;padding:2rem}}
h1{{color:#88c0d0}}h2{{color:#81a1c1;border-bottom:1px solid #3b4252;padding-bottom:.3rem}}
.summary{{background:#2e3440;border-radius:6px;padding:1rem;margin:1rem 0}}
.summary span{{margin-right:2rem;font-weight:bold}}.sc{{color:#a3be8c}}.sr{{color:#ebcb8b}}.sd{{color:#bf616a}}
table{{width:100%;border-collapse:collapse;margin-top:1rem;font-size:.85em}}
th{{background:#3b4252;color:#88c0d0;padding:.5rem;text-align:left}}
tr:nth-child(even){{background:#252b37}}
tr.c td.st{{color:#a3be8c}}tr.r td.st{{color:#ebcb8b}}tr.d td.st{{color:#bf616a}}
.hi{{color:#a3be8c}}.mi{{color:#ebcb8b}}.lo{{color:#bf616a}}
td{{padding:.4rem .5rem;border-bottom:1px solid #2e3440}}.rr{{color:#88c0d0;font-size:.8em}}.h{{color:#ebcb8b}}
</style></head><body>
<h1>🍛 RaagDosa v{APP_VERSION} — Session Report</h1>
<div class="summary"><span>Session: {html.escape(session_id)}</span><span>Profile: {html.escape(profile_name)}</span><span>Source: {html.escape(str(source_root))}</span></div>
<div class="summary"><span>Total: {len(proposals)}</span><span class="sc">✓ Clean: {cn}</span><span class="sr">⚠ Review: {rn}</span><span class="sd">✗ Dupes: {dn}</span></div>
<h2>Proposals</h2><table><thead><tr><th>Status</th><th>Conf</th><th>Original Folder</th><th>Proposed Name</th><th>Tracks</th><th>Format(s)</th><th>Reasons</th></tr></thead><tbody>{rows}</tbody></table>
</body></html>"""
    (session_dir/"report.html").write_text(html_out,encoding="utf-8")


# ── folder move execution ─────────────────────────────────────

def apply_folder_moves(cfg:Dict[str,Any],proposals:List[FolderProposal],interactive:bool,
                       auto_above:Optional[float],dry_run:bool,session_id:str,
                       source_root:Optional[Path]=None)->List[Dict[str,Any]]:
    mc=cfg.get("move",{})
    if not mc.get("enabled",True): return []
    policy=mc.get("on_collision","suffix"); sfmt=mc.get("suffix_format"," ({n})"); use_cs=bool(mc.get("use_checksum",False))
    dc=cfg.get("decision",{}); req_confirm=bool(dc.get("require_confirmation",True)); interactive_below=float(dc.get("interactive_below",0.92))
    dj=cfg.get("dj_safety",{}); halt_dj=bool(dj.get("halt_on_dj_databases",False)); warn_dj=bool(dj.get("warn_on_dj_databases",True))
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    hist_path=Path(cfg["logging"]["history_log"]); skip_path=Path(cfg["logging"]["skipped_log"])

    if not dry_run and proposals:
        try:
            dst_parent=Path(proposals[0].target_path).parent.parent
            total_size=sum(get_folder_size(Path(p.folder_path)) for p in proposals if Path(p.folder_path).exists())
            free=shutil.disk_usage(str(dst_parent)).free
            if free<int(total_size*1.10):
                err(f"⛔ Disk: need ~{total_size/1024/1024:.0f} MB, only {free/1024/1024:.0f} MB free. Aborting."); sys.exit(1)
        except Exception: pass

    applied:List[Dict[str,Any]]=[]
    # Size-aware progress bar — compute per-folder sizes up front
    _folder_sizes:Dict[str,int]={}
    for p in proposals:
        sp=Path(p.folder_path)
        _folder_sizes[p.folder_path]=get_folder_size(sp) if sp.exists() else 0
    _total_bytes=sum(_folder_sizes.values())
    prog=SizeProgress(_total_bytes,len(proposals),"Moving")

    for p in proposals:
        if should_stop(): out(f"\n{C.YELLOW}Stopped. {len(applied)}/{len(proposals)} applied.{C.RESET}"); break
        src=Path(p.folder_path); dst=Path(p.target_path)
        _cur_size=_folder_sizes.get(p.folder_path,0)
        if not src.exists():
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"missing_source","src":str(src)}); continue
        if not check_path_length(dst): warn(f"Path length >{260}: {dst}")
        locked=check_folder_locked(src,exts)
        if locked:
            warn(f"Locked files in {src.name}: {[lf.name for lf in locked[:3]]}")
            if interactive and input(f"  Skip '{src.name}'? [Y/n] ").strip().lower()!="n":
                prog.tick(_cur_size,src.name)
                append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"locked_files","src":str(src)}); continue
        if warn_dj:
            dj_dbs=find_dj_databases(src)
            if dj_dbs:
                warn(f"DJ databases in {src.name}: {', '.join(dj_dbs)}")
                if halt_dj:
                    prog.tick(_cur_size,src.name)
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dj_halt","src":str(src)}); continue
        # ── v9.0: DJ Crate explosion (per-track routing) ────────────────
        _is_crate_explode=(p.decision.get("is_crate") and p.decision.get("crate_type")=="singles"
                           and cfg.get("djcrates",{}).get("explode_to_artist_folders",False))
        if _is_crate_explode:
            # Get audio files from source folder
            _audio_exts=set(e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a",".wav",".aiff",".ogg",".opus"]))
            _audio_files=sorted([f for f in src.iterdir() if f.is_file() and f.suffix.lower() in _audio_exts])
            if not _audio_files:
                prog.tick(_cur_size,src.name)
                append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"crate","reason":"no_audio_files","src":str(src)}); continue

            # Build explosion plan
            _profile=cfg.get("profiles",{}).get(cfg.get("active_profile","incoming"),{})
            _src_root=Path(_profile.get("source_root",src.parent)).expanduser()
            _plans,_embedded=build_crate_explosion_plan(src,_audio_files,cfg,_profile,_src_root,p.confidence)

            # Count unique artist destinations
            _unique_artists=len(set(tp.artist or "_Unsorted" for tp in _plans))
            _review_count=sum(1 for tp in _plans if tp.destination=="review")
            _embedded_count=sum(1 for tp in _plans if tp.embedded_release)
            _singles_count=len(_plans)-_embedded_count

            # Interactive confirmation for crate explosion
            should_prompt=(interactive or req_confirm) and not(auto_above is not None and p.confidence>=auto_above)
            if should_prompt:
                print(f"\n  {C.BOLD}DJ CRATE DETECTED{C.RESET}  {src.name}/")
                print(f"    {len(_audio_files)} tracks → {_unique_artists} artist folders")
                if _embedded:
                    print(f"    {C.GREEN}Embedded releases found: {len(_embedded)}{C.RESET}")
                    for rel in _embedded:
                        _partial=f" (partial {len(rel.tracks)}/{rel.total_in_album})" if rel.is_partial and rel.total_in_album else ""
                        print(f"      {rel.artist_display} - {rel.album_display} ({len(rel.tracks)} tracks){_partial}")
                    print(f"    Remaining singles: {_singles_count}")
                if _review_count: print(f"    {C.YELLOW}{_review_count} track(s) need review{C.RESET}")
                print(f"    Crate confidence: {conf_color(p.confidence)}")
                print(f"    {C.DIM}Reason: {p.decision.get('crate_reason','')}{C.RESET}")
                # Show first few track plans
                print(f"    {C.DIM}{'─'*50}{C.RESET}")
                for tp in _plans[:5]:
                    _art=tp.artist or "_Unsorted"
                    if tp.embedded_release:
                        print(f"    {tp.filename[:40]:40s} → {_art}/{tp.embedded_release}/")
                    else:
                        print(f"    {tp.filename[:40]:40s} → {_art}/Singles/")
                if len(_plans)>5:
                    print(f"    {C.DIM}... and {len(_plans)-5} more{C.RESET}")
                print()
                _choice=input("  [e] Explode as crate  [v] Keep as VA  [s] Skip  [d] Show all tracks  > ").strip().lower()
                if _choice=="d":
                    for tp in _plans:
                        _art=tp.artist or "_Unsorted"
                        if tp.embedded_release:
                            _dest_label=f"{_art}/{tp.embedded_release}/{tp.track_number:02d} - {tp.title or tp.filename}"
                        else:
                            _dest_label=f"{_art}/Singles/{tp.title or tp.filename}"
                        print(f"    {tp.filename[:40]:40s} → {_dest_label}  "
                              f"conf={conf_color(tp.confidence)}")
                    _choice=input("  [e] Explode  [v] Keep as VA  [s] Skip  > ").strip().lower()
                if _choice=="v":
                    # Reclassify as VA and fall through to normal folder move
                    p.decision["is_crate"]=False; p.decision["folder_type"]="va"; p.decision["is_va"]=True
                    # Offer to learn — find sibling folders with similar suffix/prefix
                    _fn=p.folder_name
                    _parent=src.parent
                    _siblings=[d.name for d in _parent.iterdir() if d.is_dir() and d.name!=_fn] if _parent.exists() else []
                    _learned=False
                    for sep in [" - ","_"," "]:
                        parts=_fn.rsplit(sep,1)
                        if len(parts)==2 and len(parts[1])>=3:
                            _suffix=parts[1].lower()
                            _matching=[s for s in _siblings if s.lower().endswith(_suffix)]
                            if _matching:
                                print(f"\n    {C.CYAN}Other folders ending with '{_suffix}': {', '.join(_matching[:5])}{C.RESET}")
                                _learn_ans=input(f"    [l] Learn '*{_suffix}' as NOT crate  [n] No, one-off  > ").strip().lower()
                                if _learn_ans=="l":
                                    _veto_pat=rf".*[\s_\-]{re.escape(_suffix)}$"
                                    _dc=cfg.setdefault("djcrates",{})
                                    _veto_list=_dc.setdefault("crate_veto_patterns",[])
                                    if _veto_pat not in _veto_list:
                                        _veto_list.append(_veto_pat)
                                        _cp=cfg.get("_cfg_path")
                                        if _cp: write_yaml(Path(_cp),cfg)
                                        print(f"    {C.GREEN}✓ Added veto pattern to config{C.RESET}")
                                    _learned=True
                                break
                    pass  # will fall through to normal move logic below
                elif _choice!="e":
                    prog.tick(_cur_size,src.name)
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"crate","reason":"user_skipped","src":str(src)}); continue
                else:
                    # User chose explode — offer to learn crate pattern
                    _fn=p.folder_name
                    _parent=src.parent
                    _siblings=[d.name for d in _parent.iterdir() if d.is_dir() and d.name!=_fn] if _parent.exists() else []
                    for sep in [" - ","_"," "]:
                        parts=_fn.rsplit(sep,1)
                        if len(parts)==2 and len(parts[1])>=3:
                            _suffix=parts[1].lower()
                            _matching=[s for s in _siblings if s.lower().endswith(_suffix)]
                            if _matching:
                                print(f"\n    {C.CYAN}Other folders ending with '{_suffix}': {', '.join(_matching[:5])}{C.RESET}")
                                _learn_ans=input(f"    [l] Learn '*{_suffix}' as crate pattern  [n] No, one-off  > ").strip().lower()
                                if _learn_ans=="l":
                                    _crate_pat=rf".*[\s_\-]{re.escape(_suffix)}$"
                                    _dc=cfg.setdefault("djcrates",{})
                                    _crate_list=_dc.setdefault("custom_crate_patterns",[])
                                    if _crate_pat not in _crate_list:
                                        _crate_list.append(_crate_pat)
                                        _cp=cfg.get("_cfg_path")
                                        if _cp: write_yaml(Path(_cp),cfg)
                                        print(f"    {C.GREEN}✓ Added crate pattern to config{C.RESET}")
                                break
                    pass  # proceed to explosion

            # Only explode if user confirmed or non-interactive
            if p.decision.get("is_crate"):
                _emb_note=f", {len(_embedded)} embedded release(s)" if _embedded else ""
                if dry_run:
                    out(f"\n  {C.DIM}[dry-run]{C.RESET} {C.BOLD}CRATE EXPLODE{C.RESET}: {src.name}  ({len(_plans)} tracks → {_unique_artists} artist folders{_emb_note})")
                    _crate_applied=apply_crate_explosion(src,_plans,session_id,cfg,dry_run=True,source_root=source_root)
                else:
                    out(f"\n  {C.BOLD}CRATE EXPLODE{C.RESET}: {src.name}  ({len(_plans)} tracks → {_unique_artists} artist folders{_emb_note})")
                    _crate_applied=apply_crate_explosion(src,_plans,session_id,cfg,dry_run=False,source_root=source_root)
                    applied.extend(_crate_applied)
                    _emb_moved=sum(1 for a in _crate_applied if a.get("embedded_release"))
                    _sin_moved=len(_crate_applied)-_emb_moved
                    _parts=[f"{_sin_moved} singles"]
                    if _emb_moved: _parts.append(f"{_emb_moved} album tracks")
                    out(f"    {C.GREEN}✓ {len(_crate_applied)}/{len(_plans)} tracks moved ({', '.join(_parts)}){C.RESET}")
                prog.tick(_cur_size,src.name)
                continue

        # ── Normal folder move logic ─────────────────────────────────────
        dst2=collision_resolve(dst,policy,sfmt)
        if dst2 is None:
            warn(f"Collision skip: {dst.name}")
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)}); continue
        elif dst2!=dst: warn(f"Collision — renamed to: {dst2.name}")
        should_prompt=(interactive or (req_confirm and p.confidence<interactive_below)) and not(auto_above is not None and p.confidence>=auto_above)
        if should_prompt:
            # v6.1: Show richer context for review decisions
            _art_info=f"  artist: {p.decision.get('albumartist_display','?')}" if p.decision.get("albumartist_display") else ""
            _va_info=f"  {C.YELLOW}[VA]{C.RESET}" if p.decision.get("is_va") else ""
            _crate_info=f"  {C.YELLOW}[CRATE]{C.RESET}" if p.decision.get("is_crate") else ""
            _reasons=", ".join(p.decision.get("route_reasons",[]))
            print(f"\n  {C.DIM}FROM:{C.RESET} {src.name}")
            print(f"  {C.DIM}  TO:{C.RESET} {dst2.name}  {status_tag(p.destination)}  conf={conf_color(p.confidence)}{_va_info}{_crate_info}")
            if _art_info or _reasons:
                print(f"  {C.DIM}    {_art_info}{'  |  ' if _art_info and _reasons else ''}{_reasons}{C.RESET}")
            if p.decision.get("used_heuristic"): warn("name from heuristic — no tags")
            if input("  Apply? [y/N] ").strip().lower()!="y":
                prog.tick(_cur_size,src.name)
                append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"user_skipped","src":str(src)}); continue
        if dry_run:
            same=_same_device(src,dst2)
            method_note=f"{C.DIM}[rename]{C.RESET}" if same else f"{C.DIM}[copy]{C.RESET}"
            out(f"  {C.DIM}[dry-run]{C.RESET} {method_note} {src.name}  →  {dst2.name}  {status_tag(p.destination)}  conf={conf_color(p.confidence)}")
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)}); continue
        ensure_dir(dst2.parent)
        try: move_method,move_elapsed=safe_move_folder(src,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"⛔ Move failed ({src.name}): {e}")
            prog.tick(_cur_size,src.name)
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)}); continue
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
               "move_method":move_method}
        append_jsonl(hist_path,entry); applied.append(entry)
        if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
        elif p.destination=="review": _write_review_sidecar(dst2,p,session_id)
        method_tag=f"  {C.DIM}[{move_method} {move_elapsed*1000:.0f}ms]{C.RESET}" if _ui._verbosity>=VERBOSE else ""
        out(f"  MOVED {status_tag(p.destination)} {C.DIM}{src.name}{C.RESET}  →  {dst2.name}  conf={conf_color(p.confidence)}{method_tag}")
        prog.tick(_cur_size,dst2.name)
        # Clean up empty parent directories left behind in source
        if source_root:
            try: cleanup_empty_parents(src,source_root)
            except Exception: pass
    prog.done()
    # Summary: show rename vs copy counts so user knows which path was taken
    if applied:
        renames=sum(1 for a in applied if a.get("move_method")=="rename")
        copies =sum(1 for a in applied if a.get("move_method")=="copy")
        if renames and copies:
            out(f"  {C.DIM}Move methods: {renames} instant rename + {copies} cross-device copy{C.RESET}")
        elif renames:
            out(f"  {C.DIM}All {renames} folder(s) moved via instant rename (same filesystem){C.RESET}")
        elif copies:
            out(f"  {C.DIM}All {copies} folder(s) copied across devices{C.RESET}")
    return applied

# ─────────────────────────────────────────────
# Track renaming
# ─────────────────────────────────────────────
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


# ── track renaming ───────────────────────────────────────────

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

def rename_tracks_in_clean_folder(cfg:Dict[str,Any],folder:Path,folder_decision:Dict[str,Any],interactive:bool,dry_run:bool,session_id:str)->List[Dict[str,Any]]:
    trc=cfg.get("track_rename",{})
    if not trc.get("enabled",True): return []
    allowed={e.lower() for e in trc.get("allowed_extensions",[".mp3",".flac",".m4a"])}
    skip_exts={e.lower() for e in trc.get("skip_extensions",[])}
    files=[p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in allowed and p.suffix.lower() not in skip_exts and not is_hidden_file(p)]
    if not files: return []
    classification=classify_folder_for_tracks(folder_decision,cfg); disc_multi=folder_is_multidisc(files,cfg)
    thist=Path(cfg["logging"]["track_history_log"]); tskip=Path(cfg["logging"]["track_skipped_log"])
    applied:List[Dict[str,Any]]=[]; skip_count=0
    for f in sorted(files):
        # Extension case normalisation: .MP3 → .mp3
        ext_fixed=normalise_extension(f)
        if ext_fixed and not dry_run:
            try: f.rename(ext_fixed); f=ext_fixed
            except Exception: pass
        tags=read_audio_tags(f,cfg); new_name,conf,reason,meta=build_track_filename(classification,tags,f,cfg,folder_decision,disc_multi,total_tracks=len(files))
        if not new_name:
            out(f"    ↷ SKIP {f.name}  ({reason})",level=VERBOSE); skip_count+=1
            append_jsonl(tskip,{"timestamp":now_iso(),"session_id":session_id,"type":"track","reason":reason,"file":str(f),"meta":meta}); continue
        dst=f.with_name(new_name)
        if normalize_unicode(dst.name)==normalize_unicode(f.name):
            out(f"    ✓ {f.name}  {C.DIM}(already clean){C.RESET}",level=VERBOSE)
            continue
        if dst.exists():
            n=1
            while True:
                cand=dst.with_name(dst.stem+f" ({n})"+dst.suffix)
                if not cand.exists(): dst=cand; break
                n+=1
        if dry_run:
            out(f"    {C.DIM}[dry-run]{C.RESET} {f.name}\n          → {dst.name}  [{classification}]  conf={conf_color(conf)}")
            append_jsonl(tskip,{"timestamp":now_iso(),"session_id":session_id,"type":"track","reason":"dry_run","file":str(f),"dst":str(dst)}); continue
        if interactive:
            print(f"\n    {f.name}\n    → {dst.name}  conf={conf_color(conf)}")
            if input("    Rename? [y/N] ").strip().lower()!="y":
                append_jsonl(tskip,{"timestamp":now_iso(),"session_id":session_id,"type":"track","reason":"user_skipped","file":str(f)}); continue
        f.rename(dst)
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"track","folder":str(folder),
               "original_path":str(f),"target_path":str(dst),"confidence":conf,"classification":classification,"meta":meta}
        append_jsonl(thist,entry); applied.append(entry)
        out(f"    RENAMED: {f.name}  →  {dst.name}",level=VERBOSE)
    if skip_count: out(f"    ↷ {skip_count} track(s) skipped in {folder.name}")
    elif applied:  out(f"    ✓ {len(applied)} track(s) renamed in {folder.name}")
    return applied

# ─────────────────────────────────────────────
# show — single folder debug
# ─────────────────────────────────────────────


# ── folder scanning ──────────────────────────────────────────

import dataclasses
import datetime as dt
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from typing import Set, Tuple

import raagdosa.ui as ui_module
from raagdosa.core import make_session_id, register_stop_handler
from raagdosa.files import list_audio_files, write_json
from raagdosa.tagreader import _get_tag_cache
from raagdosa.ui import VERBOSE, Progress, human_size
from raagdosa.config import folder_matches_ignore
from raagdosa.session import (setup_logging_paths, ensure_roots, read_manifest,
    derive_wrapper_root, derive_clean_root, derive_review_root)
from raagdosa.pipeline import build_skip_sets, folder_mtime, resolve_genre_roots
from raagdosa.proposal import (build_folder_proposal, _route_proposal,
    _REASON_DESCRIPTIONS, _FACTOR_DESCRIPTIONS,
)
from raagdosa.library import _resolve_lib_cfg as resolve_lib_cfg

def scan_folders(cfg:Dict[str,Any],profile_name:str,since:Optional[dt.datetime]=None,genre_roots:Optional[List[str]]=None,itunes_mode:bool=False,session_name:str="")->Tuple[str,Path,List[FolderProposal]]:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    # v5.5 — resolve log paths and skip-sets from config
    setup_logging_paths(cfg, profile, source_root)
    skip_exts, skip_folder_names = build_skip_sets(cfg)

    # v4.1 — resolve effective genre roots (CLI flag + config)
    effective_genre_roots = resolve_genre_roots(cfg, genre_roots)

    roots=ensure_roots(profile,source_root,create=False)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    # Also skip the wrapper folder itself during walk (it contains Clean/, Review/, logs/)
    wrapper_root_str=str(derive_wrapper_root(profile,source_root).resolve())+os.sep
    clean_root_str =str(derive_clean_root(profile,source_root).resolve())+os.sep
    review_root_str=str(derive_review_root(profile,source_root).resolve())+os.sep

    session_id=make_session_id(profile_name, str(source_root), session_name=session_name)
    # Deduplicate if same-minute session already exists
    session_base=Path(cfg["logging"]["session_dir"])/session_id
    if session_base.exists():
        for i in range(2,20):
            candidate=Path(cfg["logging"]["session_dir"])/f"{session_id}_{i}"
            if not candidate.exists():
                session_id=f"{session_id}_{i}"; session_base=candidate; break
    session_dir=session_base; ensure_dir(session_dir)

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])
    # v5.5: skip sets from build_skip_sets (replaces _init_skip_sets globals)

    # Initialise tag cache for this scan run
    _get_tag_cache(cfg)

    # Collect candidates with progress
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        # Skip the entire wrapper folder (contains Clean/, Review/, logs/)
        if rp_str.startswith(wrapper_root_str): dirs[:]=[] ; continue
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[] ; continue
        # v4.1: skip __MACOSX and other noise folders
        dirs[:] = [d for d in dirs if d not in skip_folder_names]
        # v4.1: genre root protection — skip renaming them but recurse inside
        if effective_genre_roots and rp.name in effective_genre_roots and rp != source_root:
            continue  # don't process genre root itself as a candidate
        if leaf_only and dirs: continue
        audio_count = sum(1 for f in files
                          if Path(f).suffix.lower() in exts
                          and Path(f).suffix.lower() not in skip_exts
                          and not f.startswith("._"))
        if audio_count >= min_tracks: candidates.append(rp)

    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
        out(f"  --since: {len(candidates)} folders modified after {since.strftime('%Y-%m-%d %H:%M')}",level=VERBOSE)

    # Filter ignores before submitting to workers
    candidates=[rp for rp in candidates if not folder_matches_ignore(rp.name,ignore_patterns)]

    # ── Parallel scan ────────────────────────────────────────────────────
    # Tag reading (mutagen) is I/O-bound → threads are the right tool.
    # Each worker is independent — they share the tag cache (thread-safe)
    # but build their own local vote counters.
    workers=int(sc.get("workers", min(8, (os.cpu_count() or 4))))
    out(f"  Scanning {len(candidates)} folder(s) with {workers} worker(s)…",level=VERBOSE)

    proposals_raw:List[FolderProposal]=[]; proposals_lock=Lock()
    prog=Progress(len(candidates),"Scanning")

    def _scan_one(rp:Path)->Optional[FolderProposal]:
        if should_stop(): return None
        audio_files=list_audio_files(rp,exts,follow_sym)
        if len(audio_files)<min_tracks:
            prog.tick(rp.name); return None
        prog.tick(rp.name)
        return build_folder_proposal(rp,audio_files,source_root,profile,cfg)

    if workers<=1 or len(candidates)<=4:
        # Single-threaded path (avoids thread overhead for small runs)
        for rp in candidates:
            if should_stop(): out(f"\n{C.YELLOW}Stopped after scanning {len(proposals_raw)} folders.{C.RESET}"); break
            prop=_scan_one(rp)
            if prop: proposals_raw.append(prop)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures={pool.submit(_scan_one,rp):rp for rp in candidates}
            for fut in as_completed(futures):
                if should_stop():
                    # Cancel pending (best-effort — running tasks will complete)
                    for f in futures:
                        if not f.done(): f.cancel()
                    out(f"\n{C.YELLOW}Stop requested — waiting for in-flight tasks…{C.RESET}")
                    break
                try:
                    prop=fut.result()
                    if prop:
                        with proposals_lock: proposals_raw.append(prop)
                except Exception as e:
                    rp=futures[fut]; err(f"  scan error in {rp.name}: {e}")

    prog.done()

    # Preserve input order (as_completed returns in completion order)
    # Sort by original path for deterministic proposals/reports
    proposals=sorted(proposals_raw,key=lambda p:p.folder_path)

    # Save tag cache to disk after scan completes
    _tc = _get_tag_cache(cfg)
    if _tc is not None:
        _tc.save()
        out(f"  Tag cache: {_tc.size} entries",level=VERBOSE)

    # Routing
    existing_clean:Set[str]=set()
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir(): existing_clean.add(normalize_unicode(item.name))
        except PermissionError as e:
            out(f"  {C.DIM}Permission denied scanning clean library: {e}{C.RESET}",level=VERBOSE)
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder
    seen_names:Counter=Counter()

    for p in proposals:
        _route_proposal(p,cfg,seen_names,existing_clean,manifest_entries,
                        review_albums,dup_root,mixes_root,all_proposals=proposals)

    payload={"app":cfg.get("app",{}),"session_id":session_id,"timestamp":now_iso(),"profile":profile_name,
             "source_root":str(source_root),"since":since.isoformat() if since else None,
             "folder_proposals":[dataclasses.asdict(p) for p in proposals]}
    write_json(session_dir/"proposals.json",payload)
    _write_session_reports(session_id,profile_name,source_root,proposals,session_dir,cfg)

    clean_n=sum(1 for p in proposals if p.destination=="clean")
    rev_n  =sum(1 for p in proposals if p.destination=="review")
    dup_n  =sum(1 for p in proposals if p.destination=="duplicate")
    since_note=f"  (since {since.strftime('%Y-%m-%d %H:%M')})" if since else ""
    # Scan summary: total tracks, size, format breakdown
    total_tracks=sum(p.stats.tracks_total for p in proposals)
    total_bytes=sum(get_folder_size(Path(p.folder_path)) for p in proposals if Path(p.folder_path).exists())
    fmt_totals:Counter=Counter()
    for p in proposals:
        for ext,cnt in (p.stats.extensions or {}).items():
            fmt_totals[ext.upper().lstrip(".")]+=cnt
    fmt_str=" · ".join(f"{fmt}: {cnt}" for fmt,cnt in fmt_totals.most_common()) if fmt_totals else ""
    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    out(f"{C.BOLD}Found:{C.RESET}     {len(proposals)} folders, {total_tracks} tracks ({human_size(total_bytes)}){since_note}")
    if fmt_str:
        out(f"           {C.DIM}{fmt_str}{C.RESET}")
    out(f"{C.BOLD}Routing:{C.RESET}   {C.GREEN}Clean: {clean_n}{C.RESET} | {C.YELLOW}Review: {rev_n}{C.RESET} | {C.RED}Dupes: {dup_n}{C.RESET}")
    # v9.0: DJ crate summary
    crate_singles_n=sum(1 for p in proposals if p.decision.get("is_crate") and p.decision.get("crate_type")=="singles")
    crate_set_n=sum(1 for p in proposals if p.decision.get("is_crate") and p.decision.get("crate_type")=="set")
    if crate_singles_n or crate_set_n:
        crate_tracks=sum(p.stats.tracks_total for p in proposals if p.decision.get("is_crate"))
        parts=[]
        if crate_singles_n: parts.append(f"singles: {crate_singles_n}")
        if crate_set_n: parts.append(f"set prep: {crate_set_n}")
        out(f"{C.BOLD}Crates:{C.RESET}    {', '.join(parts)} ({crate_tracks} tracks)")

    # ── Compact per-folder table (sorted by confidence) ────────────────
    sorted_props=sorted(proposals,key=lambda p:p.confidence)
    out(f"\n{C.BOLD}{'Conf':>6}  {'Route':<7}  {'Folder':<45}  {'Tracks':>6}  Format{C.RESET}")
    out(f"{C.DIM}{'─'*90}{C.RESET}")
    for p in sorted_props:
        dest_color=C.GREEN if p.destination=="clean" else (C.YELLOW if p.destination=="review" else C.RED)
        dest_label={"clean":"Clean","review":"Review","duplicate":"Dupe"}.get(p.destination,p.destination)
        exts_str="+".join(k.upper().lstrip(".") for k in sorted(p.stats.extensions or {})) or "--"
        crate_flag=" [CRATE]" if p.decision.get("is_crate") else ""
        va_flag=" [VA]" if p.decision.get("is_va") else ""
        flags=f"{crate_flag}{va_flag}"
        name_display=p.folder_name[:43]+".." if len(p.folder_name)>45 else p.folder_name
        out(f"  {conf_color(p.confidence):>6}  {dest_color}{dest_label:<7}{C.RESET}  {name_display:<45}  {p.stats.tracks_total:>4}    {exts_str}{flags}")
        # Verbose: show why confidence is high or low
        reasons=p.decision.get("route_reasons",[])
        factors=p.decision.get("confidence_factors",{})
        if reasons and ui_module._verbosity>=VERBOSE:
            # Show weak factors (below 0.5) and route reasons
            weak=[f"{_FACTOR_DESCRIPTIONS.get(k,k)}: {v:.0%}" for k,v in factors.items() if v<0.5]
            reason_strs=[_REASON_DESCRIPTIONS.get(r.split(":")[0],r) for r in reasons]
            detail_parts=reason_strs+weak
            if detail_parts:
                out(f"          {C.DIM}  └ {' · '.join(detail_parts)}{C.RESET}")

    # Tag coverage summary — shows how well populated the tags are for the active template
    lib=resolve_lib_cfg(profile,cfg)
    tpl=lib.get("template","{artist}/{album}")
    # Detect which tokens the template uses
    tpl_tokens=set(re.findall(r"\{(\w+)\}",tpl))
    # Map tokens to the tag field needed
    _TOKEN_TAG_MAP={"genre":"genre","decade":"year","bpm_range":"bpm","camelot_key":"key","label":"label"}
    needed={tok:_TOKEN_TAG_MAP[tok] for tok in tpl_tokens if tok in _TOKEN_TAG_MAP}
    if needed and proposals:
        total_folders=len(proposals)
        out(f"\n{C.BOLD}Tag coverage{C.RESET} (for template: {tpl})")
        for tok,tag_key in sorted(needed.items()):
            has_count=sum(1 for p in proposals if p.decision.get(tag_key))
            pct=int(has_count/total_folders*100) if total_folders else 0
            color=C.GREEN if pct>=80 else (C.YELLOW if pct>=50 else C.RED)
            out(f"  {tok:<14} {color}{has_count}/{total_folders} folders ({pct}%){C.RESET}")
        missing_count=sum(1 for p in proposals for tok,tag_key in needed.items() if not p.decision.get(tag_key))
        if missing_count:
            out(f"  {C.DIM}Folders with missing tags will be placed under fallback folders (_Unsorted, etc.){C.RESET}")

    out(f"{C.DIM}Reports:   {session_dir}/report.{{txt,csv,html}}{C.RESET}")
    return session_id,session_dir,proposals


# ─────────────────────────────────────────────
# Date parsing for --since
# ─────────────────────────────────────────────

def _parse_since(val:Optional[str],cfg:Dict[str,Any])->Optional[dt.datetime]:
    if not val: return None
    if val=="last_run": return manifest_get_last_run(cfg)
    try: return dt.datetime.fromisoformat(val)
    except Exception: err(f"Cannot parse --since '{val}'. Use ISO date or 'last_run'."); sys.exit(1)


# ─────────────────────────────────────────────
# Artifact classification and quarantine
# ─────────────────────────────────────────────

def _cue_has_paired_audio(path:Path)->bool:
    """Return True if a .cue sheet has a matching .flac or .ape file alongside."""
    stem=path.stem
    for ext in (".flac",".ape",".wav"):
        if (path.parent/(stem+ext)).exists():
            return True
    return False

def classify_artifacts(folder:Path, cfg:Dict[str,Any])->Dict[str,List[Path]]:
    """
    Classify all non-audio, non-hidden files in folder into:
        keep       — moves with the album (jpg/jpeg/pdf/qualifying cue)
        quarantine — sent to Review/Artifacts/<folder_name>/
        ignore     — hidden/system files (already filtered)
        unknown    — unrecognised extensions → quarantine by default

    Returns dict with those four keys.
    """
    ac=cfg.get("artifacts",{})
    if not ac.get("enabled",True):
        return {"keep":[],"quarantine":[],"ignore":[],"unknown":[]}

    keep_exts   ={e.lower() for e in ac.get("keep_extensions",[".jpg",".jpeg",".pdf"])}
    smart_exts  ={e.lower() for e in ac.get("smart_extensions",[])}
    quar_exts   ={e.lower() for e in ac.get("quarantine_extensions",
                    [".nfo",".sfv",".txt",".url",".log",".m3u",".m3u8"])}
    min_dim     =int(ac.get("smart_png_min_dimension",800))
    quar_unknown=bool(ac.get("quarantine_unknown",True))
    audio_exts  ={".mp3",".flac",".m4a",".aiff",".wav",".ogg",".opus",".wma"}

    result:Dict[str,List[Path]]={"keep":[],"quarantine":[],"ignore":[],"unknown":[]}
    try:
        for p in folder.iterdir():
            if not p.is_file(): continue
            if is_hidden_file(p): result["ignore"].append(p); continue
            sfx=p.suffix.lower()
            if sfx in audio_exts: continue          # audio files handled elsewhere
            if sfx in keep_exts:
                # Special case: .cue — keep only if paired audio exists
                if sfx==".cue":
                    if _cue_has_paired_audio(p): result["keep"].append(p)
                    else: result["quarantine"].append(p)
                else:
                    result["keep"].append(p)
            elif sfx in smart_exts:
                # .png — always quarantine (policy: let go of all PNGs)
                result["quarantine"].append(p)
            elif sfx in quar_exts:
                result["quarantine"].append(p)
            else:
                if quar_unknown: result["quarantine"].append(p)
                else: result["unknown"].append(p)
    except PermissionError as e:
        out(f"  {C.DIM}Permission denied scanning artifacts: {e}{C.RESET}",level=VERBOSE)
    return result

def move_artifacts_to_quarantine(
    artifacts:Dict[str,List[Path]],
    folder_name:str,
    cfg:Dict[str,Any],
    profile_obj:Dict[str,Any],
    source_root:Path,
    dry_run:bool=False,
    session_id:str=""
)->int:
    """Move quarantine files to Review/Artifacts/<folder_name>/. Returns count moved."""
    files=artifacts.get("quarantine",[])
    if not files: return 0
    ac=cfg.get("artifacts",{})
    qfolder_rel=ac.get("quarantine_folder","Review/Artifacts")
    roots=ensure_roots(profile_obj,source_root)
    qfolder=roots["review_root"]/Path(qfolder_rel).parts[-1]/folder_name
    moved=0
    for f in files:
        if not f.exists(): continue
        if dry_run:
            out(f"    {C.DIM}[artifact dry-run]{C.RESET} quarantine: {f.name}")
            continue
        try:
            ensure_dir(qfolder)
            dst=qfolder/f.name
            if dst.exists(): dst=qfolder/(f.stem+"_"+uuid.uuid4().hex[:6]+f.suffix)
            shutil.move(str(f),str(dst))
            moved+=1
        except Exception as e:
            err(f"    artifact move failed ({f.name}): {e}")
    return moved


# ─────────────────────────────────────────────
# Duplicate resolution — merge missing tracks
# ─────────────────────────────────────────────

def merge_missing_tracks(
    missing_files:List[Path],
    existing_path:Path,
    cfg:Dict[str,Any],
    session_id:str,
    dry_run:bool=False
)->List[str]:
    """
    Copy missing tracks into existing_path and re-run track rename on the folder.
    Returns list of copied filenames.
    """
    copied=[]
    hist_path=Path(cfg.get("logging",{}).get("track_history_log","logs/track_history.jsonl"))
    for f in missing_files:
        dst=existing_path/f.name
        if dst.exists():
            # avoid collision
            dst=existing_path/(f.stem+"_merged"+f.suffix)
        if dry_run:
            out(f"    {C.DIM}[merge dry-run]{C.RESET} would copy: {f.name}")
            copied.append(f.name); continue
        try:
            shutil.copy2(str(f),str(dst))
            copied.append(f.name)
            append_jsonl(hist_path,{"action_id":uuid.uuid4().hex[:10],"timestamp":now_iso(),
                "session_id":session_id,"type":"track_merge","src":str(f),"dst":str(dst)})
        except Exception as e:
            err(f"    merge copy failed ({f.name}): {e}")
    return copied

