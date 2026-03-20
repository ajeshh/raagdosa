"""RaagDosa CLI — parser and main dispatch."""
from __future__ import annotations

import argparse, sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from raagdosa import APP_VERSION
from raagdosa.ui import C, out, err, VERBOSE, QUIET, set_verbosity
from raagdosa.config import load_config_with_paths, _validate_config
from raagdosa.session import load_last_session
from raagdosa.commands import (
    cmd_help, cmd_init, cmd_show, cmd_verify, cmd_learn, cmd_learn_crates,
    cmd_reference, cmd_dump_tree, cmd_orphans, cmd_artists, cmd_review_list,
    cmd_review_promote, cmd_diff, cmd_status, cmd_sessions, cmd_history,
    cmd_undo, cmd_template_list, cmd_template_show, cmd_genre, cmd_tree,
    cmd_catchall, cmd_cache, cmd_clean_report, cmd_extract_by_artist,
    cmd_compare_folders, cmd_report, cmd_doctor, cmd_scan, cmd_apply,
    cmd_go, cmd_folders_only, cmd_tracks_only, cmd_resume,
    profile_list, profile_show, profile_add, profile_set, profile_delete, profile_use,
    _parse_since,
)
from raagdosa.tags_cmd import cmd_tags_status, cmd_tags_review, cmd_tags_apply, cmd_tags_undo


def build_parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(prog="raagdosa",description=f"RaagDosa v{APP_VERSION} — deterministic music library cleanup.",
        formatter_class=argparse.RawDescriptionHelpFormatter,epilog="""
Examples:
  raagdosa init                                 # guided first-time setup
  raagdosa doctor                               # verify config, deps, disk
  raagdosa go --dry-run                         # preview — nothing moves
  raagdosa go                                   # scan + move + rename tracks
  raagdosa go --genre-roots "B,House,Techno"   # protect genre root folders
  raagdosa go --itunes                          # strip iTunes Genre/ layer first
  raagdosa tree /Volumes/music/Incoming        # snapshot the directory tree
  raagdosa tree --list                          # show all saved snapshots
  raagdosa tree --diff snap_a snap_b            # what changed between snapshots
  raagdosa catchall /path/to/_Dump             # group loose files by artist
  raagdosa genre add "Bass"                    # declare a persistent genre root
  raagdosa genre list                          # show all genre roots
  raagdosa sessions                            # review past sessions
  raagdosa undo --session last                 # undo a whole session
  raagdosa orphans                             # find loose audio files
  raagdosa artists --list                      # list all artists in Clean
  raagdosa review-list --older-than 30         # Review folders >30 days old
  raagdosa review-promote "Album Name"         # force VA→album re-evaluation
  raagdosa review-promote "Album" --artist "X" # force artist + re-evaluate
  raagdosa clean-report                        # stats on your Clean library
  raagdosa tags status                         # show tag proposal summary
  raagdosa tags review --risk safe             # review safe-tier proposals
  raagdosa tags apply --dry-run                # preview tag changes
""")
    p.add_argument("--config",default="config.yaml"); p.add_argument("--version",action="version",version=f"raagdosa {APP_VERSION}")
    p.add_argument("--verbose",action="store_true"); p.add_argument("--quiet",action="store_true")
    sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("help",help="Grouped command reference")
    sub.add_parser("init",help="Guided first-time setup")
    sp=sub.add_parser("profile",help="Manage profiles"); psub=sp.add_subparsers(dest="profile_cmd",required=True)
    psub.add_parser("list"); psub.add_parser("show").add_argument("name")
    add=psub.add_parser("add"); add.add_argument("name"); add.add_argument("--source",required=True)
    add.add_argument("--clean-mode",default="inside_root",choices=["inside_root","inside_parent"])
    add.add_argument("--clean-folder",default="Clean"); add.add_argument("--review-folder",default="Review")
    add.add_argument("--template",metavar="TEMPLATE",help="Builtin template ID or custom pattern (e.g. 'genre' or '{genre}/{artist}/{album}')")
    setp=psub.add_parser("set"); setp.add_argument("name"); setp.add_argument("--source"); setp.add_argument("--clean-mode"); setp.add_argument("--clean-folder"); setp.add_argument("--review-folder")
    setp.add_argument("--template",metavar="TEMPLATE",help="Builtin template ID or custom pattern")
    psub.add_parser("delete").add_argument("name"); psub.add_parser("use").add_argument("name")
    # Template commands
    tp=sub.add_parser("template",help="Library organisation templates"); tsub=tp.add_subparsers(dest="template_cmd",required=True)
    tsub.add_parser("list",help="List all builtin templates"); tsub.add_parser("show",help="Show template details and example tree").add_argument("name")
    sc=sub.add_parser("scan",help="Scan → proposals.json"); sc.add_argument("--profile"); sc.add_argument("--out"); sc.add_argument("--since")
    sc.add_argument("--genre-roots",metavar="ROOTS",help="Comma-separated genre root folder names (session-only)")
    sc.add_argument("--itunes",action="store_true",help="Strip iTunes Genre/ layer before scanning")
    sc.add_argument("--session-name",metavar="NAME",help="Human-friendly session name (e.g. 'Bandcamp Friday')")
    ap=sub.add_parser("apply",help="Apply proposals.json"); ap.add_argument("proposals",nargs="?"); ap.add_argument("--last-session",action="store_true"); ap.add_argument("--interactive",action="store_true"); ap.add_argument("--auto-above",type=float); ap.add_argument("--dry-run",action="store_true")
    for nc in ("run","go"):
        c=sub.add_parser(nc,help="Scan + apply (folders + tracks)")
        c.add_argument("--profile"); c.add_argument("--interactive",action="store_true")
        c.add_argument("--dry-run",action="store_true"); c.add_argument("--since")
        c.add_argument("--performance",choices=["slow","medium","fast","ultra"],default=None,metavar="TIER",help="Hardware tier: slow|medium|fast|ultra")
        c.add_argument("--genre-roots",metavar="ROOTS",help="Comma-separated genre root folder names (session-only)")
        c.add_argument("--itunes",action="store_true",help="Strip iTunes Genre/ layer before scanning")
        c.add_argument("--threshold",type=float,metavar="SCORE",help="Interactive: only review folders below this confidence score")
        c.add_argument("--sort",choices=["name","date-created","date-modified","confidence","confidence-desc"],default="name",help="Sort order: name, date-created, date-modified, confidence (hardest first), confidence-desc (easiest first)")
        c.add_argument("--force",action="store_true",help="Nuclear option: bypass triage, process all folders without confirmation (original streaming behaviour)")
        c.add_argument("--auto-above",type=float,metavar="SCORE",dest="auto_above",help="Override auto-approve threshold for triage (default: review_rules.auto_approve_threshold)")
        c.add_argument("--session-name",metavar="NAME",help="Human-friendly session name (e.g. 'Bandcamp Friday')")
    fo=sub.add_parser("folders",help="Folder pass only"); fo.add_argument("--profile"); fo.add_argument("--interactive",action="store_true"); fo.add_argument("--dry-run",action="store_true"); fo.add_argument("--since")
    fo.add_argument("--genre-roots",metavar="ROOTS"); fo.add_argument("--itunes",action="store_true")
    fo.add_argument("--sort",choices=["name","date-created","date-modified","confidence","confidence-desc"],default="name",help="Sort order")
    tr=sub.add_parser("tracks",help="Track rename pass"); tr.add_argument("--profile"); tr.add_argument("--interactive",action="store_true"); tr.add_argument("--dry-run",action="store_true")
    sub.add_parser("status",help="Library overview").add_argument("--profile")
    rs=sub.add_parser("resume",help="Resume interrupted session"); rs.add_argument("session_id"); rs.add_argument("--interactive",action="store_true"); rs.add_argument("--dry-run",action="store_true")
    sh=sub.add_parser("show",help="Debug a single folder"); sh.add_argument("folder"); sh.add_argument("--profile"); sh.add_argument("--tracks",action="store_true",help="Also show per-track rename preview")
    sub.add_parser("verify",help="Audit Clean library health").add_argument("--profile")
    le=sub.add_parser("learn",help="Suggest config improvements"); le.add_argument("--session")
    lc=sub.add_parser("learn-crates",help="Discover crate naming patterns from existing folders")
    lc.add_argument("path",help="Directory to scan for crate patterns")
    lc.add_argument("--min-tracks",type=int,default=3,help="Minimum audio files to consider a folder (default: 3)")
    rp=sub.add_parser("report",help="View session report"); rp.add_argument("--session"); rp.add_argument("--format",default="txt",choices=["txt","csv","html"])
    sub.add_parser("doctor",help="Check config, deps, disk, DJ databases")
    hi=sub.add_parser("history",help="Show history"); hi.add_argument("--last",type=int,default=50); hi.add_argument("--session"); hi.add_argument("--match"); hi.add_argument("--tracks",action="store_true")
    un=sub.add_parser("undo",help="Undo moves or renames"); un.add_argument("--id"); un.add_argument("--session"); un.add_argument("--last",action="store_true",help="Undo last session (shortcut for --session last)"); un.add_argument("--from-path"); un.add_argument("--tracks",action="store_true"); un.add_argument("--folder")
    se=sub.add_parser("sessions",help="List recent sessions with move counts"); se.add_argument("--last",type=int,default=20)

    # dump-tree command (legacy)
    dt_p = sub.add_parser("dump-tree", help="Export raw folder/file tree to a text file")
    dt_p.add_argument("--profile")
    dt_p.add_argument("--out", required=True, help="Output text file path")
    dt_p.add_argument("--include-clean", action="store_true")
    dt_p.add_argument("--include-review", action="store_true")
    dt_p.add_argument("--include-logs", action="store_true")
    dt_p.add_argument("--folders-only", action="store_true")
    dt_p.add_argument("--files-only", action="store_true")

    # v4.1 — tree command
    tr_p = sub.add_parser("tree", help="Snapshot a directory tree to logs/trees/")
    tr_p.add_argument("path", nargs="?", help="Path to scan (omit with --list or --diff)")
    tr_p.add_argument("--audio-only", action="store_true", help="Strip non-audio files from output")
    tr_p.add_argument("--depth", type=int, default=None, metavar="N", help="Limit recursion depth")
    tr_p.add_argument("--list", dest="list_mode", action="store_true", help="Show all saved snapshots")
    tr_p.add_argument("--diff", nargs=2, metavar=("A", "B"), help="Diff two snapshots")

    # v4.1 — catchall command
    ca_p = sub.add_parser("catchall", help="Group loose files in a dump folder by artist")
    ca_p.add_argument("path", help="Path to the flat dump folder")
    ca_p.add_argument("--profile")
    ca_p.add_argument("--dry-run", action="store_true")
    ca_p.add_argument("--genre-roots", metavar="ROOTS")

    # v4.1 — genre command
    gn_p = sub.add_parser("genre", help="Manage persistent genre root declarations")
    gn_sub = gn_p.add_subparsers(dest="genre_cmd", required=True)
    gn_sub.add_parser("list", help="List all declared genre roots")
    gn_add = gn_sub.add_parser("add", help="Declare a folder as a genre root")
    gn_add.add_argument("name", help="Folder name to protect")
    gn_rem = gn_sub.add_parser("remove", help="Remove a genre root declaration")
    gn_rem.add_argument("name")
    gn_sub.add_parser("clear", help="Remove all genre root declarations")
    gn_show = gn_sub.add_parser("show", help="Check if a folder is a declared genre root")
    gn_show.add_argument("name")

    # v3.5 commands
    ar=sub.add_parser("artists",help="List or find artists in Clean library"); ar.add_argument("--profile")
    ar.add_argument("--list",dest="list_mode",action="store_true",help="List all artists")
    ar.add_argument("--find",dest="find_query",metavar="QUERY",help="Fuzzy-find an artist")
    rl=sub.add_parser("review-list",help="Summarise Review folder contents"); rl.add_argument("--profile")
    rl.add_argument("--older-than",type=int,metavar="DAYS",help="Only show folders older than N days")
    rp=sub.add_parser("review-promote",help="Re-evaluate a Review folder as album (not VA)")
    rp.add_argument("folder",help="Folder name or path in Review/"); rp.add_argument("--profile")
    rp.add_argument("--artist",metavar="NAME",help="Force artist name for re-evaluation")
    rp.add_argument("--dry-run",action="store_true",help="Preview only — don't move")
    sub.add_parser("clean-report",help="Stats and health report for Clean library").add_argument("--profile")
    ex=sub.add_parser("extract",help="Extract tracks from a VA/mix folder"); ex.add_argument("folder"); ex.add_argument("--profile")
    ex.add_argument("--by-artist",action="store_true",required=True,help="Group tracks by artist tag")
    ex.add_argument("--dry-run",action="store_true")
    cp=sub.add_parser("compare",help="Compare two folders"); cp.add_argument("--folder",nargs=2,metavar="FOLDER",required=True)
    df=sub.add_parser("diff",help="Diff two session reports"); df.add_argument("session_a",metavar="SESSION_A"); df.add_argument("session_b",metavar="SESSION_B")
    ca=sub.add_parser("cache",help="Manage the tag cache (status/clear/evict)")
    ca.add_argument("action",nargs="?",default="status",choices=["status","clear","evict"])
    sub.add_parser("orphans",help="Find loose audio files in Clean/Review").add_argument("--profile")
    # v7.0 — reference (musical reference) commands
    ref_p=sub.add_parser("reference",help="Manage the musical reference (aliases, labels, patterns)")
    ref_sub=ref_p.add_subparsers(dest="ref_cmd",required=True)
    ref_sub.add_parser("list",help="Show reference contents summary")
    ref_imp=ref_sub.add_parser("import",help="Import a reference file")
    ref_imp.add_argument("file",help="Path to reference YAML file to import")
    ref_exp=ref_sub.add_parser("export",help="Export reference to shareable file")
    ref_exp.add_argument("--section",help="Export only this section (e.g. artist_aliases)")
    ref_exp.add_argument("--out",metavar="FILE",help="Output file path (default: reference_export.yaml)")
    # v10.0 — tags command
    tg_p=sub.add_parser("tags",help="Review, apply, and undo tag fixes from scanner proposals")
    tg_sub=tg_p.add_subparsers(dest="tags_cmd",required=True)
    tg_status=tg_sub.add_parser("status",help="Show proposal summary by risk tier and status")
    tg_status.add_argument("--db",metavar="PATH",help="Path to scanner database")
    tg_review=tg_sub.add_parser("review",help="Interactively review pending proposals")
    tg_review.add_argument("--db",metavar="PATH",help="Path to scanner database")
    tg_review.add_argument("--folder",metavar="PATH",help="Only review proposals for this folder")
    tg_review.add_argument("--risk",choices=["safe","moderate","destructive"],help="Only review proposals of this risk tier")
    tg_review.add_argument("--fix-type",metavar="TYPE",help="Only review proposals of this fix type")
    tg_review.add_argument("--auto",action="store_true",help="Auto-accept proposals above threshold for safe risk tiers")
    tg_review.add_argument("--include-protected",action="store_true",help="Include proposals for protected fields (e.g. title)")
    tg_apply=tg_sub.add_parser("apply",help="Apply accepted proposals to audio files")
    tg_apply.add_argument("--db",metavar="PATH",help="Path to scanner database")
    tg_apply.add_argument("--dry-run",action="store_true",help="Preview changes without writing")
    tg_apply.add_argument("--max-batch",type=int,metavar="N",help="Override max batch size")
    tg_undo=tg_sub.add_parser("undo",help="Revert applied tag changes")
    tg_undo.add_argument("--db",metavar="PATH",help="Path to scanner database")
    tg_undo.add_argument("--session",metavar="ID",help="Undo a specific apply session")
    tg_undo.add_argument("--last",action="store_true",help="Undo the most recent apply session")
    return p


def _parse_genre_roots_arg(roots_str: Optional[str]) -> Optional[List[str]]:
    """Parse --genre-roots "A,B,C" into a list of stripped strings."""
    if not roots_str: return None
    return [r.strip() for r in roots_str.split(",") if r.strip()]


def main()->None:
    parser=build_parser(); args=parser.parse_args()
    if getattr(args,"verbose",False): set_verbosity(VERBOSE)
    elif getattr(args,"quiet",False): set_verbosity(QUIET)
    cfg_path=Path(args.config); cmd=args.cmd
    if cmd=="help": cmd_help(); return
    if cmd=="init": cmd_init(cfg_path); return
    cfg=load_config_with_paths(cfg_path)
    cfg["_cfg_path"]=str(cfg_path)  # runtime-only, for learning features
    _validate_config(cfg)
    # Human-friendly check: warn if no profiles have source_root configured
    profiles_cfg=cfg.get("profiles",{})
    has_source=any(isinstance(p,dict) and "source_root" in p for p in profiles_cfg.values())
    if not has_source:
        paths_file=cfg_path.parent/"paths.local.yaml"
        example_file=cfg_path.parent/"paths.local.example.yaml"
        if not paths_file.exists():
            err(f"No source paths configured — paths.local.yaml not found.")
            out(f"  To get started:")
            if example_file.exists():
                out(f"    cp {example_file} {paths_file}")
            else:
                out(f"    Create {paths_file} with your source_root paths.")
            out(f"    Then edit it to point at your music folders.")
            sys.exit(2)
    def gp()->str:
        p=getattr(args,"profile",None) or cfg.get("active_profile")
        if not p: raise ValueError("No profile specified and no active_profile set.")
        return p
    if cmd=="profile":
        pc=args.profile_cmd
        if pc=="list":    profile_list(cfg)
        elif pc=="show":  profile_show(cfg,args.name)
        elif pc=="add":   profile_add(cfg_path,cfg,args.name,args.source,args.clean_mode,args.clean_folder,args.review_folder,template=getattr(args,"template",None))
        elif pc=="set":   profile_set(cfg_path,cfg,args.name,args.source,args.clean_mode,args.clean_folder,args.review_folder,template=getattr(args,"template",None))
        elif pc=="delete": profile_delete(cfg_path,cfg,args.name)
        elif pc=="use":   profile_use(cfg_path,cfg,args.name)
    elif cmd=="template":
        tc=args.template_cmd
        if tc=="list":   cmd_template_list(cfg)
        elif tc=="show": cmd_template_show(cfg,args.name)
    elif cmd=="scan":
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_scan(cfg_path,cfg,gp(),args.out,getattr(args,"since",None),genre_roots=gr,itunes_mode=bool(getattr(args,"itunes",False)),session_name=getattr(args,"session_name","") or "")
    elif cmd=="apply":
        pp=load_last_session(cfg) if args.last_session else (Path(args.proposals) if args.proposals else None)
        if not pp: err("Provide proposals.json or --last-session"); sys.exit(1)
        cmd_apply(cfg,pp,interactive=bool(args.interactive),auto_above=args.auto_above,dry_run=bool(args.dry_run))
    elif cmd in("run","go"):
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_go(cfg_path,cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run),since=getattr(args,"since",None),perf_tier=getattr(args,"performance",None),genre_roots=gr,itunes_mode=bool(getattr(args,"itunes",False)),review_threshold=getattr(args,"threshold",None),sort_by=getattr(args,"sort","name"),force=bool(getattr(args,"force",False)),auto_above=getattr(args,"auto_above",None),session_name=getattr(args,"session_name","") or "")
    elif cmd=="folders":
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_folders_only(cfg_path,cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run),since=getattr(args,"since",None),genre_roots=gr,itunes_mode=bool(getattr(args,"itunes",False)),sort_by=getattr(args,"sort","name"))
    elif cmd=="tracks":   cmd_tracks_only(cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run))
    elif cmd=="status":   cmd_status(cfg,gp())
    elif cmd=="resume":   cmd_resume(cfg,args.session_id,interactive=bool(args.interactive),dry_run=bool(args.dry_run))
    elif cmd=="show":     cmd_show(cfg,args.folder,getattr(args,"profile",None) or cfg.get("active_profile",""),show_tracks=bool(getattr(args,"tracks",False)))
    elif cmd=="verify":   cmd_verify(cfg,gp())
    elif cmd=="learn":    cmd_learn(cfg_path,cfg,getattr(args,"session",None))
    elif cmd=="learn-crates": cmd_learn_crates(cfg_path,cfg,args.path,min_tracks=getattr(args,"min_tracks",3))
    elif cmd=="report":   cmd_report(cfg,getattr(args,"session",None),args.format)
    elif cmd=="doctor":   cmd_doctor(cfg_path,cfg)
    elif cmd=="history":  cmd_history(cfg,last=args.last,session=args.session,match=args.match,tracks=bool(args.tracks))
    elif cmd=="undo":     cmd_undo(cfg,action_id=args.id,session_id=("last" if getattr(args,"last",False) else args.session),from_path=args.from_path,tracks=bool(args.tracks),folder=args.folder)
    elif cmd=="sessions": cmd_sessions(cfg,last=getattr(args,"last",20))
    elif cmd=="dump-tree":
        cmd_dump_tree(
            cfg,
            gp(),
            args.out,
            include_clean=args.include_clean,
            include_review=args.include_review,
            include_logs=args.include_logs,
            folders_only=args.folders_only,
            files_only=args.files_only,
        )

    # v4.1 commands
    elif cmd=="tree":
        cmd_tree(cfg,path_str=getattr(args,"path","") or "",audio_only=bool(getattr(args,"audio_only",False)),
                 depth=getattr(args,"depth",None),list_mode=bool(getattr(args,"list_mode",False)),
                 diff_a=(getattr(args,"diff",None) or [None,None])[0],
                 diff_b=(getattr(args,"diff",None) or [None,None])[1])
    elif cmd=="catchall":
        gr=_parse_genre_roots_arg(getattr(args,"genre_roots",None))
        cmd_catchall(cfg,args.path,getattr(args,"profile",None) or cfg.get("active_profile",""),
                     dry_run=bool(getattr(args,"dry_run",False)),genre_roots=gr)
    elif cmd=="genre":
        cmd_genre(cfg_path,cfg,action=args.genre_cmd,name=getattr(args,"name",None))
    # v3.5 commands
    elif cmd=="orphans":      cmd_orphans(cfg,gp())
    elif cmd=="artists":      cmd_artists(cfg,gp(),list_mode=bool(getattr(args,"list_mode",False)),find_query=getattr(args,"find_query",None))
    elif cmd=="review-list":  cmd_review_list(cfg,gp(),older_than_days=getattr(args,"older_than",None))
    elif cmd=="review-promote": cmd_review_promote(cfg,gp(),args.folder,dry_run=bool(getattr(args,"dry_run",False)),artist_override=getattr(args,"artist",None))
    elif cmd=="clean-report": cmd_clean_report(cfg,gp())
    elif cmd=="extract":      cmd_extract_by_artist(cfg,gp(),args.folder,dry_run=bool(getattr(args,"dry_run",False)))
    elif cmd=="compare":      cmd_compare_folders(cfg,args.folder[0],args.folder[1])
    elif cmd=="diff":         cmd_diff(cfg,args.session_a,args.session_b)
    elif cmd=="cache":        cmd_cache(cfg,args.action)
    elif cmd=="reference":
        rc=args.ref_cmd
        cmd_reference(cfg_path,cfg,action=rc,
                      import_path=getattr(args,"file",None),
                      section=getattr(args,"section",None),
                      export_path=getattr(args,"out",None))
    elif cmd=="tags":
        tc=args.tags_cmd
        db_path=getattr(args,"db",None) or cfg.get("tag_fix",{}).get("scanner_db","raagdosa_scanner.db")
        if tc=="status":  cmd_tags_status(cfg,db_path)
        elif tc=="review": cmd_tags_review(cfg,db_path,folder=getattr(args,"folder",None),risk=getattr(args,"risk",None),fix_type=getattr(args,"fix_type",None),auto=bool(getattr(args,"auto",False)),include_protected=bool(getattr(args,"include_protected",False)))
        elif tc=="apply":  cmd_tags_apply(cfg,db_path,dry_run=bool(getattr(args,"dry_run",False)),max_batch=getattr(args,"max_batch",None))
        elif tc=="undo":   cmd_tags_undo(cfg,db_path,session_id=getattr(args,"session",None),last=bool(getattr(args,"last",False)))
    else: parser.error(f"Unknown: {cmd}")


