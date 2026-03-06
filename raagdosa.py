#!/usr/bin/env python3
"""
RaagDosa v3.0
Deterministic library cleanup for DJ music folders — CLI-first, safe-by-default, undoable.

Commands:
  go / run / scan / apply / folders / tracks / resume
  show / verify / learn / init / status / report
  profile / history / undo / doctor
"""
from __future__ import annotations

import argparse, csv, dataclasses, datetime as dt, hashlib, html
import json, os, platform, re, shutil, signal, sys, unicodedata, uuid
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

APP_VERSION = "3.0"

# ─────────────────────────────────────────────
# Output / colour / verbosity
# ─────────────────────────────────────────────
_IS_TTY = sys.stdout.isatty() and sys.stderr.isatty()

class C:
    if _IS_TTY:
        RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
        GREEN="\033[32m"; YELLOW="\033[33m"; RED="\033[31m"
        CYAN="\033[36m"; BLUE="\033[34m"; MAGENTA="\033[35m"
    else:
        RESET=BOLD=DIM=GREEN=YELLOW=RED=CYAN=BLUE=MAGENTA=""

QUIET=0; NORMAL=1; VERBOSE=2
_verbosity=NORMAL

def set_verbosity(v:int)->None:
    global _verbosity; _verbosity=v

def out(msg:str,level:int=NORMAL,file=None)->None:
    if _verbosity>=level: print(msg,file=file or sys.stdout)

def err(msg:str)->None:
    print(f"{C.RED}{msg}{C.RESET}",file=sys.stderr)

def warn(msg:str)->None:
    if _verbosity>=QUIET: print(f"{C.YELLOW}⚠  {msg}{C.RESET}")

def ok_msg(msg:str)->None:
    out(f"{C.GREEN}✓  {msg}{C.RESET}")

def status_tag(dest:str)->str:
    m={"clean":f"{C.GREEN}[CLEAN ]{C.RESET}","review":f"{C.YELLOW}[REVIEW]{C.RESET}","duplicate":f"{C.RED}[DUPE  ]{C.RESET}"}
    return m.get(dest,f"[{dest.upper()[:6]:6}]")

def conf_color(c:float)->str:
    if c>=0.90: return f"{C.GREEN}{c:.2f}{C.RESET}"
    if c>=0.75: return f"{C.YELLOW}{c:.2f}{C.RESET}"
    return f"{C.RED}{c:.2f}{C.RESET}"

# ─────────────────────────────────────────────
# Progress bar
# ─────────────────────────────────────────────
class Progress:
    def __init__(self,total:int,label:str="Scanning"):
        self.total=total; self.current=0; self.label=label
        self._active=_IS_TTY and _verbosity>=NORMAL
    def tick(self,msg:str="")->None:
        self.current+=1
        if not self._active: return
        pct=int(self.current/max(self.total,1)*100)
        bw=22; filled=int(bw*self.current/max(self.total,1))
        bar="█"*filled+"░"*(bw-filled)
        line=f"\r{C.CYAN}{self.label}{C.RESET} [{bar}] {self.current}/{self.total} {pct}%  {C.DIM}{msg[:35]:<35}{C.RESET}"
        print(line,end="",flush=True)
    def done(self)->None:
        if self._active: print()

# ─────────────────────────────────────────────
# Graceful stop (SIGINT)
# ─────────────────────────────────────────────
_stop_after_current=False
_force_stop=False
_sigint_count=0

def _sigint_handler(sig,frame)->None:
    global _stop_after_current,_force_stop,_sigint_count
    _sigint_count+=1
    if _sigint_count==1:
        _stop_after_current=True
        print(f"\n{C.YELLOW}⚡ Ctrl+C — finishing current folder then stopping. Press again to force quit.{C.RESET}")
    else:
        _force_stop=True
        print(f"\n{C.RED}⚡ Force stop.{C.RESET}"); sys.exit(130)

def register_stop_handler()->None:
    signal.signal(signal.SIGINT,_sigint_handler)

def should_stop()->bool:
    return _stop_after_current or _force_stop

# ─────────────────────────────────────────────
# Core utilities
# ─────────────────────────────────────────────
def now_iso()->str: return dt.datetime.now().isoformat(timespec="seconds")

def make_session_id()->str:
    return f"{dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')}_{uuid.uuid4().hex[:6]}"

def read_yaml(path:Path)->Dict[str,Any]:
    if yaml is None: raise RuntimeError("Missing: pyyaml — pip install pyyaml")
    if not path.exists(): raise FileNotFoundError(f"Config not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

def write_yaml(path:Path,cfg:Dict[str,Any])->None:
    if yaml is None: raise RuntimeError("Missing: pyyaml — pip install pyyaml")
    path.write_text(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True),encoding="utf-8")

def ensure_dir(path:Path)->None: path.mkdir(parents=True,exist_ok=True)
def write_json(path:Path,obj:Any)->None: path.write_text(json.dumps(obj,indent=2,ensure_ascii=False),encoding="utf-8")
def read_json(path:Path)->Any: return json.loads(path.read_text(encoding="utf-8"))

def append_jsonl(path:Path,obj:Dict[str,Any])->None:
    ensure_dir(path.parent)
    with path.open("a",encoding="utf-8") as f: f.write(json.dumps(obj,ensure_ascii=False)+"\n")

def iter_jsonl(path:Path)->List[Dict[str,Any]]:
    if not path.exists(): return []
    out_list:List[Dict[str,Any]]=[]
    with path.open("r",encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try: out_list.append(json.loads(line))
            except Exception: continue
    return out_list

def normalize_unicode(s:str)->str: return unicodedata.normalize("NFC",s or "")
def lower(s:Optional[str])->str: return (s or "").strip().lower()

def is_hidden_file(p:Path)->bool:
    n=p.name.lower()
    return n in {".ds_store","thumbs.db","desktop.ini",".localized"} or n.startswith("._") or n.startswith("__macosx")

def parse_int_prefix(s:str)->Optional[int]:
    m=re.match(r"^\s*(\d+)",s or "")
    return int(m.group(1)) if m else None

def sanitize_name(name:str,repl:str=" - ",trim:bool=True)->str:
    name=re.sub(r"[\/\\]"," ",name)
    name=re.sub(r'[\:\*\?\"\<\>\|]',repl,name)
    name=re.sub(r"\s+"," ",name).strip()
    return name.rstrip(". ").strip() if trim else name

def folder_mtime(path:Path)->float:
    try: return path.stat().st_mtime
    except Exception: return 0.0

# ─────────────────────────────────────────────
# Artist normalisation
# ─────────────────────────────────────────────
def normalize_artist_name(name:str,cfg:Dict[str,Any])->str:
    """
    Full normalisation pipeline:
    1. Unicode NFC
    2. Optional unicode char map  (MØ → MO etc — user-defined)
    3. ALL-CAPS → Title Case
    4. Alias map lookup  (Jay Z / JAYZ / jay-z → Jay-Z)
    5. Hyphen variant normalisation
    6. "The" prefix policy: keep-front | move-to-end | strip
    """
    if not name: return name
    acfg=cfg.get("artist_normalization",{})
    if not acfg.get("enabled",True): return name

    s=normalize_unicode(name.strip())

    # Unicode char map (opt-in, e.g. {"Ø":"O","ø":"o"})
    for src_c,dst_c in (acfg.get("unicode_map",{}) or {}).items():
        s=s.replace(src_c,dst_c)

    # ALL CAPS → Title Case
    words=s.split()
    if words and all(w.isupper() for w in words if len(w)>2):
        small={"a","an","the","and","but","or","for","of","in","at","to","by","vs"}
        s=" ".join(w.capitalize() if i==0 or w.lower() not in small else w.lower() for i,w in enumerate(words))

    # Alias map (case-insensitive exact match wins immediately)
    aliases:Dict[str,str]=acfg.get("aliases",{}) or {}
    for alias_key,canonical in aliases.items():
        if alias_key.lower()==s.lower(): return canonical

    # Hyphen variants → ASCII hyphen
    if acfg.get("normalize_hyphens",True):
        s=re.sub(r"[\u2013\u2014\u2010\u2212]","-",s)

    # "The" prefix
    the_policy=acfg.get("the_prefix","keep-front")
    m=re.match(r"^[Tt]he\s+(.+)$",s)
    if m:
        base=m.group(1)
        if the_policy=="move-to-end": s=f"{base}, The"
        elif the_policy=="strip":     s=base
        # keep-front → no change

    return s

def artists_are_same(a:str,b:str,cfg:Dict[str,Any])->bool:
    thresh=float(cfg.get("artist_normalization",{}).get("fuzzy_dedup_threshold",0.92))
    an=normalize_unicode(a.strip().lower()); bn=normalize_unicode(b.strip().lower())
    if an==bn: return True
    ac=re.sub(r"^the\s+","",an); bc=re.sub(r"^the\s+","",bn)
    if ac==bc: return True
    aset=set(ac.split()); bset=set(bc.split())
    if not aset or not bset: return False
    return len(aset&bset)/len(aset|bset)>=thresh

# ─────────────────────────────────────────────
# Library path template
# ─────────────────────────────────────────────
def resolve_library_path(base:Path,artist:str,album:str,year:Optional[int],
                          is_flac_only:bool,is_va:bool,is_single:bool,cfg:Dict[str,Any])->Path:
    lib=cfg.get("library",{})
    template=lib.get("template","{artist}/{album}")
    va_folder=lib.get("va_folder","_Various Artists")
    singles_folder=lib.get("singles_folder","_Singles")
    unknown=lib.get("unknown_artist_label","_Unknown")
    flac_seg=bool(lib.get("flac_segregation",False))

    artist_c=sanitize_name(artist or unknown)
    album_c =sanitize_name(album  or "_Untitled")
    album_y =f"{album_c} ({year})" if year else album_c

    if is_va:   return base/va_folder/album_y
    if is_single: return base/artist_c/singles_folder
    if flac_seg and is_flac_only: return base/artist_c/"FLAC"/album_c

    try:
        sub=template.format(artist=artist_c,album=album_c,year=year or "",album_year=album_y)
    except KeyError:
        sub=f"{artist_c}/{album_c}"
    return base/sub

# ─────────────────────────────────────────────
# Config validation
# ─────────────────────────────────────────────
def validate_config(cfg:Dict[str,Any])->List[str]:
    warns:List[str]=[]
    cv=str(cfg.get("app",{}).get("version",""))
    if cv and cv!=APP_VERSION:
        warns.append(f"Config version '{cv}' (script is v{APP_VERSION}). Run 'raagdosa init --update' to review new options.")
    for sec in ["scan","decision","review_rules","year","track_rename","logging"]:
        if sec not in cfg: warns.append(f"Missing section '{sec}' — defaults used.")
    ap=cfg.get("active_profile")
    if not ap: warns.append("active_profile not set.")
    elif ap not in cfg.get("profiles",{}): warns.append(f"active_profile '{ap}' not found.")
    conf=cfg.get("review_rules",{}).get("min_confidence_for_clean",0.85)
    if not(0.0<float(conf)<=1.0): warns.append(f"min_confidence_for_clean={conf} out of range (0,1].")
    return warns

# ─────────────────────────────────────────────
# Manifest — persistent Clean index
# ─────────────────────────────────────────────
def _mfpath(cfg:Dict[str,Any])->Path:
    return Path(cfg.get("logging",{}).get("root_dir","logs"))/"clean_manifest.json"

def read_manifest(cfg:Dict[str,Any])->Dict[str,Any]:
    p=_mfpath(cfg)
    if not p.exists(): return {"version":APP_VERSION,"last_run":None,"entries":{}}
    try: return read_json(p)
    except Exception: return {"version":APP_VERSION,"last_run":None,"entries":{}}

def write_manifest(cfg:Dict[str,Any],m:Dict[str,Any])->None:
    ensure_dir(_mfpath(cfg).parent); write_json(_mfpath(cfg),m)

def manifest_add(cfg:Dict[str,Any],name:str,entry:Dict[str,Any])->None:
    m=read_manifest(cfg); m["entries"][normalize_unicode(name)]={**entry,"committed_at":now_iso()}; write_manifest(cfg,m)

def manifest_has(cfg:Dict[str,Any],name:str)->bool:
    return normalize_unicode(name) in read_manifest(cfg).get("entries",{})

def manifest_set_last_run(cfg:Dict[str,Any])->None:
    m=read_manifest(cfg); m["last_run"]=now_iso(); write_manifest(cfg,m)

def manifest_get_last_run(cfg:Dict[str,Any])->Optional[dt.datetime]:
    ts=read_manifest(cfg).get("last_run")
    if not ts: return None
    try: return dt.datetime.fromisoformat(ts)
    except Exception: return None

# ─────────────────────────────────────────────
# Proposal path validation (anti-traversal)
# ─────────────────────────────────────────────
def validate_proposal_paths(raw_props:List[Dict],allowed_roots:List[Path])->List[str]:
    viols:List[str]=[]; resolved=[r.resolve() for r in allowed_roots]
    for p in raw_props:
        t=Path(p.get("target_path",""))
        try:
            rt=t.resolve()
            if not any(str(rt).startswith(str(r)) for r in resolved):
                viols.append(f"Target escapes allowed roots: {t}")
        except Exception as e:
            viols.append(f"Cannot resolve {t}: {e}")
    return viols

# ─────────────────────────────────────────────
# Disk + file safety
# ─────────────────────────────────────────────
def file_checksum(path:Path,algo:str="md5")->str:
    h=hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda:f.read(65536),b""): h.update(chunk)
    return h.hexdigest()

def get_folder_size(path:Path)->int:
    total=0
    try:
        for p in path.rglob("*"):
            if p.is_file():
                try: total+=p.stat().st_size
                except Exception: pass
    except Exception: pass
    return total

def is_file_locked(path:Path)->bool:
    try:
        with path.open("a+b"): pass
        return False
    except (IOError,OSError,PermissionError): return True

def check_folder_locked(folder:Path,exts:List[str])->List[Path]:
    locked:List[Path]=[]
    try:
        for p in folder.rglob("*"):
            if p.is_file() and p.suffix.lower() in exts and is_file_locked(p): locked.append(p)
    except Exception: pass
    return locked

def check_path_length(path:Path,limit:int=260)->bool: return len(str(path))<=limit

def safe_move_folder(src:Path,dst:Path,use_checksum:bool=False)->None:
    shutil.copytree(str(src),str(dst))
    sf=sorted([f for f in src.rglob("*") if f.is_file()])
    df=sorted([f for f in dst.rglob("*") if f.is_file()])
    if len(sf)!=len(df):
        shutil.rmtree(str(dst),ignore_errors=True)
        raise RuntimeError(f"File count mismatch: src={len(sf)} dst={len(df)}")
    ss=sum(f.stat().st_size for f in sf); ds=sum(f.stat().st_size for f in df)
    if ss!=ds:
        shutil.rmtree(str(dst),ignore_errors=True)
        raise RuntimeError(f"Size mismatch: src={ss:,} dst={ds:,} bytes")
    if use_checksum:
        for s,d in zip(sf,df):
            if file_checksum(s)!=file_checksum(d):
                shutil.rmtree(str(dst),ignore_errors=True)
                raise RuntimeError(f"Checksum mismatch: {s.name}")
    shutil.rmtree(str(src))

# ─────────────────────────────────────────────
# DJ database detection
# ─────────────────────────────────────────────
_DJ_PATTERNS=["export.pdb","database2","rekordbox.xml","_Serato_","Serato Scratch","Serato DJ","PIONEER"]

def find_dj_databases(source_root:Path)->List[str]:
    found:List[str]=[]
    for pat in _DJ_PATTERNS:
        try: matches=list(source_root.rglob(f"*{pat}*"))
        except Exception: matches=[]
        if matches: found.append(f"'{pat}' ({len(matches)})")
    return found

# ─────────────────────────────────────────────
# Log rotation
# ─────────────────────────────────────────────
def rotate_log_if_needed(log_path:Path,max_mb:float=10.0)->None:
    if not log_path.exists(): return
    if log_path.stat().st_size/(1024*1024)>max_mb:
        archive=log_path.with_suffix(f".{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.archive.jsonl")
        log_path.rename(archive)
        out(f"[log rotation] Archived {log_path.name} → {archive.name}",level=VERBOSE)

# ─────────────────────────────────────────────
# Roots
# ─────────────────────────────────────────────
def _rname(profile:Dict,key:str,default:str)->str: return profile.get(key,default)

def derive_clean_root(profile:Dict,source_root:Path)->Path:
    base=source_root.parent if profile.get("clean_mode")=="inside_parent" else source_root
    return base/_rname(profile,"clean_folder_name","Clean")

def derive_review_root(profile:Dict,source_root:Path)->Path:
    base=source_root.parent if profile.get("clean_mode")=="inside_parent" else source_root
    return base/_rname(profile,"review_folder_name","Review")

def derive_clean_albums_root(profile:Dict,source_root:Path)->Path:
    return derive_clean_root(profile,source_root)/_rname(profile,"clean_albums_folder_name","Albums")

def derive_review_albums_root(profile:Dict,source_root:Path)->Path:
    return derive_review_root(profile,source_root)/_rname(profile,"review_albums_folder_name","Albums")

def derive_duplicates_root(profile:Dict,source_root:Path)->Path:
    return derive_review_root(profile,source_root)/_rname(profile,"duplicates_folder_name","Duplicates")

def ensure_roots(profile:Dict,source_root:Path)->Dict[str,Path]:
    roots={
        "clean_root":    derive_clean_root(profile,source_root),
        "review_root":   derive_review_root(profile,source_root),
        "clean_albums":  derive_clean_albums_root(profile,source_root),
        "clean_tracks":  derive_clean_root(profile,source_root)/_rname(profile,"clean_tracks_folder_name","Tracks"),
        "review_albums": derive_review_albums_root(profile,source_root),
        "duplicates":    derive_duplicates_root(profile,source_root),
        "review_orphans":derive_review_root(profile,source_root)/_rname(profile,"orphans_folder_name","Orphans"),
    }
    for p in roots.values(): ensure_dir(p)
    return roots

# ─────────────────────────────────────────────
# Normalise for voting (not for display names)
# ─────────────────────────────────────────────
def normalize_for_vote(s:str,cfg:Dict[str,Any])->str:
    nc=cfg.get("normalize",{})
    o=(s or "").strip()
    if nc.get("lower_case",True): o=o.lower()
    if nc.get("strip_bracketed_phrases_for_voting",True): o=re.sub(r"[\(\[].*?[\)\]]","",o)
    if nc.get("strip_punctuation_for_voting",True): o=re.sub(r"[^\w\s]"," ",o)
    if nc.get("collapse_whitespace",True): o=re.sub(r"\s+"," ",o).strip()
    for suf in nc.get("strip_common_suffixes_for_voting",[]) or []:
        if o.endswith(suf.lower()): o=o[:-len(suf)].strip()
    return o

def string_similarity(a:str,b:str)->float:
    a=re.sub(r"\s+"," ",a.strip().lower()); b=re.sub(r"\s+"," ",b.strip().lower())
    if not a or not b: return 0.0
    if a==b: return 1.0
    aset=set(a.split()); bset=set(b.split())
    j=len(aset&bset)/max(1,len(aset|bset))
    pref=sum(1 for ca,cb in zip(a,b) if ca==cb)
    return max(j,pref/max(len(a),len(b)))

# ─────────────────────────────────────────────
# Tag reading
# ─────────────────────────────────────────────
def mutagen_first(tag_obj:Any,keys:List[str])->Optional[str]:
    if not tag_obj: return None
    for k in keys:
        if k in tag_obj:
            v=tag_obj.get(k)
            if isinstance(v,list): v=v[0] if v else None
            if v is not None: return str(v)
    return None

def read_audio_tags(path:Path,cfg:Dict[str,Any])->Dict[str,Optional[str]]:
    keys=cfg.get("tags",{})
    result:Dict[str,Optional[str]]={k:None for k in ["album","albumartist","artist","title","tracknumber","discnumber","year","bpm","key"]}
    if MutagenFile is None: return result
    try:
        mf=MutagenFile(str(path),easy=True)
        if not mf or not getattr(mf,"tags",None): return result
        t=mf.tags
        result["album"]      =mutagen_first(t,keys.get("album_keys",["album"]))
        result["albumartist"]=mutagen_first(t,keys.get("albumartist_keys",["albumartist"]))
        result["artist"]     =mutagen_first(t,keys.get("artist_keys",["artist"]))
        result["title"]      =mutagen_first(t,keys.get("title_keys",["title"]))
        result["tracknumber"]=mutagen_first(t,keys.get("tracknumber_keys",["tracknumber"]))
        result["discnumber"] =mutagen_first(t,keys.get("discnumber_keys",["discnumber"]))
        result["bpm"]        =mutagen_first(t,keys.get("bpm_keys",["bpm","tbpm"]))
        result["key"]        =mutagen_first(t,keys.get("key_keys",["initialkey","key"]))
        for yk in keys.get("year_keys_prefer",["date","year"]):
            if yk in t:
                yv=t.get(yk); yv=yv[0] if isinstance(yv,list) else yv
                if yv:
                    m=re.search(r"(\d{4})",str(yv))
                    if m: result["year"]=m.group(1); break
    except Exception: pass
    return result

def detect_bpm_dj_encoding(stem:str)->bool:
    return bool(re.match(r"^\d{2,3}\s*bpm|^\d{2,3}\s*[-–]\s*[A-Ga-g][#b]?\s*(m|min|maj)?\s*[-–]",stem.strip(),re.IGNORECASE))

# ─────────────────────────────────────────────
# Folder name heuristic parser
# ─────────────────────────────────────────────
def smart_title_case(s:str)->str:
    if not s: return s
    words=s.split()
    if [w for w in words if len(w)>2] and all(w.isupper() for w in words if len(w)>2):
        small={"a","an","the","and","but","or","for","nor","on","at","to","by","in","of","vs"}
        return " ".join(w.capitalize() if i==0 or w.lower() not in small else w.lower() for i,w in enumerate(words))
    return s

def parse_folder_name_heuristic(folder_name:str)->Dict[str,Optional[str]]:
    result:Dict[str,Optional[str]]={"artist":None,"album":None,"year":None}
    name=normalize_unicode(folder_name.strip())
    name=re.sub(r"\s*\[[A-Z0-9\.\s]+\]\s*$","",name,flags=re.IGNORECASE).strip()
    name=re.sub(r"_-_"," - ",name).replace("_"," ")
    name=re.sub(r"\s+"," ",name).strip()
    ym=re.search(r"\b(19\d{2}|20\d{2})\b",name)
    if ym: result["year"]=ym.group(1)
    name_ny=re.sub(r"[\(\[]\s*(19\d{2}|20\d{2})\s*[\)\]]","",name).strip()
    name_ny=re.sub(r"\b(19\d{2}|20\d{2})\b","",name_ny).strip()
    name_ny=re.sub(r"\s+"," ",name_ny).strip().rstrip("-–").strip()
    m=re.match(r"^[\[\(]?(19\d{2}|20\d{2})[\]\)]?\s*[-–]\s*(.+?)\s*[-–]\s*(.+)$",name)
    if m:
        result["year"]=m.group(1); result["artist"]=smart_title_case(m.group(2).strip())
        result["album"]=smart_title_case(re.sub(r"\s*[\(\[](19\d{2}|20\d{2})[\)\]]\s*$","",m.group(3)).strip()); return result
    m2=re.match(r"^(.+?)\s*[-–]\s*(.+)$",name_ny)
    if m2:
        result["artist"]=smart_title_case(m2.group(1).strip())
        result["album"]=smart_title_case(m2.group(2).strip().rstrip("-– ")); return result
    if name_ny: result["album"]=smart_title_case(name_ny)
    return result

# ─────────────────────────────────────────────
# Folder scanning + proposals
# ─────────────────────────────────────────────
@dataclasses.dataclass
class FolderStats:
    tracks_total:int; tracks_tagged:int; tracks_unreadable:int
    extensions:Dict[str,int]; format_duplicates:List[str]=dataclasses.field(default_factory=list)

@dataclasses.dataclass
class FolderProposal:
    folder_path:str; folder_name:str; proposed_folder_name:str
    target_path:str; destination:str; confidence:float
    decision:Dict[str,Any]; stats:FolderStats

def fp_from_dict(d:Dict[str,Any])->FolderProposal:
    sd=d.get("stats",{})
    stats=FolderStats(tracks_total=sd.get("tracks_total",0),tracks_tagged=sd.get("tracks_tagged",0),
                      tracks_unreadable=sd.get("tracks_unreadable",0),extensions=sd.get("extensions",{}),
                      format_duplicates=sd.get("format_duplicates",[])) if isinstance(sd,dict) else sd
    return FolderProposal(folder_path=d["folder_path"],folder_name=d["folder_name"],
                          proposed_folder_name=d["proposed_folder_name"],target_path=d["target_path"],
                          destination=d["destination"],confidence=d["confidence"],
                          decision=d["decision"],stats=stats)

def list_audio_files(folder:Path,exts:List[str],follow_symlinks:bool=False)->List[Path]:
    out_f:List[Path]=[]
    try:
        for p in folder.iterdir():
            if not follow_symlinks and p.is_symlink(): continue
            if p.is_file() and p.suffix.lower() in exts and not is_hidden_file(p): out_f.append(p)
    except PermissionError: pass
    return out_f

def detect_format_dupes(files:List[Path])->List[str]:
    by_stem:Dict[str,List[str]]={}
    for f in files: by_stem.setdefault(normalize_unicode(f.stem.lower()),[]).append(f.suffix.lower())
    return [f"{s}: {', '.join(sorted(set(e)))}" for s,e in by_stem.items() if len(set(e))>1]

def compute_dominant(counter:Counter)->Tuple[Optional[str],float,int]:
    if not counter: return None,0.0,0
    total=sum(counter.values()); key,cnt=counter.most_common(1)[0]
    return key,(cnt/total if total else 0.0),cnt

def recover_display(norm_key:Optional[str],raw_by_norm:Dict[str,Counter])->Optional[str]:
    if not norm_key: return None
    rc=raw_by_norm.get(norm_key)
    return rc.most_common(1)[0][0] if rc else norm_key

def detect_va(aa_norm:str,track_artists:List[str],cfg:Dict[str,Any])->bool:
    vc=cfg.get("various_artists",{}); matches={m.lower() for m in vc.get("albumartist_matches",[])}
    if aa_norm and aa_norm in matches: return True
    if not vc.get("enable_heuristics",True): return False
    non_empty=[a for a in track_artists if a]
    return bool(non_empty) and len(set(non_empty))/len(non_empty)>=float(vc.get("unique_artist_ratio_above",0.50))

def pick_year(year_counts:Counter,tracks_with_year:int,total:int,cfg:Dict[str,Any])->Tuple[Optional[int],Dict[str,Any]]:
    yc=cfg.get("year",{})
    if not yc.get("enabled",True) or total==0: return None,{"included":False}
    pres=tracks_with_year/total
    if pres<float(yc.get("require_presence_ratio",0.50)): return None,{"included":False,"reason":"presence_ratio_low"}
    yv,ys,_=compute_dominant(year_counts)
    if yv is None: return None,{"included":False,"reason":"no_year_votes"}
    if ys<float(yc.get("agreement_threshold",0.70)): return None,{"included":False,"reason":"agreement_low"}
    try: y=int(yv)
    except Exception: return None,{"included":False,"reason":"year_not_int"}
    amin=int(yc.get("allowed_range",{}).get("min",1900)); amax=int(yc.get("allowed_range",{}).get("max",2100))
    if not(amin<=y<=amax): return None,{"included":False,"reason":"year_out_of_range"}
    return y,{"included":True,"presence_ratio":pres,"agreement":ys}

def build_folder_proposal(folder:Path,audio_files:List[Path],source_root:Path,profile:Dict[str,Any],cfg:Dict[str,Any])->Optional[FolderProposal]:
    albums_norm:Counter=Counter(); albumartists_norm:Counter=Counter(); track_artists_norm:Counter=Counter(); years:Counter=Counter()
    albums_raw:Dict[str,Counter]={}; albumartists_raw:Dict[str,Counter]={}
    tracks_with_year=0; tagged=0; unreadable=0
    extensions:Counter=Counter(p.suffix.lower() for p in audio_files)

    for f in audio_files:
        tags=read_audio_tags(f,cfg)
        if all(v is None for v in tags.values()): unreadable+=1; continue
        alb_r=(tags.get("album") or "").strip(); aa_r=(tags.get("albumartist") or "").strip()
        art_r=(tags.get("artist") or "").strip(); yr_r=(tags.get("year") or "").strip()
        alb_n=normalize_for_vote(alb_r,cfg) if alb_r else ""
        aa_n =normalize_for_vote(aa_r,cfg)  if aa_r  else ""
        art_n=normalize_for_vote(art_r,cfg) if art_r else ""
        if alb_n: albums_norm[alb_n]+=1; albums_raw.setdefault(alb_n,Counter())[alb_r]+=1
        if aa_n:  albumartists_norm[aa_n]+=1; albumartists_raw.setdefault(aa_n,Counter())[aa_r]+=1
        if art_n: track_artists_norm[art_n]+=1
        if yr_r:
            m=re.search(r"(\d{4})",yr_r)
            if m: years[m.group(1)]+=1; tracks_with_year+=1
        tagged+=1

    total=len(audio_files)
    dom_alb_n,alb_share,_=compute_dominant(albums_norm)
    dom_aa_n,aa_share,_  =compute_dominant(albumartists_norm)
    dom_art_n,art_share,_=compute_dominant(track_artists_norm)
    dom_alb=recover_display(dom_alb_n,albums_raw); dom_aa=recover_display(dom_aa_n,albumartists_raw)

    used_heuristic=False
    if not dom_alb and tagged==0:
        parsed=parse_folder_name_heuristic(folder.name)
        dom_alb=parsed.get("album"); dom_aa=parsed.get("artist")
        alb_share=0.50 if dom_alb else 0.0; aa_share=0.50 if dom_aa else 0.0
        used_heuristic=True
        if not tracks_with_year and parsed.get("year"): years[parsed["year"]]=1; tracks_with_year=1

    va_label=cfg.get("various_artists",{}).get("label","VA")
    is_va=detect_va(dom_aa_n or "",list(track_artists_norm.keys()),cfg)

    if is_va:
        artist_for_folder:Optional[str]=va_label
    elif dom_aa:
        artist_for_folder=normalize_artist_name(dom_aa,cfg)
    elif cfg.get("decision",{}).get("allow_artist_fallback",True):
        raw_art=recover_display(dom_art_n,{}) or dom_art_n
        artist_for_folder=normalize_artist_name(raw_art or "",cfg) or None
    else:
        artist_for_folder=None

    if not dom_alb or not artist_for_folder: return None

    year_val,year_meta=pick_year(years,tracks_with_year,max(total,1) if used_heuristic else total,cfg)

    fmt=cfg.get("format",{})
    pat=fmt.get("pattern_with_year" if year_val else "pattern_no_year","{albumartist} - {album}")
    proposed=pat.format(albumartist=artist_for_folder,album=dom_alb,year=year_val or "")
    proposed=sanitize_name(proposed,repl=fmt.get("replace_illegal_chars_with"," - "))

    sfx=cfg.get("format_suffix",{})
    if sfx.get("enabled",True) and sfx.get("only_if_all_same_extension",True) and len(extensions)==1:
        ext1=next(iter(extensions.keys()))
        if ext1 and ext1!=lower(sfx.get("ignore_extension",".mp3")) and sfx.get("style","brackets_upper")=="brackets_upper":
            proposed=f"{proposed} [{ext1.lstrip('.').upper()}]"

    is_flac_only=set(extensions.keys())=={".flac"}
    clean_albums=derive_clean_albums_root(profile,source_root)
    target_dir=resolve_library_path(clean_albums,artist_for_folder,dom_alb,year_val,is_flac_only,is_va,False,cfg)

    confidence=(alb_share*0.60+aa_share*0.40) if dom_aa and not is_va else (alb_share*0.70+art_share*0.30*0.85)
    if used_heuristic: confidence*=0.60

    fmt_dupes=detect_format_dupes(audio_files)
    decision={
        "dominant_album":dom_alb_n,"dominant_album_display":dom_alb,"dominant_album_share":alb_share,
        "dominant_albumartist":dom_aa_n,"dominant_albumartist_display":dom_aa,"dominant_albumartist_share":aa_share,
        "dominant_artist":dom_art_n,"dominant_artist_share":art_share,
        "is_va":is_va,"albumartist_display":artist_for_folder,"year":year_val,"year_meta":year_meta,
        "unreadable_ratio":(unreadable/total) if total else 0.0,"used_heuristic":used_heuristic,"is_flac_only":is_flac_only,
    }
    stats=FolderStats(tracks_total=total,tracks_tagged=tagged,tracks_unreadable=unreadable,extensions=dict(extensions),format_duplicates=fmt_dupes)
    return FolderProposal(folder_path=str(folder),folder_name=folder.name,proposed_folder_name=proposed,
                          target_path=str(target_dir),destination="clean",confidence=float(confidence),decision=decision,stats=stats)

def scan_folders(cfg:Dict[str,Any],profile_name:str,since:Optional[dt.datetime]=None)->Tuple[str,Path,List[FolderProposal]]:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    roots=ensure_roots(profile,source_root)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    clean_root_str =str(derive_clean_root(profile,source_root).resolve())+os.sep
    review_root_str=str(derive_review_root(profile,source_root).resolve())+os.sep

    session_id=make_session_id(); session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_names:Set[str]=set(cfg.get("ignore",{}).get("ignore_folder_names",[]))

    # Collect candidates with progress
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[] ; continue
        if leaf_only and dirs: continue
        if sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_tracks: candidates.append(rp)

    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
        out(f"  --since: {len(candidates)} folders modified after {since.strftime('%Y-%m-%d %H:%M')}",level=VERBOSE)

    proposals:List[FolderProposal]=[]; prog=Progress(len(candidates),"Scanning")

    for rp in candidates:
        if should_stop(): out(f"\n{C.YELLOW}Stopped after scanning {len(proposals)} folders.{C.RESET}"); break
        if rp.name in ignore_names: prog.tick(rp.name); continue
        audio_files=list_audio_files(rp,exts,follow_sym)
        if len(audio_files)<min_tracks: prog.tick(rp.name); continue
        prog.tick(rp.name)
        prop=build_folder_proposal(rp,audio_files,source_root,profile,cfg)
        if prop: proposals.append(prop)
    prog.done()

    # Routing
    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    max_unread=float(sc.get("max_unreadable_track_ratio",0.25))
    within_run=Counter(p.proposed_folder_name for p in proposals)
    existing_clean:Set[str]=set()
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir(): existing_clean.add(normalize_unicode(item.name))
        except Exception: pass
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())

    for p in proposals:
        reasons:List[str]=[]; dest="clean"
        if rr.get("route_questionable_to_review",True) and p.confidence<min_conf:
            dest="review"; reasons.append("low_confidence")
        if rr.get("route_duplicates",True) and within_run[p.proposed_folder_name]>1:
            dest="duplicate"; reasons.append("duplicate_in_run")
        norm_prop=normalize_unicode(p.proposed_folder_name)
        if rr.get("route_cross_run_duplicates",True) and (norm_prop in existing_clean or norm_prop in manifest_entries):
            dest="duplicate"; reasons.append("already_in_clean")
        if p.decision.get("unreadable_ratio",0.0)>max_unread:
            dest="review"; reasons.append("unreadable_ratio_high")
        if p.decision.get("used_heuristic",False):
            if dest=="clean": dest="review"
            reasons.append("heuristic_fallback")
        if p.stats.format_duplicates: reasons.append(f"format_dupes({len(p.stats.format_duplicates)})")
        p.destination=dest; p.decision["route_reasons"]=reasons
        if dest=="review":    p.target_path=str(review_albums/p.proposed_folder_name)
        elif dest=="duplicate": p.target_path=str(dup_root/p.proposed_folder_name)

    payload={"app":cfg.get("app",{}),"session_id":session_id,"timestamp":now_iso(),"profile":profile_name,
             "source_root":str(source_root),"since":since.isoformat() if since else None,
             "folder_proposals":[dataclasses.asdict(p) for p in proposals]}
    write_json(session_dir/"proposals.json",payload)
    _write_session_reports(session_id,profile_name,source_root,proposals,session_dir,cfg)

    clean_n=sum(1 for p in proposals if p.destination=="clean")
    rev_n  =sum(1 for p in proposals if p.destination=="review")
    dup_n  =sum(1 for p in proposals if p.destination=="duplicate")
    since_note=f"  (since {since.strftime('%Y-%m-%d %H:%M')})" if since else ""
    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    out(f"{C.BOLD}Results:{C.RESET}   {len(proposals)} proposals{since_note} | {C.GREEN}Clean: {clean_n}{C.RESET} | {C.YELLOW}Review: {rev_n}{C.RESET} | {C.RED}Dupes: {dup_n}{C.RESET}")
    out(f"{C.DIM}Reports:   {session_dir}/report.{{txt,csv,html}}{C.RESET}")
    return session_id,session_dir,proposals

# ─────────────────────────────────────────────
# Session reports: TXT + CSV + HTML
# ─────────────────────────────────────────────
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
        lines.append(f"{tag:<9} {p.confidence:>6.2f}  {p.folder_name[:45]:<45}  {p.proposed_folder_name}{heur}{('  ['+rr+']') if rr else ''}")
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

# ─────────────────────────────────────────────
# Apply folder moves
# ─────────────────────────────────────────────
def collision_resolve(dst:Path,policy:str,suffix_fmt:str)->Optional[Path]:
    if not dst.exists(): return dst
    if policy=="skip": return None
    n=1
    while True:
        cand=Path(str(dst)+suffix_fmt.format(n=n))
        if not cand.exists(): return cand
        n+=1

def apply_folder_moves(cfg:Dict[str,Any],proposals:List[FolderProposal],interactive:bool,
                       auto_above:Optional[float],dry_run:bool,session_id:str)->List[Dict[str,Any]]:
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

    applied:List[Dict[str,Any]]=[]; prog=Progress(len(proposals),"Applying ")

    for p in proposals:
        if should_stop(): out(f"\n{C.YELLOW}Stopped. {len(applied)}/{len(proposals)} applied.{C.RESET}"); break
        src=Path(p.folder_path); dst=Path(p.target_path); prog.tick(src.name)
        if not src.exists():
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"missing_source","src":str(src)}); continue
        if not check_path_length(dst): warn(f"Path length >{260}: {dst}")
        locked=check_folder_locked(src,exts)
        if locked:
            warn(f"Locked files in {src.name}: {[lf.name for lf in locked[:3]]}")
            if interactive and input(f"  Skip '{src.name}'? [Y/n] ").strip().lower()!="n":
                append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"locked_files","src":str(src)}); continue
        if warn_dj:
            dj_dbs=find_dj_databases(src)
            if dj_dbs:
                warn(f"DJ databases in {src.name}: {', '.join(dj_dbs)}")
                if halt_dj:
                    append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dj_halt","src":str(src)}); continue
        dst2=collision_resolve(dst,policy,sfmt)
        if dst2 is None:
            warn(f"Collision skip: {dst.name}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"collision_skip","src":str(src)}); continue
        elif dst2!=dst: warn(f"Collision — renamed to: {dst2.name}")
        should_prompt=(interactive or (req_confirm and p.confidence<interactive_below)) and not(auto_above is not None and p.confidence>=auto_above)
        if should_prompt:
            print(f"\n  {C.DIM}{src}{C.RESET}\n  → {dst2}  {status_tag(p.destination)}  conf={conf_color(p.confidence)}")
            if p.decision.get("used_heuristic"): warn("name from heuristic — no tags")
            if input("  Apply? [y/N] ").strip().lower()!="y":
                append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"user_skipped","src":str(src)}); continue
        if dry_run:
            out(f"  {C.DIM}[dry-run]{C.RESET} {src.name}  →  {dst2.name}  {status_tag(p.destination)}  conf={conf_color(p.confidence)}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)}); continue
        ensure_dir(dst2.parent)
        try: safe_move_folder(src,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"⛔ Move failed ({src.name}): {e}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)}); continue
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p.destination,"confidence":p.confidence,"decision":p.decision}
        append_jsonl(hist_path,entry); applied.append(entry)
        if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
        out(f"  MOVED {status_tag(p.destination)} {C.DIM}{src.name}{C.RESET}  →  {dst2.name}  conf={conf_color(p.confidence)}")
    prog.done()
    return applied

# ─────────────────────────────────────────────
# Track renaming
# ─────────────────────────────────────────────
_TLDS="com|net|org|info|biz|co|io|me|fm|tv|cc|us|uk|de|fr|es|it|nl|ru|br|mx|in|jp|cn|au|ca|ch|se|no|fi|dk|pl|cz"

def strip_trailing_domains(s:str)->str:
    s=re.sub(rf"(\s*[\(\[]\s*(https?:\/\/)?(www\.)?[\w\-]+(\.[\w\-]+)+\.({_TLDS})\s*[\)\]]\s*)$","",s.strip(),flags=re.IGNORECASE)
    s=re.sub(rf"(\s*[-–—]\s*)?(https?:\/\/)?(www\.)?[\w\-]+(\.[\w\-]+)+\.({_TLDS})\s*$","",s,flags=re.IGNORECASE)
    return re.sub(r"(\s*[-–—]\s*)?www\s*$","",s,flags=re.IGNORECASE).strip()

def cleanup_title(title:str,cfg:Dict[str,Any])->str:
    tc=cfg.get("title_cleanup",{})
    if not tc.get("enabled",True): return title.strip()
    o=title.strip().lstrip("\ufeff\u200b\u200c\u200d\u00a0")
    n=tc.get("normalize",{})
    if n.get("replace_underscores",True): o=o.replace("_"," ")
    if tc.get("strip_trailing_domains",True): o=strip_trailing_domains(o)
    if tc.get("strip_trailing_handles",True): o=re.sub(r"(\s*[-–—]\s*)?@[\w\.\-]+\s*$","",o.strip())
    phrases=[ph.lower() for ph in (tc.get("strip_trailing_phrases",[]) or [])]
    keep=[k.lower() for k in (tc.get("keep_parenthetical_if_contains",[]) or [])]
    def prot(seg:str)->bool: return any(k in seg.lower() for k in keep)
    changed=True
    while changed:
        changed=False
        m=re.search(r"(\s*[\(\[]([^)\]]+)[\)\]]\s*)$",o)
        if m and not prot(m.group(2)) and any(ph in m.group(2).lower() for ph in phrases):
            o=o[:m.start()].strip(); changed=True; continue
        m2=re.search(r"(\s*[-–—]\s*([^-–—]+)\s*)$",o)
        if m2 and any(ph in m2.group(2).lower() for ph in phrases) and not prot(m2.group(2)):
            o=o[:m2.start()].strip(); changed=True; continue
    o=re.sub(r"\s+\d{1,3}$","",o).strip()
    if n.get("collapse_whitespace",True): o=re.sub(r"\s+"," ",o).strip()
    if n.get("trim_dots_spaces",True): o=o.rstrip(". ").strip()
    return o

def parse_artist_title_from_fn(stem:str)->Tuple[Optional[str],Optional[str]]:
    s=normalize_unicode(re.sub(r"\s+"," ",stem.strip()))
    if detect_bpm_dj_encoding(s): return None,None
    s=re.sub(r"_-_"," - ",s).replace("_"," "); s=re.sub(r"\s+"," ",s).strip()
    s=re.sub(r"^[\[\(][^\]\)]+[\]\)]\s*","",s)
    s2=re.sub(r"^\(?\d{1,3}\)?\s*[-–—\.]\s*","",s); s2=re.sub(r"^\d{1,3}\s+","",s2)
    parts=[p.strip() for p in re.split(r"\s*[-–—]\s*",s2) if p.strip()]
    if len(parts)>=2: return parts[0]," - ".join(parts[1:])
    return None,None

def extract_mix_suffix(title:str,cfg:Dict[str,Any])->Tuple[str,str]:
    mc=cfg.get("mix_info",{}); kw=[k.lower() for k in (mc.get("detect_keywords",[]) or [])]
    if not mc.get("enabled",True) or not kw: return title,""
    m=re.search(r"\s*(\(([^)]+)\))\s*$",title)
    if m and any(k in m.group(2).lower() for k in kw): return title[:m.start()].strip(),f" {m.group(1)}"
    m2=re.search(r"\s*[-–—]\s*([^-–—]+)\s*$",title)
    if m2 and any(k in m2.group(1).lower() for k in kw):
        clean=title[:m2.start()].strip(); sty=mc.get("style","parenthetical")
        return (clean,f" - {m2.group(1).strip()}") if sty=="dash" else (clean,f" ({m2.group(1).strip()})")
    return title,""

def classify_folder_for_tracks(decision:Dict[str,Any],cfg:Dict[str,Any])->str:
    if float(decision.get("dominant_album_share",0.0))<float(cfg.get("decision",{}).get("album_dominance_threshold",0.75)): return "mixed"
    return "various" if bool(decision.get("is_va",False)) else "album"

def build_track_filename(classification:str,tags:Dict[str,Optional[str]],src:Path,cfg:Dict[str,Any],decision:Dict[str,Any],disc_multi:bool)->Tuple[Optional[str],float,str,Dict[str,Any]]:
    trc=cfg.get("track_rename",{}); pat=trc.get("patterns",{}); ext=src.suffix.lower()
    title=(tags.get("title") or "").strip(); artist=(tags.get("artist") or "").strip()
    track_raw=(tags.get("tracknumber") or "").strip(); disc_raw=(tags.get("discnumber") or "").strip()
    fn_artist,fn_title=parse_artist_title_from_fn(src.stem)
    if not title and fn_title: title=fn_title
    if title: title=cleanup_title(title,cfg)
    if not title: return None,0.0,"missing_title",{}
    title_c,mix_suf=extract_mix_suffix(title,cfg); title_c=cleanup_title(title_c,cfg)
    if not title_c: return None,0.0,"title_cleaned_empty",{}
    meta:Dict[str,Any]={}
    if not artist and fn_artist: artist=fn_artist; meta["artist_src"]="filename"
    elif artist: meta["artist_src"]="tag"
    if artist and cfg.get("artists",{}).get("feature_handling",{}).get("normalize_tokens",True):
        artist=re.sub(r"\bfeaturing\b|\bft\.?\b|\bfeat\.?\b","feat.",artist,flags=re.IGNORECASE)
        artist=re.sub(r"\s+"," ",artist).strip()
    track_n=parse_int_prefix(track_raw) if track_raw else None
    if track_n is None and trc.get("track_numbers",{}).get("fallback_to_filename_order",False):
        om=re.match(r"^(\d{1,3})",src.stem.strip())
        if om: track_n=int(om.group(1)); meta["track_src"]="filename_order"
    if track_n is None and classification in ("album","various") and trc.get("track_numbers",{}).get("required_for_album",True):
        return None,0.0,"missing_track_number",{}
    disc_prefix=""
    if trc.get("disc",{}).get("enabled",True):
        disc_n=parse_int_prefix(disc_raw) if disc_raw else None
        if disc_multi and disc_n: disc_prefix=trc.get("disc",{}).get("format","{disc}-").format(disc=disc_n)
    if classification=="album":
        if track_n is None: return None,0.0,"missing_track_number",{}
        tmpl=pat.get("album","{disc_prefix}{track:02d} - {title}{mix_suffix}{ext}")
        return sanitize_name(tmpl.format(disc_prefix=disc_prefix,track=int(track_n),title=title_c,mix_suffix=mix_suf,ext=ext)),0.95,"ok",meta
    if classification=="various":
        if not artist: return None,0.0,"missing_artist",meta
        if track_n is None: return None,0.0,"missing_track_number",meta
        tmpl=pat.get("various","{disc_prefix}{track:02d} - {artist} - {title}{mix_suffix}{ext}")
        fname=tmpl.format(disc_prefix=disc_prefix,track=int(track_n),artist=artist,title=title_c,mix_suffix=mix_suf,ext=ext)
        return sanitize_name(fname),0.92,"ok",meta
    if not artist: return None,0.0,"missing_artist",meta
    tmpl=pat.get("mixed","{artist} - {title}{mix_suffix}{ext}")
    return sanitize_name(tmpl.format(artist=artist,title=title_c,mix_suffix=mix_suf,ext=ext,disc_prefix="",track=0)),0.90,"ok",meta

def folder_is_multidisc(files:List[Path],cfg:Dict[str,Any])->bool:
    discs:set=set()
    for f in files:
        dn=(read_audio_tags(f,cfg).get("discnumber") or ""); d=parse_int_prefix(dn) if dn else None
        if d: discs.add(d)
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
        tags=read_audio_tags(f,cfg); new_name,conf,reason,meta=build_track_filename(classification,tags,f,cfg,folder_decision,disc_multi)
        if not new_name:
            out(f"    ↷ SKIP {f.name}  ({reason})",level=VERBOSE); skip_count+=1
            append_jsonl(tskip,{"timestamp":now_iso(),"session_id":session_id,"type":"track","reason":reason,"file":str(f),"meta":meta}); continue
        dst=f.with_name(new_name)
        if normalize_unicode(dst.name)==normalize_unicode(f.name): continue
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
def cmd_show(cfg:Dict[str,Any],folder_path:str,profile_name:str)->None:
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
    out(f"  album tag:          '{d.get('dominant_album_display')}' ({d.get('dominant_album_share',0):.0%} dominance)")
    out(f"  albumartist tag:    '{d.get('dominant_albumartist_display')}' ({d.get('dominant_albumartist_share',0):.0%} dominance)")
    out(f"  year:               {d.get('year') or 'not included'}")
    out(f"  VA:                 {'yes' if d.get('is_va') else 'no'}")
    out(f"  FLAC only:          {'yes' if d.get('is_flac_only') else 'no'}")
    out(f"  heuristic:          {'yes (no tags found)' if d.get('used_heuristic') else 'no'}")
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

# ─────────────────────────────────────────────
# verify — library audit
# ─────────────────────────────────────────────
def cmd_verify(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root); clean_albums=roots["clean_albums"]
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

# ─────────────────────────────────────────────
# learn — config suggestion from Review patterns
# ─────────────────────────────────────────────
def cmd_learn(cfg_path:Path,cfg:Dict[str,Any],session_id:Optional[str])->None:
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

# ─────────────────────────────────────────────
# report
# ─────────────────────────────────────────────
def cmd_report(cfg:Dict[str,Any],session_id:Optional[str],fmt:str)->None:
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
def cmd_status(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root)
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
    out(f"\n{C.BOLD}🍛 RaagDosa v{APP_VERSION} — Setup Wizard{C.RESET}\n{'='*50}")
    if cfg_path.exists():
        if input(f"\n  {cfg_path} exists. Overwrite? [y/N] ").strip().lower()!="y":
            out("Aborted."); return
    source=input("\n  Source root (your messy music folder): ").strip()
    if not source: err("Source root is required."); sys.exit(1)
    out("\n  Library template:\n    [1] artist/album  (recommended)\n    [2] album only  (flat)")
    template="{artist}/{album}" if (input("  Choose [1]: ").strip() or "1")=="1" else "{album}"
    flac_seg=input("\n  Segregate FLAC into artist/FLAC/album/? [y/N] ").strip().lower()=="y"
    out("\n  'The' prefix policy:\n    [1] keep-front (The Beatles)  [2] move-to-end (Beatles, The)  [3] strip (Beatles)")
    the_map={"1":"keep-front","2":"move-to-end","3":"strip"}
    the_policy=the_map.get(input("  Choose [1]: ").strip(),"keep-front")
    new_cfg={"app":{"name":"RaagDosa","version":APP_VERSION},
             "profiles":{"incoming":{"source_root":source,"clean_mode":"inside_root",
                "clean_folder_name":"Clean","review_folder_name":"Review",
                "clean_albums_folder_name":"Albums","clean_tracks_folder_name":"Tracks",
                "review_albums_folder_name":"Albums","duplicates_folder_name":"Duplicates","orphans_folder_name":"Orphans"}},
             "active_profile":"incoming",
             "library":{"template":template,"flac_segregation":flac_seg,"singles_folder":"_Singles","va_folder":"_Various Artists","unknown_artist_label":"_Unknown"},
             "artist_normalization":{"enabled":True,"the_prefix":the_policy,"normalize_hyphens":True,"fuzzy_dedup_threshold":0.92,"unicode_map":{},"aliases":{}},
             "scan":{"audio_extensions":[".mp3",".flac",".m4a",".aiff",".wav"],"leaf_folders_only":True,"min_tracks":3,"max_unreadable_track_ratio":0.25,"follow_symlinks":False},
             "ignore":{"ignore_folder_names":["Singles","One-Offs","Dump","_dump","Clean","Review"]},
             "tags":{"album_keys":["album"],"albumartist_keys":["albumartist","album_artist","album artist"],
                     "artist_keys":["artist"],"title_keys":["title"],"tracknumber_keys":["tracknumber","track"],
                     "discnumber_keys":["discnumber","disc"],"year_keys_prefer":["originaldate","date","year"],
                     "bpm_keys":["bpm","tbpm"],"key_keys":["initialkey","key","tkey"]},
             "normalize":{"lower_case":True,"strip_whitespace":True,"collapse_whitespace":True,
                          "strip_punctuation_for_voting":True,"strip_bracketed_phrases_for_voting":True,
                          "strip_common_suffixes_for_voting":["deluxe edition","expanded edition","remaster","remastered","anniversary edition","bonus tracks","explicit","special edition"]},
             "fuzzy":{"enabled":True,"similarity_threshold":0.88,"prompt_threshold":0.75},
             "decision":{"album_dominance_threshold":0.75,"allow_artist_fallback":True,"require_confirmation":True,"auto_approve_above":0.92,"interactive_below":0.92},
             "various_artists":{"label":"VA","albumartist_matches":["various artists","various","va","v/a"],"enable_heuristics":True,"unique_artist_ratio_above":0.50},
             "year":{"enabled":True,"allowed_range":{"min":1900,"max":2030},"require_presence_ratio":0.50,"agreement_threshold":0.70},
             "format":{"pattern_no_year":"{albumartist} - {album}","pattern_with_year":"{albumartist} - {album} ({year})","replace_illegal_chars_with":" - ","trim_trailing_dots_spaces":True},
             "format_suffix":{"enabled":True,"only_if_all_same_extension":True,"ignore_extension":".mp3","style":"brackets_upper"},
             "review_rules":{"min_confidence_for_clean":0.85,"route_duplicates":True,"route_cross_run_duplicates":True,"route_questionable_to_review":True,"route_heuristic_to_review":True},
             "move":{"enabled":True,"on_collision":"suffix","suffix_format":" ({n})","use_checksum":False},
             "dj_safety":{"detect_dj_databases":True,"warn_on_dj_databases":True,"halt_on_dj_databases":False},
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
             "undo":{"allow_undo_by_id":True,"allow_undo_by_original_path":True,"allow_undo_by_session":True}}
    write_yaml(cfg_path,new_cfg)
    out(f"\n{C.GREEN}✓ Config written: {cfg_path}{C.RESET}")
    out("\nNext steps:\n  1. Review config.yaml — especially artist_normalization.aliases\n  2. raagdosa doctor\n  3. raagdosa go --dry-run\n  4. raagdosa go")

# ─────────────────────────────────────────────
# High-level flows
# ─────────────────────────────────────────────
def load_last_session(cfg:Dict[str,Any])->Optional[Path]:
    sdir=Path(cfg["logging"]["session_dir"])
    if not sdir.exists(): return None
    for s in sorted([p for p in sdir.iterdir() if p.is_dir()],key=lambda p:p.name,reverse=True):
        cand=s/"proposals.json"
        if cand.exists(): return cand
    return None

def _parse_since(val:Optional[str],cfg:Dict[str,Any])->Optional[dt.datetime]:
    if not val: return None
    if val=="last_run": return manifest_get_last_run(cfg)
    try: return dt.datetime.fromisoformat(val)
    except Exception: err(f"Cannot parse --since '{val}'. Use ISO date or 'last_run'."); sys.exit(1)

def cmd_scan(cfg_path:Path,cfg:Dict[str,Any],profile:str,out_path:Optional[str],since:Optional[str])->str:
    sid,sdir,_=scan_folders(cfg,profile,since=_parse_since(since,cfg))
    if out_path: shutil.copyfile(str(sdir/"proposals.json"),out_path)
    return sid

def cmd_apply(cfg:Dict[str,Any],proposals_path:Path,interactive:bool,auto_above:Optional[float],dry_run:bool)->None:
    payload=read_json(proposals_path); session_id=payload.get("session_id") or make_session_id()
    raw_props=payload.get("folder_proposals",[]); profile_name=payload.get("profile","")
    profiles=cfg.get("profiles",{})
    if profile_name in profiles:
        prof=profiles[profile_name]; source_root=Path(prof["source_root"]).expanduser()
        roots=ensure_roots(prof,source_root)
        viols=validate_proposal_paths(raw_props,[roots["clean_albums"],roots["review_albums"],roots["duplicates"]])
        if viols: err("⛔ Path validation failed:"); [err(f"  {v}") for v in viols]; sys.exit(1)
    folder_props=[fp_from_dict(fp) for fp in raw_props]
    applied=apply_folder_moves(cfg,folder_props,interactive=interactive,auto_above=auto_above,dry_run=dry_run,session_id=session_id)
    trc=cfg.get("track_rename",{})
    if trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"):
        for a in applied:
            if a.get("destination")=="clean":
                rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),a.get("decision",{}),interactive=interactive,dry_run=dry_run,session_id=session_id)

def _run_core(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,since_str:Optional[str])->None:
    for lk in ["history_log","track_history_log"]:
        lp=Path(cfg.get("logging",{}).get(lk,""))
        if lp.name: rotate_log_if_needed(lp,float(cfg.get("logging",{}).get("rotate_log_max_mb",10.0)))
    register_stop_handler()
    sid,sdir,_=scan_folders(cfg,profile,since=_parse_since(since_str,cfg))
    pp=Path(cfg["logging"]["session_dir"])/sid/"proposals.json"
    cmd_apply(cfg,pp,interactive=interactive,auto_above=None,dry_run=dry_run)
    manifest_set_last_run(cfg)

def cmd_go(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,since:Optional[str])->None:
    _run_core(cfg_path,cfg,profile,interactive,dry_run,since)

def cmd_folders_only(cfg_path:Path,cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool,since:Optional[str])->None:
    register_stop_handler(); sid,_,proposals=scan_folders(cfg,profile,since=_parse_since(since,cfg))
    applied=apply_folder_moves(cfg,proposals,interactive=interactive,auto_above=None,dry_run=dry_run,session_id=sid)
    out(f"Folders applied: {len(applied)}"); manifest_set_last_run(cfg)

def cmd_tracks_only(cfg:Dict[str,Any],profile:str,interactive:bool,dry_run:bool)->None:
    profiles=cfg.get("profiles",{})
    if profile not in profiles: raise ValueError(f"Unknown profile: {profile}")
    prof=profiles[profile]; source_root=Path(prof["source_root"]).expanduser()
    roots=ensure_roots(prof,source_root); clean_albums=roots["clean_albums"]
    sid=make_session_id(); out(f"Session: {sid} (tracks-only)"); register_stop_handler()
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]
    for folder in sorted([p for p in clean_albums.rglob("*") if p.is_dir()]):
        if should_stop(): break
        af=list_audio_files(folder,exts)
        if not af: continue
        prop=build_folder_proposal(folder,af,source_root,prof,cfg)
        decision=prop.decision if prop else {"dominant_album_share":0.0,"is_va":False}
        rename_tracks_in_clean_folder(cfg,folder,decision,interactive=interactive,dry_run=dry_run,session_id=sid)

def cmd_resume(cfg:Dict[str,Any],session_id:str,interactive:bool,dry_run:bool)->None:
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
def cmd_history(cfg:Dict[str,Any],last:int,session:Optional[str],match:Optional[str],tracks:bool)->None:
    hist_path=Path(cfg["logging"]["track_history_log"] if tracks else cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
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
    hist_path=Path(cfg["logging"]["track_history_log"] if tracks else cfg["logging"]["history_log"])
    hist=iter_jsonl(hist_path)
    if not hist: err("No history."); return
    selected:List[Dict[str,Any]]=[]
    if action_id:   selected=[h for h in hist if h.get("action_id")==action_id]
    elif session_id: selected=[h for h in hist if h.get("session_id")==session_id]
    elif from_path:  selected=[h for h in hist if from_path in h.get("original_path","")]
    elif tracks and folder: selected=[h for h in hist if h.get("folder")==folder]
    else: err("Specify --id, --session, --from-path"+(" or --folder" if tracks else "")); sys.exit(1)
    if not selected: err("No matches."); return
    selected=sorted(selected,key=lambda x:x.get("timestamp",""),reverse=True); undone=0
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
            ensure_dir(dst.parent); src.rename(dst) if tracks else shutil.move(str(src),str(dst))
            undone+=1; out(f"  UNDONE: {h.get('action_id')}  {src.name}  →  {dst.name}")
        except Exception as e: err(f"  FAILED: {h.get('action_id')} — {e}")
    out(f"\nUndo complete. Reverted: {undone}")

# ─────────────────────────────────────────────
# Doctor
# ─────────────────────────────────────────────
def cmd_doctor(cfg_path:Path,cfg:Dict[str,Any])->None:
    is_ok=True
    out(f"\n{C.BOLD}RaagDosa v{APP_VERSION} — Doctor{C.RESET}")
    out(f"Python {sys.version.split()[0]}  |  {platform.system()} {platform.release()}")
    out(f"Config:  {cfg_path}  ({'exists' if cfg_path.exists() else C.RED+'MISSING'+C.RESET})")
    out(f"PyYAML:  {'✓' if yaml else C.RED+'✗ pip install pyyaml'+C.RESET}")
    out(f"Mutagen: {'✓' if MutagenFile else C.RED+'✗ pip install mutagen'+C.RESET}")
    warns=validate_config(cfg)
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
        roots=ensure_roots(prof,source_root)
        try:
            s=shutil.disk_usage(str(source_root)); fgb=s.free/1024**3
            out(f"Disk:    {fgb:.1f} GB free / {s.total/1024**3:.1f} GB total")
            if fgb<5.0: warn("Less than 5 GB free.")
        except Exception: pass
        dj=cfg.get("dj_safety",{})
        if dj.get("detect_dj_databases",True):
            dbs=find_dj_databases(source_root)
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
    try:
        ensure_dir(log_root); t=log_root/".write_test"; t.write_text("ok",encoding="utf-8"); t.unlink(); ok_msg(f"Logs writable: {log_root}")
    except Exception as e: err(f"Logs not writable: {log_root} — {e}"); is_ok=False
    out(f"\n{'✓ Doctor complete.' if is_ok else C.YELLOW+'⚠ Doctor complete — see warnings.'+C.RESET}")

# ─────────────────────────────────────────────
# Profile CRUD
# ─────────────────────────────────────────────
def profile_list(cfg:Dict[str,Any])->None:
    for n in cfg.get("profiles",{}).keys():
        mark=f"  {C.GREEN}* active{C.RESET}" if n==cfg.get("active_profile") else ""
        out(f"  {n}{mark}")

def profile_show(cfg:Dict[str,Any],name:str)->None:
    p=cfg.get("profiles",{}).get(name)
    if not p: err("No such profile."); return
    out(json.dumps(p,indent=2))

def profile_add(cfg_path:Path,cfg:Dict[str,Any],name:str,source:str,clean_mode:str,clean_folder:str,review_folder:str)->None:
    cfg.setdefault("profiles",{})
    if name in cfg["profiles"]: raise ValueError("Profile already exists.")
    cfg["profiles"][name]={"source_root":source,"clean_mode":clean_mode,"clean_folder_name":clean_folder,
                            "review_folder_name":review_folder,"clean_albums_folder_name":"Albums",
                            "clean_tracks_folder_name":"Tracks","review_albums_folder_name":"Albums",
                            "duplicates_folder_name":"Duplicates","orphans_folder_name":"Orphans"}
    write_yaml(cfg_path,cfg); out(f"Added profile: {name}")

def profile_set(cfg_path:Path,cfg:Dict[str,Any],name:str,source:Optional[str],clean_mode:Optional[str],clean_folder:Optional[str],review_folder:Optional[str])->None:
    prof=cfg.get("profiles",{}).get(name)
    if not prof: raise ValueError("No such profile.")
    if source:        prof["source_root"]=source
    if clean_mode:    prof["clean_mode"]=clean_mode
    if clean_folder:  prof["clean_folder_name"]=clean_folder
    if review_folder: prof["review_folder_name"]=review_folder
    write_yaml(cfg_path,cfg); out(f"Updated profile: {name}")

def profile_delete(cfg_path:Path,cfg:Dict[str,Any],name:str)->None:
    if name not in cfg.get("profiles",{}): raise ValueError("No such profile.")
    del cfg["profiles"][name]
    if cfg.get("active_profile")==name: cfg["active_profile"]=None
    write_yaml(cfg_path,cfg); out(f"Deleted: {name}")

def profile_use(cfg_path:Path,cfg:Dict[str,Any],name:str)->None:
    if name not in cfg.get("profiles",{}): raise ValueError("No such profile.")
    cfg["active_profile"]=name; write_yaml(cfg_path,cfg); out(f"Active profile: {name}")

# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def build_parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(prog="raagdosa",description=f"RaagDosa v{APP_VERSION} — deterministic music library cleanup.",
        formatter_class=argparse.RawDescriptionHelpFormatter,epilog="""
Examples:
  raagdosa init                       # guided first-time setup
  raagdosa doctor                     # verify config, deps, disk
  raagdosa go --dry-run               # preview — nothing moves
  raagdosa go                         # scan + move folders + rename tracks
  raagdosa go --since last_run        # only new folders since last run
  raagdosa go --interactive           # confirm each folder
  raagdosa show "/path/to/folder"     # debug a single folder
  raagdosa status                     # library overview
  raagdosa verify                     # audit Clean library health
  raagdosa learn                      # suggest config improvements
  raagdosa report --format html       # open last session report
  raagdosa undo --session <id>        # undo a whole session
""")
    p.add_argument("--config",default="config.yaml"); p.add_argument("--version",action="version",version=f"raagdosa {APP_VERSION}")
    p.add_argument("--verbose",action="store_true"); p.add_argument("--quiet",action="store_true")
    sub=p.add_subparsers(dest="cmd",required=True)
    sub.add_parser("init",help="Guided first-time setup")
    sp=sub.add_parser("profile",help="Manage profiles"); psub=sp.add_subparsers(dest="profile_cmd",required=True)
    psub.add_parser("list"); psub.add_parser("show").add_argument("name")
    add=psub.add_parser("add"); add.add_argument("name"); add.add_argument("--source",required=True)
    add.add_argument("--clean-mode",default="inside_root",choices=["inside_root","inside_parent"])
    add.add_argument("--clean-folder",default="Clean"); add.add_argument("--review-folder",default="Review")
    setp=psub.add_parser("set"); setp.add_argument("name"); setp.add_argument("--source"); setp.add_argument("--clean-mode"); setp.add_argument("--clean-folder"); setp.add_argument("--review-folder")
    psub.add_parser("delete").add_argument("name"); psub.add_parser("use").add_argument("name")
    sc=sub.add_parser("scan",help="Scan → proposals.json"); sc.add_argument("--profile"); sc.add_argument("--out"); sc.add_argument("--since")
    ap=sub.add_parser("apply",help="Apply proposals.json"); ap.add_argument("proposals",nargs="?"); ap.add_argument("--last-session",action="store_true"); ap.add_argument("--interactive",action="store_true"); ap.add_argument("--auto-above",type=float); ap.add_argument("--dry-run",action="store_true")
    for nc in ("run","go"):
        c=sub.add_parser(nc,help="Scan + apply (folders + tracks)"); c.add_argument("--profile"); c.add_argument("--interactive",action="store_true"); c.add_argument("--dry-run",action="store_true"); c.add_argument("--since")
    fo=sub.add_parser("folders",help="Folder pass only"); fo.add_argument("--profile"); fo.add_argument("--interactive",action="store_true"); fo.add_argument("--dry-run",action="store_true"); fo.add_argument("--since")
    tr=sub.add_parser("tracks",help="Track rename pass"); tr.add_argument("--profile"); tr.add_argument("--interactive",action="store_true"); tr.add_argument("--dry-run",action="store_true")
    sub.add_parser("status",help="Library overview").add_argument("--profile")
    rs=sub.add_parser("resume",help="Resume interrupted session"); rs.add_argument("session_id"); rs.add_argument("--interactive",action="store_true"); rs.add_argument("--dry-run",action="store_true")
    sh=sub.add_parser("show",help="Debug a single folder"); sh.add_argument("folder"); sh.add_argument("--profile")
    sub.add_parser("verify",help="Audit Clean library health").add_argument("--profile")
    le=sub.add_parser("learn",help="Suggest config improvements"); le.add_argument("--session")
    rp=sub.add_parser("report",help="View session report"); rp.add_argument("--session"); rp.add_argument("--format",default="txt",choices=["txt","csv","html"])
    sub.add_parser("doctor",help="Check config, deps, disk, DJ databases")
    hi=sub.add_parser("history",help="Show history"); hi.add_argument("--last",type=int,default=50); hi.add_argument("--session"); hi.add_argument("--match"); hi.add_argument("--tracks",action="store_true")
    un=sub.add_parser("undo",help="Undo moves or renames"); un.add_argument("--id"); un.add_argument("--session"); un.add_argument("--from-path"); un.add_argument("--tracks",action="store_true"); un.add_argument("--folder")
    return p

def main()->None:
    parser=build_parser(); args=parser.parse_args()
    if getattr(args,"verbose",False): set_verbosity(VERBOSE)
    elif getattr(args,"quiet",False): set_verbosity(QUIET)
    cfg_path=Path(args.config); cmd=args.cmd
    if cmd=="init": cmd_init(cfg_path); return
    cfg=read_yaml(cfg_path)
    def gp()->str:
        p=getattr(args,"profile",None) or cfg.get("active_profile")
        if not p: raise ValueError("No profile specified and no active_profile set.")
        return p
    if cmd=="profile":
        pc=args.profile_cmd
        if pc=="list":    profile_list(cfg)
        elif pc=="show":  profile_show(cfg,args.name)
        elif pc=="add":   profile_add(cfg_path,cfg,args.name,args.source,args.clean_mode,args.clean_folder,args.review_folder)
        elif pc=="set":   profile_set(cfg_path,cfg,args.name,args.source,args.clean_mode,args.clean_folder,args.review_folder)
        elif pc=="delete": profile_delete(cfg_path,cfg,args.name)
        elif pc=="use":   profile_use(cfg_path,cfg,args.name)
    elif cmd=="scan":    cmd_scan(cfg_path,cfg,gp(),args.out,getattr(args,"since",None))
    elif cmd=="apply":
        pp=load_last_session(cfg) if args.last_session else (Path(args.proposals) if args.proposals else None)
        if not pp: err("Provide proposals.json or --last-session"); sys.exit(1)
        cmd_apply(cfg,pp,interactive=bool(args.interactive),auto_above=args.auto_above,dry_run=bool(args.dry_run))
    elif cmd in("run","go"): cmd_go(cfg_path,cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run),since=getattr(args,"since",None))
    elif cmd=="folders":  cmd_folders_only(cfg_path,cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run),since=getattr(args,"since",None))
    elif cmd=="tracks":   cmd_tracks_only(cfg,gp(),interactive=bool(args.interactive),dry_run=bool(args.dry_run))
    elif cmd=="status":   cmd_status(cfg,gp())
    elif cmd=="resume":   cmd_resume(cfg,args.session_id,interactive=bool(args.interactive),dry_run=bool(args.dry_run))
    elif cmd=="show":     cmd_show(cfg,args.folder,getattr(args,"profile",None) or cfg.get("active_profile",""))
    elif cmd=="verify":   cmd_verify(cfg,gp())
    elif cmd=="learn":    cmd_learn(cfg_path,cfg,getattr(args,"session",None))
    elif cmd=="report":   cmd_report(cfg,getattr(args,"session",None),args.format)
    elif cmd=="doctor":   cmd_doctor(cfg_path,cfg)
    elif cmd=="history":  cmd_history(cfg,last=args.last,session=args.session,match=args.match,tracks=bool(args.tracks))
    elif cmd=="undo":     cmd_undo(cfg,action_id=args.id,session_id=args.session,from_path=args.from_path,tracks=bool(args.tracks),folder=args.folder)
    else: parser.error(f"Unknown: {cmd}")

if __name__=="__main__":
    main()
