#!/usr/bin/env python3
"""
RaagDosa v3.5
Deterministic library cleanup for DJ music folders — CLI-first, safe-by-default, undoable.

Commands:
  go / run / scan / apply / folders / tracks / resume
  show / verify / learn / init / status / report
  profile / history / undo / doctor
  orphans / artists / review-list / clean-report
  extract / compare / diff
"""
from __future__ import annotations

import argparse, csv, dataclasses, datetime as dt, fnmatch, hashlib, html
import json, os, platform, re, shutil, signal, sys, unicodedata, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import yaml
except Exception:
    yaml = None

try:
    from mutagen import File as MutagenFile
except Exception:
    MutagenFile = None

APP_VERSION = "3.5.2"

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
        self._lock=Lock()
        self._start=dt.datetime.now()
    def tick(self,msg:str="")->None:
        with self._lock:
            self.current+=1
            if not self._active: return
            pct=int(self.current/max(self.total,1)*100)
            bw=22; filled=int(bw*self.current/max(self.total,1))
            bar="█"*filled+"░"*(bw-filled)
            # ETA calculation
            elapsed=(dt.datetime.now()-self._start).total_seconds()
            rate=self.current/elapsed if elapsed>0 else 0
            remaining=self.total-self.current
            eta_s=remaining/rate if rate>0 else 0
            if eta_s>3600:   eta=f"~{eta_s/3600:.0f}h"
            elif eta_s>60:   eta=f"~{eta_s/60:.0f}m"
            elif eta_s>0:    eta=f"~{eta_s:.0f}s"
            else:            eta=""
            rate_s=f"{rate:.0f}/s" if rate>=1 else f"{rate*60:.0f}/min"
            line=(f"\r{C.CYAN}{self.label}{C.RESET} [{bar}] {self.current}/{self.total} {pct}%"
                  f"  {C.DIM}{rate_s}  {eta}  {msg[:28]:<28}{C.RESET}")
            print(line,end="",flush=True)
    def done(self)->None:
        if self._active:
            elapsed=(dt.datetime.now()-self._start).total_seconds()
            rate=self.current/elapsed if elapsed>0 else 0
            print(f"  {C.DIM}({elapsed:.1f}s, {rate:.0f} folders/s){C.RESET}")

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

def file_mtime(path:Path)->float:
    try: return path.stat().st_mtime
    except Exception: return 0.0

# ─────────────────────────────────────────────
# Tag cache — persisted between runs
# ─────────────────────────────────────────────
# Cache key: absolute path string
# Cache value: {"mtime": float, "tags": dict}
# Invalidated whenever the file's mtime changes.
# Written atomically after each scan pass.

class TagCache:
    """
    Persistent tag cache. Keyed by absolute path → {mtime, tags}.
    Thread-safe for concurrent reads/writes during parallel scan.
    """
    def __init__(self,cache_path:Path):
        self._path=cache_path
        self._lock=Lock()
        self._dirty=False
        self._data:Dict[str,Any]={}
        self._load()

    def _load(self)->None:
        if not self._path.exists(): return
        try:
            raw=read_json(self._path)
            # Support versioned format
            self._data=raw.get("entries",raw) if isinstance(raw,dict) else {}
        except Exception:
            self._data={}

    def get(self,path:Path)->Optional[Dict[str,Optional[str]]]:
        """Return cached tags if file mtime matches, else None."""
        key=str(path.resolve())
        mtime=file_mtime(path)
        with self._lock:
            entry=self._data.get(key)
        if entry and abs(entry.get("mtime",0)-mtime)<0.01:
            return entry["tags"]
        return None

    def set(self,path:Path,tags:Dict[str,Optional[str]])->None:
        """Store tags for path with current mtime."""
        key=str(path.resolve())
        mtime=file_mtime(path)
        with self._lock:
            self._data[key]={"mtime":mtime,"tags":tags}
            self._dirty=True

    def save(self)->None:
        """Flush cache to disk if dirty. Atomic write via temp file."""
        with self._lock:
            if not self._dirty: return
            tmp=self._path.with_suffix(".tmp")
            try:
                ensure_dir(self._path.parent)
                payload={"version":APP_VERSION,"saved":now_iso(),"entries":self._data}
                tmp.write_text(json.dumps(payload,ensure_ascii=False,separators=(',',':')),encoding="utf-8")
                tmp.replace(self._path)
                self._dirty=False
            except Exception as e:
                try: tmp.unlink(missing_ok=True)
                except Exception: pass

    def evict_missing(self)->int:
        """Remove entries for files that no longer exist. Returns count removed."""
        removed=0
        with self._lock:
            keys=list(self._data.keys())
        for k in keys:
            if not Path(k).exists():
                with self._lock:
                    self._data.pop(k,None); removed+=1
                self._dirty=True
        return removed

    @property
    def size(self)->int:
        with self._lock: return len(self._data)

# Module-level cache singleton — initialised in scan_folders
_tag_cache:Optional["TagCache"]=None

def _get_tag_cache(cfg:Dict[str,Any])->Optional["TagCache"]:
    global _tag_cache
    if _tag_cache is not None: return _tag_cache
    if not cfg.get("scan",{}).get("tag_cache_enabled",True): return None
    cache_path=Path(cfg.get("logging",{}).get("root_dir","logs"))/"tag_cache.json"
    _tag_cache=TagCache(cache_path)
    return _tag_cache

def reset_tag_cache()->None:
    global _tag_cache; _tag_cache=None

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
# Windows reserved filename sanitisation
# ─────────────────────────────────────────────
_WINDOWS_RESERVED = {
    "CON","PRN","AUX","NUL",
    "COM1","COM2","COM3","COM4","COM5","COM6","COM7","COM8","COM9",
    "LPT1","LPT2","LPT3","LPT4","LPT5","LPT6","LPT7","LPT8","LPT9",
}

def sanitize_windows_reserved(name:str)->str:
    """Append underscore if name (sans extension) is a Windows reserved word."""
    stem=Path(name).stem.upper()
    if stem in _WINDOWS_RESERVED: return name+"_"
    return name

# ─────────────────────────────────────────────
# Extension case normalisation
# ─────────────────────────────────────────────
def normalise_extension(path:Path)->Optional[Path]:
    """Return new Path with lowercase extension if it needs renaming, else None."""
    if path.suffix and path.suffix != path.suffix.lower():
        return path.with_suffix(path.suffix.lower())
    return None

# ─────────────────────────────────────────────
# Empty parent cleanup
# ─────────────────────────────────────────────
def cleanup_empty_parents(start:Path,stop_at:Path)->None:
    """Remove start and then any empty ancestor directories up to (not including) stop_at."""
    current=start
    while True:
        if current==stop_at or current==current.parent: break
        try:
            if current.exists() and not any(current.iterdir()):
                current.rmdir()
                current=current.parent
            else:
                break
        except Exception:
            break

# ─────────────────────────────────────────────
# Bracket content classifier
# ─────────────────────────────────────────────
_BRACKET_YEAR   = re.compile(r'^(19|20)\d{2}$')
_BRACKET_FORMAT = re.compile(r'\b(mp3|flac|aac|ogg|wav|aiff|320|256|192|128|lossless|hi.?res|24.?bit|vinyl|vinyl.?rip|web|cd|dvd)\b',re.I)
_BRACKET_EDITION= re.compile(r'\b(deluxe|expanded|anniversary|remaster(ed)?|special|bonus|collector|limited|explicit|censored|edition|version|complete|extended|import|original)\b',re.I)
_BRACKET_REMIX  = re.compile(r'\b(remix|edit|mix|rework|bootleg|mashup|vip|flip|dub|club|radio|instrumental|acapella|version)\b',re.I)
_BRACKET_PROMO  = re.compile(r'www\.|\.com|\.net|\.org|free\s+download|promo\s+only|for\s+promo|not\s+for\s+sale|leaked|rip|ripped|uploaded|download',re.I)
_BRACKET_NOISE  = re.compile(r'^\s*(hd|hq|\d+k|\d+\s*hz|\d+\s*kbps|stereo|mono|clean|dirty)\s*$',re.I)

def classify_bracket(text:str)->str:
    """Classify a bracket group's contents: year|format|edition|remix|promo|noise"""
    t=text.strip()
    if _BRACKET_YEAR.match(t):              return "year"
    if _BRACKET_PROMO.search(t):            return "promo"
    if _BRACKET_FORMAT.search(t):           return "format"
    if _BRACKET_EDITION.search(t):          return "edition"
    if _BRACKET_REMIX.search(t):            return "remix"
    if _BRACKET_NOISE.match(t):             return "noise"
    return "unknown"

# ─────────────────────────────────────────────
# Mojibake detection
# ─────────────────────────────────────────────
_MOJIBAKE_CHARS = set('ÃÂ€„†‡ˆ‰Šš›œžŸ¡¢£¤¥¦§¨©ª«¬\xad®¯°±²³´µ¶·¸¹º»¼½¾¿')
_MOJIBAKE_SEQ   = re.compile(r'Ã[\x80-\xff]|Â[\x80-\xff]',re.S)

def detect_mojibake(s:str)->bool:
    """True if the string likely contains double-encoded or misencoded Unicode."""
    if not s: return False
    if _MOJIBAKE_SEQ.search(s): return True
    return sum(1 for c in s if c in _MOJIBAKE_CHARS) >= 3

# ─────────────────────────────────────────────
# Garbage naming detection
# ─────────────────────────────────────────────
_PROMO_WATERMARK = re.compile(
    r'www\.|\.com\b|\.net\b|\.org\b|free\s+download|promo\s+only|not\s+for\s+sale|'
    r'leaked|visit\s+us|check\s+out|follow\s+us|\bvk\.com\b|\bsoundcloud\.com\b',
    re.I)

def detect_garbage_name(name:str)->List[str]:
    """Return list of garbage reasons (empty = clean). Does not modify the name."""
    reasons:List[str]=[]
    # Token flood: 5+ parenthetical/bracket groups
    groups=re.findall(r'[\[\(][^\[\]\(\)]*[\]\)]',name)
    if len(groups)>=5: reasons.append("token_flood")
    # Promo watermarks
    if _PROMO_WATERMARK.search(name): reasons.append("promo_watermark")
    # Mojibake
    if detect_mojibake(name): reasons.append("mojibake")
    # All-caps + very long (shouting noise filenames)
    words=[w for w in name.split() if w.isalpha()]
    if len(words)>=4 and all(w.isupper() for w in words) and len(name)>60:
        reasons.append("all_caps_long")
    return reasons

def strip_bracket_stack(name:str,max_passes:int=8)->str:
    """Repeatedly strip trailing bracket groups classified as noise/promo."""
    result=name.strip()
    for _ in range(max_passes):
        m=re.search(r'\s*[\[\(]([^\[\]\(\)]*)[\]\)]\s*$',result)
        if not m: break
        cls=classify_bracket(m.group(1))
        if cls in ("noise","promo","format"):
            result=result[:m.start()].strip()
        else:
            break
    return result

# ─────────────────────────────────────────────
# Display name noise stripping
# ─────────────────────────────────────────────
_DISPLAY_NOISE_PATTERNS=[
    re.compile(r'\s*\[official\s+(?:audio|video|music\s+video)\]\s*$',re.I),
    re.compile(r'\s*\(official\s+(?:audio|video|music\s+video)\)\s*$',re.I),
    re.compile(r'\s*-\s*official\s+(?:audio|video|music\s+video)\s*$',re.I),
    re.compile(r'\s*\[(?:hd|hq|4k|1080p|720p)\]\s*$',re.I),
    re.compile(r'\s*\((?:lyrics?|lyric\s+video)\)\s*$',re.I),
    re.compile(r'\s*\[(?:lyrics?|lyric\s+video)\]\s*$',re.I),
    re.compile(r'\s*-\s*lyrics?\s*$',re.I),
]

def strip_display_noise(name:str)->str:
    """Strip common display/upload noise suffixes from album/track names."""
    result=name
    for pat in _DISPLAY_NOISE_PATTERNS:
        result=pat.sub('',result).strip()
    return result

# ─────────────────────────────────────────────
# Disc indicator stripping from album names
# ─────────────────────────────────────────────
_DISC_INDICATOR=re.compile(r'\s*[-–—:,]\s*(?:disc|cd|disk|volume|vol\.?)\s*\d+\s*$',re.I)
_DISC_INDICATOR2=re.compile(r'\s*\((?:disc|cd|disk|volume|vol\.?)\s*\d+\)\s*$',re.I)
_DISC_INDICATOR3=re.compile(r'\s*\[(?:disc|cd|disk|volume|vol\.?)\s*\d+\]\s*$',re.I)

def strip_disc_indicator(album_name:str)->str:
    """Remove trailing disc/cd/vol indicators from an album name for unified matching."""
    s=album_name
    for pat in (_DISC_INDICATOR,_DISC_INDICATOR2,_DISC_INDICATOR3):
        s=pat.sub('',s).strip()
    return s

# ─────────────────────────────────────────────
# Capitalisation pathology fixes
# ─────────────────────────────────────────────
def apply_title_case(s:str,cfg:Dict[str,Any])->str:
    """
    Apply intelligent title-casing with configurable exceptions.
    Handles: ALL CAPS, all lowercase, Every Word Capitalised (over-casing).
    Respects title_case.never_cap and title_case.always_cap lists from config.
    """
    if not s: return s
    tc=cfg.get("title_case",{})
    never:Set[str]={w.lower() for w in (tc.get("never_cap",[]) or [])}
    always:Set[str]={w.lower() for w in (tc.get("always_cap",[]) or [])}

    # Default small words (never-cap unless first word)
    _default_small={"a","an","the","and","but","or","for","nor","on","at","to","by","in","of","vs","via","feat","feat."}
    never=never | _default_small

    words=s.split()
    if not words: return s

    # Detect pathologies
    alpha_words=[w for w in words if any(c.isalpha() for c in w)]
    if not alpha_words: return s
    is_all_caps=all(w.isupper() for w in alpha_words if len(w)>1)
    is_all_lower=all(w.islower() for w in alpha_words)

    if not (is_all_caps or is_all_lower): return s  # already mixed — leave alone

    result=[]
    for i,word in enumerate(words):
        w_lower=word.lower()
        # Strip leading/trailing punct to check the word
        core=re.sub(r'^[^\w]+|[^\w]+$','',word)
        core_lower=core.lower()
        if core_lower in always:
            result.append(word.upper() if len(core)<=3 else word[0:word.index(core)]+core.upper()+word[word.index(core)+len(core):])
        elif i==0 or core_lower not in never:
            # Capitalise
            if core:
                idx=word.index(core)
                result.append(word[:idx]+core[0].upper()+core[1:].lower()+word[idx+len(core):])
            else:
                result.append(word)
        else:
            result.append(word.lower())
    return " ".join(result)

# ─────────────────────────────────────────────
# Vinyl track notation (A1/B2/C3)
# ─────────────────────────────────────────────
_VINYL_RE=re.compile(r'^([A-Da-d])(\d{1,2})$')

def parse_vinyl_track(s:str)->Optional[Tuple[str,int,int]]:
    """
    Parse vinyl side-track notation like A1, B2, C3.
    Returns (side_letter, side_track_num, absolute_track_num) or None.
    Absolute: A1=1, A2=2 … A8=8, B1=9 … B8=16, C1=17 …
    """
    m=_VINYL_RE.match((s or "").strip())
    if not m: return None
    side=m.group(1).upper(); n=int(m.group(2))
    absolute=(ord(side)-ord('A'))*8+n
    return side,n,absolute

# ─────────────────────────────────────────────
# EP detection
# ─────────────────────────────────────────────
def detect_ep(audio_files:List[Path],cfg:Dict[str,Any])->bool:
    """True if file count falls in the EP range (default 3–6 tracks)."""
    ep=cfg.get("ep_detection",{})
    if not ep.get("enabled",True): return False
    mn=int(ep.get("min_tracks",3)); mx=int(ep.get("max_tracks",6))
    return mn<=len(audio_files)<=mx

# ─────────────────────────────────────────────
# Mix / chart folder classifier
# ─────────────────────────────────────────────
_MIX_FOLDER_KW=re.compile(
    r'\b(mix(tape)?|presents|sessions?|podcast|promo\s+set|live\s+at|'
    r'compiled\s+by|mixed\s+by|chart|top\s*\d+|best\s+of|'
    r'greatest\s+hits|collection|discography|anthology)\b',re.I)

def classify_folder_content(
    audio_files:List[Path],
    folder_name:str,
    all_tags:List[Dict[str,Optional[str]]],
    cfg:Dict[str,Any]
)->str:
    """
    Returns one of: 'album' | 'va' | 'mix' | 'ep'
    Priority: override > keyword > artist-ratio heuristic
    """
    mc=cfg.get("mix_detection",{}); enabled=mc.get("enabled",True)

    # EP first
    if detect_ep(audio_files,cfg): return "ep"

    if not enabled: return "album"

    # Folder-name keywords → mix
    extra_kw=[k.lower() for k in (mc.get("folder_name_keywords",[]) or [])]
    if _MIX_FOLDER_KW.search(folder_name) or any(k in folder_name.lower() for k in extra_kw):
        return "mix"

    tagged=[t for t in all_tags if any(v for v in t.values())]
    if not tagged: return "album"

    # High unique-artist ratio
    artists=[t.get("artist","").strip().lower() for t in tagged if t.get("artist")]
    if len(artists)>=3:
        unique_ratio=len(set(artists))/len(artists)
        if unique_ratio>=float(mc.get("unique_artist_ratio_mix",0.65)):
            # Check for explicit VA albumartist tag
            albumartists=[t.get("albumartist","").strip().lower() for t in tagged if t.get("albumartist")]
            if albumartists:
                aa_set=set(albumartists)
                if any(x in aa_set for x in ["various artists","various","va","v/a"]): return "va"
            return "mix"

    return "album"

# ─────────────────────────────────────────────
# Named confidence factors
# ─────────────────────────────────────────────
def detect_track_gaps(track_nums:List[int])->List[int]:
    """Return list of missing track numbers in the sequence."""
    if len(track_nums)<2: return []
    s=sorted(set(track_nums)); gaps=[]
    for i in range(s[0],s[-1]+1):
        if i not in set(s): gaps.append(i)
    return gaps

def detect_duplicate_track_numbers(track_nums:List[int])->List[int]:
    """Return list of track numbers that appear more than once."""
    c=Counter(track_nums)
    return [n for n,cnt in c.items() if cnt>1]

def compute_meaningful_title_ratio(titles:List[str])->float:
    """Fraction of titles that look like real titles vs garbage/missing."""
    if not titles: return 0.5
    meaningful=0
    for t in titles:
        if not t or len(t.strip())<2: continue
        g=detect_garbage_name(t)
        if not g and not detect_mojibake(t): meaningful+=1
    return meaningful/len(titles)

def compute_filename_tag_consistency(
    audio_files:List[Path],
    all_tags:List[Dict[str,Optional[str]]]
)->float:
    """
    Score 0–1 measuring how well filename stem content agrees with tag content.
    Compares the 'Artist - Title' pattern from filenames against tags.
    """
    if not audio_files or not all_tags: return 0.5
    scores:List[float]=[]
    for f,tags in zip(audio_files,all_tags):
        fn_artist,fn_title=_parse_fn_artitle(f.stem)
        tag_title=(tags.get("title") or "").strip()
        tag_artist=(tags.get("artist") or "").strip()
        if fn_title and tag_title:
            scores.append(string_similarity(fn_title,tag_title))
        elif not fn_title and not tag_title:
            scores.append(0.5)
    return sum(scores)/len(scores) if scores else 0.5

def _parse_fn_artitle(stem:str)->Tuple[Optional[str],Optional[str]]:
    """Quick filename stem → (artist, title) without full cleanup overhead."""
    s=re.sub(r'_-_',' - ',stem).replace('_',' ')
    s=re.sub(r'^\d{1,3}\s*[-–—\.]\s*','',s)
    parts=[p.strip() for p in re.split(r'\s*[-–—]\s*',s,maxsplit=1) if p.strip()]
    if len(parts)>=2: return parts[0],parts[1]
    return None,parts[0] if parts else None

def compute_albumartist_consistency(all_tags:List[Dict[str,Optional[str]]])->float:
    """Fraction of tagged tracks sharing the dominant albumartist."""
    tagged=[t for t in all_tags if t.get("albumartist")]
    if not tagged: return 0.5
    c=Counter(t["albumartist"].strip().lower() for t in tagged)
    _,dom_share,_=compute_dominant(c)
    return dom_share

def compute_folder_alignment_bonus(folder_name:str,proposed_name:str)->float:
    """
    0.0–1.0 bonus based on how closely folder_name already matches proposed_name.
    Perfect match = 1.0, partial = proportional.
    """
    fn=normalize_unicode(folder_name.strip().lower())
    pn=normalize_unicode(proposed_name.strip().lower())
    if fn==pn: return 1.0
    return string_similarity(fn,pn)

def compute_confidence_factors(
    audio_files:List[Path],
    tagged:int,
    alb_share:float,
    aa_share:float,
    art_share:float,
    used_heuristic:bool,
    folder_name:str,
    proposed_name:str,
    all_tags:List[Dict[str,Optional[str]]],
)->Dict[str,float]:
    """
    Compute named confidence factors that replace/complement the legacy single float.
    Returns a dict of factor_name → value (0–1 each).
    """
    total=len(audio_files)
    factors:Dict[str,float]={}

    # tag_coverage: what fraction of tracks have any tags
    factors["tag_coverage"]=tagged/total if total else 0.0

    # dominance: core voting quality
    if aa_share>0:
        factors["dominance"]=alb_share*0.60+aa_share*0.40
    else:
        factors["dominance"]=alb_share*0.70+art_share*0.30

    # title_quality: meaningful title ratio
    titles=[t.get("title","") for t in all_tags if t.get("title")]
    factors["title_quality"]=compute_meaningful_title_ratio(titles) if titles else 0.5

    # filename_consistency: filename stems vs tag content
    factors["filename_consistency"]=compute_filename_tag_consistency(audio_files,all_tags)

    # completeness: penalise track-number gaps and duplicates
    track_nums:List[int]=[]
    for t in all_tags:
        raw=t.get("tracknumber") or ""
        # Handle vinyl notation
        vt=parse_vinyl_track(raw.split("/")[0].strip())
        n=vt[2] if vt else parse_int_prefix(raw)
        if n: track_nums.append(n)
    gaps=detect_track_gaps(track_nums)
    dupes=detect_duplicate_track_numbers(track_nums)
    gap_pen=min(0.30,len(gaps)*0.06)
    dup_pen=min(0.20,len(dupes)*0.07)
    factors["completeness"]=max(0.0,1.0-gap_pen-dup_pen)
    factors["track_gaps"]=len(gaps)    # informational integer stored as float
    factors["track_dupes"]=len(dupes)

    # albumartist_consistency
    factors["aa_consistency"]=compute_albumartist_consistency(all_tags)

    # folder_alignment: bonus if folder already matches proposed
    factors["folder_alignment"]=compute_folder_alignment_bonus(folder_name,proposed_name)

    return factors

def confidence_from_factors(factors:Dict[str,float],used_heuristic:bool)->float:
    """Compute a single 0–1 confidence score from the factor dict."""
    weights={"dominance":0.40,"tag_coverage":0.15,"title_quality":0.12,
             "filename_consistency":0.10,"completeness":0.12,
             "aa_consistency":0.06,"folder_alignment":0.05}
    score=sum(factors.get(k,0.5)*w for k,w in weights.items())
    if used_heuristic: score*=0.60
    return min(1.0,max(0.0,score))

# ─────────────────────────────────────────────
# Per-folder .raagdosa override file
# ─────────────────────────────────────────────
def load_folder_override(folder:Path)->Optional[Dict[str,Any]]:
    """
    Load a .raagdosa YAML override from inside a folder.
    Supported keys: name, artist, album, year, skip, force_clean, confidence_boost
    """
    p=folder/".raagdosa"
    if not p.exists(): return None
    if yaml is None: return None
    try:
        data=yaml.safe_load(p.read_text(encoding="utf-8"))
        return data if isinstance(data,dict) else None
    except Exception: return None

# ─────────────────────────────────────────────
# ignore_folder_names glob support
# ─────────────────────────────────────────────
def folder_matches_ignore(folder_name:str,patterns:List[str])->bool:
    """True if folder_name matches any pattern (exact or glob)."""
    for pat in patterns:
        if folder_name==pat or fnmatch.fnmatch(folder_name,pat): return True
    return False

# ─────────────────────────────────────────────
# Library path template
# ─────────────────────────────────────────────
def resolve_library_path(base:Path,artist:str,album:str,year:Optional[int],
                          is_flac_only:bool,is_va:bool,is_single:bool,
                          is_mix:bool,cfg:Dict[str,Any])->Path:
    lib=cfg.get("library",{})
    template=lib.get("template","{artist}/{album}")
    va_folder=lib.get("va_folder","_Various Artists")
    singles_folder=lib.get("singles_folder","_Singles")
    mixes_folder=lib.get("mixes_folder","_Mixes")
    unknown=lib.get("unknown_artist_label","_Unknown")
    flac_seg=bool(lib.get("flac_segregation",False))

    artist_c=sanitize_name(artist or unknown)
    album_c =sanitize_name(album  or "_Untitled")
    album_y =f"{album_c} ({year})" if year else album_c

    if is_mix:    return base/mixes_folder/album_y
    if is_va:     return base/va_folder/album_y
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

def _same_device(a:Path,b:Path)->bool:
    """True if two paths are on the same filesystem device.
    Walks up b's ancestry until finding an existing path (dest may not exist yet)."""
    try:
        pb=b
        while not pb.exists():
            parent=pb.parent
            if parent==pb: return False  # hit filesystem root
            pb=parent
        return a.stat().st_dev==pb.stat().st_dev
    except Exception:
        return False

def safe_move_folder(src:Path,dst:Path,use_checksum:bool=False)->Tuple[str,float]:
    """
    Move src folder to dst.

    Fast path (same filesystem):
      Uses os.rename() — atomic, ~1ms regardless of folder size.
      No copy, no verify needed (rename is guaranteed consistent by the OS).

    Slow path (cross-device):
      copytree → count+size verify → optional checksum → rmtree.
      Only triggered when source and destination are on different drives.

    Returns (method, elapsed_seconds).
    """
    t0=dt.datetime.now().timestamp()

    if _same_device(src,dst):
        # ── Fast path: atomic rename ──────────────────────────────────────
        # Ensure destination parent exists
        ensure_dir(dst.parent)
        try:
            src.rename(dst)
        except OSError:
            # rename() can still fail cross-mount even with same st_dev on some
            # network/virtual filesystems — fall through to copy path
            pass
        else:
            return "rename", dt.datetime.now().timestamp()-t0

    # ── Slow path: copy → verify → delete ────────────────────────────────
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
    return "copy", dt.datetime.now().timestamp()-t0

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
    # Cache check — skip mutagen entirely for unchanged files
    cache=_tag_cache
    if cache is not None:
        cached=cache.get(path)
        if cached is not None: return cached

    keys=cfg.get("tags",{})
    result:Dict[str,Optional[str]]={k:None for k in ["album","albumartist","artist","title","tracknumber","discnumber","year","bpm","key"]}
    if MutagenFile is None: return result
    try:
        mf=MutagenFile(str(path),easy=True)
        if not mf or not getattr(mf,"tags",None):
            if cache is not None: cache.set(path,result)
            return result
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
    if cache is not None: cache.set(path,result)
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
    # ── per-folder override ──────────────────────────────────────────────
    override=load_folder_override(folder)
    if override and override.get("skip"): return None

    albums_norm:Counter=Counter(); albumartists_norm:Counter=Counter(); track_artists_norm:Counter=Counter(); years:Counter=Counter()
    albums_raw:Dict[str,Counter]={}; albumartists_raw:Dict[str,Counter]={}
    tracks_with_year=0; tagged=0; unreadable=0
    extensions:Counter=Counter(p.suffix.lower() for p in audio_files)
    all_tags:List[Dict[str,Optional[str]]]=[]

    for f in audio_files:
        tags=read_audio_tags(f,cfg)
        all_tags.append(tags)
        if all(v is None for v in tags.values()): unreadable+=1; continue
        alb_r=(tags.get("album") or "").strip(); aa_r=(tags.get("albumartist") or "").strip()
        art_r=(tags.get("artist") or "").strip(); yr_r=(tags.get("year") or "").strip()

        # Strip display noise and disc indicators from album for voting
        alb_r_clean=strip_disc_indicator(strip_display_noise(alb_r)) if alb_r else alb_r

        alb_n=normalize_for_vote(alb_r_clean,cfg) if alb_r_clean else ""
        aa_n =normalize_for_vote(aa_r,cfg)         if aa_r         else ""
        art_n=normalize_for_vote(art_r,cfg)        if art_r        else ""
        if alb_n: albums_norm[alb_n]+=1; albums_raw.setdefault(alb_n,Counter())[alb_r_clean or alb_r]+=1
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

    # ── override: force name / artist ───────────────────────────────────
    if override:
        if override.get("album"):  dom_alb=str(override["album"])
        if override.get("artist") or override.get("albumartist"):
            dom_aa=str(override.get("albumartist") or override.get("artist"))
        if override.get("year"):
            try: years[str(int(override["year"]))]=total; tracks_with_year=total
            except Exception: pass

    va_label=cfg.get("various_artists",{}).get("label","VA")
    is_va=detect_va(dom_aa_n or "",list(track_artists_norm.keys()),cfg)

    # ── folder content type ─────────────────────────────────────────────
    folder_type=classify_folder_content(audio_files,folder.name,all_tags,cfg)
    is_mix=(folder_type=="mix")
    is_ep =(folder_type=="ep")
    if is_va and folder_type not in ("mix",): folder_type="va"

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

    # ── apply title-case fix to album name ──────────────────────────────
    dom_alb=apply_title_case(dom_alb,cfg)
    # Strip residual display noise from voted album name
    dom_alb=strip_display_noise(dom_alb)
    # Strip garbage bracket stack from album name
    garbage=detect_garbage_name(dom_alb)
    if garbage: dom_alb=strip_bracket_stack(dom_alb)

    year_val,year_meta=pick_year(years,tracks_with_year,max(total,1) if used_heuristic else total,cfg)

    fmt=cfg.get("format",{})
    ep_suffix=" [EP]" if is_ep and fmt.get("label_eps",True) else ""
    pat=fmt.get("pattern_with_year" if year_val else "pattern_no_year","{albumartist} - {album}")
    proposed=pat.format(albumartist=artist_for_folder,album=dom_alb+ep_suffix,year=year_val or "")
    proposed=sanitize_name(proposed,repl=fmt.get("replace_illegal_chars_with"," - "))
    proposed=sanitize_windows_reserved(proposed)

    sfx=cfg.get("format_suffix",{})
    if sfx.get("enabled",True) and sfx.get("only_if_all_same_extension",True) and len(extensions)==1:
        ext1=next(iter(extensions.keys()))
        if ext1 and ext1!=lower(sfx.get("ignore_extension",".mp3")) and sfx.get("style","brackets_upper")=="brackets_upper":
            proposed=f"{proposed} [{ext1.lstrip('.').upper()}]"

    is_flac_only=set(extensions.keys())=={".flac"}
    clean_albums=derive_clean_albums_root(profile,source_root)
    target_dir=resolve_library_path(clean_albums,artist_for_folder,dom_alb,year_val,
                                     is_flac_only,is_va,False,is_mix,cfg)

    # ── named confidence factors ─────────────────────────────────────────
    conf_factors=compute_confidence_factors(
        audio_files,tagged,alb_share,aa_share,art_share,
        used_heuristic,folder.name,proposed,all_tags)
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
        "albumartist_display":artist_for_folder,"year":year_val,"year_meta":year_meta,
        "unreadable_ratio":(unreadable/total) if total else 0.0,
        "used_heuristic":used_heuristic,"is_flac_only":is_flac_only,
        "garbage_reasons":garbage,"confidence_factors":conf_factors,
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
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])

    # Initialise tag cache for this scan run
    _get_tag_cache(cfg)

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
    if _tag_cache is not None:
        _tag_cache.save()
        out(f"  Tag cache: {_tag_cache.size} entries",level=VERBOSE)

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

    # Build mixes root
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder

    for p in proposals:
        reasons:List[str]=[]; dest="clean"
        if rr.get("route_questionable_to_review",True) and p.confidence<min_conf:
            dest="review"; reasons.append("low_confidence")
        if rr.get("route_duplicates",True) and within_run[p.proposed_folder_name]>1:
            dest="duplicate"
            # Find the other colliding folder name for detail
            colliders=[q.folder_name for q in proposals if q.proposed_folder_name==p.proposed_folder_name and q is not p]
            reasons.append(f"duplicate_in_run:{colliders[0][:40] if colliders else '?'}")
        norm_prop=normalize_unicode(p.proposed_folder_name)
        if rr.get("route_cross_run_duplicates",True) and (norm_prop in existing_clean or norm_prop in manifest_entries):
            dest="duplicate"; reasons.append("already_in_clean")
        if p.decision.get("unreadable_ratio",0.0)>max_unread:
            dest="review"; reasons.append("unreadable_ratio_high")
        if p.decision.get("used_heuristic",False):
            if dest=="clean": dest="review"
            reasons.append("heuristic_fallback")
        if p.stats.format_duplicates: reasons.append(f"format_dupes({len(p.stats.format_duplicates)})")
        # EP stays in clean unless confidence too low
        if p.decision.get("is_ep"): reasons.append("ep")
        # Mix: route to mixes folder (clean side)
        if p.decision.get("is_mix") and dest=="clean":
            reasons.append("mix_folder")
            ensure_dir(mixes_root)
            p.target_path=str(mixes_root/p.proposed_folder_name)
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
            same=_same_device(src,dst2)
            method_note=f"{C.DIM}[rename]{C.RESET}" if same else f"{C.DIM}[copy]{C.RESET}"
            out(f"  {C.DIM}[dry-run]{C.RESET} {method_note} {src.name}  →  {dst2.name}  {status_tag(p.destination)}  conf={conf_color(p.confidence)}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":"dry_run","src":str(src),"dst":str(dst2)}); continue
        ensure_dir(dst2.parent)
        try: move_method,move_elapsed=safe_move_folder(src,dst2,use_checksum=use_cs)
        except RuntimeError as e:
            err(f"⛔ Move failed ({src.name}): {e}")
            append_jsonl(skip_path,{"timestamp":now_iso(),"session_id":session_id,"type":"folder","reason":f"move_failed:{e}","src":str(src)}); continue
        action_id=uuid.uuid4().hex[:10]
        entry={"action_id":action_id,"timestamp":now_iso(),"session_id":session_id,"type":"folder",
               "original_path":str(src),"original_parent":str(src.parent),"original_folder_name":src.name,
               "target_path":str(dst2),"target_parent":str(dst2.parent),"target_folder_name":dst2.name,
               "destination":p.destination,"confidence":p.confidence,"decision":p.decision,
               "move_method":move_method}
        append_jsonl(hist_path,entry); applied.append(entry)
        if p.destination=="clean": manifest_add(cfg,dst2.name,{"original_path":str(src),"confidence":p.confidence,"session_id":session_id})
        method_tag=f"  {C.DIM}[{move_method} {move_elapsed*1000:.0f}ms]{C.RESET}" if _verbosity>=VERBOSE else ""
        out(f"  MOVED {status_tag(p.destination)} {C.DIM}{src.name}{C.RESET}  →  {dst2.name}  conf={conf_color(p.confidence)}{method_tag}")
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
    # Try vinyl track notation first (A1, B2 etc.)
    vinyl=parse_vinyl_track(track_raw.split("/")[0].strip()) if track_raw else None
    if vinyl:
        track_n=vinyl[2]; meta["vinyl_side"]=vinyl[0]; meta["track_src"]="vinyl_notation"
    else:
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
        # Extension case normalisation: .MP3 → .mp3
        ext_fixed=normalise_extension(f)
        if ext_fixed and not dry_run:
            try: f.rename(ext_fixed); f=ext_fixed
            except Exception: pass
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
def cmd_show(cfg:Dict[str,Any],folder_path:str,profile_name:str,show_tracks:bool=False)->None:
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
    out(f"  Folder type:        {d.get('folder_type','album')}{'  [EP]' if d.get('is_ep') else ''}{'  [MIX]' if d.get('is_mix') else ''}")
    out(f"  album tag:          '{d.get('dominant_album_display')}' ({d.get('dominant_album_share',0):.0%} dominance)")
    out(f"  albumartist tag:    '{d.get('dominant_albumartist_display')}' ({d.get('dominant_albumartist_share',0):.0%} dominance)")
    out(f"  year:               {d.get('year') or 'not included'}")
    out(f"  VA:                 {'yes' if d.get('is_va') else 'no'}")
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
                new_name,conf,reason,meta=build_track_filename(cls,tags,f,cfg,prop.decision,disc_multi)
                if new_name:
                    changed=normalize_unicode(new_name)!=normalize_unicode(f.name)
                    sym=f"{C.GREEN}→{C.RESET}" if changed else f"{C.DIM}={C.RESET}"
                    out(f"  {C.DIM}{f.name:<50}{C.RESET} {sym} {new_name}  {conf_color(conf)}")
                else:
                    out(f"  {C.DIM}{f.name:<50}{C.RESET} {C.YELLOW}SKIP ({reason}){C.RESET}")

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
# orphans — find loose audio files
# ─────────────────────────────────────────────
def cmd_orphans(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root)
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

# ─────────────────────────────────────────────
# artists — list/find artists in Clean
# ─────────────────────────────────────────────
def cmd_artists(cfg:Dict[str,Any],profile_name:str,list_mode:bool,find_query:Optional[str])->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root); clean_albums=roots["clean_albums"]
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

# ─────────────────────────────────────────────
# review-list — summarise Review folder
# ─────────────────────────────────────────────
def cmd_review_list(cfg:Dict[str,Any],profile_name:str,older_than_days:Optional[int])->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root); review_albums=roots["review_albums"]
    exts=[e.lower() for e in cfg.get("scan",{}).get("audio_extensions",[".mp3",".flac",".m4a"])]

    out(f"\n{C.BOLD}{'═'*60}{C.RESET}\n{C.BOLD}raagdosa review-list — {profile_name}{C.RESET}\n{'═'*60}")
    if not review_albums.exists(): out("Review/Albums not found — empty."); return

    now=dt.datetime.now()
    cutoff=now-dt.timedelta(days=older_than_days) if older_than_days else None

    # Build a lookup from session proposals for confidence/reason data
    conf_map:Dict[str,float]={}; reason_map:Dict[str,str]={}
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
                        reason_map[nm]=", ".join(fp.get("decision",{}).get("route_reasons",[]))
            except Exception: continue

    folders=sorted([d for d in review_albums.rglob("*") if d.is_dir()
                    and any(f.suffix.lower() in exts for f in d.iterdir() if f.is_file())],
                   key=lambda d:folder_mtime(d))

    if cutoff:
        folders=[d for d in folders if dt.datetime.fromtimestamp(folder_mtime(d))<=cutoff]

    if not folders: out(f"No Review folders{' older than '+str(older_than_days)+' days' if older_than_days else ''}."); return
    out(f"\n{'Folder':<45} {'Age':>7}  {'Conf':>6}  Reason")
    out("─"*90)
    for d in folders:
        mtime=dt.datetime.fromtimestamp(folder_mtime(d))
        age=(now-mtime).days
        conf=conf_map.get(d.name)
        reason=reason_map.get(d.name,"")
        conf_s=conf_color(conf) if conf else f"{C.DIM}n/a{C.RESET}"
        age_col=C.RED if age>60 else (C.YELLOW if age>14 else C.DIM)
        out(f"  {d.name[:43]:<43}  {age_col}{age:>5}d{C.RESET}  {conf_s}  {C.DIM}{reason[:50]}{C.RESET}")
    out(f"\n{C.DIM}Total: {len(folders)} folder(s) in Review{C.RESET}")

# ─────────────────────────────────────────────
# cache — manage the tag cache
# ─────────────────────────────────────────────
def cmd_cache(cfg:Dict[str,Any],action:str)->None:
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

# ─────────────────────────────────────────────
# clean-report — audit Clean library
# ─────────────────────────────────────────────
def cmd_clean_report(cfg:Dict[str,Any],profile_name:str)->None:
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root); clean_albums=roots["clean_albums"]
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
    profiles=cfg.get("profiles",{})
    if profile_name not in profiles: raise ValueError(f"Unknown profile: {profile_name}")
    profile=profiles[profile_name]; source_root=Path(profile["source_root"]).expanduser()
    roots=ensure_roots(profile,source_root)
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
        dest=clean_albums/sanitize_name(artist)/"_Singles"
        out(f"  {C.GREEN}{artist:<40}{C.RESET}  {len(files):3d} track(s)  →  {dest}")
    if dry_run: out(f"\n{C.DIM}[dry-run] No files moved.{C.RESET}"); return
    if input("\nProceed? [y/N] ").strip().lower()!="y": out("Aborted."); return
    moved=0
    for artist,files in by_artist.items():
        dest=clean_albums/sanitize_name(artist)/"_Singles"; ensure_dir(dest)
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
    """
    Streaming pipeline: scan a batch → apply that batch → scan next batch in parallel.

    Architecture:
      - Candidate folders are split into batches (default 50 folders each).
      - A scanner thread fills a queue with completed FolderProposal batches.
      - The main thread drains the queue, routes proposals, and applies moves.
      - While the main thread is applying batch N, the scanner is already
        reading tags for batch N+1.
      - Within-run duplicate tracking is maintained across batches via a
        shared seen_names set that grows as each batch is committed.

    Result: first folder moves within seconds of starting even on huge libraries.
    On same-filesystem setups each move is a ~1ms rename, so apply is nearly
    instant and the scanner is always ahead.
    """
    import queue as _queue

    for lk in ["history_log","track_history_log"]:
        lp=Path(cfg.get("logging",{}).get(lk,""))
        if lp.name: rotate_log_if_needed(lp,float(cfg.get("logging",{}).get("rotate_log_max_mb",10.0)))
    register_stop_handler()

    profiles=cfg.get("profiles",{}); profile_obj=profiles[profile]
    source_root=Path(profile_obj["source_root"]).expanduser()
    if not source_root.exists(): raise FileNotFoundError(f"source_root missing: {source_root}")

    roots=ensure_roots(profile_obj,source_root)
    clean_albums=roots["clean_albums"]; review_albums=roots["review_albums"]; dup_root=roots["duplicates"]
    clean_root_str =str(roots["clean_root"].resolve())+os.sep
    review_root_str=str(roots["review_root"].resolve())+os.sep

    sc=cfg.get("scan",{}); exts=[e.lower() for e in sc.get("audio_extensions",[".mp3",".flac",".m4a"])]
    min_tracks=int(sc.get("min_tracks",3)); follow_sym=bool(sc.get("follow_symlinks",False))
    leaf_only=bool(sc.get("leaf_folders_only",True))
    ignore_patterns:List[str]=list(cfg.get("ignore",{}).get("ignore_folder_names",[]) or [])
    workers=int(sc.get("workers", min(8,(os.cpu_count() or 4))))
    batch_size=int(sc.get("streaming_batch_size",50))
    since=_parse_since(since_str,cfg)

    # Session setup
    session_id=make_session_id()
    session_dir=Path(cfg["logging"]["session_dir"])/session_id; ensure_dir(session_dir)
    out(f"\n{C.BOLD}Session:{C.RESET}   {session_id}")
    out(f"{C.DIM}Pipeline: streaming batches of {batch_size}, {workers} scan workers{C.RESET}",level=VERBOSE)

    # Routing state — built incrementally across batches
    rr=cfg.get("review_rules",{}); min_conf=float(rr.get("min_confidence_for_clean",0.85))
    max_unread=float(sc.get("max_unreadable_track_ratio",0.25))
    seen_names:Counter=Counter()   # accumulates proposed_folder_name counts across batches
    existing_clean:Set[str]=set()
    if clean_albums.exists():
        try:
            for item in clean_albums.rglob("*"):
                if item.is_dir(): existing_clean.add(normalize_unicode(item.name))
        except Exception: pass
    manifest_entries:Set[str]=set(read_manifest(cfg).get("entries",{}).keys())
    lib=cfg.get("library",{}); mixes_folder=lib.get("mixes_folder","_Mixes")
    mixes_root=clean_albums.parent/mixes_folder

    # ── Phase 1: collect candidates (fast — just os.walk, no tag reads) ─
    candidates:List[Path]=[]
    for root,dirs,files in os.walk(source_root,followlinks=follow_sym):
        rp=Path(root); rp_str=str(rp.resolve())+os.sep
        if rp_str.startswith(clean_root_str) or rp_str.startswith(review_root_str): dirs[:]=[] ; continue
        if leaf_only and dirs: continue
        if sum(1 for f in files if Path(f).suffix.lower() in exts)>=min_tracks: candidates.append(rp)
    if since:
        candidates=[f for f in candidates if dt.datetime.fromtimestamp(folder_mtime(f))>=since]
        out(f"  --since: {len(candidates)} folders modified after {since.strftime('%Y-%m-%d %H:%M')}",level=VERBOSE)
    candidates=[rp for rp in candidates if not folder_matches_ignore(rp.name,ignore_patterns)]

    out(f"{C.DIM}Candidates: {len(candidates)} folder(s){C.RESET}",level=VERBOSE)

    # Initialise tag cache
    _get_tag_cache(cfg)

    # ── Streaming pipeline ───────────────────────────────────────────────
    # scanner_queue carries completed batches: List[FolderProposal] | None (sentinel)
    scanner_queue:_queue.Queue=_queue.Queue(maxsize=3)  # backpressure: don't scan too far ahead
    all_proposals:List[FolderProposal]=[]
    total_applied=0; total_clean=0; total_review=0; total_dup=0
    prog=Progress(len(candidates),"Scanning")

    def _scanner_worker():
        """Background thread: scan candidates in batches, push to queue."""
        batches=[candidates[i:i+batch_size] for i in range(0,len(candidates),batch_size)]
        if not batches:
            scanner_queue.put(None); return

        for batch in batches:
            if should_stop(): break
            batch_proposals:List[FolderProposal]=[]
            # Use thread pool within each batch for parallel tag reading
            if workers>1 and len(batch)>1:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures={pool.submit(_scan_one_folder,rp,exts,follow_sym,min_tracks,source_root,profile_obj,cfg,prog):rp
                             for rp in batch}
                    for fut in as_completed(futures):
                        if should_stop(): break
                        try:
                            prop=fut.result()
                            if prop: batch_proposals.append(prop)
                        except Exception as e:
                            err(f"  scan error: {e}")
            else:
                for rp in batch:
                    if should_stop(): break
                    prop=_scan_one_folder(rp,exts,follow_sym,min_tracks,source_root,profile_obj,cfg,prog)
                    if prop: batch_proposals.append(prop)

            # Sort for determinism before pushing
            batch_proposals.sort(key=lambda p:p.folder_path)
            scanner_queue.put(batch_proposals)

        scanner_queue.put(None)  # sentinel: scanner done
        # Save cache after all scanning complete
        if _tag_cache is not None:
            _tag_cache.save()
            out(f"  Tag cache: {_tag_cache.size} entries saved",level=VERBOSE)

    def _scan_one_folder(rp,exts,follow_sym,min_tracks,source_root,profile_obj,cfg,prog):
        if should_stop(): return None
        audio_files=list_audio_files(rp,exts,follow_sym)
        if len(audio_files)<min_tracks:
            prog.tick(rp.name); return None
        prog.tick(rp.name)
        return build_folder_proposal(rp,audio_files,source_root,profile_obj,cfg)

    def _route_batch(batch:List[FolderProposal])->List[FolderProposal]:
        """Route a batch of proposals. Updates shared seen_names in-place."""
        # Count within this batch first, then add to seen_names
        batch_names=Counter(p.proposed_folder_name for p in batch)
        for p in batch:
            reasons:List[str]=[]; dest="clean"
            if rr.get("route_questionable_to_review",True) and p.confidence<min_conf:
                dest="review"; reasons.append("low_confidence")
            # within-run duplicate: count across all batches seen so far + this batch
            total_count=seen_names[p.proposed_folder_name]+batch_names[p.proposed_folder_name]
            if rr.get("route_duplicates",True) and total_count>1:
                dest="duplicate"
                reasons.append(f"duplicate_in_run")
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
            p.destination=dest; p.decision["route_reasons"]=reasons
            if dest=="review":     p.target_path=str(review_albums/p.proposed_folder_name)
            elif dest=="duplicate": p.target_path=str(dup_root/p.proposed_folder_name)
        # Update global seen_names after routing this batch
        seen_names.update(p.proposed_folder_name for p in batch)
        return batch

    # Start scanner in background thread
    import threading as _threading
    scanner_thread=_threading.Thread(target=_scanner_worker,daemon=True)
    scanner_thread.start()

    trc=cfg.get("track_rename",{})
    do_tracks=(trc.get("enabled",True) and trc.get("scope","clean_only") in ("clean_only","both"))

    # Main thread: drain queue → route → apply → track rename
    while True:
        batch=scanner_queue.get()
        if batch is None: break   # sentinel
        if not batch: continue
        if should_stop():
            out(f"\n{C.YELLOW}Stop requested — flushing remaining queue…{C.RESET}")
            # Drain remaining batches without applying
            while True:
                b=scanner_queue.get()
                if b is None: break
            break

        routed=_route_batch(batch)
        all_proposals.extend(routed)

        # Apply this batch immediately
        applied=apply_folder_moves(cfg,routed,interactive=interactive,auto_above=None,
                                    dry_run=dry_run,session_id=session_id,source_root=source_root)
        total_applied+=len(applied)
        total_clean +=sum(1 for a in applied if a.get("destination")=="clean")
        total_review +=sum(1 for a in applied if a.get("destination")=="review")
        total_dup    +=sum(1 for a in applied if a.get("destination")=="duplicate")

        # Track rename immediately for clean folders in this batch
        if do_tracks:
            for a in applied:
                if a.get("destination")=="clean":
                    rename_tracks_in_clean_folder(cfg,Path(a["target_path"]),
                                                   a.get("decision",{}),
                                                   interactive=interactive,dry_run=dry_run,
                                                   session_id=session_id)

    prog.done()
    scanner_thread.join(timeout=5)

    # Write consolidated session report
    payload={"app":cfg.get("app",{}),"session_id":session_id,"timestamp":now_iso(),"profile":profile,
             "source_root":str(source_root),"since":since.isoformat() if since else None,
             "folder_proposals":[dataclasses.asdict(p) for p in all_proposals]}
    write_json(session_dir/"proposals.json",payload)
    _write_session_reports(session_id,profile,source_root,all_proposals,session_dir,cfg)

    clean_n=sum(1 for p in all_proposals if p.destination=="clean")
    rev_n  =sum(1 for p in all_proposals if p.destination=="review")
    dup_n  =sum(1 for p in all_proposals if p.destination=="duplicate")
    out(f"\n{C.BOLD}Results:{C.RESET}   {len(all_proposals)} proposals | {C.GREEN}Clean: {clean_n}{C.RESET} | {C.YELLOW}Review: {rev_n}{C.RESET} | {C.RED}Dupes: {dup_n}{C.RESET}")
    out(f"{C.DIM}Reports:   {session_dir}/report.{{txt,csv,html}}{C.RESET}")

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
  raagdosa init                            # guided first-time setup
  raagdosa doctor                          # verify config, deps, disk
  raagdosa go --dry-run                    # preview — nothing moves
  raagdosa go                              # scan + move folders + rename tracks
  raagdosa go --since last_run             # only new folders since last run
  raagdosa go --interactive                # confirm each folder
  raagdosa show "/path/to/folder"          # debug a single folder
  raagdosa show "/path/to/folder" --tracks # also show per-track renames
  raagdosa status                          # library overview
  raagdosa verify                          # audit Clean library health
  raagdosa learn                           # suggest config improvements
  raagdosa report --format html            # open last session report
  raagdosa undo --session <id>             # undo a whole session
  raagdosa orphans                         # find loose audio files
  raagdosa artists --list                  # list all artists in Clean
  raagdosa artists --find "portishead"     # fuzzy-find an artist
  raagdosa review-list                     # see what's waiting in Review
  raagdosa review-list --older-than 30     # Review folders >30 days old
  raagdosa clean-report                    # stats on your Clean library
  raagdosa extract "/path/to/mix" --by-artist   # split mix by artist
  raagdosa compare --folder A B            # diff two folders
  raagdosa diff last prev                  # compare last two sessions
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
    sh=sub.add_parser("show",help="Debug a single folder"); sh.add_argument("folder"); sh.add_argument("--profile"); sh.add_argument("--tracks",action="store_true",help="Also show per-track rename preview")
    sub.add_parser("verify",help="Audit Clean library health").add_argument("--profile")
    le=sub.add_parser("learn",help="Suggest config improvements"); le.add_argument("--session")
    rp=sub.add_parser("report",help="View session report"); rp.add_argument("--session"); rp.add_argument("--format",default="txt",choices=["txt","csv","html"])
    sub.add_parser("doctor",help="Check config, deps, disk, DJ databases")
    hi=sub.add_parser("history",help="Show history"); hi.add_argument("--last",type=int,default=50); hi.add_argument("--session"); hi.add_argument("--match"); hi.add_argument("--tracks",action="store_true")
    un=sub.add_parser("undo",help="Undo moves or renames"); un.add_argument("--id"); un.add_argument("--session"); un.add_argument("--from-path"); un.add_argument("--tracks",action="store_true"); un.add_argument("--folder")
    # v3.5 commands
    sub.add_parser("orphans",help="Find loose audio files in Clean/Review").add_argument("--profile")
    ar=sub.add_parser("artists",help="List or find artists in Clean library"); ar.add_argument("--profile")
    ar.add_argument("--list",dest="list_mode",action="store_true",help="List all artists")
    ar.add_argument("--find",dest="find_query",metavar="QUERY",help="Fuzzy-find an artist")
    rl=sub.add_parser("review-list",help="Summarise Review folder contents"); rl.add_argument("--profile")
    rl.add_argument("--older-than",type=int,metavar="DAYS",help="Only show folders older than N days")
    sub.add_parser("clean-report",help="Stats and health report for Clean library").add_argument("--profile")
    ex=sub.add_parser("extract",help="Extract tracks from a VA/mix folder"); ex.add_argument("folder"); ex.add_argument("--profile")
    ex.add_argument("--by-artist",action="store_true",required=True,help="Group tracks by artist tag")
    ex.add_argument("--dry-run",action="store_true")
    cp=sub.add_parser("compare",help="Compare two folders"); cp.add_argument("--folder",nargs=2,metavar="FOLDER",required=True)
    df=sub.add_parser("diff",help="Diff two session reports"); df.add_argument("session_a",metavar="SESSION_A"); df.add_argument("session_b",metavar="SESSION_B")
    ca=sub.add_parser("cache",help="Manage the tag cache (status/clear/evict)")
    ca.add_argument("action",nargs="?",default="status",choices=["status","clear","evict"])
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
    elif cmd=="show":     cmd_show(cfg,args.folder,getattr(args,"profile",None) or cfg.get("active_profile",""),show_tracks=bool(getattr(args,"tracks",False)))
    elif cmd=="verify":   cmd_verify(cfg,gp())
    elif cmd=="learn":    cmd_learn(cfg_path,cfg,getattr(args,"session",None))
    elif cmd=="report":   cmd_report(cfg,getattr(args,"session",None),args.format)
    elif cmd=="doctor":   cmd_doctor(cfg_path,cfg)
    elif cmd=="history":  cmd_history(cfg,last=args.last,session=args.session,match=args.match,tracks=bool(args.tracks))
    elif cmd=="undo":     cmd_undo(cfg,action_id=args.id,session_id=args.session,from_path=args.from_path,tracks=bool(args.tracks),folder=args.folder)
    # v3.5 commands
    elif cmd=="orphans":      cmd_orphans(cfg,gp())
    elif cmd=="artists":      cmd_artists(cfg,gp(),list_mode=bool(getattr(args,"list_mode",False)),find_query=getattr(args,"find_query",None))
    elif cmd=="review-list":  cmd_review_list(cfg,gp(),older_than_days=getattr(args,"older_than",None))
    elif cmd=="clean-report": cmd_clean_report(cfg,gp())
    elif cmd=="extract":      cmd_extract_by_artist(cfg,gp(),args.folder,dry_run=bool(getattr(args,"dry_run",False)))
    elif cmd=="compare":      cmd_compare_folders(cfg,args.folder[0],args.folder[1])
    elif cmd=="diff":         cmd_diff(cfg,args.session_a,args.session_b)
    elif cmd=="cache":        cmd_cache(cfg,args.action)
    else: parser.error(f"Unknown: {cmd}")

if __name__=="__main__":
    main()
